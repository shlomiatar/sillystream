//! `sillystream`: tokens, stream and sink admin, publish, tail and the load generator.

mod bench;

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::process::ExitCode;

use sdk::http::{HttpClient, Response};
use sdk::{
    AckLevel, BatchConfig, Client, ClientConfig, Event, FROM_LIVE, FROM_START, PublisherConfig,
    Subscribe, WireClause,
};

const USAGE: &str = "\
sillystream token issue --secret-file FILE --author N --grant PATTERN:PERM[,PERM].. [--grant ..]
                        --ttl-ms N
sillystream stream create|config NAME [--retention R] [--default-ack N] [--min-ack N]
sillystream stream info|delete NAME
sillystream stream core NAME --core N      (give the stream its own shard on core N)
sillystream sink create|update STREAM ID --kind K [--sink-url U] [--target T] [--filter F]
                        [--on-expire E] [--concurrency N] [--start S] [--secret K]
                        [--interval-ms N] [--budget-bytes N]
sillystream sink delete|pause|resume STREAM ID
sillystream sink reset STREAM ID [--start S]
sillystream publish STREAM [--options N] [--ack 0-3] [--no-compress] [DATA ..]   (no DATA: lines of stdin)
sillystream tail STREAM [--from start|live|ID] [--to ID] [--filter F] [--limit N] [--name CURSOR]
sillystream bench latency|throughput|idle|catch-up|many-streams|puts|idle-streams|ack-matrix|
                  fan-out|recovery|all
                        [--server-bin FILE] [--count N] [--seconds N] [--idle-count N]
                        [--levels 0,1,2,3] [--cores N] [--catch-up-mib N] [--idle-streams N]
                        [--seal-windows MS,MS] [--subscribers N,N] [--wal-mib N,N] [--runs N]
                        [--timeout-seconds N]

Connecting takes --url HOST:PORT, --ca PEM and --token TEXT or --token-file FILE, or
SILLYSTREAM_URL, SILLYSTREAM_CA and SILLYSTREAM_TOKEN.
Filters: eq:MASK:VALUE, any:MASK or author:N, with clauses separated by commas.
Permissions: read, publish, state, admin.
";

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("sillystream: {message}");
            ExitCode::FAILURE
        }
    }
}

/// `--name value` flags, the repeats of them, and the words around them.
pub(crate) struct Args {
    words: Vec<String>,
    flags: HashMap<String, Vec<String>>,
    switches: Vec<String>,
}

const SWITCHES: [&str; 1] = ["--no-compress"];

pub(crate) fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut parsed = Args {
        words: Vec::new(),
        flags: HashMap::new(),
        switches: Vec::new(),
    };
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if SWITCHES.contains(&arg.as_str()) {
            parsed.switches.push(arg.clone());
        } else if arg.starts_with("--") {
            let value = iter.next().ok_or_else(|| format!("{arg} needs a value"))?;
            parsed
                .flags
                .entry(arg.clone())
                .or_default()
                .push(value.clone());
        } else {
            parsed.words.push(arg.clone());
        }
    }
    Ok(parsed)
}

impl Args {
    pub(crate) fn one(&self, name: &str) -> Option<&str> {
        self.flags
            .get(name)
            .and_then(|all| all.last())
            .map(String::as_str)
    }

    pub(crate) fn number(&self, name: &str) -> Result<Option<u64>, String> {
        self.one(name)
            .map(|text| parse_u64(text).ok_or_else(|| format!("{name}: {text} isn't a number")))
            .transpose()
    }

    /// Refuses anything the command doesn't read, so that a typo isn't quietly ignored.
    fn only(&self, known: &[&str]) -> Result<(), String> {
        let globals = ["--url", "--ca", "--token", "--token-file"];
        for name in self.flags.keys() {
            if !known.contains(&name.as_str()) && !globals.contains(&name.as_str()) {
                return Err(format!("{name} doesn't apply here"));
            }
        }
        Ok(())
    }
}

fn parse_u64(text: &str) -> Option<u64> {
    match text.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => text.parse().ok(),
    }
}

fn setting(args: &Args, flag: &str, env: &str) -> Result<String, String> {
    args.one(flag)
        .map(str::to_owned)
        .or_else(|| std::env::var(env).ok())
        .ok_or_else(|| format!("needs {flag} or {env}"))
}

fn connection(args: &Args) -> Result<(String, Vec<u8>, String), String> {
    let url = setting(args, "--url", "SILLYSTREAM_URL")?;
    let ca_file = setting(args, "--ca", "SILLYSTREAM_CA")?;
    let ca_pem = std::fs::read(&ca_file).map_err(|error| format!("{ca_file}: {error}"))?;
    let token = match args.one("--token-file") {
        Some(file) => std::fs::read_to_string(file)
            .map_err(|error| format!("{file}: {error}"))?
            .trim()
            .to_owned(),
        None => setting(args, "--token", "SILLYSTREAM_TOKEN")?,
    };
    Ok((url, ca_pem, token))
}

fn http(args: &Args) -> Result<HttpClient, String> {
    let (url, ca_pem, token) = connection(args)?;
    HttpClient::new(&url, &ca_pem, &token).map_err(|error| error.to_string())
}

/// Prints a reply's body, and makes anything but success the command's failure.
fn finish(response: Response) -> Result<(), String> {
    if response.is_ok() {
        println!("{}", response.text());
        Ok(())
    } else {
        Err(format!("HTTP {}: {}", response.status, response.text()))
    }
}

fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Percent-encodes a path segment. Stream ids can hold anything but `/`.
fn segment(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~:*".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

async fn run(args: &[String]) -> Result<(), String> {
    let parsed = parse_args(args)?;
    let words: Vec<&str> = parsed.words.iter().map(String::as_str).collect();
    match words.as_slice() {
        ["token", "issue"] => token_issue(&parsed),
        ["stream", action, name] => stream_command(&parsed, action, name).await,
        ["sink", action, stream, id] => sink_command(&parsed, action, stream, id).await,
        ["publish", stream, data @ ..] => publish(&parsed, stream, data).await,
        ["tail", stream] => tail(&parsed, stream).await,
        ["bench", what] => bench::run(&parsed, what).await,
        _ => Err(format!("usage:\n{USAGE}")),
    }
}

fn parse_perm(name: &str) -> Result<u8, String> {
    Ok(match name {
        "read" => auth::PERM_READ,
        "publish" => auth::PERM_PUBLISH,
        "state" => auth::PERM_STATE,
        "admin" => auth::PERM_ADMIN,
        other => return Err(format!("{other} isn't a permission")),
    })
}

fn token_issue(args: &Args) -> Result<(), String> {
    args.only(&["--secret-file", "--author", "--grant", "--ttl-ms"])?;
    let secret_file = args.one("--secret-file").ok_or("needs --secret-file")?;
    let secret = std::fs::read(secret_file).map_err(|error| format!("{secret_file}: {error}"))?;
    let author = args.number("--author")?.ok_or("needs --author")?;
    let author = std::num::NonZeroU64::new(author).ok_or("--author can't be 0: 0 is the system")?;
    let ttl_ms = args.number("--ttl-ms")?.ok_or("needs --ttl-ms")?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis() as u64;
    let mut grants = Vec::new();
    for grant in args.flags.get("--grant").into_iter().flatten() {
        let (pattern, perms) = grant
            .rsplit_once(':')
            .ok_or_else(|| format!("{grant}: a grant is PATTERN:PERM,PERM"))?;
        let mut bits = 0;
        for perm in perms.split(',') {
            bits |= parse_perm(perm)?;
        }
        grants.push(auth::Grant {
            pattern: pattern.to_owned(),
            perms: bits,
        });
    }
    if grants.is_empty() {
        return Err("needs at least one --grant".to_owned());
    }
    let token = auth::Token {
        expiry_ms: now_ms.saturating_add(ttl_ms),
        author,
        grants,
    }
    .sign(&secret)
    .map_err(|error| format!("{error:?}"))?;
    println!("{token}");
    Ok(())
}

async fn stream_command(args: &Args, action: &str, name: &str) -> Result<(), String> {
    let client = http(args)?;
    let path = format!("/v1/stream/{}", segment(name));
    let response = match action {
        "create" | "config" => {
            args.only(&["--retention", "--default-ack", "--min-ack"])?;
            let mut fields = Vec::new();
            if let Some(retention) = args.one("--retention") {
                fields.push(format!("\"retention\":{}", json_string(retention)));
            }
            for (flag, field) in [("--default-ack", "default_ack"), ("--min-ack", "min_ack")] {
                if let Some(level) = args.number(flag)? {
                    fields.push(format!("\"{field}\":{level}"));
                }
            }
            let body = format!("{{{}}}", fields.join(","));
            client.request("PUT", &path, &[], body.as_bytes()).await
        }
        "core" => {
            args.only(&["--core"])?;
            let core = args.number("--core")?.ok_or("--core is required")?;
            let body = format!("{{\"core\":{core}}}");
            let path = format!("{path}/core");
            client.request("PUT", &path, &[], body.as_bytes()).await
        }
        "info" => {
            args.only(&[])?;
            client.request("GET", &path, &[], b"").await
        }
        "delete" => {
            args.only(&[])?;
            client.request("DELETE", &path, &[], b"").await
        }
        other => return Err(format!("stream {other}?\n{USAGE}")),
    };
    finish(response.map_err(|error| error.to_string())?)
}

async fn sink_command(args: &Args, action: &str, stream: &str, id: &str) -> Result<(), String> {
    let client = http(args)?;
    let path = format!("/v1/stream/{}/sink/{}", segment(stream), segment(id));
    let response = match action {
        "create" | "update" => {
            args.only(&[
                "--kind",
                "--target",
                "--filter",
                "--on-expire",
                "--concurrency",
                "--start",
                "--sink-url",
                "--secret",
                "--interval-ms",
                "--budget-bytes",
            ])?;
            let kind = args.one("--kind").ok_or("needs --kind")?;
            let mut fields = vec![format!("\"kind\":{}", json_string(kind))];
            for (flag, field) in [
                ("--sink-url", "url"),
                ("--target", "target"),
                ("--filter", "filter"),
                ("--on-expire", "on_expire"),
                ("--start", "start"),
                ("--secret", "secret"),
            ] {
                if let Some(value) = args.one(flag) {
                    fields.push(format!("\"{field}\":{}", json_string(value)));
                }
            }
            if let Some(concurrency) = args.number("--concurrency")? {
                fields.push(format!("\"concurrency\":{concurrency}"));
            }
            if let Some(interval_ms) = args.number("--interval-ms")? {
                fields.push(format!("\"interval_ms\":{interval_ms}"));
            }
            if let Some(budget_bytes) = args.number("--budget-bytes")? {
                fields.push(format!("\"budget_bytes\":{budget_bytes}"));
            }
            let body = format!("{{{}}}", fields.join(","));
            client.request("PUT", &path, &[], body.as_bytes()).await
        }
        "delete" => {
            args.only(&[])?;
            client.request("DELETE", &path, &[], b"").await
        }
        "pause" | "resume" => {
            args.only(&[])?;
            client
                .request("POST", &format!("{path}/{action}"), &[], b"")
                .await
        }
        "reset" => {
            args.only(&["--start"])?;
            let body = match args.one("--start") {
                Some(start) => format!("{{\"start\":{}}}", json_string(start)),
                None => String::new(),
            };
            client
                .request("POST", &format!("{path}/reset"), &[], body.as_bytes())
                .await
        }
        other => return Err(format!("sink {other}?\n{USAGE}")),
    };
    finish(response.map_err(|error| error.to_string())?)
}

pub(crate) fn parse_ack(level: u64) -> Result<AckLevel, String> {
    Ok(match level {
        0 => AckLevel::None,
        1 => AckLevel::Memory,
        2 => AckLevel::WalFsync,
        3 => AckLevel::SegmentInS3,
        other => return Err(format!("{other} isn't an ack level")),
    })
}

async fn publish(args: &Args, stream: &str, data: &[&str]) -> Result<(), String> {
    args.only(&["--options", "--ack"])?;
    let (url, ca_pem, token) = connection(args)?;
    let client = Client::connect(ClientConfig::new(&url, &ca_pem, &token))
        .await
        .map_err(|error| error.to_string())?;
    let options = args.number("--options")?.unwrap_or(0);
    let mut config = PublisherConfig {
        ack: parse_ack(args.number("--ack")?.unwrap_or(2))?,
        ..PublisherConfig::default()
    };
    if args.switches.iter().any(|name| name == "--no-compress") {
        config.batch = BatchConfig {
            zstd_level: None,
            ..config.batch
        };
    }
    let mut publisher = client
        .publisher(stream, config)
        .map_err(|error| error.to_string())?;
    if data.is_empty() {
        for line in std::io::stdin().lock().lines() {
            publisher.add(
                options,
                line.map_err(|error| error.to_string())?.into_bytes(),
            );
        }
    } else {
        for item in data {
            publisher.add(options, item.as_bytes().to_vec());
        }
    }
    if publisher.pending_count() == 0 {
        return Err("nothing to publish".to_owned());
    }
    let acks = publisher.flush().await.map_err(|error| error.to_string())?;
    for ack in acks {
        println!("first_id={} count={}", ack.first_id, ack.count);
    }
    Ok(())
}

pub(crate) fn parse_filter(text: &str, spec: &mut Subscribe) -> Result<(), String> {
    for clause in text.split(',') {
        let parts: Vec<&str> = clause.split(':').collect();
        let number = |text: &str| parse_u64(text).ok_or_else(|| format!("{clause}: bad number"));
        match parts.as_slice() {
            ["eq", mask, value] => spec.clauses.push(WireClause {
                mask: number(mask)?,
                value: number(value)?,
                op: 0,
            }),
            ["any", mask] => spec.clauses.push(WireClause {
                mask: number(mask)?,
                value: 0,
                op: 1,
            }),
            ["author", author] => spec.author = Some(number(author)?),
            _ => return Err(format!("{clause} isn't a filter clause")),
        }
    }
    Ok(())
}

async fn tail(args: &Args, stream: &str) -> Result<(), String> {
    args.only(&["--from", "--to", "--filter", "--limit", "--name"])?;
    let (url, ca_pem, token) = connection(args)?;
    let client = Client::connect(ClientConfig::new(&url, &ca_pem, &token))
        .await
        .map_err(|error| error.to_string())?;
    let from = match args.one("--from").unwrap_or("live") {
        "start" => FROM_START,
        "live" => FROM_LIVE,
        id => parse_u64(id).ok_or_else(|| format!("--from: {id}?"))?,
    };
    let mut spec = Subscribe::new(stream, from);
    if let Some(filter) = args.one("--filter") {
        parse_filter(filter, &mut spec)?;
    }
    spec.name = args.one("--name").map(str::to_owned);
    spec.until = args.number("--to")?;
    let named = spec.name.is_some();
    let mut left = args.number("--limit")?;
    let mut subscription = client.subscribe(spec).map_err(|error| error.to_string())?;
    let mut out = std::io::stdout().lock();
    while left != Some(0) {
        let Some(event) = subscription.next().await else {
            if subscription.reached_until() {
                break;
            }
            return Err("the connection stopped".to_owned());
        };
        let Event::Items(items) = event.map_err(|error| error.to_string())? else {
            continue;
        };
        let mut printed = None;
        for item in &items {
            if left == Some(0) {
                break;
            }
            printed = Some(item.id);
            let shown = String::from_utf8_lossy(&item.bytes);
            let line = format!(
                "{} {:#x} {} {}\n",
                item.id, item.options, item.author, shown
            );
            out.write_all(line.as_bytes())
                .map_err(|error| error.to_string())?;
            left = left.map(|count| count - 1);
        }
        out.flush().map_err(|error| error.to_string())?;
        // Only what was printed, however much came.
        if named && let Some(id) = printed {
            subscription.commit(id).map_err(|error| error.to_string())?;
        }
    }
    // A commit is one way, and this process is about to end.
    drop(subscription);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(())
}
