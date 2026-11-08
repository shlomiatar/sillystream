//! `silly bench webhook-receiver`: an HTTPS endpoint for `webhook.deliver` sinks that answers
//! every POST with 200 and prints how many records arrive each second, for benchmarking a sink
//! across a real network. It makes its own self-signed certificate for `--name` and writes it to
//! `--ca-out`, for the server's `--sink-ca-file`. A connection carries as many POSTs as the sink
//! sends on it, and a compressed body is decoded before its records are counted; the MiB it
//! prints are as they came over the network.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::Args;

#[derive(Default)]
struct Counts {
    records: AtomicU64,
    items: AtomicU64,
    bytes: AtomicU64,
    posts: AtomicU64,
}

pub(crate) async fn run(args: &Args) -> Result<(), String> {
    args.only(&["--listen", "--name", "--ca-out", "--delay-ms"])?;
    let listen = args.one("--listen").unwrap_or("0.0.0.0:8443");
    let name = args
        .one("--name")
        .ok_or("needs --name, the host the sink's URL names")?;
    let ca_out = args.one("--ca-out").ok_or("needs --ca-out FILE")?;
    let delay = Duration::from_millis(args.number("--delay-ms")?.unwrap_or(0));

    let certified = rcgen::generate_simple_self_signed(vec![name.to_owned()])
        .map_err(|error| error.to_string())?;
    std::fs::write(ca_out, certified.cert.pem()).map_err(|error| format!("{ca_out}: {error}"))?;
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(certified.key_pair.serialize_der().into());
    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certified.cert.der().clone()], key)
        .map_err(|error| error.to_string())?;
    let acceptor = TlsAcceptor::from(Arc::new(tls));
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|error| format!("{listen}: {error}"))?;
    println!("listening on https://{listen}, certificate for {name} in {ca_out}");

    let counts = Arc::new(Counts::default());
    tokio::spawn(report(Arc::clone(&counts)));
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            continue;
        };
        let _ = socket.set_nodelay(true);
        let (acceptor, counts) = (acceptor.clone(), Arc::clone(&counts));
        tokio::spawn(async move {
            if let Ok(stream) = acceptor.accept(socket).await {
                serve(stream, &counts, delay).await;
            }
        });
    }
}

/// Reads POSTs with a `Content-Length` one after another and answers each with 200.
async fn serve(mut socket: impl AsyncRead + AsyncWrite + Unpin, counts: &Counts, delay: Duration) {
    let mut buffer = Vec::new();
    let mut chunk = vec![0u8; 256 * 1024];
    loop {
        let head_end = loop {
            if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            match socket.read(&mut chunk).await {
                Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
                _ => return,
            }
        };
        let head = String::from_utf8_lossy(&buffer[..head_end]).to_ascii_lowercase();
        let len: usize = head
            .split("\r\n")
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        while buffer.len() < head_end + len {
            match socket.read(&mut chunk).await {
                Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
                _ => return,
            }
        }
        let encoding = head
            .split("\r\n")
            .find_map(|line| line.strip_prefix("content-encoding:"))
            .map(str::trim);
        let body = match decoded(&buffer[head_end..head_end + len], encoding) {
            Some(body) => body,
            None => return,
        };
        let records = format::Records::new(&body).count() as u64;
        // What the SDK packed into those records, which is what a producer published.
        let items = sdk::decode_records(&body, 0, usize::MAX).map_or(0, |items| items.len()) as u64;
        buffer.drain(..head_end + len);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        counts.records.fetch_add(records, Ordering::Relaxed);
        counts.items.fetch_add(items, Ordering::Relaxed);
        counts.bytes.fetch_add(len as u64, Ordering::Relaxed);
        counts.posts.fetch_add(1, Ordering::Relaxed);
        let reply = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        if socket.write_all(reply).await.is_err() {
            return;
        }
    }
}

fn decoded(body: &[u8], encoding: Option<&str>) -> Option<Vec<u8>> {
    use std::io::Read;
    match encoding {
        None => Some(body.to_vec()),
        Some("zstd") => zstd::decode_all(body).ok(),
        Some("gzip") => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(body)
                .read_to_end(&mut out)
                .ok()?;
            Some(out)
        }
        Some(_) => None,
    }
}

/// A line each second records arrive, and a total once they stop for 3 s.
async fn report(counts: Arc<Counts>) {
    // When the burst started and last had records, and [records, items, bytes, POSTs] in it.
    let mut burst: Option<(Instant, Instant, [u64; 4])> = None;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let second = [
            counts.records.swap(0, Ordering::Relaxed),
            counts.items.swap(0, Ordering::Relaxed),
            counts.bytes.swap(0, Ordering::Relaxed),
            counts.posts.swap(0, Ordering::Relaxed),
        ];
        let now = Instant::now();
        if second[3] > 0 {
            print_rate("", second, 1.0);
            let (first, _, mut all) = burst.unwrap_or((now - Duration::from_secs(1), now, [0; 4]));
            for (total, add) in all.iter_mut().zip(second) {
                *total += add;
            }
            burst = Some((first, now, all));
        } else if let Some((first, last, all)) = burst
            && now.duration_since(last) >= Duration::from_secs(3)
        {
            print_rate("total over ", all, last.duration_since(first).as_secs_f64());
            burst = None;
        }
    }
}

fn print_rate(label: &str, [records, items, bytes, posts]: [u64; 4], seconds: f64) {
    let per = |count: u64| count as f64 / seconds;
    println!(
        "{label}{seconds:.0} s: {records} records, {items} items, {posts} POSTs, {:.1} MiB: \
         {:.0} records/s, {:.0} items/s, {:.1} POSTs/s, {:.1} MiB/s",
        bytes as f64 / 1_048_576.0,
        per(records),
        per(items),
        per(posts),
        per(bytes) / 1_048_576.0,
    );
}
