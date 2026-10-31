//! `sillystream-server`: a node behind its TLS port (SPEC sections 6, 11, 13, D13).

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use server::net::{Server, ServerConfig};
use server::{LocalCacheConfig, NodeConfig, RecoverConfig, SystemClock};
use storage::{ObjectStoreConfig, S3Config, build_store};

const USAGE: &str = "\
sillystream-server --listen ADDR --wal-dir DIR --data-dir DIR --cert PEM --key PEM \\
    --token-secret-file FILE --object-store local:DIR|memory|s3:BUCKET
  [--cores N] [--seal-window-ms N] [--node-name NAME] [--memory-cap-bytes N]
  [--local-cache-cap-bytes N] [--wal-preallocate-bytes N] [--max-frame-bytes N]
  [--active-stream-limit N]

For s3:BUCKET, S3_REGION and S3_ENDPOINT are read from the environment, and keys from the
usual AWS variables. Real S3 has not been exercised.";

/// Sparse on the file systems here (D51), so this costs only what is written.
const WAL_PREALLOCATE_DEFAULT_BYTES: u64 = 64 * 1024 * 1024;
/// How long a clean stop waits for a last seal. A restart replays whatever this leaves.
const LAST_SEAL_WAIT_MS: u64 = 10_000;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("sillystream-server: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mut listen = None;
    let mut wal_dir = None;
    let mut data_dir = None;
    let mut cert = None;
    let mut key = None;
    let mut secret_file = None;
    let mut object_store = None;
    let mut cores = None;
    let mut seal_window_ms = None;
    let mut node_name = None;
    let mut memory_cap_bytes = None;
    let mut cache_cap_bytes = 10 * 1024 * 1024 * 1024u64;
    let mut preallocate_bytes = WAL_PREALLOCATE_DEFAULT_BYTES;
    let mut max_frame_bytes = None;
    let mut active_stream_limit = None;
    while let Some(flag) = args.next() {
        if flag == "--help" {
            println!("{USAGE}");
            return Ok(());
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        let number = |value: &str| -> Result<u64, String> {
            value.parse().map_err(|_| format!("{flag} is a number"))
        };
        match flag.as_str() {
            "--listen" => listen = Some(value),
            "--wal-dir" => wal_dir = Some(PathBuf::from(value)),
            "--data-dir" => data_dir = Some(PathBuf::from(value)),
            "--cert" => cert = Some(PathBuf::from(value)),
            "--key" => key = Some(PathBuf::from(value)),
            "--token-secret-file" => secret_file = Some(PathBuf::from(value)),
            "--object-store" => object_store = Some(value),
            "--cores" => cores = Some(number(&value)? as usize),
            "--seal-window-ms" => seal_window_ms = Some(number(&value)?),
            "--node-name" => node_name = Some(value),
            "--memory-cap-bytes" => memory_cap_bytes = Some(number(&value)?),
            "--local-cache-cap-bytes" => cache_cap_bytes = number(&value)?,
            "--wal-preallocate-bytes" => preallocate_bytes = number(&value)?,
            "--max-frame-bytes" => max_frame_bytes = Some(number(&value)?),
            "--active-stream-limit" => active_stream_limit = Some(number(&value)? as usize),
            _ => return Err(format!("unknown flag {flag}\n{USAGE}")),
        }
    }
    let need = |what: &str| format!("{what} is required\n{USAGE}");
    let listen = listen.ok_or_else(|| need("--listen"))?;
    let listen = listen
        .parse()
        .map_err(|_| format!("--listen isn't an address: {listen}"))?;
    let wal_dir = wal_dir.ok_or_else(|| need("--wal-dir"))?;
    let data_dir = data_dir.ok_or_else(|| need("--data-dir"))?;
    let read = |path: Option<PathBuf>, what: &str| -> Result<Vec<u8>, String> {
        let path = path.ok_or_else(|| need(what))?;
        std::fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))
    };
    let cert_pem = read(cert, "--cert")?;
    let key_pem = read(key, "--key")?;
    let mut token_secret = read(secret_file, "--token-secret-file")?;
    while token_secret.last().is_some_and(u8::is_ascii_whitespace) {
        token_secret.pop();
    }
    let store_config = parse_store(&object_store.ok_or_else(|| need("--object-store"))?)?;
    let store = build_store(&store_config).map_err(|error| error.to_string())?;

    std::fs::create_dir_all(&wal_dir).map_err(|error| format!("{}: {error}", wal_dir.display()))?;
    std::fs::create_dir_all(&data_dir)
        .map_err(|error| format!("{}: {error}", data_dir.display()))?;
    let core_count = cores.unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
    });
    let mut node = NodeConfig::new(core_count);
    if let Some(name) = node_name {
        node.node_name = name;
    }
    node.seal_window_ms = Some(seal_window_ms.unwrap_or(10_000).clamp(1_000, 120_000));
    if let Some(cap_bytes) = memory_cap_bytes {
        node.memory_cap_bytes = cap_bytes;
    }
    if let Some(frame_bytes) = max_frame_bytes {
        node.max_frame_bytes = frame_bytes;
    }
    node.active_stream_limit = active_stream_limit;
    node.local_cache = Some(LocalCacheConfig {
        dir: data_dir.join("cache"),
        cap_bytes: cache_cap_bytes,
    });
    let config = ServerConfig {
        node,
        listen,
        cert_pem,
        key_pem,
        token_secret,
    };
    let (server, report) = Server::start_recovering(
        config,
        RecoverConfig {
            wal_dir,
            metadata_dir: data_dir.join("metadata"),
            wal_preallocate_bytes: preallocate_bytes,
        },
        store,
        Arc::new(SystemClock),
    )
    .map_err(|error| error.to_string())?;
    if let Some(warning) = report.warning() {
        eprintln!("sillystream-server: {warning}");
    }
    eprintln!(
        "sillystream-server: generation {}, last valid id {}, {} WAL entries replayed, {} streams \
         restored, {} objects looked at",
        report.generation,
        report.last_valid_id,
        report.replayed_entry_count,
        report.restored_stream_count,
        report.scanned_object_count
    );
    // A parent waiting for the port reads this line.
    println!("listening on {}", server.local_addr());

    wait_for_signal().map_err(|error| error.to_string())?;
    if !server.seal_before_stop(LAST_SEAL_WAIT_MS) {
        eprintln!("sillystream-server: the last seal didn't finish, and a restart replays it");
    }
    let exit = server.shutdown();
    if let Err(error) = exit.wal {
        return Err(format!("the WAL stopped with an error: {error}"));
    }
    Ok(())
}

fn parse_store(text: &str) -> Result<ObjectStoreConfig, String> {
    if text == "memory" {
        return Ok(ObjectStoreConfig::Memory);
    }
    if let Some(root) = text.strip_prefix("local:") {
        std::fs::create_dir_all(root).map_err(|error| format!("{root}: {error}"))?;
        return Ok(ObjectStoreConfig::Local { root: root.into() });
    }
    if let Some(bucket) = text.strip_prefix("s3:") {
        return Ok(ObjectStoreConfig::S3(S3Config {
            bucket: bucket.to_owned(),
            region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
            endpoint: std::env::var("S3_ENDPOINT").ok(),
            access_key_id: None,
            secret_access_key: None,
            allow_http: false,
        }));
    }
    Err(format!("--object-store isn't one of the forms: {text}"))
}

fn wait_for_signal() -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    })
}
