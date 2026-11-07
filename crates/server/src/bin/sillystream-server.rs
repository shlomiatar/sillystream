//! `sillystream-server`: a node behind its TLS port (SPEC sections 6, 11, 13, D13).

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use format::IdentityKey;
use server::net::{Server, ServerConfig};
use server::{
    Clock, CompactionConfig, Keys, LocalCacheConfig, NodeConfig, RecoverConfig, SinkConfig,
    SystemClock,
};
use storage::{ObjectStoreConfig, build_store, check_conditional_create, parse_store_config};

const USAGE: &str = "\
sillystream-server --listen ADDR --wal-dir DIR --data-dir DIR --cert PEM --key PEM \\
    --token-secret-file FILE --stream-key-file FILE \\
    --object-store local:DIR|memory|gs:BUCKET|s3:BUCKET
  [--cores N] [--seal-window-ms N] [--memory-cap-bytes N]
  [--local-cache-cap-bytes N] [--wal-preallocate-bytes N] [--max-frame-bytes N]
  [--active-stream-limit N] [--compact-period-ms N] [--expire-period-ms N]
  [--compact-merge-wait-ms N] [--sink-allow-private-addresses true|false]
  [--sink-ca-file PEM] [--sink-archive-store local:DIR|memory|gs:BUCKET|s3:BUCKET]
  [--sink-pool-threads N] [--sink-max-in-flight N] [--trace-catch-up true|false]
  [--max-connections N] [--listen-backlog N]

--stream-key-file holds the deployment's stream identity key as 64 hex digits, such as the output
of `openssl rand -hex 32`. Every stream is stored under a hash made with it, so it can never change,
and it must not be kept in the bucket.

--wal-dir decides what a level 2 ack survives: an acked record survives machine loss only if
EBS is mounted there. On local disk, machine loss can take up to one seal window. See
docs/DEPLOYMENT.md.

For gs:BUCKET, GOOGLE_SERVICE_ACCOUNT is a key file's path (SINK_ARCHIVE_GOOGLE_SERVICE_ACCOUNT
for an archive). For --object-store s3:BUCKET, S3_REGION and S3_ENDPOINT are read from the environment, and keys
from the usual AWS variables. For --sink-archive-store s3:BUCKET, the same names with the prefix
SINK_ARCHIVE_ are read: SINK_ARCHIVE_S3_REGION, SINK_ARCHIVE_S3_ENDPOINT,
SINK_ARCHIVE_AWS_ACCESS_KEY_ID and SINK_ARCHIVE_AWS_SECRET_ACCESS_KEY, so that an archive to R2
can sit beside a node on AWS. Real S3 and R2 have not been exercised.";

const ARCHIVE_ENV_PREFIX: &str = "SINK_ARCHIVE_";

/// Sparse on the file systems here (D51), so this costs only what is written.
const WAL_PREALLOCATE_DEFAULT_BYTES: u64 = 64 * 1024 * 1024;
/// How long a clean stop waits for a last seal. A restart replays whatever this leaves.
const LAST_SEAL_WAIT_MS: u64 = 10_000;

/// 64 hex digits, with any whitespace around them.
fn parse_stream_key(text: &[u8]) -> Result<IdentityKey, String> {
    let refused = || "--stream-key-file holds 64 hex digits".to_owned();
    let text = std::str::from_utf8(text).map_err(|_| refused())?.trim();
    if text.len() != 2 * IdentityKey::BYTES || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(refused());
    }
    let mut key = [0u8; IdentityKey::BYTES];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * index..2 * index + 2], 16).map_err(|_| refused())?;
    }
    Ok(IdentityKey::new(key))
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("sillystream-server: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Raises the soft limit on open files as far as the hard limit and the system allow, since each
/// sink attempt and client holds one, and returns the soft limit then in force (D295).
#[allow(unsafe_code)]
fn raise_open_files_limit() -> u64 {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit only writes the struct it is given.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) } != 0 {
        return 256;
    }
    // macOS refuses a soft limit past its per-process maximum, which can be below the hard one,
    // so OPEN_MAX (10,240) is tried after it.
    for wanted in [limit.rlim_max.min(1 << 20), 10_240] {
        if wanted <= limit.rlim_cur {
            break;
        }
        let raised = libc::rlimit {
            rlim_cur: wanted,
            rlim_max: limit.rlim_max,
        };
        // SAFETY: setrlimit only reads the struct it is given.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const raised) } == 0 {
            limit.rlim_cur = wanted;
            break;
        }
    }
    limit.rlim_cur
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mut listen = None;
    let mut wal_dir = None;
    let mut data_dir = None;
    let mut cert = None;
    let mut key = None;
    let mut secret_file = None;
    let mut stream_key_file = None;
    let mut object_store = None;
    let mut cores = None;
    let mut seal_window_ms = None;
    let mut max_connections = None;
    let mut listen_backlog = None;
    let mut memory_cap_bytes = None;
    let mut cache_cap_bytes = 10 * 1024 * 1024 * 1024u64;
    let mut preallocate_bytes = WAL_PREALLOCATE_DEFAULT_BYTES;
    let mut max_frame_bytes = None;
    let mut active_stream_limit = None;
    let mut compaction = CompactionConfig::default();
    let mut sink = SinkConfig::default();
    let mut sink_archive_store = None;
    let mut sink_ca_file = None;
    let mut sink_max_in_flight = None;
    let mut trace_catch_up = false;
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
            "--stream-key-file" => stream_key_file = Some(PathBuf::from(value)),
            "--object-store" => object_store = Some(value),
            "--cores" => cores = Some(number(&value)? as usize),
            "--seal-window-ms" => seal_window_ms = Some(number(&value)?),
            "--max-connections" => max_connections = Some(number(&value)?),
            "--listen-backlog" => listen_backlog = Some(number(&value)? as i32),
            "--memory-cap-bytes" => memory_cap_bytes = Some(number(&value)?),
            "--local-cache-cap-bytes" => cache_cap_bytes = number(&value)?,
            "--wal-preallocate-bytes" => preallocate_bytes = number(&value)?,
            "--max-frame-bytes" => max_frame_bytes = Some(number(&value)?),
            "--active-stream-limit" => active_stream_limit = Some(number(&value)? as usize),
            "--compact-period-ms" => compaction.compact_period_ms = Some(number(&value)?),
            "--expire-period-ms" => compaction.expire_period_ms = Some(number(&value)?),
            "--compact-merge-wait-ms" => compaction.merge_wait_ms = number(&value)?,
            "--sink-allow-private-addresses" => {
                sink.allow_private_addresses = match value.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(format!("{flag} is true or false")),
                };
            }
            "--sink-ca-file" => sink_ca_file = Some(PathBuf::from(value)),
            "--sink-archive-store" => sink_archive_store = Some(value),
            "--sink-pool-threads" => sink.pool_thread_count = number(&value)? as usize,
            "--sink-max-in-flight" => {
                sink_max_in_flight = Some(
                    u32::try_from(number(&value)?).map_err(|_| format!("{flag} is too large"))?,
                );
            }
            "--trace-catch-up" => {
                trace_catch_up = match value.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(format!("{flag} is true or false")),
                };
            }
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
    let stream_key = parse_stream_key(&read(stream_key_file, "--stream-key-file")?)?;
    let store_config = parse_store(&object_store.ok_or_else(|| need("--object-store"))?, "")?;
    check_store(&store_config)?;
    let store = build_store(&store_config).map_err(|error| error.to_string())?;

    std::fs::create_dir_all(&wal_dir).map_err(|error| format!("{}: {error}", wal_dir.display()))?;
    std::fs::create_dir_all(&data_dir)
        .map_err(|error| format!("{}: {error}", data_dir.display()))?;
    let core_count = cores.unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
    });
    let mut node = NodeConfig::new(core_count);
    node.keys = Keys::new(stream_key);
    // Section 7.1's range. A value outside it is a mistake to report, not to change quietly.
    let seal_window_ms = seal_window_ms.unwrap_or(10_000);
    if !(1_000..=120_000).contains(&seal_window_ms) {
        return Err(format!(
            "--seal-window-ms is 1000 to 120000, not {seal_window_ms}"
        ));
    }
    node.seal_window_ms = Some(seal_window_ms);
    if let Some(max_connections) = max_connections {
        node.max_connections = max_connections;
    }
    if let Some(listen_backlog) = listen_backlog {
        node.listen_backlog = listen_backlog;
    }
    if let Some(cap_bytes) = memory_cap_bytes {
        node.memory_cap_bytes = cap_bytes;
    }
    if let Some(frame_bytes) = max_frame_bytes {
        node.max_frame_bytes = frame_bytes;
    }
    node.active_stream_limit = active_stream_limit;
    node.trace_catch_up = trace_catch_up;
    node.compaction = Some(compaction);
    if let Some(path) = sink_ca_file {
        sink.extra_roots_pem =
            Some(std::fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?);
    }
    if let Some(text) = sink_archive_store {
        let config = parse_store(&text, ARCHIVE_ENV_PREFIX)?;
        check_store(&config)?;
        sink.archive_store = Some(build_store(&config).map_err(|error| error.to_string())?);
    }
    let open_files = raise_open_files_limit();
    sink.in_flight_max_count = sink_max_in_flight.unwrap_or_else(|| {
        let machine_cores =
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        server::in_flight_max_for(machine_cores, open_files)
    });
    node.sink = sink;
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
    let (mut server, report) = Server::start_recovering(
        config,
        RecoverConfig {
            wal_dir,
            metadata_dir: data_dir.join("metadata"),
            wal_preallocate_bytes: preallocate_bytes,
        },
        store,
        clock(),
    )
    .map_err(|error| error.to_string())?;
    for warning in [
        report.warning(),
        report.fenced_warning(),
        report.damaged_warning(),
    ]
    .into_iter()
    .flatten()
    {
        eprintln!("sillystream-server: {warning}");
    }
    eprintln!(
        "sillystream-server: generation {}, last valid id {}, {} WAL files synced, {} WAL entries \
         replayed, {} streams restored, {} objects looked at",
        report.generation,
        report.last_valid_id,
        report.synced_wal_file_count,
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

/// With `test-hooks`, `SILLYSTREAM_CLOCK_OFFSET_MS` moves the time ids and expiry read, so that
/// retention can be reached end to end. A release build has no such thing (D32).
#[cfg(feature = "test-hooks")]
fn clock() -> Arc<dyn Clock> {
    struct OffsetClock(u64);
    impl Clock for OffsetClock {
        fn now_ms(&self) -> u64 {
            SystemClock.now_ms() + self.0
        }
    }
    let offset_ms = std::env::var("SILLYSTREAM_CLOCK_OFFSET_MS")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(0);
    Arc::new(OffsetClock(offset_ms))
}

#[cfg(not(feature = "test-hooks"))]
fn clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

/// On a client and a runtime of its own, since neither outlives the check.
fn check_store(config: &ObjectStoreConfig) -> Result<(), String> {
    let store = build_store(config).map_err(|error| error.to_string())?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(check_conditional_create(store.as_ref()))
        .map_err(|error| format!("the object store failed its startup check: {error}"))
}

fn parse_store(text: &str, prefix: &str) -> Result<ObjectStoreConfig, String> {
    let config = parse_store_config(text, prefix, |name| std::env::var(name).ok())?;
    if let ObjectStoreConfig::Local { root } = &config {
        std::fs::create_dir_all(root).map_err(|error| format!("{}: {error}", root.display()))?;
    }
    Ok(config)
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
