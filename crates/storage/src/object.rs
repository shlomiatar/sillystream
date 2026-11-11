//! Object store access and paths (SPEC section 7.3). Everything is tested on
//! `LocalFileSystem` and `InMemory`. Native GCS has been run against a real bucket, by hand. Real
//! S3 exists only behind configuration here, and has never been exercised.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

/// S3 or anything that speaks it, such as R2 or MinIO. Without keys, the credentials
/// come from the environment's usual sources when a request is first made (D13).
#[derive(Clone, PartialEq, Eq)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub allow_http: bool,
}

impl std::fmt::Debug for S3Config {
    /// Leaves the keys out, so that config can't leak them into a log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("allow_http", &self.allow_http)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    Memory,
    Local {
        root: PathBuf,
    },
    S3(S3Config),
    /// Google Cloud Storage's own API, because its S3 endpoint ignores a conditional create.
    /// Without a key file, credentials come from the environment's default sources.
    Gcs {
        bucket: String,
        service_account_path: Option<String>,
    },
}

/// Builds the client. This never connects: the first request does.
pub fn build_store(config: &ObjectStoreConfig) -> object_store::Result<Arc<dyn ObjectStore>> {
    match config {
        ObjectStoreConfig::Memory => Ok(Arc::new(InMemory::new())),
        ObjectStoreConfig::Local { root } => Ok(Arc::new(LocalFileSystem::new_with_prefix(root)?)),
        ObjectStoreConfig::S3(s3) => {
            // Conditional create is how seals and ownership records refuse to overwrite.
            // With its own keys, nothing comes from the environment: a session token or endpoint
            // meant for another destination would otherwise go along with them.
            let base = if s3.access_key_id.is_some() {
                AmazonS3Builder::new()
            } else {
                AmazonS3Builder::from_env()
            };
            let mut builder = base
                .with_bucket_name(&s3.bucket)
                .with_region(&s3.region)
                .with_allow_http(s3.allow_http)
                .with_conditional_put(S3ConditionalPut::ETagMatch);
            if let Some(endpoint) = &s3.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if let Some(key) = &s3.access_key_id {
                builder = builder.with_access_key_id(key);
            }
            if let Some(secret) = &s3.secret_access_key {
                builder = builder.with_secret_access_key(secret);
            }
            Ok(Arc::new(builder.build()?))
        }
        ObjectStoreConfig::Gcs {
            bucket,
            service_account_path,
        } => {
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
            if let Some(path) = service_account_path {
                builder = builder.with_service_account_path(path);
            }
            Ok(Arc::new(builder.build()?))
        }
    }
}

/// Creates one scratch object twice, and fails unless the second is refused. Seals and ownership
/// records depend on that refusal, and a store that ignores it overwrites them with no error.
pub async fn check_conditional_create(store: &dyn ObjectStore) -> Result<(), String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    let path = Path::from(format!("startup-checks/{}-{nanos}", std::process::id()));
    let create = || PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    store
        .put_opts(&path, PutPayload::from_static(b"first"), create())
        .await
        .map_err(|error| format!("creating {path}: {error}"))?;
    let second = store
        .put_opts(&path, PutPayload::from_static(b"second"), create())
        .await;
    let _ = store.delete(&path).await;
    match second {
        Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Ok(_) => Err(
            "this store overwrote an object that a conditional create should have refused, which \
             would lose acked records. On Google Cloud Storage use gs:BUCKET, not s3:BUCKET"
                .to_owned(),
        ),
        Err(error) => Err(format!("creating {path} a second time: {error}")),
    }
}

/// Reads `local:DIR`, `memory`, `gs:BUCKET` or `s3:BUCKET`. For `gs:`, `env` is asked for
/// `<prefix>GOOGLE_SERVICE_ACCOUNT`, a key file's path. For `s3:`, `env` is asked for `<prefix>S3_REGION`,
/// `<prefix>S3_ENDPOINT`, `<prefix>AWS_ACCESS_KEY_ID` and `<prefix>AWS_SECRET_ACCESS_KEY`. With an
/// empty prefix, keys left out come from the usual AWS variables in `build_store` (D250).
pub fn parse_store_config(
    text: &str,
    prefix: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<ObjectStoreConfig, String> {
    if text == "memory" {
        return Ok(ObjectStoreConfig::Memory);
    }
    if let Some(root) = text.strip_prefix("local:") {
        return Ok(ObjectStoreConfig::Local { root: root.into() });
    }
    let var = |name: &str| env(&format!("{prefix}{name}"));
    if let Some(bucket) = text.strip_prefix("gs:") {
        return Ok(ObjectStoreConfig::Gcs {
            bucket: bucket.to_owned(),
            service_account_path: var("GOOGLE_SERVICE_ACCOUNT"),
        });
    }
    let Some(bucket) = text.strip_prefix("s3:") else {
        return Err(format!(
            "a store is local:DIR, memory, gs:BUCKET or s3:BUCKET, not {text}"
        ));
    };
    let (access_key_id, secret_access_key) = if prefix.is_empty() {
        (None, None)
    } else {
        match (var("AWS_ACCESS_KEY_ID"), var("AWS_SECRET_ACCESS_KEY")) {
            (Some(id), Some(secret)) => (Some(id), Some(secret)),
            (None, None) => (None, None),
            _ => {
                return Err(format!(
                    "{prefix}AWS_ACCESS_KEY_ID and {prefix}AWS_SECRET_ACCESS_KEY go together"
                ));
            }
        }
    };
    Ok(ObjectStoreConfig::S3(S3Config {
        bucket: bucket.to_owned(),
        region: var("S3_REGION").unwrap_or_else(|| "us-east-1".to_owned()),
        endpoint: var("S3_ENDPOINT"),
        access_key_id,
        secret_access_key,
        allow_http: false,
    }))
}

pub const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;

/// The classes that are compacted. `1d` isn't one: it is served from `sealed/` (section 7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RetentionClass {
    Days7,
    Days30,
    Days365,
}

impl RetentionClass {
    pub const ALL: [RetentionClass; 3] = [
        RetentionClass::Days7,
        RetentionClass::Days30,
        RetentionClass::Days365,
    ];

    pub fn name(self) -> &'static str {
        match self {
            RetentionClass::Days7 => "7d",
            RetentionClass::Days30 => "30d",
            RetentionClass::Days365 => "365d",
        }
    }

    /// Where its compacted objects go (section 7.3).
    pub fn prefix(self) -> &'static str {
        match self {
            RetentionClass::Days7 => "compacted/7d",
            RetentionClass::Days30 => "compacted/30d",
            RetentionClass::Days365 => "compacted/365d",
        }
    }

    pub fn length_ms(self) -> u64 {
        match self {
            RetentionClass::Days7 => 7 * DAY_MS,
            RetentionClass::Days30 => 30 * DAY_MS,
            RetentionClass::Days365 => 365 * DAY_MS,
        }
    }

    /// The named cursor of this class's worker. Clients can't use these names.
    pub fn cursor_name(self) -> String {
        format!("compact:{}", self.name())
    }

    /// Its code in an object table row: 1 to 3, with 0 for sealed objects.
    pub fn code(self) -> u8 {
        self as u8 + 1
    }

    pub fn of_code(code: u8) -> Option<RetentionClass> {
        RetentionClass::ALL
            .get(usize::from(code).checked_sub(1)?)
            .copied()
    }

    /// Its bit in a row's mask of the classes an object holds.
    pub fn bit(self) -> u8 {
        1 << (self as u8)
    }

    /// `None` for what stays in `sealed/`.
    pub fn of_retention(retention_ms: u64) -> Option<RetentionClass> {
        if retention_ms <= DAY_MS {
            None
        } else if retention_ms <= 7 * DAY_MS {
            Some(RetentionClass::Days7)
        } else if retention_ms <= 30 * DAY_MS {
            Some(RetentionClass::Days30)
        } else {
            Some(RetentionClass::Days365)
        }
    }
}

/// 2026-01-01T00:00:00Z, where an object id's time starts (section 7.3).
pub const OBJECT_EPOCH_MS: u64 = 1_767_225_600_000;

const TIME_BITS: u32 = 40;
const GENERATION_BITS: u32 = 16;
const INDEX_BITS: u32 = 8;

/// The highest generation an object id holds. A node past it can't name its objects.
pub const MAX_GENERATION: u64 = (1 << GENERATION_BITS) - 1;

/// An object's id: a time in ms since `OBJECT_EPOCH_MS` (40 bits), the generation (16) and an
/// index (8) (section 7.3). Its path is computed from it, and printed as 16 hex digits, so that
/// names sort as ids do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

impl ObjectId {
    /// `time_ms` is Unix time in ms. One before the epoch, which only a test's clock gives,
    /// counts as the epoch.
    pub fn new(time_ms: u64, generation: u64, index: u8) -> ObjectId {
        assert!(
            generation <= MAX_GENERATION,
            "generation {generation} has no room in an id"
        );
        let time = time_ms.saturating_sub(OBJECT_EPOCH_MS);
        assert!(
            time < 1 << TIME_BITS,
            "{time_ms} ms is past an object id's time"
        );
        ObjectId(
            time << (GENERATION_BITS + INDEX_BITS) | generation << INDEX_BITS | u64::from(index),
        )
    }

    /// Unix time in ms.
    pub fn time_ms(self) -> u64 {
        OBJECT_EPOCH_MS + (self.0 >> (GENERATION_BITS + INDEX_BITS))
    }

    pub fn generation(self) -> u64 {
        (self.0 >> INDEX_BITS) & MAX_GENERATION
    }

    pub fn index(self) -> u8 {
        self.0 as u8
    }

    /// Hours since the Unix epoch, the hour in its path.
    pub fn hour(self) -> u64 {
        self.time_ms() / HOUR_MS
    }

    /// The id a compacted object of `hour` takes: the hour's last ms (section 7.3).
    pub fn compacted(hour: u64, generation: u64, index: u8) -> ObjectId {
        ObjectId::new((hour + 1) * HOUR_MS - 1, generation, index)
    }

    /// Whether it is a compacted object's: only those take an hour's last ms.
    pub fn is_compacted(self) -> bool {
        self.time_ms() % HOUR_MS == HOUR_MS - 1
    }

    /// The id in a path this module made.
    pub fn of_path(path: &Path) -> Option<ObjectId> {
        let name = path.filename()?.strip_suffix(".seg")?;
        if name.len() != 16 {
            return None;
        }
        u64::from_str_radix(name, 16).ok().map(ObjectId)
    }
}

/// The time a sealed object takes for `time_ms`: the next ms when it is an hour's last, which
/// only compacted objects take (section 7.3).
pub fn sealed_time_ms(time_ms: u64) -> u64 {
    if time_ms % HOUR_MS == HOUR_MS - 1 {
        time_ms + 1
    } else {
        time_ms
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Where new objects go (section 7.4).
pub const SEALED_PREFIX: &str = "sealed";

/// `<root>/<hour>/<id in hex>.seg`, with `root` `sealed` or a class's `compacted/<class>`
/// (section 7.3). The hour comes from the id, so nothing else is in the name.
pub fn object_path(root: &str, id: ObjectId) -> Path {
    Path::from(format!("{root}/{}/{id}.seg", id.hour()))
}

/// A sealed object's path.
pub fn sealed_path(id: ObjectId) -> Path {
    object_path(SEALED_PREFIX, id)
}

/// The generation in an object's name (section 7.3).
pub fn object_generation(path: &Path) -> Option<u64> {
    ObjectId::of_path(path).map(ObjectId::generation)
}

/// The ingest hour in an object's name (D53).
pub fn object_hour(path: &Path) -> Option<u64> {
    ObjectId::of_path(path).map(ObjectId::hour)
}

/// The hours under a prefix such as `sealed/`, oldest first.
pub async fn list_hours(store: &dyn ObjectStore, root: &str) -> object_store::Result<Vec<u64>> {
    let listed = store.list_with_delimiter(Some(&Path::from(root))).await?;
    let mut hours: Vec<u64> = listed
        .common_prefixes
        .iter()
        .filter_map(|prefix| prefix.filename()?.parse().ok())
        .collect();
    hours.sort_unstable();
    Ok(hours)
}

/// Every object of one hour under a prefix.
pub async fn list_hour_objects(
    store: &dyn ObjectStore,
    root: &str,
    hour: u64,
) -> object_store::Result<Vec<object_store::ObjectMeta>> {
    let prefix = Path::from(format!("{root}/{hour}"));
    Ok(store.list_with_delimiter(Some(&prefix)).await?.objects)
}

/// Every object under `sealed/`, listing hour by hour.
pub async fn list_sealed_objects(
    store: &dyn ObjectStore,
) -> object_store::Result<Vec<object_store::ObjectMeta>> {
    let mut out = Vec::new();
    for hour in list_hours(store, SEALED_PREFIX).await? {
        out.extend(list_hour_objects(store, SEALED_PREFIX, hour).await?);
    }
    Ok(out)
}
