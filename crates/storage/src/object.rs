//! Object store access and paths (SPEC section 7.3). Everything is tested on
//! `LocalFileSystem` and `InMemory`. Native GCS has been run against a real bucket, by hand. Real
//! S3 exists only behind configuration here, and has never been exercised.

use std::path::PathBuf;
use std::sync::Arc;

use format::{KeyHash, id_ms};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::{Path, PathPart};
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

const HOUR_MS: u64 = 3_600_000;

/// `sealed/<ingest_hour>/<node-or-stream>/<generation>-<first_id>.seg` (section 7.3).
/// `ingest_hour` is hours since the epoch, from the object's highest id (D175), so nothing in
/// it is newer than its hour and the delete job can go by the name. `PathPart` encodes anything
/// in a stream id that isn't safe in a path.
pub fn sealed_path(owner: &str, generation: u64, first_id: u64, last_id: u64) -> Path {
    let hour = (id_ms(last_id) / HOUR_MS).to_string();
    let name = format!("{generation:020}-{first_id:020}.seg");
    Path::from_iter([
        PathPart::from("sealed"),
        PathPart::from(hour.as_str()),
        PathPart::from(owner),
        PathPart::from(name.as_str()),
    ])
}

/// The longest path part any store here takes, as encoded in the path. It is the file
/// name limit of common local file systems. With a part this long, a whole key stays
/// well under S3's 1,024 bytes, so one limit gives the same result on every store (D59).
pub const MAX_PATH_PART_BYTES: usize = 255;

/// Whether a path has a part no store could hold, which no retry can change.
pub(crate) fn path_fits_stores(path: &Path) -> bool {
    path.parts()
        .all(|part| part.as_ref().len() <= MAX_PATH_PART_BYTES)
}

/// The path of a node's shared object for one seal whose records are all in `first_id`'s hour.
pub fn seal_path(node: &str, generation: u64, first_id: u64) -> Path {
    sealed_path(node, generation, first_id, first_id)
}

/// The path of a dedicated object whose records are all in `first_id`'s hour.
pub fn dedicated_path(stream: KeyHash, generation: u64, first_id: u64) -> Path {
    sealed_path(&stream.to_string(), generation, first_id, first_id)
}

/// The generation in an object's name (section 7.3).
pub fn object_generation(path: &Path) -> Option<u64> {
    let name = path.filename()?.strip_suffix(".seg")?;
    name.split_once('-')?.0.parse().ok()
}

/// The prefix a compacted object goes in, or `sealed` for new ones (section 7.4).
pub const SEALED_PREFIX: &str = "sealed";

/// `<class>/<ingest_hour>/<node>/<generation>-<first_id>.seg`, the same form as `sealed/`. The
/// hour is the sources', so a prefix holds what expires together.
pub fn compacted_path(class: &str, hour: u64, node: &str, generation: u64, first_id: u64) -> Path {
    let hour = hour.to_string();
    let name = format!("{generation:020}-{first_id:020}.seg");
    Path::from_iter([
        PathPart::from(class),
        PathPart::from(hour.as_str()),
        PathPart::from(node),
        PathPart::from(name.as_str()),
    ])
}

/// The ingest hour in an object's path (D53).
pub fn object_hour(path: &Path) -> Option<u64> {
    path.parts().nth(1)?.as_ref().parse().ok()
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
    let prefix = Path::from_iter([root, hour.to_string().as_str()]);
    let owners = store.list_with_delimiter(Some(&prefix)).await?;
    let mut out = owners.objects;
    for owner in owners.common_prefixes {
        out.extend(store.list_with_delimiter(Some(&owner)).await?.objects);
    }
    Ok(out)
}

/// Every object under `sealed/`, listing hour by hour and owner by owner.
pub async fn list_sealed_objects(
    store: &dyn ObjectStore,
) -> object_store::Result<Vec<object_store::ObjectMeta>> {
    let mut out = Vec::new();
    let hours = store
        .list_with_delimiter(Some(&Path::from("sealed")))
        .await?;
    for hour in hours.common_prefixes {
        let owners = store.list_with_delimiter(Some(&hour)).await?;
        for owner in owners.common_prefixes {
            out.extend(store.list_with_delimiter(Some(&owner)).await?.objects);
        }
    }
    Ok(out)
}
