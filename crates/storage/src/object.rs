//! Object store access and paths (SPEC section 7.3). Everything is tested on
//! `LocalFileSystem` and `InMemory`. Real S3 exists only behind configuration here,
//! and has never been exercised.

use std::path::PathBuf;
use std::sync::Arc;

use format::id_ms;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::{Path, PathPart};

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
    Local { root: PathBuf },
    S3(S3Config),
}

/// Builds the client. This never connects: the first request does.
pub fn build_store(config: &ObjectStoreConfig) -> object_store::Result<Arc<dyn ObjectStore>> {
    match config {
        ObjectStoreConfig::Memory => Ok(Arc::new(InMemory::new())),
        ObjectStoreConfig::Local { root } => Ok(Arc::new(LocalFileSystem::new_with_prefix(root)?)),
        ObjectStoreConfig::S3(s3) => {
            // Conditional create is how seals and ownership records refuse to overwrite.
            let mut builder = AmazonS3Builder::from_env()
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
    }
}

const HOUR_MS: u64 = 3_600_000;

/// `sealed/<ingest_hour>/<node-or-stream>/<generation>-<first_id>.seg` (section 7.3).
/// `ingest_hour` is hours since the epoch, from `first_id` (D53). `PathPart` encodes
/// anything in a stream id that isn't safe in a path.
fn sealed_path(owner: &str, generation: u64, first_id: u64) -> Path {
    let hour = (id_ms(first_id) / HOUR_MS).to_string();
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

/// Whether a stream can have a dedicated object. A stream whose id can't be a path part
/// stays in the node's shared seals, where the id appears only in the footer (D59).
pub fn stream_fits_dedicated_path(stream: &str) -> bool {
    PathPart::from(stream).as_ref().len() <= MAX_PATH_PART_BYTES
}

/// The path of a node's shared object for one seal.
pub fn seal_path(node: &str, generation: u64, first_id: u64) -> Path {
    sealed_path(node, generation, first_id)
}

/// The path of a dedicated object for one busy stream.
pub fn dedicated_path(stream: &str, generation: u64, first_id: u64) -> Path {
    sealed_path(stream, generation, first_id)
}
