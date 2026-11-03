//! The key hashes a node makes (SPEC section 1). Stream ids and cursor names are hashed where
//! they arrive, and the node's own names here, so nothing past this keeps a name.

use format::{IdentityKey, KeyHash};

/// The stream the seal manifests go in (section 7.5).
pub const MANIFEST_STREAM: &str = "_sys.manifests";

#[derive(Debug, Clone)]
pub struct Keys {
    identity: IdentityKey,
    /// `MANIFEST_STREAM`'s key hash.
    pub manifest: KeyHash,
}

impl Keys {
    pub fn new(identity: IdentityKey) -> Keys {
        let manifest = identity.hash(MANIFEST_STREAM);
        Keys { identity, manifest }
    }

    /// The key hash of a stream id, or of a cursor name.
    pub fn hash(&self, name: &str) -> KeyHash {
        self.identity.hash(name)
    }

    /// The named cursor of a sink (section 9).
    pub fn sink_cursor(&self, sink_id: u64) -> KeyHash {
        self.hash(&format!("sink:{sink_id}"))
    }
}
