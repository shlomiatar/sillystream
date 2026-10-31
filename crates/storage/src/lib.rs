//! WAL, segments, object store, uploader, metadata store and local segment cache
//! (SPEC sections 6.3, 6.4, 7.3, 7.6). No compression codec may enter this crate.

mod cache;
mod checkpoint;
mod crash;
mod meta_format;
mod metadata;
mod object;
mod ownership;
mod segment_builder;
mod segment_format;
mod segment_reader;
mod uploader;
mod wal_format;
mod wal_replay;
mod wal_writer;

pub use cache::{CacheError, SegmentCache};
pub use checkpoint::{
    BuiltCheckpoint, CheckpointError, CheckpointFile, CheckpointHead, CheckpointSet,
    build_checkpoint, checkpoint_path, merge_meta, put_checkpoint, put_checkpoint_payload,
    retry_delay,
};
pub use crash::crash_point;
#[cfg(feature = "test-hooks")]
pub use crash::hold_wal_writes;
pub use meta_format::{
    MetaCursor, MetaDedupe, MetaError, MetaExtent, MetaSink, MetaSinkGap, StreamMeta,
    decode_extent, decode_meta, encode_extent, encode_meta,
};
pub use metadata::{MAX_METADATA_KEY_BYTES, MetadataEntry, MetadataError, MetadataStore};
pub use object::{
    MAX_PATH_PART_BYTES, ObjectStoreConfig, S3Config, SEALED_PREFIX, build_store, compacted_path,
    dedicated_path, list_hour_objects, list_hours, list_sealed_objects, object_generation,
    object_hour, seal_path, sealed_path, stream_fits_dedicated_path,
};
pub use ownership::{
    OWNERSHIP_MAGIC, OWNERSHIP_RECORD_BYTES, Ownership, OwnershipError, claim_generation,
    decode_ownership, encode_ownership, ownership_path, read_ownership,
};
pub use segment_builder::{BuiltSegment, SegmentBuilder};
pub use segment_format::{
    BLOCK_ENTRY_BYTES, BlockEntry, BlockTable, DEFAULT_BLOCK_BYTES, DEFAULT_PAGE_BYTES,
    SEGMENT_MAGIC, SegmentError, StreamExtent, TRAILER_BYTES, TopIndex, decode_block_table,
    decode_page, decode_top_index, stream_hash,
};
pub use segment_reader::{SEGMENT_TAIL_READ_BYTES, SegmentReader};
pub use uploader::{
    Section, SectionExtent, UnfinishedUpload, UploadEvent, UploadKey, UploadSendError, Uploader,
    UploaderConfig, UploaderExit, UploaderSender, put_new_object,
};
pub use wal_format::{
    WAL_FILE_HEADER_BYTES, WAL_MAGIC, WalEntry, WalEntryError, encode_file_header, encode_wal_entry,
};
pub use wal_replay::{
    ReplayedEntry, ReplayedFile, WalFileInfo, list_wal_files, replay_bytes, replay_dir,
    replay_file, sync_wal_files, wal_file_name,
};
pub use wal_writer::{
    DirVolume, DurableNotify, DurableWatch, WalConfig, WalError, WalFile, WalHandle, WalRotated,
    WalSendError, WalSender, WalStats, WalVolume, durable_watches, start_wal_writer,
};
