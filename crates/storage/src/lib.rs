//! WAL, segments, object store, uploader, metadata store and local segment cache
//! (SPEC sections 6.3, 6.4, 7.3, 7.6). No compression codec may enter this crate.

mod cache;
mod checkpoint;
mod crash;
mod meta_format;
mod metadata;
mod object;
mod object_table;
mod ownership;
mod segment_builder;
mod segment_format;
mod segment_reader;
mod uploader;
mod wal_format;
mod wal_replay;
mod wal_writer;

pub use cache::{
    CacheError, FillReceiver, FillSender, FillWriter, MappedSegments, PendingWriter, SegmentCache,
    fill_channel, list_pending, upload_pending,
};
pub use checkpoint::{
    BuiltCheckpoint, CheckpointError, CheckpointFile, CheckpointHead, CheckpointSet, NewerObjects,
    build_checkpoint, checkpoint_path, put_checkpoint, put_checkpoint_payload, retry_delay,
};
pub use crash::crash_point;
#[cfg(feature = "test-hooks")]
pub use crash::hold_wal_writes;
pub use meta_format::{
    META_VERSION, MetaCursor, MetaError, MetaSink, MetaSinkGap, StreamMeta, decode_ids,
    decode_meta, decode_sink, encode_ids, encode_meta, encode_sink,
};
pub use metadata::{MAX_METADATA_KEY_BYTES, MetadataEntry, MetadataError, MetadataStore};
pub use object::{
    HOUR_MS, MAX_GENERATION, OBJECT_EPOCH_MS, ObjectId, ObjectStoreConfig, RetentionClass,
    S3Config, SEALED_PREFIX, build_store, check_conditional_create, list_hour_objects, list_hours,
    list_sealed_objects, object_generation, object_hour, object_path, parse_store_config,
    sealed_path, sealed_time_ms,
};
pub use object_table::{
    ObjectKey, ObjectRow, ObjectState, ObjectTable, ROW_KEY_BYTES, Replacement, decode_rows,
    encode_rows,
};
pub use ownership::{
    OWNERSHIP_MAGIC, OWNERSHIP_RECORD_BYTES, Ownership, OwnershipError, claim_generation,
    decode_ownership, encode_ownership, ownership_path, read_ownership,
};
pub use segment_builder::{BuiltSegment, SegmentBuilder};
pub use segment_format::{
    BLOCK_ENTRY_BYTES, BlockEntry, BlockTable, DEFAULT_BLOCK_BYTES, DEFAULT_PAGE_BYTES, IndexEntry,
    SEGMENT_MAGIC, SegmentError, StreamExtent, TRAILER_BYTES, TopIndex, decode_block_table,
    decode_last_ids, decode_top_index, stream_hash,
};
pub use segment_reader::{SEGMENT_TAIL_READ_BYTES, SegmentReader};
pub use uploader::{
    Handoff, SealExtent, UnfinishedUpload, UploadEvent, UploadKey, UploadSealer, UploadSendError,
    Uploader, UploaderConfig, UploaderExit, UploaderSender, put_compacted, put_object,
};
pub use wal_format::{
    DedupeCarry, WAL_FILE_HEADER_BYTES, WAL_MAGIC, WalEntry, WalEntryError, encode_file_header,
    encode_wal_entry,
};
pub use wal_replay::{
    ReplayedEntry, ReplayedFile, WalFileInfo, list_wal_files, replay_bytes, replay_dir,
    replay_file, replay_files, sync_wal_files, wal_file_name,
};
pub use wal_writer::{
    DirVolume, DurableNotify, DurableWatch, Sealer, WalConfig, WalError, WalFile, WalHandle,
    WalRotated, WalSendError, WalSender, WalStats, WalVolume, durable_watches, start_wal_writer,
};
