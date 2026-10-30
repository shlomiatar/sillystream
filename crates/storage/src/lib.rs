//! WAL, segments, object store, uploader, metadata store and local segment cache
//! (SPEC sections 6.3, 6.4, 7.3, 7.6). No compression codec may enter this crate.

mod cache;
mod metadata;
mod object;
mod segment_builder;
mod segment_format;
mod segment_reader;
mod uploader;
mod wal_format;
mod wal_replay;
mod wal_writer;

pub use cache::{CacheError, SegmentCache};
pub use metadata::{MetadataEntry, MetadataError, MetadataStore};
pub use object::{
    MAX_PATH_PART_BYTES, ObjectStoreConfig, S3Config, build_store, dedicated_path, seal_path,
    stream_fits_dedicated_path,
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
    UploaderConfig, UploaderExit, UploaderSender,
};
pub use wal_format::{
    WAL_FILE_HEADER_BYTES, WAL_MAGIC, WalEntry, WalEntryError, encode_file_header, encode_wal_entry,
};
pub use wal_replay::{
    ReplayedEntry, ReplayedFile, WalFileInfo, list_wal_files, replay_bytes, replay_dir,
    replay_file, wal_file_name,
};
pub use wal_writer::{
    DirVolume, DurableNotify, DurableWatch, WalConfig, WalError, WalFile, WalHandle, WalRotated,
    WalSendError, WalSender, WalStats, WalVolume, durable_watches, start_wal_writer,
};
