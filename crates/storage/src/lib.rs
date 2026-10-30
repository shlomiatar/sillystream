//! WAL, segments, object store, uploader, metadata store and local segment cache
//! (SPEC sections 6.3, 6.4, 7.3, 7.6). No compression codec may enter this crate.

mod wal_format;
mod wal_replay;
mod wal_writer;

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
