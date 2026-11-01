//! What section 6.3's preallocation would save on the fsync path: one append and `sync_data` into
//! a sparse file (D51, D60), against one whose blocks were written and synced first. This measures
//! whatever file system the temp dir is on, not EBS.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use criterion::{Criterion, criterion_group, criterion_main};

const FILE_BYTES: u64 = 256 * 1024 * 1024;
const APPEND_BYTES: usize = 4096;

type Maker = fn(&std::path::Path) -> File;

fn open(path: &std::path::Path) -> File {
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}

fn sparse(path: &std::path::Path) -> File {
    let file = open(path);
    file.set_len(FILE_BYTES).unwrap();
    file
}

fn written(path: &std::path::Path) -> File {
    let mut file = open(path);
    let zeros = vec![0u8; 1024 * 1024];
    for _ in 0..FILE_BYTES / zeros.len() as u64 {
        file.write_all(&zeros).unwrap();
    }
    file.sync_all().unwrap();
    file
}

fn append_and_sync(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let bytes = vec![0x5Au8; APPEND_BYTES];
    let makers: [(&str, Maker); 2] = [("sparse", sparse), ("blocks_written_first", written)];
    let mut group = c.benchmark_group("wal_append_and_sync_data_4k");
    group.sample_size(50);
    for (name, make) in makers {
        let mut file = make(&path);
        let mut offset = 0u64;
        group.bench_function(name, |b| {
            b.iter(|| {
                if offset + APPEND_BYTES as u64 > FILE_BYTES {
                    offset = 0;
                    file = make(&path);
                }
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.write_all(&bytes).unwrap();
                file.sync_data().unwrap();
                offset += APPEND_BYTES as u64;
            });
        });
    }
    group.finish();
}

criterion_group!(benches, append_and_sync);
criterion_main!(benches);
