# Deploying a node

The first version runs on one node (SPEC section 8). This covers what the operator has to decide and watch. Real S3, R2 and EBS have not been exercised by anything in this repository: every test uses `LocalFileSystem`, `InMemory` or a local stub.

## Mount EBS at `wal_dir`

`--wal-dir` is just a path to the server. What you mount there decides what a level 2 ack guarantees (section 7.1). The server can't tell and doesn't check.

| mounted at `--wal-dir` | ack latency (estimate) | process or kernel crash | machine loss | AZ loss |
|---|---|---|---|---|
| EBS | 1–4 ms | safe | safe | up to one seal window lost |
| local NVMe | 0.1–2 ms | safe | up to one seal window lost | up to one seal window lost |

**Mount EBS at `--wal-dir`. An acked record survives machine loss only if it is on EBS.** A path on the root disk or on instance storage gives up that guarantee with no warning, because a machine that has gone can't warn.

`--data-dir` holds only caches and the local metadata store. It can be local NVMe, and losing it is safe: everything in it is rebuilt from the object store.

## Seal window

`--seal-window-ms`, 1000 to 120000, 10000 by default. A shorter window loses less on AZ loss and makes level 3 acks faster, but costs more PUTs: about 2.6M a month per node at 1 s, 260K at 10 s and 22K at 120 s.

## Starting and replacing a node

```
sillystream-server --listen 0.0.0.0:443 --wal-dir /mnt/ebs/wal --data-dir /mnt/nvme/data \
    --cert cert.pem --key key.pem --token-secret-file secret \
    --object-store s3:my-bucket
```

- **Attach before you start.** Have the supervisor attach and mount the WAL volume before it starts the process. A start that finds `--wal-dir` empty while the ownership record already has a generation logs that the WAL was lost, and takes a new generation with the last valid id from the checkpoints. Acked entries above that id are then fenced: they stay unreadable even after the volume comes back (BUILD_NOTES D228, Q43). The server warns on each later start that finds such entries and keeps their files. There is no tool yet that restores them. Don't delete those files if you may want them back.
- **Machine loss.** Something external, such as an auto-scaling group, starts the replacement. With EBS, it reattaches the volume; force-detaching is what stops the old machine's WAL writes (section 8.1).
- **Clients** resume from their cursors, and producers retry with the same `(producer_id, producer_seq)`.

## What to watch

The process logs to stderr. Have your supervisor alert on these. There is no health route yet.

- `the WAL writer stopped`: the WAL writer has hit an error it won't recover from, such as a full disk partway through a file (D79, D225, D233). **The process stays up and keeps serving reads, so a process-level health check still passes**, but all writes are refused with code 4 for good, and level 1 acked records are only in memory. Fix the disk and restart the process, which replays the WAL. Whether the server should exit by itself is open (BUILD_NOTES Q42).
- `the WAL was lost` and the fenced-entries warning: see above.

## Hot streams

`sillystream stream core NAME --core N`, or `PUT /v1/stream/NAME/core` with `{"core": N}` and an `admin` token, gives a very hot stream its own shard on core N (section 6.1). Up to 64 streams can have one, and asking again moves the shard. The table is in memory: after a restart the stream goes back to its hashed shard, and you set it again.

## Object stores

- `--object-store s3:BUCKET` reads `S3_REGION`, `S3_ENDPOINT` and the usual AWS key variables.
- `--sink-archive-store s3:BUCKET` reads `SINK_ARCHIVE_S3_REGION`, `SINK_ARCHIVE_S3_ENDPOINT`, `SINK_ARCHIVE_AWS_ACCESS_KEY_ID` and `SINK_ARCHIVE_AWS_SECRET_ACCESS_KEY`, so an archive to R2 can sit beside a node on AWS. With no archive keys it uses the AWS variables.
- Seals and the ownership record depend on conditional puts (`ETagMatch`). That this works on R2 is unverified, so test it before relying on it. An archive sink also writes with a conditional create.

## Memory

`--memory-cap-bytes` (4 GiB by default) applies per core. Publishers wait while a core's slabs reach the cap, and slabs are freed once their records are uploaded.
