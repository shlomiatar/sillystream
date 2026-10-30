# Decisions

Each entry: what we chose, what we rejected, and why. Reopen only with a new reason.

| # | decision | rejected | why |
|---|---|---|---|
| 1 | One owner per stream assigns ids | consensus per write | total order without consensus on the hot path |
| 2 | 64-bit `options` with masked-equality filters | 256-bit tag bitmap | covers both flags and value fields; header fits 32 B logically |
| 3 | No `target` field | per-message recipients | access is all-or-nothing per stream; private data goes to its own stream |
| 4 | `author` is a keyed 64-bit hash | 16-bit slots, 32-bit hashes | no slot mapping; collisions negligible at 64 bits |
| 5 | Server never reads payloads | server-side compression, parsing, payload filters | producers can encrypt later with no server change |
| 6 | State is plaintext CBOR the server can read | encrypted state | enables merge patches, validation, server compression, `_sys` keys |
| 7 | Baseline = one message with its own compressed payload; batches are only an optimization | frame/lane/sorted block compression as the default | server never decompresses; block codec stays a later option |
| 8 | Columnar batch: shared fields in a 64 B header, per-message `options`, `len`, `event_delta` | 32 B header per message | half the per-message size; O(1) stamping; SIMD filter scan |
| 9 | Delivery as batches (whole or SPARSE) | per-message frames | zero-copy for unfiltered subscribers |
| 10 | No server-side wildcards | directory streams, prefix watches | groups, fan-in forward sinks and registry streams cover the use cases with far less server logic |
| 11 | Groups are an SDK convention using jump hash | server-managed partitions, `hash % N` | small server; jump hash moves the fewest producers for any resize |
| 12 | Shared sorted segments + dedicated objects for busy streams | one object per stream | S3 bills per request; small files kill read latency |
| 13 | Seal by size or age, 30–60 s for WAL-acked, 1–5 s for S3-acked | fixed 1 s | fewer objects; the window equals ack latency only for S3-acked streams |
| 14 | WAL on EBS (AWS) / regional PD (GCP) | cross-AZ peer replication by default | no cross-AZ transfer cost, no replication protocol; slower failover accepted |
| 15 | Local NVMe parked tier; hibernate to S3 only under disk pressure | aggressive S3 hibernation | hibernation churn can cost tens of thousands of dollars a month |
| 16 | Checkpoints in a separate keyed store | checkpoints inside expiring segments | state must outlive message retention |
| 17 | Retention classes per stream, one segment stream per class | per-stream lifecycle rules | lifecycle rules are per prefix and limited in number |
| 18 | Compaction only for classes > 1 day, run as a consumer group on `_sys/seals` | always-on Parquet/L1/L2 tiers | 1-day data isn't worth rewriting; reuses cursors and redelivery |
| 19 | Parquet/Iceberg is an export sink, not core storage | Parquet as the storage tier | payloads are opaque; analytics is opt-in |
| 20 | One sink runtime (cursor + destination plugin + policy) | separate systems for webhooks, forwarding, export | one mechanism for delivery, retries, pausing and status |
| 21 | Plain files for WAL and segments; fjall/RocksDB for keyed metadata only | RocksDB for segments | double logging, write amplification, and segment bytes must equal wire bytes |
| 22 | Config in `_sys` as template reference + overrides | full config copied into every stream | ~30–60 B per stream; waking needs no extra reads |
| 23 | WebSocket primary, SSE read-only fallback | SSE primary | SSE is text-only and one-directional |
| 24 | No WebSocket-level compression | permessage-deflate | per-connection compression contexts cost more memory than the connection |
| 25 | TLS 1.3 resumption with cluster-shared ticket keys, ECDSA certs, 0-RTT only for idempotent frames | full handshakes only; RSA | cheap reconnect storms after failover |
| 26 | Webhook HTTP client: HTTP/1.1 + HTTP/2, ticket cache, no HTTP/3 | HTTP/3 client | few receivers support it; it doesn't reduce TLS work |
| 27 | Server-side HTTP/3 / WebTransport later, optional, for mobile/browser | HTTP/3 to save CPU | QUIC costs more CPU per byte than TCP + kTLS |
| 28 | System entries marked by the batch `SYSTEM` flag | a reserved `options` bit | keeps all 64 option bits for applications |
| 29 | Name: Sillystream; the unit is a stream | Tether, Beck, Weir, Silly String | Silly String is an existing trademark |

## Open questions

- Separate permission for state writes (read / publish / state-write)?
- Should some state history outlive message retention?
- Per-org limits: streams, subscriptions per connection, publish rate.
- Is a single 64-bit `options` enough, or do some profiles need a second metadata word?
- Which export sink ships first after v1, and who runs the client-side export worker?
- Explicit vs implicit stream creation as the default.
