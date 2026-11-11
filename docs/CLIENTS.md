# Sillystream client libraries

Status: agreed with the user on 2026-11-11. `docs/SPEC.md` is the source of truth, and this document only describes clients built on it. Where something here would need a SPEC change, it says so, and nothing changes until the user agrees.

Three clients share one behaviour and differ only in API shape:

- **Rust** (`crates/sdk`, exists): the reference. Other clients are checked against it.
- **JavaScript/TypeScript** for Bun and Node: pure JS, no native addons.
- **Go**: pure Go, no cgo.

## 1. Decisions so far

| topic | decision |
|---|---|
| order | Rust gains every feature first and produces the test vectors, then JS/TS, then Go |
| dictionaries | zstd's own frame header carries the dictionary id, so codec stays 1 and the SPEC doesn't change |
| encryption | a new codec value, 2, for an encrypted envelope that holds the inner codec. **Needs a one-line SPEC change** (section 2's codec list) |
| cipher | AES-256-GCM, random 96-bit nonce per record |
| AAD | `options` and the envelope header, not the stream id, so forward sinks keep working |
| dictionaries and keys | the app supplies both. The SDK takes a map of dictionary id to bytes and a keyring. Distribution is the app's job |
| publishing | two layers: the explicit batch (`add`/`send`/`flush`) and an auto-batching `Producer` on top |
| commits | manual `commit(id)`, plus a handler helper that commits after each batch it handled |
| JS transports | WebSocket and HTTP in one package. HTTP covers publish, read, state and cursors, with no live tailing |
| JS zstd | runtime built-ins only (Bun, Node ≥ 22.15). Dictionaries work where the built-in supports them |
| Go dependencies | `klauspost/compress/zstd`, `coder/websocket`, crypto from the standard library |
| tokens | clients carry a token or a token provider and renew before expiry. Minting stays in `silly token issue` |
| TLS | system roots everywhere. Rust and Go take an optional extra CA, and JS uses `NODE_EXTRA_CA_CERTS` |
| location | `clients/js` and `clients/go` in this repo, so one CI runs the shared vectors and server scenario against all three |

## 2. Record data

The runtime never looks inside data (SPEC section 2). Everything in this section happens in the client, and all three clients produce and accept the same bytes.

### 2.1 Layers

Writing goes top to bottom and reading bottom to top:

1. **Items.** With `count = 1`, the item's bytes. With `count > 1`, `count` little-endian `u32` end offsets, then the item bytes (D117).
2. **Compression.** zstd over all of layer 1, with or without a dictionary. Kept only if smaller, and skipped below 64 bytes. Codec 1 if compressed, 0 if not.
3. **Encryption** (optional). AES-256-GCM over the output of layer 2. Codec 2, with the layer 2 codec stored inside the envelope.

Compression always comes before encryption, because ciphertext doesn't compress.

### 2.2 Codec values

| codec | data |
|---|---|
| 0 | layer 1, raw |
| 1 | one zstd frame of layer 1, with a dictionary id in the frame header if a dictionary was used |
| 2 | an encrypted envelope (2.4) whose plaintext is codec 0 or 1 data |

Any other value is a record this client can't open: it is reported as a bad record (section 6.3) and the subscription carries on.

### 2.3 Dictionaries

- A dictionary is a standard formatted zstd dictionary (magic `0xEC30A437`), whose header holds its own 32-bit id. A writer uses that id, and zstd writes it into each frame's header.
- A reader reads the dictionary id from the frame header (`Dictionary_ID` field, 0 or absent = none) and looks it up in the dictionaries it was given. A missing one makes that record a bad record with the reason `missing_dictionary`.
- Ids are chosen by the app. Ids below 32,768 and at or above 2^31 are reserved by zstd, so the SDKs refuse them.
- Training: `train_dictionary(samples, size, id)` in Rust (zstd's ZDICT) and Go (`klauspost/compress/dict`). JS has no trainer.
- JS uses the runtime's zstd. A runtime that can't load a dictionary can still read and write records without one, and reports dictionary records as `missing_dictionary`.
- Raw-content dictionaries (no header) aren't supported, because they carry no id.

### 2.4 Encrypted envelope (codec 2)

```
offset size field
0      4    key_id       u32, chosen by the app, never 0
4      2    inner_codec  u16, 0 or 1
6      2    reserved     u16, 0
8      12   nonce        random
20          ciphertext, then a 16-byte tag
```

- **Cipher:** AES-256-GCM with a 256-bit key. 36 bytes per record of overhead.
- **AAD:** `options u64 LE | bytes 0..8 of the envelope`. Moving ciphertext to other options, or changing its key id or inner codec, makes it fail to decrypt.
- **Not bound:** id and author, which the server stamps after encryption, and the stream id, so that a `forward` sink's copy still decrypts in the target stream. A record replayed into another stream that shares the key will decrypt there. Streams that must not mix use different keys.
- **Nonces:** random per record. AES-GCM with random nonces is safe up to about 2^32 records per key, so rotate well before that. The SDK counts what it encrypted under each key in this process and warns at 2^31.
- **Failure:** an unknown key id or a failed tag is a bad record with the reason `missing_key` or `decrypt_failed`. The subscription carries on.

### 2.5 Keys and rotation

A keyring is `{ current: key_id, keys: key_id → 32 bytes }`. Writers encrypt with `current`, and readers decrypt with any key in `keys`.

Rotation:

1. Add the new key to every reader's keyring.
2. Switch writers' `current` to it.
3. Drop the old key once nothing that needs it is left: the stream's retention class has passed, and any archive you still want to read has been re-encrypted.

The SDKs take a keyring and never store, fetch or derive keys. A keyring can be swapped at runtime (`set_keyring`), so an app can rotate without reconnecting.

### 2.6 What encryption leaves visible

Filtering still works because the header stays plaintext: `options`, `author`, `count`, `len` and the id (which holds the time). So:

- Put nothing secret in `options`. It is for routing and filtering only.
- Record sizes and counts are visible.
- State (SPEC section 10) is plaintext CBOR so the server can merge it, and isn't encrypted. Secrets don't belong there.

## 3. Shared behaviour

All three clients do the following the same way. Rust's behaviour today is the reference, and parts marked **new** aren't in Rust yet.

### 3.1 Connection

- One WebSocket per client, with `HELLO` and `WELCOME` (SPEC section 11.1). `max_frame` from `WELCOME` limits every frame sent.
- Reconnects with exponential backoff, 50 ms to 5 s by default. A connect attempt has one time limit from socket to `WELCOME`, the larger of the ping interval and 10 s.
- `PING` every 15 s, and a connection silent for two intervals is dropped.
- After a reconnect: unacked publishes are sent again in order with the same `(producer_id, producer_seq)`, subscriptions resume from their cursors, and the last commit of each named cursor is sent again.
- A state op unanswered when the connection drops fails with `Disconnected`, because it has no idempotency key. The caller decides.
- **TLS:** TLS 1.3, trusting the system's roots. Rust and Go also take an optional CA PEM, added to the roots, for tests and private deployments. JS has no CA option: on Node and Bun a private CA goes in `NODE_EXTRA_CA_CERTS`. **New for Rust**, which today trusts only the PEM it is given (D119, which this replaces).

### 3.2 Tokens

- Config takes a token, or a `token_provider` callback that returns one.
- **New:** the client reads `expiry_ms` and `author` from the token body without verifying it (the body is plain hex, `expiry_ms u64 | author u64 | grants`). With a provider, it asks for a fresh token at 80% of the token's lifetime, or 30 s before expiry if sooner, and sends it as a second `HELLO`.
- An error 1 on the connection asks the provider once more. If that token is refused too, everything fails with `Unauthorized` and the client stops.
- `client.author()` returns the author from the current token, which is what the server will stamp on this client's records.

### 3.3 Publishing

- **Batch** (the layer Rust has): `add(options, bytes)` holds items, and `send` sorts them stably by options, groups equal options into records of up to `record_target_bytes` (1 MiB by default), compresses, encrypts and splits them into publishes that fit `max_frame`. Each publish gets the next `producer_seq`. Anything that can never fit is refused before any of it goes.
- **Producer (new):** `publish(options, bytes)` returns that item's ack. Items collect until `linger_ms` (5 ms by default) or `max_batch_bytes` (1 MiB by default), then go as one batch. `flush()` sends what is held now.
- **Item ids:** ids follow the sorted order, and the SDK knows each item's position in it, so an item's ack carries `id = first_id + position`. When the server answers `first_id = 0` (an older retry that already committed, SPEC section 3), the item's id is unknown and is `None`/`null`/`0` by language.
- **Producer ids:** random and non-zero, with the top bit clear (forwards own it), unless the app sets one. An app that wants dedupe across its own restarts sets `producer_id` and `first_seq` and keeps them itself, together with what it hadn't seen acked.
- **Window:** `max_inflight_bytes` (64 MiB by default) of unacked publishes. `send` and `publish` wait past it.
- **Ack levels:** 1–3 per publisher, default 2 (SPEC section 7.2). Level 0 is retired: the server refuses it as malformed, so the SDKs don't offer it.
- **`expect_ver`:** 0 is unconditional, and `EXPECT_UNSET` (`u64::MAX`) succeeds only if the stream's state was never set (SPEC section 10). A publish carrying a state change with `expect_ver ≠ 0` holds back that producer's later publishes until it is answered (SPEC section 3). Other producers aren't held.
- **State in a publish:** `send_with_state(change)` attaches one state change to the first publish of the batch.

### 3.4 Subscribing

- **Spec:** stream, `from` (`start`, `live` or an id), optional `until`, filter, `linger_ms`, optional named cursor, `with_state`, `state_only`, `include_system`.
- **Cursor:** the client keeps the last id handed over or reported by `PROGRESS`. Items at or below it are skipped, inside a record or across a reconnect (SPEC section 5.2). `cursor()` returns it, for resuming elsewhere with a client-held cursor.
- **Catch-up and live:** one subscription does both. It starts wherever `from` says, the server reads history and then switches to live. `Subscribed { head }` tells the app where the head was, so it can tell catch-up from live by comparing ids.
- **`until`:** the client stops by itself once it has handed over or skipped past `until`, and the subscription ends cleanly.
- **Events:** `subscribed`, `items`, `progress`, `state`, `behind`, and errors.
- **State on a subscription (SPEC sections 10 and 11.1):** with `with_state`, `subscribed` carries the state as of `head`, and every later change comes as a `state` event holding the whole doc, after the items below its version and before any above it. So `state.ver` is never ahead of the items already handed over. Catching up gets no historic `state` events. After `behind`, reaching the head sends the current state once. With `state_only` there are no items and no `progress`, and it starts at the head. With `include_system` a change comes twice: as its system record, then as `state`. Clients must accept `state` events on any `with_state` subscription.
- **Back-pressure:** `max_queue_bytes` (64 MiB by default) of undelivered frames. Past it the client unsubscribes, and resubscribes from its cursor once the app has read half of it.
- **Decompression limit:** `max_decompressed_bytes` (256 MiB by default) per batch handed over. A frame over it comes in several batches, and one record over it is a bad record with the reason `too_large`.
- **Bad records:** a record that can't be opened (section 2) is reported as `BadRecord { first_id, last_id, reason }` after the items before it. Those ids are skipped and the subscription goes on.
- **Commits:** `commit(id)` moves a named cursor, forward only. **New:** `run(handler)` calls the handler with each batch of items and commits the batch's last id after the handler returns successfully. A handler that fails stops `run` with that error, uncommitted, so the batch comes again next time. `PROGRESS` ids are committed too, so a rare filter doesn't rescan.
- **Errors:** a server error ends the subscription, except error 8 for a bad commit.

### 3.5 Filters

- Up to 4 option clauses OR-ed together, and an optional author, AND-ed with them (SPEC section 4).
- One text form everywhere, the one `silly tail --filter` takes: `eq:MASK:VALUE`, `any:MASK` and `author:N`, separated by commas, with numbers in decimal or `0x` hex. Example: `eq:0xff00:0x0100,any:0x8000000000000000,author:7`.
- Builders too: `eq(mask, value)`, `any(mask)` and `author(n)`.
- Option layouts by name are not brought over (SPEC section 15). The app builds its own `options` values.

### 3.6 State

- `state(stream)` returns a handle with `get`, `set`, `merge` and `incr`, each with an optional `expect_ver`, which can be `EXPECT_UNSET` for the first claim on a stream (`If-None-Match: *` over HTTP). Writes return the new version, and a mismatch is a `VersionConflict` error with the current version when the server gives it.
- Docs are CBOR. Each SDK takes and returns its language's native values (Rust `serde`, Go `any` or a struct, JS plain objects) and also raw CBOR bytes.
- `get` uses a `STATE_ONLY` subscription over WebSocket, or `GET .../state` over HTTP. Watching changes is a `state_only` subscription, or `with_state` to get records and state in one order (section 3.4).
- **Lease helper:** none. A lease is a `set` with `expect_ver`, and the version is the fencing token. The docs show the pattern.

### 3.7 Admin

A small HTTP client for SPEC section 11.2's routes and the admin routes `silly` uses: stream create, config, info and delete, sinks, and cursor set. It has the same token and TLS config as the WebSocket client.

### 3.8 Errors

One set of error kinds everywhere: `Server { code, msg }` with SPEC section 11.1's codes, `VersionConflict { ver }`, `Disconnected`, `Closed`, `TooLarge`, `BadRecord { first_id, last_id, reason }`, `Connect`, and `Config`. `reason` is one of `undecodable`, `too_large`, `unknown_codec`, `missing_dictionary`, `missing_key` or `decrypt_failed`.

### 3.9 Numbers

Ids, options, authors, `producer_id` and `producer_seq` are all u64. JS uses `bigint` for every one of them, because ids are above 2^53.

## 4. API by language

These are sketches, to agree on shape. Names follow each language's habits.

### 4.1 Rust (`crates/sdk`)

The current API stays, with these additions:

```rust
let codecs = Codecs {
    zstd_level: Some(3),
    dictionaries: Dictionaries::from([dict]),   // for reading, and `write_dictionary` for writing
    write_dictionary: Some(dict.id()),
    keyring: Some(Keyring::new(7, [(7, key)])), // encrypts when set
};
let client = Client::connect(ClientConfig::new(url, token).ca_pem(pem).codecs(codecs)).await?;
let client = Client::connect(ClientConfig::with_provider(url, provider)).await?;

let producer = client.producer("logs.app", ProducerConfig { linger_ms: 5, ..Default::default() })?;
let ack: ItemAck = producer.publish(options, bytes).await?;   // { id: Option<u64>, seq }

let mut sub = client.subscribe(Subscribe::new("inbox.reviewer", FROM_START).filter("eq:0xff:1")?.named("worker"))?;
sub.run(|items| async move { handle(items).await }).await?;

let state = client.state("session.42");
let ver = state.merge(&doc, expect_ver).await?;
```

Performance work in the same step:

- Items borrow from the frame's `Bytes` where the record is raw (`Bytes::slice`), and `DATA` isn't copied on arrival.
- One reused zstd compressor and decompressor per publisher and subscription, with the dictionary loaded once, instead of a new stream decoder per record.
- Encryption in place in the record buffer.
- A benchmark in `silly bench` for codec and encryption cost per record size.

### 4.2 JavaScript/TypeScript (`clients/js`, package name to decide)

```ts
import { connect, HttpClient, filter } from "sillystream";

const client = await connect({
  url: "wss://host:7000",
  token,                       // or tokenProvider: async () => string
  codecs: { zstdLevel: 3, dictionaries: [dict], writeDictionary: 40000, keyring: { current: 7, keys: { 7: key } } },
});

const pub = client.publisher("logs.app", { ack: 2 });
pub.add(0x0100n, bytes);
const acks = await pub.flush();

const producer = client.producer("logs.app", { lingerMs: 5 });
const { id } = await producer.publish(0x0100n, bytes);

const sub = client.subscribe({ stream: "inbox.reviewer", from: "start", filter: "eq:0xff:1", name: "worker" });
for await (const ev of sub) {
  if (ev.type === "items") for (const item of ev.items) { /* item.id, item.options, item.author, item.data */ }
}
await sub.run(async (items) => { /* commits after this resolves */ });

const ver = await client.state("session.42").merge({ step: 3 }, { expectVer });

const http = new HttpClient({ url: "https://host:7000", token });
await http.publish("logs.app", [{ options: 1n, data }], { ack: 2 });
const { items, through } = await http.read("logs.app", { from: 0n, limit: 1000, filter: "author:7" });
```

- WebSocket is the runtime's own: Bun's, and Node's global `WebSocket` (Node ≥ 22).
- zstd is `Bun.zstdCompress*` or `node:zlib`'s zstd. No zstd means records are sent raw and compressed records come as bad records with `undecodable`.
- AES-GCM is WebCrypto (`crypto.subtle`), which is async, so encryption runs once per record, not per item.
- `Uint8Array` for all bytes. Items borrow from the frame buffer where the record is raw.
- CBOR: a small built-in encoder and decoder for maps, arrays, strings, byte strings, integers (as `bigint` past 2^53), floats, booleans and null. No dependency.

### 4.3 Go (`clients/go`, module path to decide)

```go
c, err := silly.Dial(ctx, silly.Config{
    URL: "wss://host:7000", Token: token, // or TokenProvider: func(ctx) (string, error)
    CAPEM: pem,
    Codecs: silly.Codecs{ZstdLevel: 3, Dictionaries: []silly.Dictionary{dict}, WriteDictionary: 40000,
        Keyring: &silly.Keyring{Current: 7, Keys: map[uint32][]byte{7: key}}},
})

p := c.Publisher("logs.app", silly.PublisherConfig{Ack: silly.AckWAL})
p.Add(0x0100, data)
acks, err := p.Flush(ctx)

pr := c.Producer("logs.app", silly.ProducerConfig{Linger: 5 * time.Millisecond})
ack, err := pr.Publish(ctx, 0x0100, data) // or PublishAsync, returning a future

sub, err := c.Subscribe(ctx, silly.Subscribe{Stream: "inbox.reviewer", From: silly.FromStart,
    Filter: silly.MustFilter("eq:0xff:1"), Name: "worker"})
for {
    ev, err := sub.Next(ctx) // ev.Items, ev.Progress, ev.State, ...
}
err = sub.Run(ctx, func(ctx context.Context, items []silly.Item) error { return nil })

ver, err := c.State("session.42").Merge(ctx, doc, silly.ExpectVer(v))
```

- One goroutine reads the socket and one writes, as Rust's driver task and writer do.
- `Item.Data` may share the frame's buffer. Frame buffers are never reused, so it is safe to keep.
- CBOR through `fxamacker/cbor`.

## 5. Conformance

- **Test vectors** in `testdata/clients/`, generated by Rust and read by every client's tests:
  - records: items to raw records (deterministic), and stamped runs to items, including skips inside a record;
  - zstd records with and without a dictionary, to items;
  - encrypted envelopes with a fixed key and nonce, both ways, and the failure cases;
  - every frame of SPEC section 11.1, both ways;
  - token bodies, and filter text to clauses.
- **zstd output isn't compared** across implementations, because two encoders can produce different valid frames. Vectors for compressed data are decode-only, plus a round trip in each client.
- **Against a server:** each client has a test that starts the real server binary, as `crates/e2e` does, and runs one shared scenario: publish with retries across a forced reconnect, catch up then go live, a filtered named cursor that resumes after a restart, a conditional state change in a publish, and a dictionary plus encryption round trip.

## 6. Order of work

0. **SPEC change** (needs the user's go-ahead): add `2 encrypted envelope` to section 2's codec list and point to this document for the envelope. The server ignores codec, so it doesn't change.
1. **Rust:** codecs (dictionaries, envelope, keyring), the `Producer`, the token provider and expiry reading, `run`, system roots (and `silly` needing `--ca` only for a private CA), the state handle, and the performance work. Then the test vectors.
2. **JS/TS:** WebSocket client, then the HTTP mode.
3. **Go:** WebSocket client and admin HTTP.

Each step ends with its client passing the vectors and the server scenario.

## 7. Open questions

- **Names:** the npm package and the Go module path.
