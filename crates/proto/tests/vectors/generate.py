#!/usr/bin/env python3
"""Writes the .hex vectors from the layouts in SPEC section 11.1.

Deliberately shares nothing with the Rust code, so the vectors check the code
against the spec and not against itself. Run it from this directory; a change to
the output must come with a matching change to SPEC.md.
"""
import struct


def u8(v): return struct.pack("<B", v)
def u16(v): return struct.pack("<H", v)
def u32(v): return struct.pack("<I", v)
def u64(v): return struct.pack("<Q", v)
def s(text): return u16(len(text.encode())) + text.encode()
def b(data): return u32(len(data)) + data


def write(name, frame_type, flags, *fields):
    """fields are (label, bytes); each becomes a commented hex line."""
    lines = [f"# type 0x{frame_type:02X} | flags 0x{flags:02X}", f"{frame_type:02X} {flags:02X}"]
    for label, data in fields:
        lines.append(f"# {label}")
        if data:
            lines.append(" ".join(f"{x:02X}" for x in data))
    with open(f"{name}.hex", "w") as out:
        out.write("\n".join(lines) + "\n")


# One real record: id 0, options 3, author 0, len 5, count 1, codec 0, "hello" + 3 pad.
RECORD = u64(0) + u64(3) + u64(0) + u32(5) + u16(1) + u16(0) + b"hello\0\0\0"
DOC = bytes([0xA1, 0x61, 0x61, 0x01])  # CBOR {"a": 1}

write("hello", 0x01, 0, ("ver 1", u16(1)), ("token 'tok'", s("tok")))


def subscribe(name, flags, clauses):
    fields = [
        ("sub", u32(0x01020304)),
        ("from", u64(0x1112131415161718)),
        ("linger_ms", u16(0x2122)),
    ]
    if flags & 0x10:
        fields.append(("author (HAS_AUTHOR)", u64(0x3132333435363738)))
    fields.append(("nfilt", u8(len(clauses))))
    for mask, value, op in clauses:
        fields.append(("mask | value | op", u64(mask) + u64(value) + u8(op)))
    fields.append(("stream 'logs.a'", s("logs.a")))
    if flags & 0x04:
        fields.append(("name 'c1' (NAMED_CURSOR)", s("c1")))
    write(name, 0x02, flags, *fields)


ONE_CLAUSE = [(0x4142434445464748, 0x5152535455565758, 0)]
for flags in range(32):
    subscribe(f"subscribe_{flags:02x}", flags, ONE_CLAUSE)
subscribe("subscribe_no_clauses", 0, [])
subscribe("subscribe_four_clauses", 0, [
    (0xFF, 0x01, 0),
    (0xFF00, 0, 1),
    (0xFF0000, 0x7, 1),  # any with a value: kept on the wire, unused
    (u64_max := 0xFFFFFFFFFFFFFFFF, u64_max, 0),
])

write("unsubscribe", 0x03, 0, ("sub", u32(7)))
write("open", 0x04, 0, ("handle", u32(9)), ("stream 'inbox.reviewer'", s("inbox.reviewer")))


def publish(name, flags, ack, records, op=2):
    fields = [
        ("handle", u32(5)),
        ("producer_id", u64(0x0102030405060708)),
        ("producer_seq", u64(42)),
        (f"ack {ack}", u8(ack)),
    ]
    if flags & 1:
        fields += [("expect_ver (HAS_STATE)", u64(99)), (f"op {op}", u8(op)), ("doc", b(DOC))]
    fields.append(("records", b(records)))
    write(name, 0x05, flags, *fields)


publish("publish_00", 0, 2, RECORD)
publish("publish_01", 1, 2, RECORD)
publish("publish_ack0", 0, 0, RECORD)
publish("publish_ack1", 0, 1, RECORD)
publish("publish_ack3", 0, 3, RECORD)
publish("publish_state_no_records", 1, 2, b"")
publish("publish_state_set", 1, 2, RECORD, op=1)
publish("publish_state_incr", 1, 2, RECORD, op=3)

write("state_op", 0x06, 0, ("handle", u32(5)), ("req", u32(6)), ("expect_ver", u64(99)),
      ("op 1 set", u8(1)), ("doc", b(DOC)))
write("commit", 0x07, 0, ("sub", u32(7)), ("id", u64(0x0102030405060708)))
write("ping", 0x08, 0, ("nonce", u32(0xDEADBEEF)))

write("welcome", 0x81, 0, ("session", u64(0x0102030405060708)),
      ("max_frame 16 MiB", u32(16 << 20)), ("server_ms", u64(1_800_000_000_000)))
write("subscribed", 0x82, 0, ("sub", u32(7)), ("head", u64(1000)), ("state_ver", u64(900)),
      ("state", b(DOC)))
write("subscribed_no_state", 0x82, 0, ("sub", u32(7)), ("head", u64(1000)), ("state_ver", u64(0)),
      ("state empty", b(b"")))
write("data", 0x83, 0, ("sub", u32(7)), ("records", b(RECORD)))
write("ack", 0x84, 0, ("handle", u32(5)), ("producer_seq", u64(42)), ("first_id", u64(1000)),
      ("count", u32(3)))
write("ack_already_committed", 0x84, 0, ("handle", u32(5)), ("producer_seq", u64(41)),
      ("first_id 0", u64(0)), ("count 0", u32(0)))
write("state", 0x85, 0, ("sub", u32(7)), ("ver", u64(900)), ("doc", b(DOC)))
write("behind", 0x86, 0, ("sub", u32(7)), ("resume_from", u64(1000)))
write("progress", 0x87, 0, ("sub", u32(7)), ("id", u64(1000)))
write("state_result_ok", 0x88, 0, ("req", u32(6)), ("ok 1", u8(1)), ("ver", u64(900)))
write("state_result_conflict", 0x88, 0, ("req", u32(6)), ("ok 0", u8(0)), ("ver", u64(901)))
write("redirect", 0x89, 0, ("stream 'logs.a'", s("logs.a")), ("host 'node2:443'", s("node2:443")))
write("nack", 0x8A, 0, ("handle", u32(5)), ("producer_seq", u64(42)),
      ("code 3 version conflict", u16(3)), ("msg 'conflict'", s("conflict")))
for code in range(1, 9):
    write(f"error_{code}", 0x8E, 0, ("ref", u32(11)), (f"code {code}", u16(code)), ("msg 'no'", s("no")))
write("pong", 0x8F, 0, ("nonce", u32(0xDEADBEEF)))
