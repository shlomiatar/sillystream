#!/usr/bin/env python3
"""Writes token.hex from the layout in D25, using Python's own HMAC-SHA256, so the
vector checks the Rust code against the format and not against itself."""
import hashlib
import hmac
import struct

SECRET = b"test-secret"


def s(text):
    return struct.pack("<H", len(text)) + text.encode()


body = struct.pack("<QQ", 1_800_000_000_000, 7)  # expiry_ms, author
body += struct.pack("<H", 2)  # n
body += s("logs.*") + bytes([0x01])  # read
body += s("inbox.reviewer") + bytes([0x06])  # publish | state
signature = hmac.new(SECRET, body, hashlib.sha256).digest()

with open("token.hex", "w") as out:
    out.write("# secret 'test-secret'. Text form of the token: these bytes in lowercase hex.\n")
    out.write("# expiry_ms 1800000000000 | author 7 | n 2\n")
    out.write(body[:18].hex(" ").upper() + "\n")
    out.write("# 'logs.*' read | 'inbox.reviewer' publish + state\n")
    out.write(body[18:].hex(" ").upper() + "\n")
    out.write("# hmac_sha256(secret, body)\n")
    out.write(signature.hex(" ").upper() + "\n")
