"""Writes the hand-built Avro object container files of test/sql/malformed.test (stdlib only: python3 -I).

A block count a reader must not trust, and its legitimate counterpart: an array of 100000 nulls, a positive count
with no payload behind it.

    python3 -I scripts/create_malformed.py
"""

import os

MAGIC = b"Obj\x01"
SYNC = bytes(range(16))
TEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test")


def varint(value):
    """A zigzag-encoded Avro long."""
    zigzag = (value << 1) ^ (value >> 63)
    out = bytearray()
    while True:
        byte = zigzag & 0x7F
        zigzag >>= 7
        if zigzag:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def string(text):
    data = text.encode()
    return varint(len(data)) + data


def container(schema, rows):
    """A null-codec file: the header, then one data block per entry of `rows` (the encoded bytes of its objects)."""
    header = MAGIC + varint(2) + string("avro.codec") + string("null") + string("avro.schema") + string(schema)
    header += varint(0) + SYNC
    blocks = b""
    for count, data in rows:
        blocks += varint(count) + varint(len(data)) + data + SYNC
    return header + blocks


def record(field_type):
    return '{"type":"record","name":"r","fields":[{"name":"a","type":%s}]}' % field_type


def write(name, data):
    path = os.path.join(TEST_DIR, name)
    with open(path, "wb") as f:
        f.write(data)
    print("wrote %d bytes to %s" % (len(data), os.path.relpath(path)))


# an array<null> block claiming 2^32 items: nothing follows, the items occupy no bytes
write(
    "malformed_array_null_count.avro",
    container(record('{"type":"array","items":"null"}'), [(1, varint(2**32))]),
)
# an array<int> block claiming 2^40 items with three one-byte ints behind it
write(
    "malformed_array_int_count.avro",
    container(record('{"type":"array","items":"int"}'), [(1, varint(2**40) + varint(1) + varint(2) + varint(3))]),
)
# a map<int> block claiming 2^40 entries with one entry behind it
write(
    "malformed_map_count.avro",
    container(record('{"type":"map","values":"int"}'), [(1, varint(2**40) + string("k") + varint(1))]),
)
# the legitimate counterpart: a positive count with no payload (100000 nulls), and an empty array
write(
    "array_of_nulls.avro",
    container(record('{"type":"array","items":"null"}'), [(2, varint(100000) + varint(0) + varint(0))]),
)
