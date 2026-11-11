"""Writes the small Avro object container files of test/corpus (stdlib only: python3 -I).

Each file is the minimal reproduction of a finding of a run over 1393 real-world Avro files (schema features other
writers produce that the reader refused or mishandled): a logical type on a type it is not implemented for, invalid
logical type attributes, the `error` record type, a map-annotated array with nullable items, non-UTF-8 header
metadata, a 10-byte varint with the 65th bit set, and the `local-timestamp-micros` / `-nanos` and `duration` logical
types. The header is hand-rolled (no Avro library validates the schema), the rows are encoded by hand.

    python3 -I scripts/create_corpus_fixtures.py
"""

import json
import os

MAGIC = b"Obj\x01"
SYNC = bytes(range(16))
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test", "corpus")


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


def bytes_(data):
    return varint(len(data)) + data


def string(text):
    return bytes_(text.encode())


def array(items):
    """One block of the items (already encoded), then the end marker."""
    return (varint(len(items)) + b"".join(items) if items else b"") + varint(0)


def container(schema, rows, metadata=None):
    """A null-codec file with one data block of `rows` (the encoded bytes of each row) and extra header entries."""
    entries = [("avro.schema", json.dumps(schema).encode()), ("avro.codec", b"null"), *(metadata or [])]
    header = MAGIC + varint(len(entries))
    for key, value in entries:
        header += string(key) + bytes_(value)
    header += varint(0) + SYNC
    data = b"".join(rows)
    return header + varint(len(rows)) + varint(len(data)) + data + SYNC


def record(fields):
    return {"type": "record", "name": "r", "fields": fields}


def write(name, data):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    with open(path, "wb") as f:
        f.write(data)
    print("wrote %d bytes to %s" % (len(data), os.path.relpath(path)))


# `uuid` annotates a string (RFC 4122 text) per the spec; fixed(16) came later. Java, Spanner exports, StarRocks and
# arrow-rs write this form.
uuid_string = record([{"name": "id", "type": ["null", {"type": "string", "logicalType": "uuid"}]}])
write(
    "uuid_on_string.avro",
    container(
        uuid_string,
        [
            varint(1) + string("123e4567-e89b-12d3-a456-426614174000"),
            varint(1) + string("123E4567-E89B-12D3-A456-426614174000"),
            varint(0),
        ],
    ),
)
# a string that is not a UUID is an error when read
write("uuid_on_string_invalid.avro", container(uuid_string, [varint(1) + string("not-a-uuid")]))

# unknown logical types (vendor names: Google Datastream's time-interval-micros, Hive's char) are ignored: the
# underlying type is read
write(
    "unknown_logical_type.avro",
    container(
        record(
            [
                {"name": "v", "type": {"type": "long", "logicalType": "time-interval-micros"}},
                {"name": "s", "type": {"type": "string", "logicalType": "char", "maxLength": 3}},
            ]
        ),
        [varint(42) + string("abc")],
    ),
)

# invalid decimal attributes (Hive 0.x wrote precision and scale as strings; precision 76 does not fit a DuckDB
# DECIMAL; scale above precision; no precision at all): the logical type is ignored, the bytes / fixed is a BLOB
write(
    "decimal_invalid.avro",
    container(
        record(
            [
                {"name": "d_str", "type": {"type": "bytes", "logicalType": "decimal", "precision": "5", "scale": "2"}},
                {"name": "d_big", "type": {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 10}},
                {"name": "d_scale", "type": {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 7}},
                {"name": "d_none", "type": {"type": "bytes", "logicalType": "decimal"}},
                {
                    "name": "d_fixed",
                    "type": {"type": "fixed", "name": "f4", "size": 4, "logicalType": "decimal", "precision": 0},
                },
            ]
        ),
        [bytes_(b"\x01\x2c") * 4 + b"\x00\x00\x01\x2c"],
    ),
)

# Java's Schema.parse accepts {"type": "error"} (the protocol error record) anywhere a record is allowed
write(
    "error_record_type.avro",
    container(
        {"type": "error", "name": "e", "fields": [{"name": "msg", "type": "string"}]},
        [string("x")],
    ),
)

# BigQuery's Iceberg manifest export: a map-annotated array whose items are a nullable key/value record. A null
# item has no place in a MAP and is skipped.
kv = {
    "type": "record",
    "name": "kv",
    "fields": [{"name": "key", "type": ["null", "int"]}, {"name": "value", "type": ["null", "long"]}],
}
array_map = record([{"name": "m", "type": ["null", {"type": "array", "logicalType": "map", "items": ["null", kv]}]}])


def kv_item(key, value):
    return varint(1) + varint(1) + varint(key) + varint(1) + varint(value)


write(
    "array_map_nullable_items.avro",
    container(
        array_map,
        [
            varint(1) + array([kv_item(1, 2), kv_item(3, 4)]),
            varint(1) + array([varint(0), kv_item(5, 6)]),
            varint(0),
        ],
    ),
)

# file metadata is map<string, bytes>: apache/avro's syncInMeta.avro stores the sync marker in an entry
write(
    "metadata_binary_value.avro",
    container(record([{"name": "a", "type": "int"}]), [varint(1)], metadata=[("user.bin", b"\xd7\xff\x00\x01")]),
)

# local-timestamp-micros / local-timestamp-nanos (Avro 1.10): 2023-11-14 22:13:20 in both
write(
    "local_timestamps.avro",
    container(
        record(
            [
                {"name": "us", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
                {"name": "ns", "type": {"type": "long", "logicalType": "local-timestamp-nanos"}},
            ]
        ),
        [varint(1700000000000000) + varint(1700000000000000000)],
    ),
)

# duration: fixed(12) of three little-endian unsigned 32-bit integers (months, days, milliseconds)
duration = record(
    [{"name": "d", "type": {"type": "fixed", "name": "dur", "size": 12, "logicalType": "duration"}}]
)


def duration_value(months, days, millis):
    return months.to_bytes(4, "little") + days.to_bytes(4, "little") + millis.to_bytes(4, "little")


write(
    "duration.avro",
    container(duration, [duration_value(1, 2, 3), duration_value(0, 0, 3_600_000), duration_value(0, 0, 0)]),
)
# a months count beyond INT32 (the INTERVAL's months are signed 32-bit)
write("duration_out_of_range.avro", container(duration, [duration_value(0x80000000, 0, 0)]))

# pyiceberg encoded a snapshot id above INT64_MAX (9876543210123456789) as a 10-byte varint whose last byte sets the
# 65th bit; Java, fastavro and the C++ extension wrap it to -653171173268680982
write(
    "varint_wrap.avro",
    container(
        record([{"name": "v", "type": "long"}]),
        [bytes([0xAB, 0xC4, 0xA3, 0x83, 0x96, 0xEE, 0xC3, 0x90, 0x92, 0x02])],
    ),
)
