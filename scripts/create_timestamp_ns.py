"""Generate the independent timestamp-nanos scan fixture (requires fastavro)."""

from pathlib import Path

from fastavro import writer


schema = {
    "type": "record",
    "name": "timestamps",
    "fields": [
        {
            "name": "utc",
            "type": [
                "null",
                {"type": "long", "logicalType": "timestamp-nanos", "adjust-to-utc": True},
            ],
        },
        {
            "name": "local",
            "type": [
                {"type": "long", "logicalType": "timestamp-nanos", "adjust-to-utc": False},
                "null",
            ],
        },
        {
            "name": "unspecified",
            "type": ["null", {"type": "long", "logicalType": "timestamp-nanos"}],
        },
    ],
}

# Use integer nanoseconds: Python datetime only preserves microseconds.
values = [None, -876543211, -1, 0, 1, 1705314896123456789]
rows = [dict.fromkeys(("utc", "local", "unspecified"), value) for value in values]
out_path = Path(__file__).resolve().parent.parent / "test" / "timestamp_nanos.avro"

with out_path.open("wb") as output:
    writer(output, schema, rows, codec="null", sync_marker=b"timestamp-nanos!")
