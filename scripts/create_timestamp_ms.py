import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from datetime import datetime, timezone
from datetime import MAXYEAR
from datetime import MINYEAR
import os

json_schema = """
{
  "type": "record",
  "name": "root",
  "fields": [
    {
      "name": "ts",
      "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]
    }
  ]
}
"""

schema = avro.schema.parse(json_schema)

out_path = os.path.join(os.path.dirname(__file__), "../test/timestamp_ms.avro")

rows = [
    {"ts": None},
    {"ts": int(datetime(MINYEAR, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)},
    {"ts": int(datetime(MAXYEAR, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)},
    {"ts": int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)},           # 2024-01-01 00:00:00
    {"ts": int(datetime(2024, 6, 15, 12, 30, 45, 123000, tzinfo=timezone.utc).timestamp() * 1000)}, # 2024-06-15 12:30:45.123
    {"ts": int(datetime(2000, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)},            # 2000-01-01 00:00:00
]

writer = DataFileWriter(open(out_path, "wb"), DatumWriter(), schema)
for row in rows:
    writer.append(row)
writer.close()

reader = DataFileReader(open(out_path, "rb"), DatumReader())
for record in reader:
    print(record)
reader.close()

print("Written to", out_path)
