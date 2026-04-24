import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from datetime import datetime, timezone, timedelta
from datetime import MAXYEAR
from datetime import MINYEAR
import os

# ================== Write Timestamp Millis =============

json_schema_timestamp_millis = """
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

schema = avro.schema.parse(json_schema_timestamp_millis)

out_path = os.path.join(os.path.dirname(__file__), "../test/timestamp_millis.avro")

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
    pass
reader.close()
print(f"wrote timestamp-millis to {out_path}")


# ================== Write TimestampTz Millis =============

json_schema_timestamp_millis = """
{
  "type": "record",
  "name": "root",
  "fields": [
    {
      "name": "ts",
      "type": ["null", {"type": "long", "adjust-to-utc": true, "logicalType": "timestamp-millis"}]
    }
  ]
}
"""

schema = avro.schema.parse(json_schema_timestamp_millis)

out_path = os.path.join(os.path.dirname(__file__), "../test/timestamptz_millis.avro")

rows = [
    {"ts": None},
    {"ts": int(datetime(MINYEAR, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)},
    {"ts": int(datetime(MAXYEAR, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)},
    {"ts": int(datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=5))).timestamp() * 1000)},           # 2024-01-01 00:00:00
    {"ts": int(datetime(2021, 6, 15, 12, 30, 45, 123000, tzinfo=timezone(timedelta(hours=10))).timestamp() * 1000)}, # 2024-06-15 12:30:45.123
    {"ts": int(datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-5))).timestamp() * 1000)},            # 2000-01-01 00:00:00
    {"ts": int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-10))).timestamp() * 1000)},            # 2000-01-01 00:00:00
]

writer = DataFileWriter(open(out_path, "wb"), DatumWriter(), schema)
for row in rows:
    writer.append(row)
writer.close()

reader = DataFileReader(open(out_path, "rb"), DatumReader())
for record in reader:
    pass
reader.close()
print(f"wrote timestamptz-millis to {out_path}")

# ================== Write Time Millis =============

json_schema_timestamp_millis = """
{
  "type": "record",
  "name": "root",
  "fields": [
    {
      "name": "ts",
      "type": ["null", {"type": "int", "logicalType": "time-millis"}]
    }
  ]
}
"""

schema = avro.schema.parse(json_schema_timestamp_millis)

out_path = os.path.join(os.path.dirname(__file__), "../test/time_millis.avro")

rows = [
    {"ts": None},
    {"ts": 0},
    {"ts": 86400000},
    {"ts": 550000}
]

writer = DataFileWriter(open(out_path, "wb"), DatumWriter(), schema)
for row in rows:
    writer.append(row)
writer.close()

reader = DataFileReader(open(out_path, "rb"), DatumReader())
for record in reader:
    pass
reader.close()
print(f"wrote time-millis to {out_path}")


# ================== Write localtimestamp millis =============

json_schema_timestamp_millis = """
{
  "type": "record",
  "name": "root",
  "fields": [
    {
      "name": "ts",
      "type": ["null", {"type": "long", "logicalType": "local-timestamp-millis"}]
    }
  ]
}
"""

schema = avro.schema.parse(json_schema_timestamp_millis)

out_path = os.path.join(os.path.dirname(__file__), "../test/localtimestamp-millis.avro")

rows = [
    {"ts": None},
    {"ts": int(datetime(MINYEAR, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=2))).timestamp() * 1000)},
    {"ts": int(datetime(MAXYEAR, 12, 31, 23, 59, 59, tzinfo=timezone(timedelta(hours=-5))).timestamp() * 1000)},
    {"ts": int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=12))).timestamp() * 1000)},           # 2024-01-01 00:00:00
    {"ts": int(datetime(2024, 6, 15, 12, 30, 45, 123000, tzinfo=timezone(timedelta(hours=0))).timestamp() * 1000)}, # 2024-06-15 12:30:45.123
    {"ts": int(datetime(2000, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-12))).timestamp() * 1000)},            # 2000-01-01 00:00:00
]

writer = DataFileWriter(open(out_path, "wb"), DatumWriter(), schema)
for row in rows:
    writer.append(row)
writer.close()

reader = DataFileReader(open(out_path, "rb"), DatumReader())
for record in reader:
    pass
reader.close()

print(f"wrote timestamp-local-millis to {out_path}")

