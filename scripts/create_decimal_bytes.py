"""Generate the independent bytes-decimal scan fixture (requires fastavro)."""

from decimal import Decimal, localcontext
from pathlib import Path

from fastavro import writer


# Exercise all four DuckDB decimal storage widths, including the DECIMAL(19,2)
# reported in duckdb/duckdb-avro#100.
decimal_types = [(4, 2), (9, 4), (18, 6), (19, 2), (38, 10)]
fields = [{"name": "row_id", "type": "int"}]
for index, (precision, scale) in enumerate(decimal_types):
    decimal_type = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": precision,
        "scale": scale,
    }
    fields.append(
        {
            "name": f"decimal_{precision}_{scale}",
            "type": ["null", decimal_type] if index % 2 == 0 else [decimal_type, "null"],
        }
    )
fields.append({"name": "plain_bytes", "type": "bytes"})
schema = {"type": "record", "name": "bytes_decimals", "fields": fields}

# Small values require sign extension to DuckDB's storage width. Values around
# +/-128 also exercise the sign byte at a change in encoded length.
unscaled_values = [None, "min", -3049, -129, -128, -1, 0, 1, 127, 128, 3049, "max"]
rows = []
with localcontext() as context:
    context.prec = 38
    for row_id, value in enumerate(unscaled_values):
        # The same payload as decimal 30.49 must remain a BLOB without annotation.
        row = {"row_id": row_id, "plain_bytes": b"\x0b\xe9"}
        for precision, scale in decimal_types:
            unscaled = value
            if value == "min":
                unscaled = -(10**precision - 1)
            elif value == "max":
                unscaled = 10**precision - 1
            row[f"decimal_{precision}_{scale}"] = (
                None if unscaled is None else Decimal(unscaled).scaleb(-scale)
            )
        rows.append(row)

out_path = Path(__file__).resolve().parent.parent / "test" / "decimal_bytes.avro"
with out_path.open("wb") as output:
    writer(output, schema, rows, codec="null", sync_marker=b"decimal-bytes!!!")
