"""Decode a .sensor file directly with pyarrow — no Spark required."""

import sys

import pyarrow as pa

from sensor._sensor import decode_records

path = sys.argv[1] if len(sys.argv) > 1 else "sample.sensor"

with open(path, "rb") as f:
    data = f.read()

batch: pa.RecordBatch = decode_records(data)

print(batch.schema)
print(f"{batch.num_rows:,} records decoded")
table = pa.Table.from_batches([batch])
print(table.slice(0, 10))
