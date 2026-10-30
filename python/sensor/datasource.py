from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import FloatType, IntegerType, LongType, StructField, StructType

from sensor._sensor import decode_records

RECORD_LEN = 16  # must match Rust
DEFAULT_CHUNK = 8 * 1024 * 1024  # 8 MiB


@dataclass
class SensorPartition(InputPartition):
    path: str
    offset: int
    length: int


class SensorDataSource(DataSource):
    """PySpark 4.0 custom DataSource for the .sensor binary format."""

    @classmethod
    def name(cls) -> str:
        return "sensor"

    def schema(self) -> StructType:
        return StructType([
            StructField("sensor_id", IntegerType(), False),
            StructField("timestamp_us", LongType(), False),
            StructField("value", FloatType(), False),
        ])

    def reader(self, schema: StructType) -> DataSourceReader:
        return SensorReader(self.options)


class SensorReader(DataSourceReader):
    def __init__(self, options: dict[str, str]) -> None:
        self.path = options["path"]
        chunk = int(options.get("chunkBytes", DEFAULT_CHUNK))
        self.chunk = chunk - (chunk % RECORD_LEN)  # align to record boundary

    def partitions(self) -> list[SensorPartition]:
        size = Path(self.path).stat().st_size
        aligned = size - (size % RECORD_LEN)
        parts: list[SensorPartition] = []
        offset = 0
        while offset < aligned:
            length = min(self.chunk, aligned - offset)
            parts.append(SensorPartition(self.path, offset, length))
            offset += length
        return parts

    def read(self, partition: SensorPartition) -> Iterator[pa.RecordBatch]:
        with open(partition.path, "rb") as f:
            f.seek(partition.offset)
            data = f.read(partition.length)
        # decode_records calls into Rust, releases the GIL, and returns a
        # PyArrow RecordBatch via the Arrow C Data Interface (zero-copy).
        yield decode_records(data)
