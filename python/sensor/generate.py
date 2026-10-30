"""Generate a sample .sensor binary file for testing."""

from __future__ import annotations

import random
import struct
import sys

SYNC = 0xAB
BASE_TS_US = 1_700_000_000_000_000  # 2023-11-14 22:13:20 UTC in microseconds


def make_record(sensor_id: int, timestamp_us: int, value: float) -> bytes:
    # Pack sync + sensor_id + timestamp + value + flags (reserved=0)
    body = struct.pack(">BBqfB", SYNC, sensor_id, timestamp_us, value, 0)
    checksum = 0
    for b in body:
        checksum ^= b
    return body + bytes([checksum])


def generate(path: str, n: int = 100_000) -> None:
    with open(path, "wb") as f:
        for i in range(n):
            sensor_id = i % 8
            timestamp_us = BASE_TS_US + i * 1_000_000
            value = random.uniform(-100.0, 100.0)
            f.write(make_record(sensor_id, timestamp_us, value))
    print(f"Wrote {n} records → {path}  ({n * 16 / 1024:.0f} KiB)")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "sample.sensor"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
    generate(out, n)
