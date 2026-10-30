# rust-pyspark-datasource-example

A minimal example of using Rust to decode a custom binary format and hand the result to Python as a **PyArrow `RecordBatch`** with zero copies, then exposing it as a **PySpark 4.0 custom DataSource**.

## The big idea

```
.sensor file  ──►  Rust (parse)  ──►  Arrow RecordBatch
                                            │
                                  C Data Interface (zero-copy)
                                            │
                                       PyArrow  ──►  PySpark DataFrame
```

The key is Arrow's [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html): Rust builds the `RecordBatch` in native memory and hands a pointer to Python — no serialisation, no copy, no `pickle`.

## Binary format

Each record is exactly **16 bytes**, big-endian:

| Bytes | Field        | Type  | Notes                          |
|-------|-------------|-------|--------------------------------|
| 0     | sync        | u8    | always `0xAB`                  |
| 1     | sensor_id   | u8    | 0–255                          |
| 2–9   | timestamp   | i64   | microseconds since Unix epoch  |
| 10–13 | value       | f32   | sensor reading                 |
| 14    | flags       | u8    | reserved, set to 0             |
| 15    | checksum    | u8    | XOR of bytes 0–14              |

Fixed-size records mean partition alignment is trivial: every byte offset that is a multiple of 16 is a valid partition boundary.

## Build

Install [maturin](https://github.com/PyO3/maturin) and a Rust toolchain, then:

```bash
maturin develop --release
```

This compiles the Rust crate and installs `sensor._sensor` into the active Python environment.

## Usage

**Generate test data:**
```bash
python -m sensor.generate sample.sensor 100000
```

**Decode without Spark:**
```bash
python examples/read_without_spark.py sample.sensor
```

**Read with PySpark:**
```python
from pyspark.sql import SparkSession
from sensor import SensorDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(SensorDataSource)

df = spark.read.format("sensor").option("path", "sample.sensor").load()
df.show()
```

## Project structure

```
├── Cargo.toml                  # Rust crate: pyo3 + arrow[pyarrow]
├── pyproject.toml              # maturin build config
├── src/
│   └── lib.rs                  # Rust decoder + PyO3 bindings
├── python/sensor/
│   ├── __init__.py
│   ├── datasource.py           # PySpark 4.0 DataSource
│   └── generate.py             # Test file generator
└── examples/
    └── read_without_spark.py   # Standalone PyArrow example
```

## How the zero-copy transfer works

In `src/lib.rs`, the `decode_records` function:

1. Releases the GIL (`py.allow_threads`) so Rust runs without blocking Python.
2. Parses the bytes into Arrow arrays using the `arrow` crate.
3. Calls `batch.to_pyarrow(py)` — this invokes the Arrow C Data Interface, exporting the buffer pointers to a Python capsule. PyArrow wraps those pointers without copying.

```rust
#[pyfunction]
fn decode_records(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    let batch = py.allow_threads(|| decode(data));
    batch.to_pyarrow(py)          // C Data Interface — zero-copy
}
```
