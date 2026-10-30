"""Read a .sensor file into a PySpark DataFrame via the custom DataSource."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from sensor import SensorDataSource

path = sys.argv[1] if len(sys.argv) > 1 else "sample.sensor"

spark = (
    SparkSession.builder.appName("sensor-example")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.dataSource.register(SensorDataSource)

df = spark.read.format("sensor").option("path", str(Path(path).resolve())).load()

print(f"Schema: {df.schema.simpleString()}")
print(f"Partitions: {df.rdd.getNumPartitions()}")
df.show(10, truncate=False)

df.createOrReplaceTempView("sensor_data")
spark.sql("""
    SELECT
        sensor_id,
        COUNT(*)           AS n,
        ROUND(AVG(value), 2) AS avg_value,
        ROUND(MIN(value), 2) AS min_value,
        ROUND(MAX(value), 2) AS max_value
    FROM sensor_data
    GROUP BY sensor_id
    ORDER BY sensor_id
""").show()

spark.stop()
