from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path

# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-trip_type-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()



file_path = Path(__file__).resolve().parents[3] / "data/trip_type.csv"

# This evaluates to: file:///home/linux/Desktop/...
trip_type = spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)

trip_type.show(truncate=False)


gold_path ="hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/trip_type_dim"



trip_type.write \
    .format("parquet") \
    .option("path", gold_path) \
    .mode("overwrite") \
    .saveAsTable("trip_type_dim")
