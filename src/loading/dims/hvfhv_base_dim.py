from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path

# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-fhvbase-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()



file_path = Path(__file__).resolve().parents[3] / "data/hvfhv_base.csv"

# This evaluates to: file:///home/linux/Desktop/...
hvhdv_base = spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)

hvhdv_base.show(truncate=False)


gold_path ="hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/hvfhv_base_dim"



hvhdv_base.write \
    .format("parquet") \
    .option("path", gold_path) \
    .mode("overwrite") \
    .saveAsTable("hvfhv_base_dim")
