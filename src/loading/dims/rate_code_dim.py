from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path

# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-RateCode-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()



file_path = Path(__file__).resolve().parents[3] / "data/rate_code.csv"

# This evaluates to: file:///home/linux/Desktop/...
rate_code_dim = spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)

rate_code_dim.show(truncate=False)


gold_path ="hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/rate_code_dim"



rate_code_dim.write \
    .format("parquet") \
    .option("path", gold_path) \
    .mode("overwrite") \
    .saveAsTable("rate_code_dim")
