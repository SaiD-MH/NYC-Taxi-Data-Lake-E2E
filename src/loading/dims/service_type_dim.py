from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path

# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-ServiceType-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()



file_path = Path(__file__).resolve().parents[3] / "data/service_types.csv"

# This evaluates to: file:///home/linux/Desktop/...
service_df = spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)

service_df.show(truncate=False)


gold_path ="hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/service_type_dim"



service_df.write \
    .format("parquet") \
    .option("path", gold_path) \
    .mode("overwrite") \
    .saveAsTable("service_type_dim")
