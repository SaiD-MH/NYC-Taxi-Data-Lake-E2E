from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path

# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-PaymentType-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()



file_path = Path(__file__).resolve().parents[3] / "data/payment_type.csv"

# This evaluates to: file:///home/linux/Desktop/...
payment_df = spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)

payment_df.show(truncate=False)


gold_path ="hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/payment_type_dim"



payment_df.write \
    .format("parquet") \
    .option("path", gold_path) \
    .mode("overwrite") \
    .saveAsTable("payment_type_dim")
