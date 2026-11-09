from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path

# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-Zone-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()



file_path = Path(__file__).resolve().parents[3] / "data/taxi_zone_lookup.csv"

# This evaluates to: file:///home/linux/Desktop/...
zone_df = spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)




normalized_zone_df = zone_df.select(func.col("LocationID").alias('location_id') ,\
                                    func.col("Borough").alias('borough'),\
                                    func.col("Zone").alias('zone'),\
                                    func.col("service_zone"))




gold_path ="hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/zone_dim"



normalized_zone_df.write \
    .format("parquet") \
    .option("path", gold_path) \
    .mode("overwrite") \
    .saveAsTable("zone_dim")
