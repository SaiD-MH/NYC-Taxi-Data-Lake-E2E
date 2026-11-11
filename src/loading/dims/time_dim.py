from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path

# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-time-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()



from datetime  import time

times = []
for h in range(0,24):
    for m in range(0,60):
        current_time = time(hour=h , minute=m)
        hour = h
        mintue = m
        time_key = current_time.strftime("%H%M")
        times.append((time_key , hour , mintue))





time_df = spark.createDataFrame(times , schema = ['time_key' , 'hour' , 'mintue'])


gold_path ="hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/time_dim"



time_df.write \
    .format("parquet") \
    .option("path", gold_path) \
    .mode("overwrite") \
    .saveAsTable("time_dim")
