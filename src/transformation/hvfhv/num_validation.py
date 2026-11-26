from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from functools import reduce
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel


spark = (
    SparkSession.builder
    .appName("NYC-Taxi - FHV Transformation")
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .getOrCreate()
)

# Hide Spark INFO and WARN logs, but keep errors
spark.sparkContext.setLogLevel("OFF")






spark.read.parquet('NYC-Taxi/silver/fhvhv/fhvhv_cleansed_data').printSchema()


# print(src) # ~ 13M
