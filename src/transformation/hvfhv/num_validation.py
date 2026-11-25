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






src = spark.read.parquet('NYC-Taxi/bronze/fhvhv/date=2019-02/fhvhv_tripdata_2019-02.parquet')\
.filter(func.col('wav_request_flag') == 'Y').count()


print(src)
