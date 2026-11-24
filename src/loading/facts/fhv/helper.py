from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path
import argparse
# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("green-Loading-revenue-fact").getOrCreate()






spark.read.parquet('NYC-Taxi/gold/facts/zone_fact').printSchema()


