from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from functools import reduce
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel

from pathlib import Path
import logging
import time
import argparse

import subprocess
import sys
log_path = Path(__file__).resolve().parents[3] / ('pipeline_logs')

logging.basicConfig(

    filename=f'{log_path}/fhv.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(funcName)s | %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"

)

transformation_logging = logging.getLogger('TRANSFORMATION')



spark = (
    SparkSession.builder
    .appName("NYC-Taxi - FHV Transformation")
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .getOrCreate()
)

# Hide Spark INFO and WARN logs, but keep errors
spark.sparkContext.setLogLevel("OFF")






src = spark.read.parquet('NYC-Taxi/bronze/fhv/date=2019-02/fhv_tripdata_2019-02.parquet')


cleansed = spark.read.parquet('NYC-Taxi/silver/fhv/fhv_cleansed_data/date=2019-02')
error = spark.read.parquet('NYC-Taxi/silver/fhv/quarantine_data/date=2019-02')






if src.count() == (cleansed.count() + error.count()):
    print("PASSED")
else:
    print("FAILED")