from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import json
from deepdiff import DeepDiff
import sys
stored_contract_path = Path(__file__).resolve().parents[1] / 'fhvhv_schema_contract.json'
schema_path = Path(__file__).resolve().parents[2]/"data_source_schema/hvfhv"


spark = (
    SparkSession.builder
    .appName("NYC-Taxi - fhvhv Schema Reading")
    .getOrCreate()
)



df = spark.read.parquet('hdfs://localhost:9000/user/linux/NYC-Taxi/bronze/fhvhv/date=2019-02/fhvhv_tripdata_2019-02.parquet')

schema_json_string = df.schema.jsonValue()

import json
with open(f'{schema_path}/fhvhv_schema_contract.json' , 'w' , encoding="utf-8") as f:
    json.dump(schema_json_string , f , indent=2)




