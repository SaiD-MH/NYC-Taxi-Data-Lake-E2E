from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import json
from deepdiff import DeepDiff
import sys
stored_contract_path = Path(__file__).resolve().parents[1] / 'green_schema_contract.json'
schema_path = Path(__file__).resolve().parents[2]/"data_source_schema/green"


spark = (
    SparkSession.builder
    .appName("NYC-Taxi - Green Schema Reading")
    .getOrCreate()
)



def read_bronze_parquet_schema(file_path):
    schema = spark.read.parquet(file_path).schema.jsonValue()

    return schema


def read_stored_contract():

    try:

        with open(stored_contract_path , 'r') as f:
            return json.load(f)

    except Exception as e:
        print(e)



def contract_check(contract , loaded):
    check_result = DeepDiff(contract , loaded)
    if len(check_result) != 0:
        print("Check: FAILED")
        print(check_result)
        sys.exit(1)
    else:
        print("Check: PASSED")

def store_current_data_schema(schema , loading_date):
    
    target_path = Path(__file__).resolve().parents[2] / f"data_source_schema/green/date={loading_date}"
    target_path.mkdir(exist_ok=True)


    with open(f'{target_path}/source_loaded_schema.json' , 'w') as f:
        json.dump(schema , f , indent= 2)



def main():

    arg = argparse.ArgumentParser("Run Configs")
    arg.add_argument('--loading_date', type=str , default='2019-02')

    run_configs = arg.parse_args()

    schema = read_bronze_parquet_schema(f"hdfs://localhost:9000/user/linux/NYC-Taxi/bronze/green/date={run_configs.loading_date}/green_tripdata_{run_configs.loading_date}.parquet")
    contract = read_stored_contract()
    store_current_data_schema(schema , run_configs.loading_date)
    contract_check(contract , schema)





if __name__ == '__main__':
    main()




