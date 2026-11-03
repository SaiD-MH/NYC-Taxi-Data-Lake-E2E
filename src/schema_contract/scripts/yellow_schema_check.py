from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import json
from deepdiff import DeepDiff
import sys
stored_contract_path = Path(__file__).resolve().parents[1] / 'yellow_schema_contract.json'


spark = (
    SparkSession.builder
    .appName("NYC-Taxi - Yellow Schema Reading")
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


def main():

    arg = argparse.ArgumentParser("Run Configs")
    arg.add_argument('--loading_date', type=str , default='2019-02')

    run_configs = arg.parse_args()

    schema = read_bronze_parquet_schema(f"hdfs://localhost:9000/user/linux/NYC-Taxi/bronze/yellow/date={run_configs.loading_date}/yellow_tripdata_{run_configs.loading_date}.parquet")
    contract = read_stored_contract()
    contract_check(contract , schema)




if __name__ == '__main__':
    main()









# df = spark.read.parquet('hdfs://localhost:9000/user/linux/NYC-Taxi/bronze/yellow/date=2019-02/yellow_tripdata_2019-02.parquet')

# schema_json_string = df.schema.jsonValue()

# import json
# with open(f'{schema_path}/yellow_schema_contract.json' , 'w' , encoding="utf-8") as f:
#     json.dump(schema_json_string , f , indent=2)








