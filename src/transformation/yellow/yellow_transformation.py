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

    filename=f'{log_path}/yellow.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(funcName)s | %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"

)

transformation_logging = logging.getLogger('TRANSFORMATION')



spark = (
    SparkSession.builder
    .appName("NYC-Taxi - Yellow Transformation")
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .getOrCreate()
)

# Hide Spark INFO and WARN logs, but keep errors
spark.sparkContext.setLogLevel("OFF")


def hdfs(*args):

    cmd = ['hdfs' , 'dfs' , *args]
    transformation_logging.info(f"Executing {str(cmd)}")

    try:
        res = subprocess.run(cmd , check=True , capture_output=True , text=True)    
    except subprocess.CalledProcessError as error:
        transformation_logging.error(f"Faild to execute {str(cmd)} - Exit Code: {error.returncode} - {error.stderr}")
        sys.exit(1)

    transformation_logging.info(f"SUCCESS |  {str(cmd)}")


def create_loading_date_dir(silver_yellow_path , loading_date):

    hdfs('-mkdir' , '-p' , f'{silver_yellow_path}/yellow_cleansed_data/date={loading_date}')
    hdfs('-mkdir' , '-p' , f'{silver_yellow_path}/quarantine_data/date={loading_date}')


def store_df_as_parquet(df , table_name , silver_path , loading_date):
    df.write\
      .mode('overwrite')\
      .parquet(f'{silver_path}/{table_name}/date={loading_date}')


def read_yellow_parque_file(loading_date):
    file_path = (
        f"hdfs://localhost:9000/user/linux/NYC-Taxi/"
        f"bronze/yellow/date={loading_date}/"
        f"yellow_tripdata_{loading_date}.parquet"
    )

    transformation_logging.info(f"read yellow parquet file path: {file_path}")

    try:
        df = spark.read.parquet(file_path)
    except Exception as e:
        transformation_logging.error(e)

    transformation_logging.info(f'SUCESS To Read - {Path(file_path).name}')
    
    return df


def split_by_rule(df, is_valid, reason):

    cleasnsed = df.filter(func.coalesce(is_valid , func.lit('False')))

    error_records = df.filter(~func.coalesce(is_valid , func.lit('False')))\
                    .withColumn('err_reason' ,func.lit(f'{reason}'))

    return  cleasnsed , error_records


def columns_renaming(yellow_df):
    
    transformation_logging.info('Start Columns Normlization Process')

    columns_mapping = {

        "vendorID":"vendor_id",
        "tpep_pickup_datetime":"pickup_dttm",
        "tpep_dropoff_datetime":"dropoff_dttm",
        "RatecodeID":"rate_code_id",
        "PULocationID":"pickup_loc_id",
        "DOLocationID":"dropoff_loc_id"
    }

    df = yellow_df.withColumnsRenamed(columns_mapping)

    transformation_logging.info('Finished Successfully')

    return df
    


def add_id_column_to_yellow(yellow_df):
    
    transformation_logging.info('Start Add Id column Process')

    hash_params= [func.col('pickup_dttm'), func.col('dropoff_dttm')  , func.col('payment_type'),func.col('trip_distance')]
    window_func = Window.partitionBy('hash_val').orderBy(func.col('pickup_dttm') , func.col('dropoff_dttm'))
    df = yellow_df.withColumn('hash_val' , func.xxhash64(*hash_params))\
                  .withColumn('occ', func.row_number().over(window_func))\
                  .withColumn('trip_id' ,func.concat_ws("-", "hash_val", "occ"))


    transformation_logging.info(f"Finished Successfully")

    return df



def drop_passanger_count_lze_zero(yellow_df):

    transformation_logging.info('Drop Passangeer Count <= 0 Started')

    rule = func.col('passenger_count')  >  0
    df , error_records = split_by_rule(yellow_df , rule , 'Null values or values less then equal zero not allowed')

    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records



def pickup_date_valid_range(yellow_df , loading_date):

    transformation_logging.info('Drop pickup dates not in same month of loading date Started')

    rule = func.date_format(func.col('pickup_dttm') ,'yyyy-MM') == f'{loading_date}'
    df , error_records = split_by_rule(yellow_df , rule , 'pickup date in YYMM must be same  as loading date')

    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records


def dropoff_date_valid_range(yellow_df , loading_date):

    transformation_logging.info('Drop dropOff dates not in same month of loading date Started')

    rule = func.date_format(func.col('dropoff_dttm') ,'yyyy-MM') == f'{loading_date}'
    df , error_records = split_by_rule(yellow_df , rule , 'drop_off date in YYMM must be same  as loading date')

    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records



def drop_invalid_vendor_id(yellow_df):

    transformation_logging.info('Drop invalid vendor id')
    

    rule = func.col('vendor_id').isin([1,2,6,7])
    df , error_records = split_by_rule(yellow_df , rule , 'vendor_id value is not in allowed list [1,2,6,7]')


    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_invalid_time_ranges_pick_and_drop(yellow_df):
    
    transformation_logging.info('Drop invalid time ranges pick must be less than drop')    

    rule = func.col("pickup_dttm") < func.col("dropoff_dttm")
    df , error_records = split_by_rule(yellow_df , rule , 'Null values or pickup >= dropoff not allowed')


    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_invalid_trip_distance_records(yellow_df):

    transformation_logging.info('Drop invalid trip distances')
    

    rule = func.col("trip_distance") > 0
    df , error_records = split_by_rule(yellow_df , rule , 'Trip distance must be greater than zero')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records

def drop_invalid_pickup_locations_records(yellow_df):

    transformation_logging.info('Drop invalid pickup location records')
    
    rule = func.col("pickup_loc_id").between(1 , 265) 
    df , error_records = split_by_rule(yellow_df , rule , 'invalid pickup location must be between [1,265]')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_invalid_dropoff_locations_records(yellow_df):

    transformation_logging.info('Drop invalid dropoff location records')


    rule = func.col("dropoff_loc_id").between(1 , 265) 
    df , error_records = split_by_rule(yellow_df , rule , 'invalid drop off location must be between [1,265]')      


    transformation_logging.info(f"Finished Successfully")
    
    return df,error_records


def drop_invalid_payment_type_records(yellow_df):

    transformation_logging.info('Drop invalid payment type records')
    
    
    rule = func.col("payment_type").between(0 , 6) 
    df , error_records = split_by_rule(yellow_df , rule , 'Invalid payment type value must be between [0,6]')      


    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_non_postive_total_amounts_records(yellow_df):

    transformation_logging.info('Drop non positive total amount records')
    
    rule = func.col("total_amount") > 0
    df , error_records = split_by_rule(yellow_df , rule , 'total amount must be > 0')      
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records

def add_service_type(yellow_df):
    return yellow_df.withColumn('service_type' , func.lit(1))

def add_pickup_month(yellow_df):
    return yellow_df.withColumn('pickup_month' , func.to_date(func.date_format(func.col('pickup_dttm') ,'yyyy-MM')))
def add_dropoff_month(yellow_df):
    return yellow_df.withColumn('dropoff_month' , func.to_date(func.date_format(func.col('dropoff_dttm') ,'yyyy-MM')))


def run_transformation():
    
    args = argparse.ArgumentParser(description="Pipeline Configs for the transformation")
    args.add_argument('--loading_date' , default='2019-02' , type=str , help='loading_date')
    args.add_argument('--silver_path' , default='/user/linux/NYC-Taxi/silver/yellow' , type=str , help='loading_date')

    pipeline_configs = args.parse_args()
    transformation_logging.info(f'Start Transformation Process loading_date ={pipeline_configs.loading_date}')
    



    df1  = read_yellow_parque_file(pipeline_configs.loading_date)
    df1.cache(StorageLevel.MEMORY_AND_DISK)
    df2  = columns_renaming(df1)
    df4 , err2= drop_passanger_count_lze_zero(df2)
    df5 , err3= drop_invalid_vendor_id(df4)
    df6 , err4= drop_invalid_time_ranges_pick_and_drop(df5)
    df7 , err5= drop_invalid_trip_distance_records(df6)
    df8 , err6= drop_invalid_pickup_locations_records(df7)
    df9 , err7= drop_invalid_dropoff_locations_records(df8)
    df10 , err8 = drop_invalid_payment_type_records(df9)
    df11 , err9 = drop_non_postive_total_amounts_records(df10)
    df12, err10 = pickup_date_valid_range(df11 , pipeline_configs.loading_date)
    df13, err11 = dropoff_date_valid_range(df12 , pipeline_configs.loading_date)
    df14 = add_id_column_to_yellow(df13)
    df15 = add_service_type(df14)
    df16 = add_pickup_month(df15)
    df17 = add_dropoff_month(df16)

    dfs = [ err2, err3, err4, err5, err6,err7, err8, err9, err10 , err11]

    yellow_errors = reduce(
        lambda df1, df2: df1.unionByName(df2),
        dfs
    )



    create_loading_date_dir(pipeline_configs.silver_path , pipeline_configs.loading_date)

    store_df_as_parquet(df17 , 'yellow_cleansed_data' , pipeline_configs.silver_path , pipeline_configs.loading_date)
    store_df_as_parquet(yellow_errors , 'quarantine_data' , pipeline_configs.silver_path , pipeline_configs.loading_date)




    error_df = spark.read.parquet(f'hdfs://localhost:9000/user/linux/NYC-Taxi/silver/yellow/quarantine_data/date={pipeline_configs.loading_date}')

    error_summary = error_df.groupBy(func.col('err_reason')).agg(func.count('*').alias('error_counts'))
    


    df_string = error_summary._show_string(n=20, truncate=False, vertical=False)

    df1.unpersist(blocking=True)


    
    transformation_logging.info(f"Transformation Process for this loading_date = {pipeline_configs.loading_date} finished Sucessfully : {df_string}")



if __name__ == '__main__':
    run_transformation()
