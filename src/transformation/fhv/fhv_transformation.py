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


def hdfs(*args):

    cmd = ['hdfs' , 'dfs' , *args]
    transformation_logging.info(f"Executing {str(cmd)}")

    try:
        res = subprocess.run(cmd , check=True , capture_output=True , text=True)    
    except subprocess.CalledProcessError as error:
        transformation_logging.error(f"Faild to execute {str(cmd)} - Exit Code: {error.returncode} - {error.stderr}")
        sys.exit(1)

    transformation_logging.info(f"SUCCESS |  {str(cmd)}")


def create_loading_date_dir(silver_fhv_path , loading_date):

    hdfs('-mkdir' , '-p' , f'{silver_fhv_path}/fhv_cleansed_data/date={loading_date}')
    hdfs('-mkdir' , '-p' , f'{silver_fhv_path}/quarantine_data/date={loading_date}')


def store_df_as_parquet(df , table_name , silver_path , loading_date):
    df.write\
      .mode('overwrite')\
      .parquet(f'{silver_path}/{table_name}/date={loading_date}')


def read_fhv_parque_file(loading_date):
    file_path = (
        f"hdfs://localhost:9000/user/linux/NYC-Taxi/"
        f"bronze/fhv/date={loading_date}/"
        f"fhv_tripdata_{loading_date}.parquet"
    )

    transformation_logging.info(f"read fhv parquet file path: {file_path}")

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


def columns_renaming(fhv_df):
    
    transformation_logging.info('Start Columns Normlization Process')

    columns_mapping = {

        "dispatching_base_num":"receiver_base_number",
        "pickup_datetime":"pickup_dttm",
        "dropoff_datetime":"dropoff_dttm",
        "PUlocationID":"pickup_loc_id",
        "DOlocationID":"dropoff_loc_id",
        "SR_Flag":"shared_trip",
        "Affiliated_base_number" :"trip_executor"
    }

    df = fhv_df.withColumnsRenamed(columns_mapping)

    transformation_logging.info('Finished Successfully')

    return df
    


def add_id_column_to_fhv(fhv_df):
    
    transformation_logging.info('Start Add Id column Process')

    hash_params= [func.col('receiver_base_number') , func.col('pickup_dttm'), func.col('dropoff_dttm')  , 
                  func.col('pickup_loc_id'),func.col('dropoff_loc_id'),func.col('shared_trip'),func.col('trip_executor')]
    
    window_func = Window.partitionBy('hash_val').orderBy(func.col('pickup_dttm') , func.col('dropoff_dttm'))
    df = fhv_df.withColumn('hash_val' , func.xxhash64(*hash_params))\
                  .withColumn('occ', func.row_number().over(window_func))\
                  .withColumn('trip_id' ,func.concat_ws("-", "hash_val", "occ"))


    transformation_logging.info(f"Finished Successfully")

    return df



def drop_missig_receiver(fhv_df):

    transformation_logging.info('Drop missing receiver_base_number Started')

    rule = func.col('receiver_base_number').isNotNull()
    df , error_records = split_by_rule(fhv_df , rule , 'Null values are not allowed')

    transformation_logging.info(f"Finished Successfully")

    return df , error_records




def drop_invalid_pickup_date_values(fhv_df , loading_date):

    transformation_logging.info('Drop pickup dates not in same month of loading date Started')

    rule = (func.col('pickup_dttm').isNotNull() )& (func.date_format(func.col('pickup_dttm') ,'yyyy-MM') == f'{loading_date}')
    df , error_records = split_by_rule(fhv_df , rule , 'pickup date in YYMM must be same as loading date or it cant be missing')
    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records


def drop_invalid_dropoff_date_values(fhv_df , loading_date):

    transformation_logging.info('Drop DropOff dates not in same month of loading date Started')

    rule = (func.col('dropoff_dttm').isNotNull()) & (func.date_format(func.col('dropoff_dttm') ,'yyyy-MM') == f'{loading_date}')
    df , error_records = split_by_rule(fhv_df , rule , 'dropoff date in YYMM must be same as loading date or it cant be missing')
    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records


def drop_invalid_pickup_dropoff_date_ranges(fhv_df):
    
    transformation_logging.info('Drop pickup >= dropoff Started')
    
    rule = func.col('pickup_dttm') < func.col('dropoff_dttm') 
    df , error_records = split_by_rule(fhv_df , rule , 'pickup datetime must be less than dropoff')
    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records



def drop_invalid_pickup_location_id(fhv_df):
    
    transformation_logging.info('Drop invalid location IDs Started')
    
    rule = (func.col('pickup_loc_id').isNotNull()) & (func.col('pickup_loc_id').between(1,265))
    df , error_records = split_by_rule(fhv_df , rule , 'pickup_loc_id must be between 1 and 265')
    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records


def drop_invalid_dropoff_location_id(fhv_df):
    
    transformation_logging.info('Drop invalid location IDs Started')
    
    rule = (func.col('dropoff_loc_id').isNotNull()) & (func.col('dropoff_loc_id').between(1,265))
    df , error_records = split_by_rule(fhv_df , rule , 'dropoff_loc_id must be between 1 and 265')
    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records

def drop_invalid_trip_executor(fhv_df):
    transformation_logging.info('Drop invalid trip executor Started')
        
    rule = func.col('trip_executor').isNotNull()
    df , error_records = split_by_rule(fhv_df , rule , 'trip_executor can not be null')
    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records



def normalize_shared_trip_flag(fhv_df):
    transformation_logging.info('Shared Flag Normalization Started')

    transformation_logging.info(f"Finished Successfully")
    df = fhv_df.withColumn('shared_trip_flag' , func.when(func.col('shared_trip').isNull(), 'N' ).otherwise('Y') )

    return df

def add_service_type_column(fhv_df):
    transformation_logging.info('Shared Flag Normalization Started')

    
    df = fhv_df.withColumn('service_type' , func.lit(3))

    transformation_logging.info(f"Finished Successfully")

    return df

def add_trip_type_column(fhv_df):

    transformation_logging.info('Adding Trip Type column Started')
    
    df = fhv_df.withColumn('trip_type' , func.lit(2))

    transformation_logging.info(f"Finished Successfully")

    return df


def add_pickup_month(fhv_df):
    return fhv_df.withColumn('pickup_month' , func.to_date(func.date_format(func.col('pickup_dttm') ,'yyyy-MM')))

def add_dropoff_month(fhv_df):
    return fhv_df.withColumn('dropoff_month' , func.to_date(func.date_format(func.col('dropoff_dttm') ,'yyyy-MM')))

def add_vendor_id(fhv_df):
    return fhv_df.withColumn('vendor_id' , func.lit(0))

def add_rate_code(fhv_df):
    return fhv_df.withColumn('rate_code' , func.lit(100))

def run_transformation():
    
    args = argparse.ArgumentParser(description="Pipeline Configs for the transformation")
    args.add_argument('--loading_date' , default='2019-02' , type=str , help='loading_date')
    args.add_argument('--silver_path' , default='/user/linux/NYC-Taxi/silver/fhv' , type=str , help='loading_date')

    pipeline_configs = args.parse_args()
    transformation_logging.info(f'Start Transformation Process loading_date ={pipeline_configs.loading_date}')
    



    df1  = read_fhv_parque_file(pipeline_configs.loading_date)
    df1.persist(StorageLevel.MEMORY_AND_DISK)
    df2  = columns_renaming(df1)
    df3 , err1 = drop_missig_receiver(df2)
    df4 , err2 = drop_invalid_pickup_date_values(df3 , pipeline_configs.loading_date)
    df5 , err3 = drop_invalid_dropoff_date_values(df4,pipeline_configs.loading_date)
    df6 , err4 = drop_invalid_pickup_location_id(df5)
    df7 , err5 = drop_invalid_dropoff_location_id(df6)
    df8 , err6 = drop_invalid_trip_executor(df7)
    df9  = normalize_shared_trip_flag(df8)
    df10 = add_service_type_column(df9)
    df11 = add_id_column_to_fhv(df10)
    df12 = add_trip_type_column(df11)
    df13 = add_pickup_month(df12)
    df14 = add_dropoff_month(df13)
    df15 = add_rate_code(df14)
    df16 = add_vendor_id(df15)





    dfs = [ err1, err2 , err3, err4, err5, err6 ] 

    fhv_errors = reduce(
        lambda df1, df2: df1.unionByName(df2),
        dfs
    )



    create_loading_date_dir(pipeline_configs.silver_path , pipeline_configs.loading_date)

    store_df_as_parquet(df16 , 'fhv_cleansed_data' , pipeline_configs.silver_path , pipeline_configs.loading_date)
    store_df_as_parquet(fhv_errors , 'quarantine_data' , pipeline_configs.silver_path , pipeline_configs.loading_date)




    error_df = spark.read.parquet(f'hdfs://localhost:9000/user/linux/NYC-Taxi/silver/fhv/quarantine_data/date={pipeline_configs.loading_date}')

    error_summary = error_df.groupBy(func.col('err_reason')).agg(func.count('*').alias('error_counts'))
    


    df_string = error_summary._show_string(n=20, truncate=False, vertical=False)

    df1.unpersist(blocking=True)


    
    transformation_logging.info(f"Transformation Process for this loading_date = {pipeline_configs.loading_date} finished Sucessfully : {df_string}")



if __name__ == '__main__':
    run_transformation()