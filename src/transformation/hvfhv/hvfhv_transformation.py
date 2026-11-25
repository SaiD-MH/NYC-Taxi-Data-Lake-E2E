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

    filename=f'{log_path}/fhvhv.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(funcName)s | %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"

)

transformation_logging = logging.getLogger('TRANSFORMATION')



spark = (
    SparkSession.builder
    .appName("NYC-Taxi - fhvhv Transformation")
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


def create_loading_date_dir(silver_fhvhv_path , loading_date):

    hdfs('-mkdir' , '-p' , f'{silver_fhvhv_path}/fhvhv_cleansed_data/date={loading_date}')
    hdfs('-mkdir' , '-p' , f'{silver_fhvhv_path}/quarantine_data/date={loading_date}')


def store_df_as_parquet(df , table_name , silver_path , loading_date):
    df.write\
      .mode('overwrite')\
      .parquet(f'{silver_path}/{table_name}/date={loading_date}')


def read_fhvhv_parque_file(loading_date):
    file_path = (
        f"hdfs://localhost:9000/user/linux/NYC-Taxi/"
        f"bronze/fhvhv/date={loading_date}/"
        f"fhvhv_tripdata_{loading_date}.parquet"
    )

    transformation_logging.info(f"read fhvhv parquet file path: {file_path}")

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


def columns_renaming(fhvhv_df):
    
    transformation_logging.info('Start Columns Normlization Process')

    columns_mapping = {

        "originating_base_num":"receiver_base_number",
        "dispatching_base_num":"trip_executor",
        "pickup_datetime":"pickup_dttm",
        "dropoff_datetime":"dropoff_dttm",
        "PUlocationID":"pickup_loc_id",
        "DOlocationID":"dropoff_loc_id",
        "trip_miles":"trip_distance",
        "request_datetime":"request_dttm",
        "base_passenger_fare":"base_amount",
        "wav_request_flag":"accessible_request_flag",
        "wav_match_flag":"accessible_match_flag",
        "on_scene_datetime":"accessiable_on_scene_dttm",
        "shared_request_flag":"passenger_agreed_to_shared_ride_flag",
        "shared_match_flag":"passenger_actually_shared_ride_flag",
        "access_a_ride_flag":"mta_access_a_ride_trip_flag"
    }

    df = fhvhv_df.withColumnsRenamed(columns_mapping)

    transformation_logging.info('Finished Successfully')

    return df
    


def add_id_column_to_fhvhv(fhvhv_df):
    
    transformation_logging.info('Start Add Id column Process')

    hash_params= [

                  func.col('hvfhs_license_num'), 
                  func.col('trip_executor'),
                  func.col('receiver_base_number'),
                  func.col('request_dttm'),
                  func.col('pickup_dttm'),
                  func.col('dropoff_dttm'),

                ]
    window_func = Window.partitionBy('hash_val').orderBy(func.col('pickup_dttm') , func.col('dropoff_dttm'))
    df = fhvhv_df.withColumn('hash_val' , func.xxhash64(*hash_params))\
                  .withColumn('occ', func.row_number().over(window_func))\
                  .withColumn('trip_id' ,func.concat_ws("-", "hash_val", "occ"))


    transformation_logging.info(f"Finished Successfully")

    return df


def add_service_type_column(fhv_df):
    transformation_logging.info('Shared Flag Normalization Started')

    
    df = fhv_df.withColumn('service_type' , func.lit(4))

    transformation_logging.info(f"Finished Successfully")

    return df

def add_trip_type_column(fhv_df):

    transformation_logging.info('Adding Trip Type column Started')
    
    df = fhv_df.withColumn('trip_type' , func.lit(2))

    transformation_logging.info(f"Finished Successfully")

    return df



def add_pickup_month(fhvhv_df):
        
    transformation_logging.info('Adding Pickup Month Started')

    return fhvhv_df.withColumn('pickup_month' , func.to_date(func.date_format(func.col('pickup_dttm') ,'yyyy-MM')))


def add_dropoff_month(fhvhv_df):
    transformation_logging.info('Adding dropoff Month Started')
    return fhvhv_df.withColumn('dropoff_month' , func.to_date(func.date_format(func.col('dropoff_dttm') ,'yyyy-MM')))

def add_airport_flag(fhvhv_df):
    transformation_logging.info('Adding airport flag Started')
    return fhvhv_df.withColumn('airport_flag' , func.when(func.col('airport_fee') > 0 , 'Y' ).otherwise('N'))


def pickup_date_valid_range(fhvhv_df , loading_date):

    transformation_logging.info('Drop pickup dates not in same month of loading date Started')

    rule = func.date_format(func.col('pickup_dttm') ,'yyyy-MM') == f'{loading_date}'
    df , error_records = split_by_rule(fhvhv_df , rule , 'pickup date in YYMM must be same  as loading date')

    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records


def dropoff_date_valid_range(fhvhv_df , loading_date):

    transformation_logging.info('Drop dropOff dates not in same month of loading date Started')

    rule = func.date_format(func.col('dropoff_dttm') ,'yyyy-MM') == f'{loading_date}'
    df , error_records = split_by_rule(fhvhv_df , rule , 'drop_off date in YYMM must be same  as loading date')

    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records





def drop_invalid_time_ranges_pick_and_drop(fhvhv_df):
    
    transformation_logging.info('Drop invalid time ranges pick must be less than drop')    

    rule = (func.col("request_dttm") < func.col("pickup_dttm")) & (func.col("pickup_dttm") < func.col("dropoff_dttm"))
    df , error_records = split_by_rule(fhvhv_df , rule , 'Null values or pickup >= dropoff not allowed')


    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_invalid_on_scene_valid_range(fhvhv_df , loading_date):

    transformation_logging.info('Drop pickup dates not in same month of loading date Started')

    rule = func.date_format(func.col('accessiable_on_scene_dttm') ,'yyyy-MM') == f'{loading_date}'
    df , error_records = split_by_rule(fhvhv_df , rule , 'accessiable_on_scene_dttm date in YYMM must be same  as loading date')

    
    transformation_logging.info(f"Finished Successfully")

    return df , error_records

def drop_invalid_pickup_locations_records(fhvhv_df):

    transformation_logging.info('Drop invalid pickup location records')
    
    rule = func.col("pickup_loc_id").between(1 , 265) 
    df , error_records = split_by_rule(fhvhv_df , rule , 'invalid pickup location must be between [1,265]')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_invalid_dropoff_locations_records(fhvhv_df):

    transformation_logging.info('Drop invalid dropoff location records')


    rule = func.col("dropoff_loc_id").between(1 , 265) 
    df , error_records = split_by_rule(fhvhv_df , rule , 'invalid drop off location must be between [1,265]')      


    transformation_logging.info(f"Finished Successfully")
    
    return df,error_records



def drop_invalid_trip_distance_records(fhvhv_df):

    transformation_logging.info('Drop invalid trip distances')
    

    rule = func.col("trip_distance") > 0
    df , error_records = split_by_rule(fhvhv_df , rule , 'Trip distance must be greater than zero')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_invalid_trip_time_records(fhvhv_df):

    transformation_logging.info('Drop invalid trip time')
    

    rule = func.col("trip_time") > 0
    df , error_records = split_by_rule(fhvhv_df , rule , 'Trip time must be greater than zero')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_invalid_hvfhs_license_num(fhvhv_df):


    transformation_logging.info('Drop invalid trip hvfhs_license_num')
    

    rule = func.col("hvfhs_license_num").isin('HV0002','HV0003','HV0004','HV0005')

    df , error_records = split_by_rule(fhvhv_df , rule , 'Trip license number must be in this allowed list [HV0002,HV0003,HV0004,HV0005]')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def drop_missing_receiver_executor_base_number(fhvhv_df):

    transformation_logging.info('Drop invalid trip hvfhs_license_num')
    

    rule = (func.col("receiver_base_number").isNotNull()) & (func.col("trip_executor").isNotNull()) 

    df , error_records = split_by_rule(fhvhv_df , rule , 'receiver_base_number or trip_executor can not be missing')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records



def drop_invalid_base_amount_records(fhvhv_df):

    transformation_logging.info('Drop invalid trip base_amount')
    

    rule = func.col("base_amount") > 0
    df , error_records = split_by_rule(fhvhv_df , rule , 'Trip base_amount must be greater than zero')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records



def drop_invalid_driver_pay(fhvhv_df):

    transformation_logging.info('Drop invalid driver pay')
    

    rule = func.col("driver_pay") > 0
    df , error_records = split_by_rule(fhvhv_df , rule , 'Trip driver_pay must be greater than zero')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records



def drop_invalid_shared_trip_info(fhvhv_df):

    transformation_logging.info('Drop invalid Shared Trip Info')
    

    rule = (func.col("passenger_agreed_to_shared_ride_flag").isin('Y' , 'N')) & ((func.col("passenger_actually_shared_ride_flag").isin('Y' , 'N'))) 
    df , error_records = split_by_rule(fhvhv_df , rule , 'Shared Trip info not valid must be in [ Y , N]')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def add_total_amount(fhvhv_df):

    return fhvhv_df.withColumn( 'total_amount',
    func.coalesce(func.col('base_amount'), func.lit(0)) +
    func.coalesce(func.col('tolls'), func.lit(0)) +
    func.coalesce(func.col('bcf'), func.lit(0)) +
    func.coalesce(func.col('sales_tax'), func.lit(0)) +
    func.coalesce(func.col('congestion_surcharge'), func.lit(0)) +
    func.coalesce(func.col('airport_fee'), func.lit(0)) +
    func.coalesce(func.col('tips'), func.lit(0))
    )


def drop_invalid_wav_request_flag(fhvhv_df):

    transformation_logging.info('Drop invalid Shared Trip Info')
    

    rule = func.col("passenger_agreed_to_shared_ride_flag").isin('Y' , 'N')
    df , error_records = split_by_rule(fhvhv_df , rule , 'wav_request_flag info not valid must be in [ Y , N]')
    

    transformation_logging.info(f"Finished Successfully")
    
    return df , error_records


def add_trip_enrichment_columns(fhvhv_df):
    transformation_logging.info('Drop Platform Take Calculation')

    return fhvhv_df.withColumn('platform_take' , func.col('base_amount') - func.col('driver_pay')  )\
                   .withColumn('driver_pay_ratio' , func.col('driver_pay') / func.col('base_amount'))\
                   .withColumn('Pay_per_mile' ,  func.col('driver_pay') / func.col('trip_distance'))\
                   .withColumn('Pay_per_minute', func.col('driver_pay') / (func.col('trip_time') / 60 ))


def run_transformation():
    
    args = argparse.ArgumentParser(description="Pipeline Configs for the transformation")
    args.add_argument('--loading_date' , default='2019-02' , type=str , help='loading_date')
    args.add_argument('--silver_path' , default='/user/linux/NYC-Taxi/silver/fhvhv' , type=str , help='loading_date')

    pipeline_configs = args.parse_args()
    transformation_logging.info(f'Start Transformation Process loading_date ={pipeline_configs.loading_date}')
    



    df1  = read_fhvhv_parque_file(pipeline_configs.loading_date)
    df2  = columns_renaming(df1)
    df3 = add_id_column_to_fhvhv(df2)
    df4 = add_service_type_column(df3)
    df5 = add_trip_type_column(df4)
    df6 = add_pickup_month(df5)
    df7 = add_dropoff_month(df6)
    df8 , err1 = pickup_date_valid_range(df7 , pipeline_configs.loading_date)
    df9, err2  = dropoff_date_valid_range(df8,  pipeline_configs.loading_date)
    df10,err3  = drop_invalid_time_ranges_pick_and_drop(df9)
    df11,err4 = drop_invalid_pickup_locations_records(df10)
    df12,err5 = drop_invalid_dropoff_locations_records(df11)
    df13,err6 = drop_invalid_trip_distance_records(df12)
    df14,err7 = drop_invalid_trip_time_records(df13)
    df15,err8 = drop_invalid_hvfhs_license_num(df14)
    df16,err9 = drop_missing_receiver_executor_base_number(df15)
    df17,err10 = drop_invalid_base_amount_records(df16)
    df18,err11 = drop_invalid_driver_pay(df17)
    df19,err12 = drop_invalid_shared_trip_info(df18)
    df20,err13 = drop_invalid_wav_request_flag(df19)
    df21,err14 = drop_invalid_on_scene_valid_range(df20 , pipeline_configs.loading_date)
    df22 = add_airport_flag(df21)
    df23 = add_total_amount(df22)
    df24 = add_trip_enrichment_columns(df23)



    # df24.show()

    # # spark.catalog.clearCache()

    
    
    
    dfs = [ err1,err2, err3, err4, err5, err6,err7, err8, err9, err10 , err11, err12 , err13,err14]

    fhvhv_errors = reduce(
        lambda df1, df2: df1.unionByName(df2),
        dfs
    )



    create_loading_date_dir(pipeline_configs.silver_path , pipeline_configs.loading_date)

    store_df_as_parquet(df21 , 'fhvhv_cleansed_data' , pipeline_configs.silver_path , pipeline_configs.loading_date)
    store_df_as_parquet(fhvhv_errors , 'quarantine_data' , pipeline_configs.silver_path , pipeline_configs.loading_date)




    error_df = spark.read.parquet(f'hdfs://localhost:9000/user/linux/NYC-Taxi/silver/fhvhv/quarantine_data/date={pipeline_configs.loading_date}')

    error_summary = error_df.groupBy(func.col('err_reason')).agg(func.count('*').alias('error_counts'))
    


    df_string = error_summary._show_string(n=20, truncate=False, vertical=False)



    
    transformation_logging.info(f"Transformation Process for this loading_date = {pipeline_configs.loading_date} finished Sucessfully : {df_string}")

 

if __name__ == '__main__':
    run_transformation()