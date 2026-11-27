from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path
import argparse
# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("fhvhv-Loading-fhvhv-fact")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")\
                    .getOrCreate()




def read_cleansed_fhvhv_from_silver(loading_date, base_path):

    path = f"{base_path}/silver/fhvhv/fhvhv_cleansed_data/date={loading_date}"

    fhvhv_df = spark.read.parquet(path)

    return fhvhv_df


def fill_fhvhv_fact(fhvhv_df):

    fhvhv_fct = fhvhv_df.select(
        func.col('trip_id'),
        func.col('service_type').alias('service_type_key'),
        func.col('hvfhs_license_num').alias('hvfhs_license_num_key'),
        func.col('pickup_loc_id').alias('pickup_zone_key'),
        func.col('dropoff_loc_id').alias('dropoff_zone_key'),
        func.col('trip_distance'),
        func.col('trip_time'),
        func.col('driver_pay'),
        func.col('base_amount'),
        func.date_format(func.col('pickup_dttm') ,'yyyyMMdd').alias('pick_date_key'),
        func.date_format(func.col('dropoff_dttm') ,'yyyyMMdd').alias('drop_date_key'),
        func.col('platform_take'),
        func.col('driver_pay_ratio'),
        func.col('Pay_per_mile'),
        func.col('Pay_per_minute')
        

    )
                         
    
    return fhvhv_fct

def store_fhvhv_fact(fhvhv_df , base_path):

    fact_path = f"{base_path}/gold/facts/fhvhv_fact"
        
   

    fhvhv_df.write \
    .mode("overwrite") \
    .partitionBy("hvfhs_license_num_key" , "pick_date_key") \
    .parquet(fact_path)


def run_fill_fhvhv_fct(loading_date , base_path):
    
    cleansed_fhvhv_df = read_cleansed_fhvhv_from_silver(
        loading_date,
        base_path
    )

    fhvhv_fct = fill_fhvhv_fact(cleansed_fhvhv_df)

    store_fhvhv_fact(
        fhvhv_fct,
        base_path
    )


def main():
    
    args = argparse.ArgumentParser(description="Fill Trip Fact Params with fhvhv data")
    args.add_argument('--loading_date' , type=str , default="2019-02", help= "loading date for processing data")
    args.add_argument('--base_path' , type=str , default="hdfs://localhost:9000/user/linux/NYC-Taxi/", help= "HDFS Base Path to Data Layers")

    run_configs = args.parse_args()
    
    run_fill_fhvhv_fct(
        run_configs.loading_date,
        run_configs.base_path
    )



if __name__ == '__main__':
    main()