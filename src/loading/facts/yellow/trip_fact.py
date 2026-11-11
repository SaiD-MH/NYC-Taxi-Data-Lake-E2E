from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path
import argparse
# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-trip-fact")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()




def read_cleansed_yellow_from_silver(loading_date, base_path):

    path = f"{base_path}/silver/yellow/yellow_cleansed_data/date={loading_date}"

    yellow_df = spark.read.parquet(path)

    return yellow_df


def fill_trip_fact(yellow_df):

    trip_fct = yellow_df.select(
        func.col('trip_id'),
        func.date_format(func.col('pickup_dttm') ,'yyyyMMdd').alias('pick_date_key'),
        func.date_format(func.col('dropoff_dttm') ,'yyyyMMdd').alias('drop_date_key'),
        func.col('pickup_loc_id').alias('pick_up_zone_key'),
        func.col('dropoff_loc_id').alias('drop_off_zone_key'),
        func.col('service_type').alias('service_type_key'),
        func.col('vendor_id').alias('vendor_key'),
        func.col('rate_code_id').alias('ratecode_key'),
        func.date_format(func.col('pickup_dttm') , "HHmm" ).alias('pick_time_key'),
        func.date_format(func.col('dropoff_dttm') , "HHmm" ).alias('drop_time_key'),
        
    ).withColumn('trip_type_key', func.lit(1))
    
    trip_fct.show(10,truncate=False)
    
    return trip_fct

def store_trip_fact(trip_df , base_path):

    fact_path = f"{base_path}/gold/facts/trip_fact"
        
    trip_df.write \
        .format("parquet") \
        .option("path", fact_path) \
        .mode("overwrite") \
        .saveAsTable("rate_code_dim")

    trip_df.write \
    .mode("overwrite") \
    .partitionBy("service_type_key" , "pick_date_key") \
    .parquet(fact_path)


def run_fill_trip_fact(loading_date , base_path):
    
    cleansed_yellow_df = read_cleansed_yellow_from_silver(
        loading_date,
        base_path
    )

    trip_fact = fill_trip_fact(cleansed_yellow_df)

    store_trip_fact(
        trip_fact,
        base_path
    )


def main():
    
    args = argparse.ArgumentParser(description="Fill Trip Fact Params with yellow data")
    args.add_argument('--loading_date' , type=str , default="2019-02", help= "loading date for processing data")
    args.add_argument('--base_path' , type=str , default="hdfs://localhost:9000/user/linux/NYC-Taxi/", help= "HDFS Base Path to Data Layers")

    run_configs = args.parse_args()
    
    run_fill_trip_fact(
        run_configs.loading_date,
        run_configs.base_path
    )



if __name__ == '__main__':
    main()