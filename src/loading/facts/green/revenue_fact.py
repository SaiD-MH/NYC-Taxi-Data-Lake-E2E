from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path
import argparse
# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-revenue-fact")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()




def read_cleansed_yellow_from_silver(loading_date, base_path):

    path = f"{base_path}/silver/yellow/yellow_cleansed_data/date={loading_date}"

    yellow_df = spark.read.parquet(path)

    return yellow_df


def fill_revenue_fact(yellow_df):

    revenue_fct = yellow_df.withColumn('airport_flg' , 
                    func.when(func.col('airport_fee').isNull() ,'unknown')\
                    .when(func.col('airport_fee')<= 0 , 'N' )\
                    .otherwise('Y')
                    ).select(
                            func.col('trip_id'),
                            func.date_format(func.col('pickup_dttm') ,'yyyyMMdd').alias('pick_date_key'),
                            func.date_format(func.col('dropoff_dttm') ,'yyyyMMdd').alias('drop_date_key'),
                            func.col('service_type').alias('service_type_key'),
                            func.col('total_amount').alias('revenue'),
                            func.col('payment_type').alias('payment_type_key'),
                            # func.col('airport_fee')
                        ).withColumn('trip_type_key', func.lit(1))\
    
     



    
    revenue_fct.filter(func.col('airport_flg')== 'unknown' ).show(10,truncate=False)
    
    return revenue_fct

def store_revenue_fact(revenue_df , base_path):

    fact_path = f"{base_path}/gold/facts/revenue_fact"
        
   

    revenue_df.write \
    .mode("overwrite") \
    .partitionBy("service_type_key" , "pick_date_key") \
    .parquet(fact_path)


def run_fill_revenue_fct(loading_date , base_path):
    
    cleansed_yellow_df = read_cleansed_yellow_from_silver(
        loading_date,
        base_path
    )

    revenue_fct = fill_revenue_fact(cleansed_yellow_df)

    store_revenue_fact(
        revenue_fct,
        base_path
    )


def main():
    
    args = argparse.ArgumentParser(description="Fill Trip Fact Params with yellow data")
    args.add_argument('--loading_date' , type=str , default="2019-02", help= "loading date for processing data")
    args.add_argument('--base_path' , type=str , default="hdfs://localhost:9000/user/linux/NYC-Taxi/", help= "HDFS Base Path to Data Layers")

    run_configs = args.parse_args()
    
    run_fill_revenue_fct(
        run_configs.loading_date,
        run_configs.base_path
    )



if __name__ == '__main__':
    main()