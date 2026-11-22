from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from pathlib import Path
import argparse
# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Loading-zone-fact")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")\
                    .getOrCreate()


args = argparse.ArgumentParser(description='Dims Locations in HDFS')
args.add_argument('--base_dim_path' ,type=str ,  default='hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims')
args.add_argument('--loading_date' , type= str , default='2019-02' )
args.add_argument('--yellow_path', type=str , default='hdfs://localhost:9000/user/linux/NYC-Taxi/silver/yellow/yellow_cleansed_data')
args.add_argument('--fact_path' , type=str ,  default='hdfs://localhost:9000/user/linux/NYC-Taxi/gold/facts')
run_config = args.parse_args()

def read_dim_table(dim_base_path , dim_name):
    return spark.read.parquet(f"{dim_base_path}/{dim_name}")


def read_cleansed_yellow(path_to_yellow , loading_date):
    return spark.read.parquet(f'{path_to_yellow}/date={loading_date}')



def load_dims_table():

    dim_tables = [ 'zone_dim' , 'service_type_dim' , 'date_dime', 'time_dim']

    loaded_dims = []
    for dim in dim_tables:
        loaded_dims.append(read_dim_table(run_config.base_dim_path , dim))

    return loaded_dims




def store_zone_fact(zone_df , base_path):

    fact_path = f"{base_path}/zone_fact"
        
   

    zone_df.write \
    .mode("overwrite") \
    .partitionBy("service_key" , "zone_key" ) \
    .parquet(fact_path)




def fill_zone_fact():


    yellow_df = read_cleansed_yellow(run_config.yellow_path , run_config.loading_date)
    zone_dim , service_dim , date_dim , time_dim = load_dims_table()

    # SELECT Key Columns from dims for cross table building
 
    zone_df = zone_dim.select(func.col('location_id').alias('zone_key'))
    service_df = service_dim.select(func.col('service_id').cast('int').alias('service_key')).filter(func.col('service_id') == 1)
    date_df = date_dim.select(func.col('date_key')).filter(
            func.date_format(func.to_date("date_key", "yyyyMMdd"), "yyyy-MM") == run_config.loading_date

    )
    time_df = time_dim.select(func.col('hour')).distinct()

    dim_joins = zone_df.join(service_df , how='cross')\
                  .join(date_df , how='cross')\
                  .join(time_df,how='cross')


    dim_joins.createOrReplaceTempView('dims')
    yellow_df.createOrReplaceTempView('yellow')


    zone_fact = spark.sql(
        """
        SELECT d.zone_key , 
               d.service_key,
               d.date_key,
               d.hour,
               (
                    SELECT count(*) 
                    FROM yellow y 
                    WHERE y.pickup_loc_id = d.zone_key AND 
                         y.service_type = d.service_key  AND 
                         date_format(y.pickup_dttm, 'yyyyMMdd')= d.date_key AND
                         hour(y.pickup_dttm) = d.hour
                ) as pickup_counts,
                (
                    SELECT count(*) 
                    FROM yellow y 
                    WHERE y.dropoff_loc_id = d.zone_key AND 
                         y.service_type = d.service_key  AND 
                         date_format(y.dropoff_dttm, 'yyyyMMdd')= d.date_key AND
                         hour(y.dropoff_dttm) = d.hour
                ) as dropoff_counts
        FROM dims d 


    """
    )

    store_zone_fact(zone_fact , run_config.fact_path)



if __name__ == '__main__':
    fill_zone_fact()