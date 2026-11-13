import great_expectations as gx
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import sys
import argparse
from datetime import datetime
import calendar
# 1. Initialize a local Spark Session
spark = SparkSession.builder.appName("Yellow-Cleased-Quality-Check")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()


def read_cleasned_yellow_data(loading_date):

    yellow_df = spark.read.parquet(f"hdfs://localhost:9000/user/linux/NYC-Taxi/silver/yellow/yellow_cleansed_data/date={loading_date}")
    return yellow_df




def yellow_quality_checks(cleansed_yellow, loading_date):

    first_date_of_month = datetime.strptime(loading_date, "%Y-%m").date()
    last_day = calendar.monthrange(first_date_of_month.year , first_date_of_month.month)[1]
    last_date_of_month = first_date_of_month.replace(day=last_day)


    # Initialize an Ephemeral Data Context
    context = gx.get_context()

    # Connect Spark to GX (Data Source & Asset)
    data_source = context.data_sources.add_spark("NYC-Taxi")
    data_asset = data_source.add_dataframe_asset("Yellow_Asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("Whole-Yellow-Data")

    # Create an Expectation Suite
    suite = context.suites.add(gx.ExpectationSuite(name="yellow_suit_rules_container"))

    # Construct the rules
    gx_rules = [
        gx.expectations.ExpectColumnValuesToBeUnique(column="trip_id" , row_condition = "trip_id is not null", condition_parser="spark") , 
        gx.expectations.ExpectColumnValuesToBeBetween(column="passenger_count",min_value=0,strict_min=True,row_condition="passenger_count IS NOT NULL",condition_parser="spark"),
        gx.expectations.ExpectColumnValuesToBeInSet(column="vendor_id", value_set=[1,2,6,7] ,row_condition = "vendor_id is not null" , condition_parser="spark"),
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(column_A="dropoff_dttm",column_B="pickup_dttm",or_equal=False,row_condition="dropoff_dttm IS NOT NULL AND pickup_dttm IS NOT NULL",condition_parser="spark"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="trip_distance",min_value=0,strict_min=True, row_condition = "trip_distance IS NOT NULL" , condition_parser="spark"),        
        gx.expectations.ExpectColumnValuesToBeBetween(column="pickup_loc_id",min_value=1,max_value=265,row_condition="pickup_loc_id IS NOT NULL",condition_parser="spark"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="dropoff_loc_id",min_value=1,max_value=265,row_condition="dropoff_loc_id IS NOT NULL",condition_parser="spark"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="payment_type",min_value=0,max_value=6,row_condition="payment_type IS NOT NULL",condition_parser="spark"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount",min_value=0,strict_min=True, row_condition = "total_amount IS NOT NULL" , condition_parser="spark"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="pickup_month",min_value=first_date_of_month,max_value=last_date_of_month,row_condition="pickup_month IS NOT NULL",condition_parser="spark"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="dropoff_month",min_value=first_date_of_month,max_value=last_date_of_month,row_condition="dropoff_month IS NOT NULL",condition_parser="spark")
    ]


    # Rule Register
    for rule in gx_rules:
        suite.add_expectation(rule)


    # Pass the Spark DataFrame and Run Validation
    batch = batch_definition.get_batch(batch_parameters={"dataframe": cleansed_yellow})
    validation_result = batch.validate(suite)

    if validation_result.success == False:
        print(validation_result)
        print("FAILED")
        sys.exit(1)
    else:
        print("PASSED")


def run_yellow_quality_gate():
    args = argparse.ArgumentParser(description="Quality Gate Params")

    args.add_argument('--loading_date' ,default="2019-02", type=str, help= "loading date for the data partitionto be checked")
        
    gate_configs = args.parse_args()

    yellow_df = read_cleasned_yellow_data(gate_configs.loading_date)
    yellow_quality_checks(yellow_df , gate_configs.loading_date)
    # yellow_df.printSchema()


if __name__ == '__main__':
    run_yellow_quality_gate()
