from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import argparse
from pathlib import Path
import calendar
from datetime import date


spark = SparkSession.builder.appName("Yellow-Loading-Date-Dim")\
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
                    .getOrCreate()




def get_full_month_dates(loading_date:str):

    cur_year = int(loading_date.split('-')[0])
    cur_month = int(loading_date.split('-')[1])

    num_days = calendar.monthrange(cur_year, cur_month)[1]   # (weekday of the 1st, number of days)

    month_data = []
    for cur_day in range(1, num_days + 1):
        d = date(cur_year, cur_month, cur_day)
        date_key = str(d).replace('-' ,'')
        week_day = d.weekday()
        day = cur_day
        month = cur_month
        year = cur_year
        week_end = "Y" if week_day in (5, 6) else "N"
        month_data.append((date_key , d , day , month , year, week_day , week_end))

    return month_data




def run_fill_date_dim():

    args = argparse.ArgumentParser(description="passing loading_date to get month dates info details")
    args.add_argument('--loading_date' , type=str , default="2019-02" , help="pass date in YYYY-MM format")

    loading_arg = args.parse_args()


    dates_details = get_full_month_dates(loading_arg.loading_date)

    dates_df = spark.createDataFrame(dates_details , ['date_key' , 'date','day_of_month', 'month' , 'year' , 'week_day','is_week_end'])

    dates_df.show(30 , truncate=False)

    gold_path =f"hdfs://localhost:9000/user/linux/NYC-Taxi/gold/dims/date_dime/date={loading_arg.loading_date}"

    dates_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_path)





def main():
    run_fill_date_dim()


if __name__ == '__main__':
    main()



