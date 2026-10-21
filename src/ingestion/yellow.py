import subprocess
import sys
import argparse
import logging
from pathlib import Path
import time
log_path = Path(__file__).resolve().parents[2] / ('logs')

logging.basicConfig(

    filename=f'{log_path}/yellow_ingestion.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"

)

ingestion_logging = logging.getLogger('INGESTION')


def hdfs(*args):

    cmd = ['hdfs' , 'dfs' , *args]
    ingestion_logging.info(f"Executing {str(cmd)}")

    try:
        res = subprocess.run(cmd , check=True , capture_output=True , text=True)    
    except subprocess.CalledProcessError as error:
        ingestion_logging.error(f"Faild to execute {str(cmd)} - Exit Code: {error.returncode} - {error.stderr}")
        sys.exit(1)

    ingestion_logging.info(f"SUCCESS |  {str(cmd)}")






def load_from_stg_to_bronze(source_path , base_path , loading_date):

    #Log Job Start Time
    start_time = time.perf_counter()


    ingestion_logging.info(f"Yellow Move From Source to HDFS Bronze - {loading_date} - Started")
    yellow_bronze_path = f"{base_path}/bronze/yellow"
    stg_path = f"{base_path}/stg_temp_write/yellow"

    # Remove any data existing in target bronze - HDFS
    hdfs('-rm' ,'-r' , '-f' , f"{yellow_bronze_path}/date={loading_date}")

    # Remove previous data in STG_TEMP HDFS
    hdfs('-rm' ,'-r' , '-f' , f"{stg_path}")


    # Build The Temp STG Write Directory
    hdfs('-mkdir' ,'-p' , f"{stg_path}")

    # Build the Bronze date=YYYY-MM Directory
    hdfs('-mkdir' ,'-p' , f"{yellow_bronze_path}/date={loading_date}")


    # Move data from STG Area to STG TEMP WRITE HDFS
    hdfs("-put" , f"{source_path}/yellow_tripdata_{loading_date}.parquet" , f"{stg_path}")
    
    # Change pointer to be in bronze directory
    hdfs("-mv" , f"{stg_path}/yellow_tripdata_{loading_date}.parquet" , f"{yellow_bronze_path}/date={loading_date}/yellow_tripdata_{loading_date}.parquet")

    end_time = time.perf_counter()

    execution_time = (end_time - start_time) / 60

    ingestion_logging.info(f"Ingestion Job Finished with execution time {execution_time: .2f}")

    ingestion_logging.info("Ingestion From STG to HDFS Bronze Successed")

def main():

    source_data_path = Path(__file__).resolve().parents[2] / 'data'    
    args = argparse.ArgumentParser(description="Basic Config for the Yellow Pipeline")
    args.add_argument('--loading_date' ,type=str ,  default='2019-02' , help='Pipeline start date')
    args.add_argument('--source_path' ,type=Path ,  default=source_data_path , help='STG Path')
    args.add_argument('--base_path' ,type=str ,  default='/user/linux/NYC-Taxi' , help='HDFS Base Path')

    pipeline_config = args.parse_args()

    load_from_stg_to_bronze(pipeline_config.source_path , pipeline_config.base_path , pipeline_config.loading_date)




if __name__ == '__main__':

   main()

