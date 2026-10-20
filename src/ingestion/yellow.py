import subprocess
import sys

LOADING_DATE='2019-02'
BASE_PATH='/user/linux/NYC-Taxi'
STG_PATH=f'{BASE_PATH}/stg_temp_write/yellow'
YELLOW_BRONZE_PATH=f'{BASE_PATH}/bronze/yellow'
SOURCE_PATH ="../../data"

def delete_inital_path_of_bronze():

    results = subprocess.run(['hdfs' , 'dfs' , '-rm' ,'-r' , '-f' , f"{YELLOW_BRONZE_PATH}/date={LOADING_DATE}"] , capture_output=True , check=True , text=True)
        
    if results.returncode != 0:
        print(f"ERROR: {results.stderr}")
        sys.exit(1)

def delete_inital_path_of_stg():

    
    results = subprocess.run(['hdfs' , 'dfs' , '-rm' ,'-r' , '-f' , f"{STG_PATH}"] , capture_output=True , check=True , text=True)
        
    if results.returncode != 0:
            print(f"ERROR: {results.stderr}")
            sys.exit(1)

def construct_path_to_bronze():
    
    results = subprocess.run(['hdfs' , 'dfs' , '-mkdir' ,'-p' , f"{YELLOW_BRONZE_PATH}/date={LOADING_DATE}"] , capture_output=True , check=True , text=True)
    print("Dir created")
    if results.returncode != 0:
            print(f"ERROR: {results.stderr}")
            sys.exit(1)

def construct_path_to_stg():
    
    try:
        results = subprocess.run(['hdfs' , 'dfs' , '-mkdir' ,'-p' , f"{STG_PATH}"] , capture_output=True , check=True , text=True)
        print("Dir created")
    except subprocess.CalledProcessError:
        print(f"ERROR: {results.stderr}")
        sys.exit(1)
    

def move_to_stg_temp_writing():
    
    results = subprocess.run(["hdfs" , "dfs" , "-put" , f"{SOURCE_PATH}/yellow_tripdata_{LOADING_DATE}.parquet" , f"{STG_PATH}"],capture_output=True , check=True , text=True)

    if results.returncode != 0:
            print(f"ERROR: {results.stderr}")
            sys.exit(1)

def move_from_temp_to_bronze():

    results = subprocess.run(["hdfs" , "dfs" , "-mv" , f"{STG_PATH}/yellow_tripdata_{LOADING_DATE}.parquet" , f"{YELLOW_BRONZE_PATH}/date={LOADING_DATE}/yellow_tripdata_{LOADING_DATE}.parquet"] , capture_output=True, check=True , text=True)
    if results.returncode != 0:
            print(f"ERROR: {results.stderr}")
            sys.exit(1)     

def move_from_stg_to_bronze():
    delete_inital_path_of_bronze()
    delete_inital_path_of_stg()
    construct_path_to_stg()
    construct_path_to_bronze()
    move_to_stg_temp_writing()
    move_from_temp_to_bronze()

if __name__ == '__main__':
    move_from_stg_to_bronze()