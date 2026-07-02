import findspark
findspark.init()
from datetime import datetime, timedelta
import os
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from utils.logger import get_logger


# ========= LOGGING ============
logger = get_logger("job_mostsearch")

# ========= SPARK CONFICT ============
jar_path = r"D:\DE\ETL _pipeline\etl_logsearch\mysql-connector-j-8.0.33.jar"

spark = (
    SparkSession.builder
    .config("spark.jars", jar_path)
    .config("spark.driver.extraClassPath", jar_path)
    .getOrCreate()
)

def convert_to_datevalue(string):
    date_value = datetime.strptime(string,"%Y%m%d").date()
    return date_value

def convert_to_stringvalue(date):
    string_value = date.strftime("%Y%m%d")
    return string_value

def convert_to_year_month(year_months, list_files):
    year_months = sorted(list(set([file[0:6] for file in list_files])))
    return year_months

def convert_to_year_month_v2(year_months, list_files):
    year_months = sorted(list(set([file[0:14] for file in list_files])))
    return year_months

def read_parquet_from_path(path):
    data = spark.read.parquet(path)
    return data

def date_range(start_date,end_date):
    date_list = []
    current_date = start_date
    while(current_date <= end_date):
        date_list.append(convert_to_stringvalue(current_date))
        current_date += timedelta(days=1)
    return date_list

def generate_range_date(start_date,end_date):
    start_date = convert_to_datevalue(start_date)
    end_date = convert_to_datevalue(end_date)
    date_list = date_range(start_date,end_date)
    return date_list

def read_parquet_from_path(path):
    data = spark.read.parquet(path)
    return data

def processing_log_search(data):
    data = data.select('user_id', 'keyword')
    data = data.groupBy('user_id', 'keyword').count()
    data = data.withColumnRenamed('count', 'TotalSearch')
    data = data.orderBy('user_id', ascending = False)
    window = Window.partitionBy('user_id').orderBy(col('TotalSearch').desc())
    data = data.withColumn('Rank', row_number().over(window)).filter(col('Rank') == 1)
    data = data.select('user_id', col('keyword').alias('Most_Search'))
    return data

def save_data(result, save_path, year_month):
    logger.info(f"Export processed results to CSV, partitioned by {year_month}")
    full_path = f"{save_path}\\{year_month}"
    (
        result.repartition(1).write.option("header", "true").mode("overwrite").csv(full_path)
    )
    logger.info(f"Data Saved Successfully to: {full_path}")

def list_files(path):
    list_files = os.listdir(path)
    logger.info(list_files)
    return list_files 

def list_files_v2(path):
    folders = [
        f for f in os.listdir(path)
        if f.isdigit() and len(f) == 6
    ]

    logger.info(folders)
    return folders

def main (input_path,output_path):                                                                                                                                                                                                                                                                                                                                                                                                             
    
    lists = list_files(input_path)

    year_months = convert_to_year_month([], lists)
    year_month = year_months[0]

    #process data batch by batch
    for year_month in year_months:
        logger.info(f"Processing for Year-Month: {year_month}")
    
    # Filter files belonging to the current processing month
        files_in_month = [file for file in lists if file.startswith(year_month)]
        
        logger.info("ETL_TASK " + input_path + files_in_month[0] + ".parquet")
        df = read_parquet_from_path(os.path.join(input_path,files_in_month[0],"*.parquet"))
    # Initial load
        for file in files_in_month[1:]:
            logger.info("ETL_TASK " + input_path + file + ".parquet")
            new_df = read_parquet_from_path(os.path.join(input_path,file,"*.parquet"))
            logger.info("Union df with new df")
            df = df.union(new_df)

        logger.info(f"Process log search data {year_month}")
        new_result = processing_log_search(df)
        logger.info(f"Saving csv output for {year_month}")
        save_data(new_result,output_path,year_month)

# ======================
# Entry Point

def run():

    logger.info("=" * 50)
    logger.info("START JOB MOST SEARCH")
    logger.info("=" * 50)

    input_path = r"D:\DE\ETL _pipeline\etl_logsearch\data\bronze"
    output_path = r"D:\DE\ETL _pipeline\etl_logsearch\data\silver"

    try:

        main(
            input_path=input_path,
            output_path=output_path
        )

        logger.info("JOB COMPLETED SUCCESSFULLY")

    except Exception as e:

        logger.exception(f"JOB FAILED: {str(e)}")
        raise

    finally:

        logger.info("=" * 50)
        logger.info("END JOB MOST SEARCH")
        logger.info("=" * 50)


if __name__ == "__main__":
    run()
