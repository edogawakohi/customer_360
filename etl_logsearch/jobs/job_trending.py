from datetime import datetime, timedelta
import os
import findspark

from utils.logger import get_logger
findspark.init()

from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.window import Window 
from jobs import job_mostsearch as kw
from dotenv import load_dotenv

# ========= LOGGING ============
logger = get_logger("job_trending")

# ========= SPARK CONFICT ============
jar_path = r"D:\DE\ETL _pipeline\etl_logsearch\mysql-connector-j-8.0.33.jar"

spark = (
    SparkSession.builder
    .config("spark.jars", jar_path)
    .config("spark.driver.extraClassPath", jar_path)
    .getOrCreate()
)



print(
    spark.sparkContext.getConf().get("spark.jars")
)
# ====== FUNCTION =============

def read_csv_from_path(path):
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return df

def rename_most_search_column(data,year_month):
    logger.info(f"Rename columns based on the month")
    year_month = str(int(year_month[4:6] ))
    data = data.withColumnRenamed('Most_Search', 'Most_Searched' + '_t' + year_month)
    data = data.withColumnRenamed('Category', 'Category_t' + year_month)
    return data

def classify_search_behavior(df):
    df = df.withColumn('Trending_Type', when(col('Category_t6') == col('Category_t7'), 'Unchanged').otherwise('Changed'))
    df = df.withColumn('Previous', when(col('Trending_Type') == 'Changed', concat_ws('-', col('Category_t6'),col('Category_t7'))).otherwise('Unchanged'))
    return df

def save_data(result, save_path, year_month):
    logger.info(f"Export processed results to CSV, partitioned by {year_month}")
    full_path = f"{save_path}\\{year_month}"
    (
        result.repartition(1).write.option("header", "true").mode("overwrite").csv(full_path)
    )
    logger.info(f"Data Saved Successfully to: {full_path}")

#====================Config import mysql=====================

def import_to_mysql(df,table_name):
    load_dotenv()

    url ="jdbc:mysql://localhost:3306/customer_360?useSSL=false&allowPublicKeyRetrieval=true"

    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')

    properties = {
        "user" : user,
        "password" : password,
        "driver" : "com.mysql.cj.jdbc.Driver"
    }
       
    df.write.jdbc(
        url = url,
        table = table_name,
        mode = "overwrite",
        properties = properties
    )
    
    logger.info(f"Imported Successflly")

def etl_main(df):
    
    logger.info("Classify Search Behavior")
    df = classify_search_behavior(df)

    logger.info("Save data")
    save_data(df, output_path, "202206_202207")

    logger.info("Importing to MySql")
    import_to_mysql(df,'user_behavior')

    return df

def main_task_categories(input_path, output_path):
    lists = kw.list_files(input_path)
    year_months = kw.convert_to_year_month([], lists)

    logger.info("Reading and merging data")

    df = read_csv_from_path(os.path.join(input_path,lists[0]))
    df = rename_most_search_column(df,year_months[0])

    logger.info("Join for subsequent months to track returning users")
    for i, file_name in enumerate(lists[1:], start= 1):
        new_df = read_csv_from_path(os.path.join(input_path,file_name))
        new_df = rename_most_search_column(new_df,year_months[i])
        df = df.join(new_df, on='user_id')
        logger.info(df.columns)
    
    df = etl_main(df)

if __name__ == "__main__":
    input_path = r"D:\DE\ETL _pipeline\etl_logsearch\data\silver\category"
    output_path = r"D:\DE\ETL _pipeline\etl_logsearch\data\gold"

    main_task_categories(input_path,output_path)
