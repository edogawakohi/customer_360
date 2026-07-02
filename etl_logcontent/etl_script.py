from dotenv import load_dotenv
import findspark
findspark.init()

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from pyspark.sql.functions import concat_ws
from datetime import datetime, timedelta
import os
import logging
from functools import reduce


#====Logging===

logging.basicConfig(
    filename=f'etl_{datetime.now().strftime("%Y%m%d")}.log', level= logging.INFO, format= '%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

#====SPARK CONFICT===
spark = SparkSession.builder\
        .config("spark.driver.memory", "8g")\
        .config("spark.executor.cores", 8)\
        .config(
            "spark.jars",
            r"D:\DE\ETL _pipeline\etl_logcontent\mysql-connector-j-8.0.33.jar"
        )\
        .getOrCreate()

logger.info("Spark Session started")

#=====FUNCTIONS=====

def category_AppName(df):
    logger.info("Start category_AppName")

    df = df.withColumn("Type", 
                       when(col("AppName") == "CHANNEL", "Truyen Hinh")
                       .when(col("AppName") == "RELAX", "Giai Tri")
                       .when(col("AppName") == "CHILD", "Thieu Nhi")
                       .when((col("AppName") == "FIMS") | (col("AppName") == "VOD"), "Phim Truyen")
                       .when((col("AppName") == "KPLUS") | (col("AppName") == "SPORT"), "The Thao")
                       .otherwise("Error")
                       )
    df = df.select('Contract', 'Type', 'TotalDuration')
    df = df.filter(df.Contract != '0')
    df = df.filter(df.Type != 'Error')

    df.printSchema()
    df.show(5)

    logger.info("Finish category_AppName")
    return df

def most_watch(df):
    logger.info("Caculating most_watch")
    
    df = df.withColumn("MostWatch",
                greatest(
                    coalesce(col("Giai Tri"), lit(0)),
                    coalesce(col("Phim Truyen"), lit(0)),
                    coalesce(col("The Thao"), lit(0)),
                    coalesce(col("Thieu Nhi"), lit(0)),
                    coalesce(col("Truyen Hinh"), lit(0)),
                ))
    df = df.withColumn("MostWatch",
                    when(col("MostWatch") == col("Truyen Hinh"), "Truyen Hinh")
                    .when(col("MostWatch") == col("Phim Truyen"), "Phim Truyen")
                    .when(col("MostWatch") == col("The Thao"), "The Thao")
                    .when(col("MostWatch") == col("Thieu Nhi"), "Thieu Nhi")
                    .when(col("MostWatch") == col("Giai Tri"), "Giai Tri")
                    )
    
    df.printSchema()
    df.show(5)
    return df

def customer_taste(df):
    logger.info("Caculating customer_taste")

    df = df.withColumn("Taste", concat_ws("-",
                        when(col("Giai Tri").isNotNull(), lit("Giai Tri")),                  
                        when(col("Phim Truyen").isNotNull(), lit("Phim Truyen")),
                        when(col("The Thao").isNotNull(), lit("The Thao")),                  
                        when(col("Thieu Nhi").isNotNull(), lit("Thieu Nhi")),                  
                        when(col("Truyen Hinh").isNotNull(), lit("Truyen Hinh")),                  
                        ))
    df.printSchema()
    df.show(5)
    return df

def convert_to_datevalue(string):
    date_value = datetime.strptime(string,"%Y%m%d").date()
    return date_value

def convert_to_stringvalue(date):
    string_value = date.strftime("%Y%m%d")
    return string_value

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

def find_Active(df):
    
    logger.info("Caculating Active users")

    
    active_df = df.groupBy("Contract").agg(
        sf.countDistinct("Date").alias("ActiveCount")
    )

   
    active_df = active_df.withColumn(
        "Active",
        when(col("ActiveCount") > 20, "High")
        .when((col("ActiveCount") >= 10) & (col("ActiveCount") <= 20), "Medium")
        .otherwise("Low")
    )

  
    df = df.join(active_df, on="Contract", how="left")

    df = df.groupBy("Contract").agg(
        sf.sum("Giai Tri").alias("Total_Giai_Tri"),
        sf.sum("Phim Truyen").alias("Total_Phim_Truyen"),
        sf.sum("The Thao").alias("Total_The_Thao"),
        sf.sum("Truyen Hinh").alias("Total_Truyen_Hinh"),
        sf.sum("Thieu Nhi").alias("Total_Thieu_Nhi"),

        sf.max("MostWatch").alias("MostWatch"),
        sf.max("Taste").alias("Taste"),

        sf.max("Active").alias("Active")
    )

    df.printSchema()
    df.show(5)

    return df

def ETL_1_Day(path, path_day):
    logger.info(f"===== START ETL {path_day} =====")

    try:
        df = spark.read.json(path + path_day + ".json")
        logger.info("Read JSON success")
        
        df = df.select("_source.*")

        df = category_AppName(df)

        df = df.groupBy("Contract").pivot("Type").sum("TotalDuration")
        logger.info("Pivot done")

        df = most_watch(df)
        df = customer_taste(df)

        df = df.withColumn("Date", to_date(lit(path_day), "yyyyMMdd"))

        logger.info(f"=====  END ETL {path_day}   =====")
        return df

    except Exception as e:
        logger.error(f"ETL FAILED {path_day}: {str(e)}")
        raise

def save_data(result, output_path):
    logger.info("Saving final dataframe")
    result.repartition(1).write.option("header","true").mode("overwrite").csv(output_path)
    logger.info("Data saved successfully")

#====================Config import supabase=====================
# def import_to_supabase(df,table_name,mode="overwrite"):
#     load_dotenv()

#     host = os.getenv('SUPABASE_HOST')
#     password = os.getenv('SUPABASE_PASS')
#     user = os.getenv('SUPABASE_USER')

#     db_url = (f"jdbc:postgresql://{host}:6543/postgres"
#         "?sslmode=require"
#         "&prepareThreshold=0")
    
#     properties = {
#         "user" : user,
#         "password" : password,
#         "driver" : "org.postgresql.Driver"
#     }

#     df = df.repartition(1)

#     df.write.mode(mode).option("batchsize", 1000).jdbc(
#         url = db_url,
#         table = table_name,
#         properties = properties
#     )
#     logger.info(f"Imported into {table_name}, mode: {mode}")

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

#====================Main=====================
def main(path, save_path):

    logger.info("START")

    date_list = generate_range_date(
        "20220401",
        "20220430"
    )

    result = ETL_1_Day(path, date_list[0])

    for d in date_list[1:]:
        logger.info(f"Processing {d}")

        result = result.unionByName(
            ETL_1_Day(path, d)
        )

    result = result.cache()
    result = result.fillna(0)

    result = find_Active(result)

    logger.info(f"Rows: {result.count()}")

    save_data(result, save_path)

    import_to_mysql(result,"customer_360_log_content")

    logger.info("DONE")

    return result

#================= RUN =======================
path = r"D:\DE\ETL _pipeline\etl_logcontent\data\bronze\log_content\\"
save_path = r"D:\DE\ETL _pipeline\etl_logcontent\data\gold\output"

df = main(path, save_path)






