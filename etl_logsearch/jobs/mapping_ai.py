
import findspark
findspark.init()
from datetime import datetime
import json
import logging
from google import genai
from google.genai import types
from dotenv import load_dotenv
import time
import os
import pandas as pd
from pyspark.sql.functions import col, lower, trim
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed
)


# ========= LOGGING ============
logging.basicConfig(
    filename=f'logs/mapping_{datetime.now().strftime("%Y%m%d")}.log', level= logging.INFO, format= '%(asctime)s | %(levelname)s | %(message)s'
)

logger = logging.getLogger(__name__)

# ========= SPARK CONFICT ============
jar_path = r"D:\DE\ETL _pipeline\etl_logsearch\mysql-connector-j-8.0.33.jar"

spark = (
    SparkSession.builder
    .config("spark.jars", jar_path)
    .config("spark.driver.extraClassPath", jar_path)
    .getOrCreate()
)


# ========= FUNCTIONS ============
def load_data_from_spark(path):
    logger.info(f"Reading data from {path}")
    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True
    )
    df = df.withColumn(
        "Most_Search",trim(lower(col("Most_Search")))
    )
    return df.toPandas()

def get_unique_keywords(df, column_name):

    unique_keywords = (
        df[column_name]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    logger.info(
        f"Total rows={len(df)} | Unique={len(unique_keywords)}"
    )

    return unique_keywords



#========= GEMINI CATEGORY MAPPING =========
def mapping_category(keyword_batch):
    # ========= GEMINI CLIENT ============
    load_dotenv()
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY not found")

    logger.info("Load Google API Key")

    #Create client
    client = genai.Client(api_key=GOOGLE_API_KEY)
    logger.info(
        "Connect success with Google AI Studio (gemini-3.1-flash-lite-preview)"
    ) 

    prompt = f"""
        Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và các loại nội dung giải trí. 
        Bạn sẽ nhận một danh sách tên có thể viết sai, viết liền không dấu, viết tắt, hoặc chỉ là cụm từ liên quan
        đến nội dung.

        ⚠️ Nguyên tắc quan trọng:
        - Không được trả về "Other" nếu có thể đoán được dù chỉ một phần ý nghĩa. 
        - Luôn cố gắng sửa lỗi, nhận diện tên gần đúng hoặc đoán thể loại gần đúng. 
        - Nếu không chắc → chọn thể loại gần nhất (VD: từ mô tả tình cảm → Romance, tên địa danh thể thao → Sports, 
        chương trình giải trí → Reality Show, v.v.)

        Nhiệm vụ của bạn:
        1. **Chuẩn hoá tên**: thêm dấu tiếng Việt nếu cần, tách từ, chỉnh chính tả.
        2. **Nhận diện tên hoặc ý nghĩa gốc gần đúng nhất**. Bao gồm:
        - Tên phim, series, show, chương trình
        - Quốc gia / đội tuyển (→ "Sports" hoặc "News")
        - Từ khoá mô tả nội dung
        3. **Gán thể loại phù hợp nhất** trong các nhóm sau: 
        - Action 
        - Romance 
        - Comedy 
        - Horror 
        - Animation 
        - Drama 
        - C Drama 
        - K Drama 
        - Sports 
        - Music 
        - Reality Show 
        - TV Channel 
        - News 
        - Other

        Một số quy tắc gợi ý nhanh:
        - Có từ “VTV”, “HTV”, “Channel” → TV Channel 
        - Có “running”, “master key”, “reality” → Reality Show 
        - Quốc gia, CLB bóng đá, sự kiện thể thao → Sports hoặc News 
        - “sex”, “romantic”, “love” → Romance 
        - “potter”, “hogwarts” → Drama / Fantasy 
        - Tên phim Việt/Trung/Hàn → ưu tiên Drama / C Drama / K Drama

        Chỉ trả về **1 JSON object**. 
        Key = tên gốc trong danh sách. 
        Value = thể loại đã phân loại.

        Ví dụ: 
        {{
        "thuyếtminh": "Other",
        "bigfoot": "Horror",
        "capdoi": "Romance",
        "ARGEN": "Sports",
        "nhật ký": "Drama",
        "PENT": "C Drama",
        "running": "Reality Show",
        "VTV3": "TV Channel"
        }}

        Danh sách:
        {keyword_batch}
        """
    max_retries = 3

    for attempt in range(max_retries):

        try:

            response = client.models.generate_content(
                model="gemini-3.1-flash-lite-preview",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )

            parsed = json.loads(response.text)

            return {
                k: parsed.get(k, "Other")
                for k in keyword_batch
            }

        except Exception as e:

            msg = str(e)

            if any(
                x in msg
                for x in [
                    "429",
                    "503",
                    "RESOURCE_EXHAUSTED",
                    "UNAVAILABLE"
                ]
            ):

                wait_time = 30 * (2 ** attempt)

                logger.warning(
                    f"Retry after {wait_time}s"
                )

                time.sleep(wait_time)

            else:

                logger.error(
                    f"Batch failed: {e}"
                )

                return {
                    k: "Other"
                    for k in keyword_batch
                }

    return {
        k: "Other"
        for k in keyword_batch
    }

def run_mapping_pipeline(
        input_path,
        output_path,
        batch_size=3000
):

    logger.info("Start pipeline")

    # Extract
    full_pdf = load_data_from_spark(input_path)

    # Unique keywords
    unique_keywords = get_unique_keywords(
        full_pdf,
        "Most_Search"
    )

    chunks = [
        unique_keywords[i:i+batch_size]
        for i in range(
            0,
            len(unique_keywords),
            batch_size
        )
    ]

    logger.info(
        f"Total batches={len(chunks)}"
    )

    all_mapping = {}

    # Gemini calls
    for idx, chunk in enumerate(chunks, start=1):

        logger.info(
            f"Processing batch {idx}/{len(chunks)}"
        )

        result = mapping_category(chunk)

        all_mapping.update(result)

        logger.info(
            f"Completed batch {idx}/{len(chunks)}"
        )
    full_pdf["Category"] = (
        full_pdf["Most_Search"]
            .astype(str)
            .map(
                lambda x:
                all_mapping.get(x,"Other")
            )
    )  
        
    full_pdf.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )

    logger.info(
        f"Saved output -> {output_path}"
    )
        
if __name__ == "__main__":
    months = ["202206", "202207"]

    for month in months:

        INPUT_FILE = (rf"D:\DE\ETL _pipeline\etl_logsearch\data\silver\{month}")

        OUTPUT_FILE = (
            rf"D:\DE\ETL _pipeline\etl_logsearch\data\silver\category\mapping_{month}.csv"
        )
    
        logger.info(f"Start processing {month}")

        run_mapping_pipeline(
            input_path=INPUT_FILE,
            output_path=OUTPUT_FILE,
            batch_size=3000
        )

        logger.info(f"Completed {month}")







    