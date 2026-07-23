from utils.logger import get_logger
from utils.spark import create_spark

from jobs.job_mostsearch import run as job_mostsearch
from jobs.job_trending import run as job_trending
from jobs.mapping_ai import run as mapping_ai


logger = get_logger("orchestrator")

def main():
    logger.info("=" * 60)
    logger.info("START ETL PIPELINE")
    logger.info("=" * 60)

    jobs = [
        ("Most Search", job_mostsearch),
        ("Mapping AI", mapping_ai),
        ("Trending", job_trending)
    ]

    for job_name, job in jobs:
        try:
            logger.info(f"START JOB: {job_name}")
           
            job()

            logger.info(f"FINISH JOB: {job_name}")
        except Exception as e:
            logger.info(f"FAILED JOB: {job_name}")
            raise e
        
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETED")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()