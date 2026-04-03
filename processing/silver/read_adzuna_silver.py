from pyspark.sql import SparkSession
from core.logger import get_job_logger

logger = get_job_logger(
    job_name="read_adzuna_silver",
    component="silver"
)

def read_adzuna_silver(spark: SparkSession, date_path: str):
    input_path = f"s3a://data-lake/silver/adzuna/jobs/dt={date_path}"

    logger.info(f"📥 Reading silver data from {input_path}")

    df = spark.read.parquet(input_path)

    logger.info(f"📊 Rows read: {df.count()}")

    return df