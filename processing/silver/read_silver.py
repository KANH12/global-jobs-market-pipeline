from core.logger import get_job_logger

logger = get_job_logger(
    job_name="silver_reader",
    component="silver"
)


def read_silver(spark, date_path: str):
    path = f"s3a://data-lake/silver/jobs/dt={date_path}"

    logger.info(f"[START] Reading silver data from {path}")

    df = spark.read.parquet(path)

    row_count = df.count()
    logger.info(f"[INFO] Rows read: {row_count}")

    return df

def read_adzuna_silver(spark, date_path: str):
    return read_silver(spark, date_path)