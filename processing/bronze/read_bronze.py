import argparse
import logging

from core.logger import get_job_logger
from core.spark_session import create_spark_session
from quality.bronze_quality import run_bronze_quality_checks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

def read_bronze(spark, source: str, date_path: str, logger=None):
    if logger is None:
        logger = get_job_logger(job_name=f"{source}_bronze_reader", component="bronze")

    path = f"s3a://data-lake/bronze/{source}/{date_path}/*.json"

    logger.info("[START] Reading bronze data")
    logger.info(f"[INFO] source={source}")
    logger.info(f"[INFO] date_path={date_path}")
    logger.info(f"[INFO] source_path={path}")

    df_bronze = (
        spark.read
        .option("multiLine", True)
        .json(path)
    )

    return df_bronze


# =========================
# Backward-compatible wrapper
# =========================
# Giữ lại để không phá vỡ code/test cũ đang import read_adzuna_bronze trực tiếp
# (vd: tests/integration/test_bronze_read_write_minio.py). Nên chuyển dần
# các chỗ gọi sang read_bronze(spark, "adzuna", date_path) rồi bỏ hàm này.
def read_adzuna_bronze(spark, date_path: str):
    return read_bronze(spark, "adzuna", date_path)

def run_bronze_pipeline(spark, source: str, date_path: str):
    logger = get_job_logger(job_name=f"{source}_bronze_reader", component="bronze")

    logger.info("=" * 60)
    logger.info(f"[START] Bronze pipeline | source={source} | date={date_path}")

    try:
        df_bronze = read_bronze(spark, source, date_path, logger=logger)

        record_count = df_bronze.count()
        logger.info(f"[INFO] Bronze record count={record_count}")

        logger.info("[INFO] Bronze schema:")
        df_bronze.printSchema()

        run_bronze_quality_checks(df_bronze, logger)

        logger.info("[SUCCESS] Bronze pipeline completed successfully")

        return df_bronze

    except Exception:
        logger.error("[ERROR] Bronze pipeline failed", exc_info=True)
        raise

# CLI Entry Point
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        required=True,
        choices=["adzuna", "jooble"],
        help="Data source name, e.g. adzuna, jooble"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date path format: YYYY/MM/DD, example: 2026/05/20"
    )
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_bronze_pipeline(spark, args.source, args.date)

    finally:
        spark.stop()
        logging.info("[STOP] Spark session stopped") 
        logging.info("=" * 60)  
