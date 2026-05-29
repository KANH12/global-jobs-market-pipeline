import argparse
import logging

from core.spark_session import create_spark_session
from core.logger import get_job_logger
from quality.bronze_quality import run_bronze_quality_checks


# =========================
# Logger setup
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = get_job_logger(
    job_name="adzuna_bronze_reader",
    component="bronze"
)


# =========================
# Read Bronze
# =========================
def read_adzuna_bronze(spark, date_path: str):
    path = f"s3a://data-lake/bronze/adzuna/{date_path}/*.json"

    logger.info("[START] Reading bronze data")
    logger.info(f"[INFO] date_path={date_path}")
    logger.info(f"[INFO] source_path={path}")

    df_bronze = (
        spark.read
        .option("multiLine", True)
        .json(path)
    )

    return df_bronze


# =========================
# Bronze Pipeline
# =========================
def run_bronze_pipeline(spark, date_path: str):
    logger.info("=" * 60)
    logger.info(f"[START] Bronze pipeline | date={date_path}")

    try:
        df_bronze = read_adzuna_bronze(spark, date_path)

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


# =========================
# CLI Entry Point
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        required=True,
        help="Date path format: YYYY/MM/DD, example: 2026/05/20"
    )
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_bronze_pipeline(spark, args.date)

    finally:
        spark.stop()
        logger.info("[STOP] Spark session stopped")
        logger.info("=" * 60)