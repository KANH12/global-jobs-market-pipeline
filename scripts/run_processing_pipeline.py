import argparse

from core.spark_session import create_spark_session
from core.logger import get_job_logger

from processing.bronze.read_adzuna_bronze import run_bronze_pipeline
from processing.silver.write_adzuna_silver import run_silver_pipeline
from processing.gold.write_adzuna_gold import run_gold_pipeline


logger = get_job_logger(
    job_name="adzuna_processing_pipeline",
    component="processing_pipeline"
)


def run_processing_pipeline(date_path: str):
    logger.info("=" * 80)
    logger.info(f"[START] Full processing pipeline | date={date_path}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_bronze_pipeline(spark, date_path)
        run_silver_pipeline(spark, date_path)
        run_gold_pipeline(spark, date_path)

        logger.info("[SUCCESS] Full processing pipeline completed")

    except Exception:
        logger.error("[ERROR] Full processing pipeline failed", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("[STOP] Spark stopped")
        logger.info("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        required=True,
        help="Date path format: YYYY/MM/DD, example: 2026/05/20"
    )
    args = parser.parse_args()

    run_processing_pipeline(args.date)