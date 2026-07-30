import argparse

from core.logger import get_job_logger
from core.spark_session import create_spark_session
from processing.bronze.read_bronze import run_bronze_pipeline
from processing.gold.build_gold import run_gold_pipeline
from processing.silver.build_silver import run_silver_pipeline

SOURCES = ["adzuna", "jooble"]

logger = get_job_logger(
    job_name="processing_pipeline",
    component="processing_pipeline"
)


def run_processing_pipeline(date_path: str, sources: list = None):
    if sources is None:
        sources = SOURCES

    logger.info("=" * 80)
    logger.info(f"[START] Full processing pipeline | sources={sources} | date={date_path}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        for source in sources:
            run_bronze_pipeline(spark, source, date_path)

        run_silver_pipeline(spark, sources, date_path)
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
    parser.add_argument(
        "--sources",
        nargs="+",
        default=SOURCES,
        choices=SOURCES,
        help="List of sources to process, e.g. --sources adzuna jooble"
    )
    args = parser.parse_args()

    run_processing_pipeline(args.date, args.sources)