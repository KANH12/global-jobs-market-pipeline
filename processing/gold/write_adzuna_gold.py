import argparse

from pyspark.sql import DataFrame

from core.spark_session import create_spark_session
from core.logger import get_job_logger

from processing.silver.read_adzuna_silver import read_adzuna_silver
from processing.gold.jobs_summary import build_jobs_summary
from processing.gold.salary_analysis import build_salary_analysis
from processing.gold.jobs_detail import build_jobs_detail


logger = get_job_logger(
    job_name="adzuna_gold_writer",
    component="gold"
)


# ===================
# Write gold dataset
# ===================
def write_gold(df: DataFrame, output_path: str):
    logger.info(f"[START] Writing gold data to {output_path}")

    # Gold output is usually small and business-ready
    df = df.repartition(1)

    count = df.count()

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info(f"[INFO] Gold output records: {count}")
    logger.info("[SUCCESS] Gold write completed")


# ===================
# Gold Pipeline
# ===================
def run_gold_pipeline(spark, date_path: str):
    logger.info("=" * 60)
    logger.info(f"[START] Gold pipeline | date={date_path}")

    try:
        # 1. Read silver
        df_silver = read_adzuna_silver(spark, date_path)

        # 2. Build jobs summary
        df_jobs_summary = build_jobs_summary(df_silver)

        # 3. Build salary analysis
        df_salary_analysis = build_salary_analysis(df_silver)

        # 4. Build jobs detail
        df_jobs_detail = build_jobs_detail(df_silver)

        # 5. Write jobs summary
        write_gold(
            df_jobs_summary,
            f"s3a://data-lake/gold/adzuna/jobs_summary/dt={date_path}"
        )

        # 6. Write salary analysis
        write_gold(
            df_salary_analysis,
            f"s3a://data-lake/gold/adzuna/salary_analysis/dt={date_path}"
        )

        # 7. Write jobs detail
        write_gold(
            df_jobs_detail,
            f"s3a://data-lake/gold/adzuna/jobs_detail/dt={date_path}"
        )

        logger.info("[SUCCESS] Gold pipeline completed successfully")

    except Exception:
        logger.error("[ERROR] Gold pipeline failed", exc_info=True)
        raise


# ===================
# CLI Entry Point
# ===================
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
        run_gold_pipeline(spark, args.date)

    finally:
        spark.stop()
        logger.info("[STOP] Spark stopped")
        logger.info("=" * 60)