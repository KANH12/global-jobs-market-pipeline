import argparse

from core.spark_session import create_spark_session
from storage.read_minio import read_jobs_summary, read_salary_analysis
from storage.postgres_writer import write_to_postgres
from core.logger import get_job_logger


logger = get_job_logger(
    job_name="adzuna_pipeline_database",
    component="database"
)


def run_pipeline(date_path: str):
    logger.info(f"[START] Database pipeline | date={date_path}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # =========================
        # 1. Read Gold data from MinIO
        # =========================
        df_jobs = read_jobs_summary(spark, date_path)
        df_salary = read_salary_analysis(spark, date_path)

        jobs_count = df_jobs.count()
        salary_count = df_salary.count()

        logger.info(f"[INFO] jobs_summary count: {jobs_count}")
        logger.info(f"[INFO] salary_analysis count: {salary_count}")

        # =========================
        # 2. Write Gold data to PostgreSQL
        # =========================
        write_to_postgres(df_jobs, "jobs_summary")
        write_to_postgres(df_salary, "salary_analysis")

        logger.info("[SUCCESS] Database pipeline completed")

    except Exception:
        logger.error("[ERROR] Database pipeline failed", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("[STOP] Spark stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        required=True,
        help="Date path format: YYYY/MM/DD, example: 2026/05/20"
    )

    args = parser.parse_args()

    run_pipeline(args.date)