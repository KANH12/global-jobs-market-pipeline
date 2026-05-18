from core.spark_session import create_spark_session
from storage.read_minio import read_jobs_summary, read_salary_analysis
from storage.postgres_writer import write_to_postgres
from core.logger import get_job_logger

logger = get_job_logger(
    job_name="adzuna_pipeline_database",
    component="database"
)


def run_pipeline(date_path: str):
    logger.info(f"[START] START database pipeline | date={date_path}")

    spark = create_spark_session()

    try:
        # =========================
        # 1. READ FROM MINIO
        # =========================
        df_jobs = read_jobs_summary(spark, date_path)
        df_salary = read_salary_analysis(spark, date_path)

        logger.info(f"[INFO] jobs_summary count: {df_jobs.count()}")
        logger.info(f"[INFO] salary_analysis count: {df_salary.count()}")

        # =========================
        # 2. WRITE TO POSTGRES
        # =========================
        write_to_postgres(df_jobs, "jobs_summary")
        write_to_postgres(df_salary, "salary_analysis")

        logger.info("[SUCCESS] Database pipeline completed")

    except Exception:
        logger.error("[WARNING] Pipeline failed", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("[STOP] Spark stopped")


if __name__ == "__main__":
    run_pipeline("2026/05/17")