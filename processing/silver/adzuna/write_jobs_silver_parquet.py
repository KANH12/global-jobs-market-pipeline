from pyspark.sql import functions as F
from core.spark_session import create_spark_session
from core.logger import get_job_logger

logger = get_job_logger(
    job_name="adzuna_jobs_silver_writer",
    component="silver"
)

def write_jobs_silver(df, date_path: str):
    logger.info("🚀 START Silver jobs transform")

    # =========================
    # 0. Explode bronze records → job-level
    # =========================
    df = (
        df
        .filter(F.col("records").isNotNull())
        .select(F.explode("records").alias("job"))
    )

    # =========================
    # 1. Filter invalid jobs
    # =========================
    before_count = df.count()

    df = df.filter(F.col("job.id").isNotNull())

    after_count = df.count()
    logger.info(f"🧹 Removed jobs with NULL id: {before_count - after_count}")

    # =========================
    # 2. Select & transform columns
    # =========================
    jobs_df = df.select(
        F.col("job.id").alias("job_id"),
        F.col("job.title"),
        F.upper(F.trim(F.col("job.contract_time"))).alias("contract_time"),
        F.upper(F.trim(F.col("job.contract_type"))).alias("contract_type"),
        F.col("job.created").cast("timestamp"),

        F.col("job.salary_min"),
        F.col("job.salary_max"),
        (F.col("job.salary_is_predicted") == "1").alias("salary_is_predicted"),

        F.col("job.latitude"),
        F.col("job.longitude"),

        # ---------- CATEGORY ----------
        F.col("job.category.label").alias("category_label"),
        F.col("job.category.tag").alias("category_tag"),
        F.sha2(
            F.concat_ws(
                "||",
                F.col("job.category.label"),
                F.col("job.category.tag")
            ),
            256
        ).alias("category_id"),

        # ---------- COMPANY ----------
        F.col("job.company.display_name").alias("company_name"),
        F.sha2(
            F.col("job.company.display_name"),
            256
        ).alias("company_id"),

        # ---------- LOCATION ----------
        F.col("job.location.display_name").alias("location_name"),
        F.sha2(
            F.concat_ws(
                "||",
                F.col("job.location.display_name"),
                F.concat_ws(",", F.col("job.location.area"))
            ),
            256
        ).alias("location_id"),
    )

    logger.info("✅ Selected & transformed columns")

    # =========================
    # 3. Write Parquet
    # =========================
    output_path = f"s3a://data-lake/silver/adzuna/jobs/dt={date_path}"

    logger.info(f"📂 Writing jobs parquet to {output_path}")

    (
        jobs_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info(f"📊 Output records: {jobs_df.count()}")
    logger.info("🎉 Silver jobs parquet written successfully")


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    date_path = "2026/02/04"

    try:
        df = (
            spark.read
            .option("multiLine", True)
            .json(f"s3a://data-lake/bronze/adzuna/{date_path}/*.json")
        )

        write_jobs_silver(df, date_path)

    except Exception:
        logger.error("❌ Silver jobs write failed", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("🛑 Spark stopped")
