import logging
from core.spark_session import create_spark_session
from core.logger import get_job_logger
from quality_process.bronze_quality import run_bronze_quality_checks
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


def read_adzuna_bronze(spark, date_path: str):
    path = f"s3a://data-lake/bronze/adzuna/{date_path}/*.json"

    logger.info("📂 Reading bronze data")
    logger.info(f"📅 date_path={date_path}")
    logger.info(f"📁 source_path={path}")

    df = (
        spark.read
        .option("multiLine", True)
        .json(path)
    )

    return df


if __name__ == "__main__":
    logger.info("🚀 START adzuna bronze reader")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    date_path = "2026/02/04"

    try:
        df = read_adzuna_bronze(spark, date_path)

        record_count = df.count()
        logger.info(f"📊 Record count={record_count}")

        logger.info("📐 Schema:")
        logger.info(df.printSchema())

        run_bronze_quality_checks(df)

        logger.info("✅ Bronze read completed successfully")

    except Exception as e:
        logger.error("❌ Failed to read bronze data", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("🛑 Spark session stopped")
