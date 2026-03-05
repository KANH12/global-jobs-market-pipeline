from pyspark.sql import DataFrame
from core.logger import get_job_logger

logger = get_job_logger(
    job_name="adzuna_bronze_quality",
    component="quality"
)

def check_record_count(df: DataFrame):

    count = df.count()

    if count == 0:
        logger.error("❌ Bronze dataset is EMPTY")
        raise Exception("Bronze data ingestion failed")

    if count < 1:
        logger.warning(f"⚠️ Very small dataset detected: {count}")

    logger.info(f"✅ Record count check passed: {count}")


def check_required_columns(df: DataFrame):

    required_columns = [
        "records",
        "batch_id",
        "source",
        "ingested_at"
    ]

    df_columns = df.columns

    missing = [c for c in required_columns if c not in df_columns]

    if missing:
        logger.error(f"❌ Missing required columns: {missing}")
        raise Exception("Bronze schema validation failed")

    logger.info("✅ Required columns check passed")


def check_records_structure(df: DataFrame):

    null_records = df.filter("records IS NULL").count()

    if null_records > 0:
        logger.error(f"❌ Found {null_records} rows with NULL records")
        raise Exception("Bronze records corrupted")

    logger.info("✅ Records structure check passed")


def run_bronze_quality_checks(df: DataFrame):

    logger.info("🔍 START Bronze quality checks")

    df.cache()

    check_record_count(df)
    check_required_columns(df)
    check_records_structure(df)

    logger.info(
        f"🎉 Bronze quality checks passed | rows={df.count()} | columns={len(df.columns)}"
    )