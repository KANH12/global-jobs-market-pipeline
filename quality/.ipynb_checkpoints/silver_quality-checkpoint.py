from pyspark.sql import DataFrame
from pyspark.sql import function as F
from core.logger import get_job_logger

logger = get_job_logger(
    job_name="adzuna_silver_quality",
    component="silver"
)

#check null
def check_required_fields(df: DataFrame):
    required_cols = ["job_id", "title", "company_name"]

    for col_name in required_cols:
        null_count = df.filter(F.col(col_name).isNull()).count()

        if null_count > 0:
            logger.warning(f"⚠️ {col_name} has {null_count} NULL values")

#check duplicate
def check_duplicates(df: DataFrame):
    dup = df.groupBy("job_id").count().filter("count > 1").count()

    if dup > 0:
        logger.warning(f"⚠️ Found {dup} duplicate job_ids")
    else:
        logger.info("✅ No duplicates found")

#check salary
def check_salary(df: DataFrame):
    invalid = df.filter(F.col("salary_min") > F.col("salary_max")).count()

    if invalid > 0:
        logger.warning(f"⚠️ Found {invalid} invalid salary ranges")

#check contract_type
def check_contract_type(df: DataFrame):
    valid = ["FULL_NAME", "PART_TIME", "CONTRACT", "UNKNOWN"]

    invalid = df.filter(~F.col("contract_type").isin(valid)).count()

    if invalid > 0:
        logger.warning(f"⚠️ Invalid contract_type rows: {invalid}")

def run_silver_quality_checks(df: DataFrame):
    logger.info("🔍 START Silver quality checks")

    df.cache()

    check_required_fields(df)
    check_duplicates(df)
    check_salary(df)
    check_contract_type(df)

    logger.info(f"🎉 Silver quality checks done | rows={df.count()} | cols={len(df.columns)}")