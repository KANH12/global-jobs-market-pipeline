from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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

#check contract fields
def check_contract_fields(df: DataFrame):
    
    #========================
    # Check contract_type
    #========================
    valid_contract_type = ["CONTRACT", "UNKNOWN", "PERMANENT"]

    invalid_contract_type_df = df.filter(~F.col("contract_type").isin(valid_contract_type))

    invalid_contract_type_count = invalid_contract_type_df.count()

    if invalid_contract_type_count == 0:
        logger.info("✅ No invalid contract_type found")
        return

    logger.warning(f"⚠️ Found {invalid_contract_type_count} invalid contract_type rows")

    invalid_contract_type_summary = (
        invalid_contract_type_df
        .groupBy("contract_type")
        .count()
        .orderBy(F.desc("count"))
    )

    invalid_contract_type_summary.show(truncate=False)

    #========================
    # Check contract_time
    #========================
    valid_contract_time = ["FULL_TIME", "PART_TIME","UNKNOWN"]

    invalid_contract_time_df = df.filter(~F.col("contract_time").isin(valid_contract_time))

    invalid_contract_time_count = invalid_contract_time_df.count()

    if invalid_contract_time_count == 0:
        logger.info("✅ No invalid contract_time found")
        return

    logger.warning(f"⚠️ Found {invalid_contract_time_count} invalid contract_time rows")

    invalid_contract_time_summary = (
        invalid_contract_time_df
        .groupBy("contract_time")
        .count()
        .orderBy(F.desc("count"))
    )

    invalid_contract_time_summary.show(truncate=False)

def run_silver_quality_checks(df: DataFrame):
    logger.info("🔍 START Silver quality checks")

    df.cache()

    check_required_fields(df)
    check_duplicates(df)
    check_salary(df)
    check_contract_fields(df)

    logger.info(f"🎉 Silver quality checks done | rows={df.count()} | cols={len(df.columns)}")