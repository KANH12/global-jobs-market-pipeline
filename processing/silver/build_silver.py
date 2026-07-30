import argparse

from pyspark.sql import functions as F

from core.logger import get_job_logger
from core.spark_session import create_spark_session
from processing.bronze.read_bronze import read_bronze
from processing.silver.normalizers import adzuna_normalizer, jooble_normalizer
from quality.silver_quality import run_silver_quality_checks

logger = get_job_logger(
    job_name="build_silver",
    component="silver"
)

NORMALIZERS = {
    "adzuna": adzuna_normalizer.normalize,
    "jooble": jooble_normalizer.normalize,
}

# 0. Explode records -> job-level
def explode_records(df_bronze):
    return (
        df_bronze
        .filter(F.col("records").isNotNull())
        .select(F.explode("records").alias("job"))
    )

# 1. Filter invalid jobs
def clean_invalid_ids(df):
    null_id_count = df.filter(F.col("job_id").isNull()).count()

    df = df.filter(F.col("job_id").isNotNull())

    logger.info(f"[INFO] Removed NULL job_id records: {null_id_count}")
    return df

# 2. Deduplicate jobs
def deduplicate_jobs(df):
    original_count = df.count()

    df_dedup = df.dropDuplicates(["job_id"])

    dedup_count = df_dedup.count()
    duplicate_count = original_count - dedup_count

    logger.info(f"[INFO] Removed duplicate job_id records: {duplicate_count}")
    return df_dedup

# 3. Standardize contract fields
def standardize_contract_fields(df):
    df = df.withColumn(
        "contract_time",
        F.when(F.col("contract_time").isNull(), "UNKNOWN")
        .otherwise(F.upper(F.trim(F.col("contract_time"))))
    )

    df = df.withColumn(
        "contract_type",
        F.when(F.col("contract_type").isNull(), "UNKNOWN")
        .otherwise(F.upper(F.trim(F.col("contract_type"))))
    )

    logger.info("[INFO] Standardized contract fields")
    return df

# 4. Normalize salary
def normalize_salary(df):
    logger.info("[START] Cleaning and normalizing salary")

    df = df.withColumn(
        "salary_min",
        F.when(
            F.col("salary_min").isNotNull() & (F.col("salary_min") < 1000),
            F.col("salary_min") * 1000
        ).otherwise(F.col("salary_min"))
    )

    df = df.withColumn(
        "salary_max",
        F.when(
            F.col("salary_max").isNotNull() & (F.col("salary_max") < 1000),
            F.col("salary_max") * 1000
        ).otherwise(F.col("salary_max"))
    )

    logger.info("[SUCCESS] Normalized salary")
    return df

def add_entity_ids(df):
    df = df.withColumn(
        "category_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("category_label"), F.lit("")),
                F.coalesce(F.col("category_tag"), F.lit(""))
            ),
            256
        )
    )

    df = df.withColumn(
        "company_id",
        F.sha2(F.coalesce(F.col("company_name"), F.lit("")), 256)
    )

    df = df.withColumn(
        "location_id",
        F.sha2(F.coalesce(F.col("location_name"), F.lit("")), 256)
    )

    return df

def build_source_silver(spark, source: str, date_path: str):
    if source not in NORMALIZERS:
        raise ValueError(f"Unknown source: {source}. Available: {list(NORMALIZERS.keys())}")

    normalize_fn = NORMALIZERS[source]

    df_bronze = read_bronze(spark, source, date_path)
    df_job = explode_records(df_bronze)
    df_common = normalize_fn(df_job, date_path)

    record_count = df_common.count()
    logger.info(f"[INFO] Normalized source={source} | records={record_count}")

    return df_common

def process_silver(spark, sources: list, date_path: str):
    logger.info(f"[START] Silver transformation | sources={sources} | date={date_path}")

    normalized_dfs = [build_source_silver(spark, source, date_path) for source in sources]

    df = normalized_dfs[0]
    for other_df in normalized_dfs[1:]:
        df = df.unionByName(other_df)

    df = clean_invalid_ids(df)
    df = deduplicate_jobs(df)
    df = standardize_contract_fields(df)
    df = normalize_salary(df)
    df = add_entity_ids(df)

    logger.info("[SUCCESS] Unified silver dataset built across all sources")
    return df

def write_jobs_silver(jobs_df, date_path: str):
    output_path = f"s3a://data-lake/silver/jobs/dt={date_path}"

    jobs_df = jobs_df.repartition(4)

    logger.info(f"[START] Writing silver jobs parquet to {output_path}")

    count = jobs_df.count()

    (
        jobs_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info(f"[INFO] Silver output records: {count}")
    logger.info("[SUCCESS] Silver jobs parquet written successfully")

def run_silver_pipeline(spark, sources: list, date_path: str):
    logger.info("=" * 60)
    logger.info(f"[START] Silver pipeline | sources={sources} | date={date_path}")

    try:
        jobs_df = process_silver(spark, sources, date_path)

        run_silver_quality_checks(jobs_df)

        write_jobs_silver(jobs_df, date_path)

        logger.info("[SUCCESS] Silver pipeline completed successfully")

        return jobs_df

    except Exception:
        logger.error("[ERROR] Silver pipeline failed", exc_info=True)
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sources",
        nargs="+",
        default=["adzuna", "jooble"],
        choices=["adzuna", "jooble"],
        help="List of sources to include, e.g. --sources adzuna jooble"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date path format: YYYY/MM/DD, example: 2026/07/28"
    )
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_silver_pipeline(spark, args.sources, args.date)

    finally:
        spark.stop()
        logger.info("[STOP] Spark stopped")
        logger.info("=" * 60)
