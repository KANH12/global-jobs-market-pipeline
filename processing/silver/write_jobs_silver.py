from pyspark.sql import functions as F
from core.spark_session import create_spark_session
from core.logger import get_job_logger
from processing.bronze.read_bronze_data import read_adzuna_bronze
from quality.silver_quality import run_silver_quality_checks

logger = get_job_logger(
    job_name="adzuna_jobs_silver_writer",
    component="silver"
)

# =========================
# 0. Explode bronze records → job-level
# =========================
def transform_bronze_to_silver(df_bronze):
    df = (
        df_bronze
        .filter(F.col("records").isNotNull())
        .select(F.explode("records").alias("job"))
        .select("job.*")
    )
    return df

# =========================
# 1. Filter invalid jobs 
# =========================
def clean_invalid_ids(df):

    null_id_count = df.filter(F.col("id").isNull()).count()

    df = df.filter(F.col("id").isNotNull())

    logger.info(f"🧹 Removed NULL id: {null_id_count}")
    return df

# =========================
# 2. Checks duplicate 
# =========================
def deduplicate_jobs(df):
    df_dedup = df.dropDuplicates(["id"])
    duplicate_count = df.count() - df_dedup.count()

    logger.info(f"🧹 Removed duplicate id: {duplicate_count}")
    return df_dedup

# =========================
# 3. Normalize column
# =========================
def standardize_contract_type(df):
    valid_types = ["FULL_TIME", "PART_TIME", "CONTRACT"]

    df = df.withColumn(
        "contract_type",
        F.when(
            ~F.col("contract_type").isin(valid_types),
            "UNKNOWN"
        ).otherwise(F.col("contract_type"))
    )

    logger.info("🧽 Standardized contract_type")
    return df


def process_silver(df, date_path):

    logger.info(f"🚀 START Silver pipeline | date={date_path}")
    df = df.cache()

    # 1. Cleaning
    df = clean_invalid_ids(df)
    df = deduplicate_jobs(df)

    # 2. Standardization
    df = standardize_contract_type(df)

    df.unpersist()
    
    # =========================
    # 3. Select & transform columns
    # =========================
    jobs_df = df.select(
        F.col("id").alias("job_id"),
        F.col("title"),
        
        F.when(
            F.col("contract_time").isNull(),
            F.lit("UNKNOWN")
        ).otherwise(
            F.upper(F.trim(F.col("contract_time")))
        ).alias("contract_time"),
        
        F.col("contract_type"),
        F.col("created").cast("timestamp"),
        F.col("salary_min"),
        F.col("salary_max"),
        (F.col("salary_is_predicted") == "1").alias("salary_is_predicted"),

        F.col("latitude"),
        F.col("longitude"),

        # ---------- CATEGORY ----------
        F.col("category.label").alias("category_label"),
        F.col("category.tag").alias("category_tag"),
        F.sha2(
            F.concat_ws(
                "||",
                F.col("category.label"),
                F.col("category.tag")
            ),
            256
        ).alias("category_id"),

        # ---------- COMPANY ----------
        F.col("company.display_name").alias("company_name"),
        F.sha2(
            F.col("company.display_name"),
            256
        ).alias("company_id"),

        # ---------- LOCATION ----------
        F.col("location.display_name").alias("location_name"),
        F.sha2(
            F.concat_ws(
                "||",
                F.col("location.display_name"),
                F.concat_ws(",", F.col("location.area"))
            ),
            256
        ).alias("location_id"),
        F.lit(date_path).alias("ingestion_date")
    )

    logger.info("✅ Selected & transformed columns")

    return jobs_df

    # =========================
    # 4. Write Parquet
    # =========================

def write_jobs_silver(jobs_df, date_path: str):
    output_path = f"s3a://data-lake/silver/adzuna/jobs/dt={date_path}"

    #avoid small files problem
    jobs_df = jobs_df.repartition(4)

    logger.info(f"📂 Writing jobs parquet to {output_path}")
    count = jobs_df.count()
    (
        jobs_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )
    logger.info(f"📊 Output records: {count}")
    logger.info("🎉 Silver jobs parquet written successfully")


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    date_path = "2026/03/26"
    
    try:
        #1. read bronze layer
        df_bronze = read_adzuna_bronze(spark, date_path)

        #2. transform bronze layer to silver layer
        df = transform_bronze_to_silver(df_bronze)

        #3. process silver layer
        jobs_df = process_silver(df, date_path)

        #4. check silver quality
        run_silver_quality_checks(jobs_df)

        #5. write silver parquet file
        write_jobs_silver(jobs_df, date_path)
        
    except Exception:
        logger.error("❌ Silver jobs running failed", exc_info=True)
        raise
        
    finally:
        spark.stop()
        logger.info("🛑 Spark stopped")

    
