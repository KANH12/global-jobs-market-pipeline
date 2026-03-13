from pyspark.sql import functions as F
from core.spark_session import create_spark_session
from core.logger import get_job_logger
from processing.bronze import read_bronze_data

logger = get_job_logger(
    job_name="adzuna_jobs_silver_writer",
    component="silver"
)

def tranform_bronze_to_silver(df_bronze):
    # =========================
    # 0. Explode bronze records → job-level
    # =========================
    df = (
        df_bronze
        .filter(F.col("records").isNotNull())
        .select(F.explode("records").alias("job"))
        .select("job.*")
    )
    return df

def process_silver(df, date_path):

    logger.info(f"🚀 START Silver pipeline | date={date_path}")

    # =========================
    # 1. Filter invalid jobs
    # =========================
    df = df.cache()
    before_count = df.count()

    df = df.filter(F.col("id").isNotNull())

    after_count = df.count()
    logger.info(f"🧹 Removed jobs with NULL id: {before_count - after_count}")

    # =========================
    # 2. Select & transform columns
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
        
        F.when(
            F.col("contract_type").isNull(),
            F.lit("UNKNOWN")
        ).otherwise(
            F.upper(F.trim(F.col("contract_type")))
        ).alias("contract_type"),

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
    # 3. Write Parquet
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

    date_path = "2026/02/04"
    
    try:
        #1. read bronze layer
        df_bronze = read_bronze_data(spark, date_path)

        #2. transform bronze layer to silver layer
        df = tranform_bronze_to_silver(df_bronze)

        #3. process silver layer
        jobs_df = process_silver(df, date_path)

        #4. write silver parquet file
        write_jobs_silver(jobs_df, date_path)
        
    except Exception:
        logger.error("❌ Silver jobs running failed", exc_info=True)
        raise
        
    finally:
        spark.stop()
        logger.info("🛑 Spark stopped")

    
