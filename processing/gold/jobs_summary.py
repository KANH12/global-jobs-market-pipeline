from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from core.logger import get_job_logger

logger = get_job_logger(
    job_name="adzuna_jobs_gold_summary",
    component="gold"
)


# =========================
# 1. Build jobs summary
# =========================
def build_jobs_summary(df_silver: DataFrame) -> DataFrame:

    logger.info("🚀 START building gold jobs_summary")

    # =========================
    # 1. Prepare salary
    # =========================
    df = df_silver.withColumn(
        "avg_salary_row",
        (F.col("salary_min") + F.col("salary_max")) / 2
    )

    # =========================
    # 2. Aggregate
    # =========================
    summary_df = (
        df
        .groupBy("ingestion_date", "category_label")
        .agg(
            # volume
            F.count("*").alias("total_jobs"),

            # salary stats
            F.avg("avg_salary_row").alias("avg_salary"),
            F.min("salary_min").alias("min_salary"),
            F.max("salary_max").alias("max_salary"),
            F.expr("percentile_approx(avg_salary_row, 0.5)").alias("median_salary"),

            # contract type counts
            F.sum(F.when(F.col("contract_type") == "FULL_TIME", 1).otherwise(0)).alias("full_time_count"),
            F.sum(F.when(F.col("contract_type") == "PART_TIME", 1).otherwise(0)).alias("part_time_count"),
            F.sum(F.when(F.col("contract_type") == "CONTRACT", 1).otherwise(0)).alias("contract_count"),
        )
    )

    # =========================
    # 3. Convert to %
    # =========================
    summary_df = (
        summary_df
        .withColumn("pct_full_time", F.col("full_time_count") / F.col("total_jobs"))
        .withColumn("pct_part_time", F.col("part_time_count") / F.col("total_jobs"))
        .withColumn("pct_contract", F.col("contract_count") / F.col("total_jobs"))
    )

    # =========================
    # 4. Cleanup
    # =========================
    summary_df = summary_df.drop(
        "full_time_count",
        "part_time_count",
        "contract_count"
    )

    logger.info("✅ Gold jobs_summary built successfully")

    return summary_df
