from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from core.logger import get_job_logger

logger = get_job_logger(
    job_name="adzuna_gold_salary_analysis",
    component="gold"
)

# =========================
# 1. Build salary analysis
# =========================
def build_salary_analysis(df_silver: DataFrame) -> DataFrame:

    logger.info("🚀 Start build gold salary_analysis")

    salary_df = (
        df_silver
        .groupBy("contract_type")
        .agg(
            F.count("*").alias("job_count"),
            F.avg("salary_min").alias("avg_salary_min"),
            F.avg("salary_max").alias("avg_salary_max"),
            F.expr("percentile_approx(salary_max, 0.5)").alias("median_salary"),
            F.max("salary_max").alias("max_salary"),
            F.min("salary_min").alias("min_salary")
        )
        .orderBy(F.desc("avg_salary_max"))
    )

    logger.info("✅ Finished salary_analysis")

    return salary_df

# =========================
# 2. Write gold
# =========================
def write_salary_analysis(df: DataFrame, date_path: str):

    output_path = f"s3a://data-lake/gold/adzuna/salary_analysis/dt={date_path}"

    logger.info(f"📂 Writing gold salary analysis → {output_path}")

    # Gold thường nhỏ → 1 file cho đẹp
    df = df.repartition(1)

    count = df.count()

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info(f"📊 Output records: {count}")
    logger.info("🎉 Gold salary_analysis written successfully")