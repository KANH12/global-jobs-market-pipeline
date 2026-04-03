from pyspark.sql import DataFrame
from core.spark_session import create_spark_session
from core.logger import get_job_logger

from processing.silver.read_adzuna_silver import read_adzuna_silver
from processing.gold.jobs_summary import build_jobs_summary
from processing.gold.salary_analysis import build_salary_analysis

logger = get_job_logger(
    job_name="adzuna_gold_writer",
    component="gold"
)

#===================
# Write gold jobs
#===================

def write_gold(df: DataFrame, output_path: str):
    logger.info(f"📂 Writing Gold data to {output_path}")

    df = df.repartition(1) # 1 data = 1 file

    count = df.count()

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info(f"📊 Output records: {count}")
    logger.info("🎉 Write completed")

#==================
# Main
#==================
if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    date_path = "2026/03/26"

    try:
        logger.info(f"🚀 START Gold pipeline | date={date_path}")

        #========================
        # 1. Read Silver
        #========================
        df_silver = read_adzuna_silver(spark, date_path)

        #========================
        # 2. Build jobs_summary
        #========================
        df_jobs_summary = build_jobs_summary(df_silver)

        #========================
        # 3. Build salary_analysis
        #========================
        df_salary_analysis = build_salary_analysis(df_silver)

        #========================
        # 4. Write jobs_summary
        #========================
        write_gold(
            df_jobs_summary,
            f"s3a://data-lake/gold/adzuna/jobs_summary/dt={date_path}"
        )

        #========================
        # 5. Write salary_analysis
        #========================
        write_gold(
            df_salary_analysis,
            f"s3a://data-lake/gold/adzuna/salary_analysis/dt={date_path}"
        )

    except Exception:
        logger.error("❌ Gold pipeline failed", exc_info=True)
        raise
        
    finally:
        spark.stop()
        logger.info("🛑 Spark stopped")
