from pyspark.sql import SparkSession

def read_jobs_summary(spark, date):
    return spark.read.parquet(
        f"s3a://data-lake/gold/adzuna/jobs_summary/dt={date}"
    )

def read_salary_analysis(spark, date):
    return spark.read.parquet(
        f"s3a://data-lake/gold/adzuna/salary_analysis/dt={date}"
    )

def read_jobs_detail(spark, date_path: str):
    return spark.read.parquet(
        f"s3a://data-lake/gold/adzuna/jobs_detail/dt={date_path}"
    )