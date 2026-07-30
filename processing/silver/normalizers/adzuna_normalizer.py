from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize(df_bronze_job: DataFrame, ingestion_date: str) -> DataFrame:
    """
    Input: df sau explode("records").alias("job"), chưa .select("job.*")
    tức là mỗi row có 1 struct cột "job" đúng schema gốc Adzuna.
    Output: DataFrame theo schema chung, sẵn sàng để union với các nguồn khác.
    """
    return df_bronze_job.select(
        F.concat(F.lit("adzuna_"), F.col("job.id")).alias("job_id"),
        F.col("job.title").alias("title"),
        F.col("job.company.display_name").alias("company_name"),
        F.col("job.location.display_name").alias("location_name"),

        F.col("job.salary_min").alias("salary_min"),
        F.col("job.salary_max").alias("salary_max"),
        (F.col("job.salary_is_predicted") == "1").alias("salary_is_predicted"),

        F.col("job.contract_time").alias("contract_time"),
        F.col("job.contract_type").alias("contract_type"),

        F.col("job.category.label").alias("category_label"),
        F.col("job.category.tag").alias("category_tag"),

        F.col("job.latitude").alias("latitude"),
        F.col("job.longitude").alias("longitude"),
        F.col("job.redirect_url").alias("job_url"),

        F.col("job.created").cast("timestamp").alias("posted_at"),

        F.lit("adzuna").alias("source"),
        F.lit(ingestion_date).alias("ingestion_date"),
    )