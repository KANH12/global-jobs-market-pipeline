import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType


def _parse_salary(salary_str):
    if not salary_str:
        return (None, None)

    s = salary_str.lower().replace(",", "")
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*(k)?", s)

    numbers = []
    for value, k_suffix in matches:
        num = float(value)
        if k_suffix:
            num *= 1000
        numbers.append(num)

    if not numbers:
        return (None, None)
    if len(numbers) == 1:
        return (numbers[0], numbers[0])
    return (min(numbers), max(numbers))


_salary_schema = StructType([
    StructField("min", DoubleType(), True),
    StructField("max", DoubleType(), True),
])

_parse_salary_udf = F.udf(_parse_salary, _salary_schema)


def normalize(df_bronze_job: DataFrame, ingestion_date: str) -> DataFrame:
    """
    Input: df sau explode("jobs").alias("job") (schema Jooble, flat, không nested).
    Output: DataFrame theo schema chung, sẵn sàng để union với các nguồn khác.
    """
    df = df_bronze_job.withColumn("_salary_parsed", _parse_salary_udf(F.col("job.salary")))

    contract_time_expr = (
        F.when(F.lower(F.col("job.type")).contains("full"), "FULL_TIME")
        .when(F.lower(F.col("job.type")).contains("part"), "PART_TIME")
        .otherwise(None)
    )
    contract_type_expr = (
        F.when(F.lower(F.col("job.type")).contains("contract"), "CONTRACT")
        .when(F.lower(F.col("job.type")).contains("permanent"), "PERMANENT")
        .when(F.lower(F.col("job.type")).contains("temporary"), "TEMPORARY")
        .otherwise(None)
    )

    return df.select(
        F.concat(F.lit("jooble_"), F.col("job.id").cast("string")).alias("job_id"),
        F.col("job.title").alias("title"),
        F.col("job.company").alias("company_name"),
        F.col("job.location").alias("location_name"),

        F.col("_salary_parsed.min").alias("salary_min"),
        F.col("_salary_parsed.max").alias("salary_max"),
        F.lit(None).cast("boolean").alias("salary_is_predicted"),  

        contract_time_expr.alias("contract_time"),
        contract_type_expr.alias("contract_type"),

        F.lit("Uncategorized").alias("category_label"),  
        F.lit(None).cast("string").alias("category_tag"),

        F.lit(None).cast("double").alias("latitude"),   
        F.lit(None).cast("double").alias("longitude"),
        F.col("job.link").alias("job_url"),

        F.to_timestamp(F.col("job.updated")).alias("posted_at"),

        F.lit("jooble").alias("source"),
        F.lit(ingestion_date).alias("ingestion_date"),
    )