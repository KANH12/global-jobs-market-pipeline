from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_jobs_detail(df_silver: DataFrame) -> DataFrame:
    df = (
        df_silver
        .withColumn(
            "salary_avg",
            F.when(
                F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull(),
                (F.col("salary_min") + F.col("salary_max")) / 2
            ).otherwise(None)
        )
        .withColumn(
            "salary_range",
            F.when(
                F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull(),
                F.concat_ws(
                    " - ",
                    F.col("salary_min").cast("string"),
                    F.col("salary_max").cast("string")
                )
            ).otherwise("Not available")
        )
        .withColumn(
            "is_salary_available",
            F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull()
        )
        .withColumn(
            "created_date",
            F.to_date(F.col("created"))
        )
        .withColumn(
            "job_search_text",
            F.lower(
                F.concat_ws(
                    " ",
                    F.col("title"),
                    F.col("company_name"),
                    F.col("category_label"),
                    F.col("location_name"),
                    F.col("contract_time"),
                    F.col("contract_type")
                )
            )
        )
        .select(
            "job_id",
            "title",
            "company_name",
            "category_label",
            "location_name",
            "contract_time",
            "contract_type",
            "created",
            "created_date",
            "salary_min",
            "salary_max",
            "salary_avg",
            "salary_range",
            "salary_is_predicted",
            "is_salary_available",
            "latitude",
            "longitude",
            "category_tag",
            "category_id",
            "company_id",
            "location_id",
            "job_search_text",
            "ingestion_date"
        )
    )

    return df