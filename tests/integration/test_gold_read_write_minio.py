from pyspark.sql.types import LongType, StringType, StructField, StructType

from processing.gold.write_adzuna_gold import write_gold

TEST_DATE = "9999/01/01"
GOLD_PATH = f"s3a://data-lake/gold/adzuna/jobs_summary/dt={TEST_DATE}"

SILVER_SCHEMA = StructType([
    StructField("ingestion_date", StringType(), True),
    StructField("category_label", StringType(), True),
    StructField("salary_min", LongType(), True),
    StructField("salary_max", LongType(), True),
    StructField("contract_time", StringType(), True),
])


def test_write_gold_round_trip(spark, cleanup_s3a_paths):
    from processing.gold.jobs_summary import build_jobs_summary

    data = [
        {"ingestion_date": TEST_DATE, "category_label": "IT",
         "salary_min": 1000, "salary_max": 2000, "contract_time": "FULL_TIME"},
        {"ingestion_date": TEST_DATE, "category_label": "IT",
         "salary_min": 2000, "salary_max": 3000, "contract_time": "PART_TIME"},
    ]
    df_silver = spark.createDataFrame(data, schema=SILVER_SCHEMA)
    df_gold = build_jobs_summary(df_silver)

    write_gold(df_gold, GOLD_PATH)
    cleanup_s3a_paths.append(GOLD_PATH)

    # Đọc lại thật từ MinIO - verify file Parquet ghi ra đọc lại đúng, không lỗi format
    result = spark.read.parquet(GOLD_PATH)

    assert result.count() == 1
    assert result.collect()[0]["total_jobs"] == 2