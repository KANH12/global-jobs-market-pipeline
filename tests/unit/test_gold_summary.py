from pyspark.sql.types import LongType, StringType, StructField, StructType

from processing.gold.jobs_summary import build_jobs_summary

SILVER_SCHEMA = StructType([
    StructField("ingestion_date", StringType(), True),
    StructField("category_label", StringType(), True),
    StructField("salary_min", LongType(), True),
    StructField("salary_max", LongType(), True),
    StructField("contract_time", StringType(), True),
])


def _make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


def test_jobs_summary_groups_by_date_and_category(spark):
    data = [
        {"ingestion_date": "2026/05/20", "category_label": "IT",
         "salary_min": 1000, "salary_max": 2000, "contract_time": "FULL_TIME"},
        {"ingestion_date": "2026/05/20", "category_label": "IT",
         "salary_min": 2000, "salary_max": 3000, "contract_time": "PART_TIME"},
        {"ingestion_date": "2026/05/20", "category_label": "Sales",
         "salary_min": 500, "salary_max": 1500, "contract_time": "FULL_TIME"},
    ]
    df = _make_df(spark, data)

    result = build_jobs_summary(df)

    assert result.count() == 2

    it_row = result.filter(result.category_label == "IT").collect()[0]
    assert it_row["total_jobs"] == 2
    assert it_row["avg_salary"] == 2000
    assert it_row["min_salary"] == 1000
    assert it_row["max_salary"] == 3000


def test_jobs_summary_contract_time_percentage(spark):
    data = [
        {"ingestion_date": "2026/05/20", "category_label": "IT",
         "salary_min": 1000, "salary_max": 2000, "contract_time": "FULL_TIME"},
        {"ingestion_date": "2026/05/20", "category_label": "IT",
         "salary_min": 1000, "salary_max": 2000, "contract_time": "PART_TIME"},
        {"ingestion_date": "2026/05/20", "category_label": "IT",
         "salary_min": 1000, "salary_max": 2000, "contract_time": "UNKNOWN"},
        {"ingestion_date": "2026/05/20", "category_label": "IT",
         "salary_min": 1000, "salary_max": 2000, "contract_time": "UNKNOWN"},
    ]
    df = _make_df(spark, data)

    result = build_jobs_summary(df)
    row = result.collect()[0]

    assert row["total_jobs"] == 4
    assert abs(row["pct_full_time"] - 0.25) < 0.001
    assert abs(row["pct_part_time"] - 0.25) < 0.001
    assert abs(row["pct_unknown"] - 0.5) < 0.001

    assert "full_time_count" not in result.columns
    assert "part_time_count" not in result.columns
    assert "unknown_count" not in result.columns