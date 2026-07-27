from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing.gold.jobs_detail import build_jobs_detail

SILVER_SCHEMA = StructType([
    StructField("job_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("category_label", StringType(), True),
    StructField("location_name", StringType(), True),
    StructField("contract_time", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("created", StringType(), True),
    StructField("salary_min", LongType(), True),
    StructField("salary_max", LongType(), True),
    StructField("salary_is_predicted", BooleanType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("category_tag", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("company_id", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])


def _base_record(**overrides):
    record = {
        "job_id": "1",
        "title": "Data Engineer",
        "company_name": "ABC",
        "category_label": "IT",
        "location_name": "HCM",
        "contract_time": "FULL_TIME",
        "contract_type": "PERMANENT",
        "created": "2026-05-20",
        "salary_min": 1000,
        "salary_max": 2000,
        "salary_is_predicted": False,
        "latitude": 10.0,
        "longitude": 106.0,
        "category_tag": "it-jobs",
        "category_id": "1",
        "company_id": "1",
        "location_id": "1",
        "ingestion_date": "2026/05/20",
    }
    record.update(overrides)
    return record


def _make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


def test_jobs_detail_computes_salary_avg_and_range(spark):
    df = _make_df(spark, [_base_record()])

    result = build_jobs_detail(df)
    row = result.collect()[0]

    assert row["salary_avg"] == 1500
    assert row["salary_range"] == "1000 - 2000"
    assert row["is_salary_available"] is True
    assert row["created_date"] is not None


def test_jobs_detail_handles_missing_salary(spark):
    df = _make_df(spark, [_base_record(salary_min=None, salary_max=None)])

    result = build_jobs_detail(df)
    row = result.collect()[0]

    assert row["salary_avg"] is None
    assert row["salary_range"] == "Not available"
    assert row["is_salary_available"] is False


def test_jobs_detail_builds_search_text(spark):
    df = _make_df(spark, [_base_record()])

    result = build_jobs_detail(df)
    row = result.collect()[0]

    assert "data engineer" in row["job_search_text"]
    assert "abc" in row["job_search_text"]
    assert "it" in row["job_search_text"]


def test_jobs_detail_output_columns(spark):
    df = _make_df(spark, [_base_record()])

    result = build_jobs_detail(df)

    expected_columns = {
        "job_id", "title", "company_name", "category_label", "location_name",
        "contract_time", "contract_type", "created", "created_date",
        "salary_min", "salary_max", "salary_avg", "salary_range",
        "salary_is_predicted", "is_salary_available", "latitude", "longitude",
        "category_tag", "category_id", "company_id", "location_id",
        "job_search_text", "ingestion_date"
    }
    assert set(result.columns) == expected_columns