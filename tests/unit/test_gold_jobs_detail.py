import pytest
from pyspark.sql.types import *

from processing.gold.jobs_detail import build_jobs_detail

SCHEMA = StructType([
    StructField("job_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("category_label", StringType(), True),
    StructField("location_name", StringType(), True),
    StructField("contract_time", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("posted_at", StringType(), True),
    StructField("salary_min", LongType(), True),
    StructField("salary_max", LongType(), True),
    StructField("salary_is_predicted", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("category_tag", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("company_id", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("job_url", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])


def _row(**kwargs):
    base = {
        "job_id": "1",
        "title": "Data Engineer",
        "company_name": "ABC",
        "category_label": "IT",
        "location_name": "HCM",
        "contract_time": "FULL_TIME",
        "contract_type": "PERMANENT",
        "posted_at": "2026-07-30",
        "salary_min": 30000,
        "salary_max": 50000,
        "salary_is_predicted": 0,
        "latitude": 10.7,
        "longitude": 106.6,
        "category_tag": "it",
        "category_id": "c1",
        "company_id": "co1",
        "location_id": "l1",
        "job_url": "url",
        "source": "adzuna",
        "ingestion_date": "2026-07-30"
    }
    base.update(kwargs)
    return base


def _df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


# =========================
# salary logic
# =========================
def test_salary_avg_and_range(spark):
    df = _df(spark, [_row()])

    result = build_jobs_detail(df).collect()[0]

    assert result["salary_avg"] == 40000
    assert result["salary_range"] == "30000 - 50000"
    assert result["is_salary_available"] is True


def test_salary_null_handling(spark):
    df = _df(spark, [_row(salary_min=None, salary_max=None)])

    result = build_jobs_detail(df).collect()[0]

    assert result["salary_avg"] is None
    assert result["salary_range"] == "Not available"
    assert result["is_salary_available"] is False


# =========================
# posted_date
# =========================
def test_posted_date_derived(spark):
    df = _df(spark, [_row(posted_at="2026-07-30")])

    result = build_jobs_detail(df).collect()[0]

    assert str(result["posted_date"]) == "2026-07-30"


# =========================
# search text
# =========================
def test_search_text_lowercase_concat(spark):
    df = _df(spark, [_row(
        title="Data Engineer",
        company_name="ABC Corp",
        location_name="HCM"
    )])

    result = build_jobs_detail(df).collect()[0]

    text = result["job_search_text"]

    assert "data engineer" in text
    assert "abc corp" in text
    assert text == text.lower()


# =========================
# output columns
# =========================
def test_output_columns(spark):
    df = _df(spark, [_row()])

    result = build_jobs_detail(df)

    expected_cols = {
        "job_id",
        "title",
        "company_name",
        "category_label",
        "location_name",
        "contract_time",
        "contract_type",
        "posted_at",
        "posted_date",
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
        "job_url",
        "source",
        "job_search_text",
        "ingestion_date"
    }

    assert set(result.columns) == expected_cols