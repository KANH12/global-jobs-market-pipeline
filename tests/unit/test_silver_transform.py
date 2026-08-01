from pyspark.sql.types import LongType, StringType, StructField, StructType

from processing.silver.build_silver import (
    clean_invalid_ids,
    deduplicate_jobs,
    normalize_salary,
    standardize_contract_fields,
)

# =========================
# Schema
# =========================
JOB_SCHEMA = StructType([
    StructField("job_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("contract_time", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("salary_min", LongType(), True),
    StructField("salary_max", LongType(), True),
])

# =========================
# Helper
# =========================
def _job(**overrides):
    base = {
        "job_id": "1",
        "title": "Data Engineer",
        "contract_time": "full_time",
        "contract_type": "permanent",
        "salary_min": 30,
        "salary_max": 50,
    }
    base.update(overrides)
    return base


def _make_df(spark, rows):
    return spark.createDataFrame(rows, schema=JOB_SCHEMA)


# =========================
# clean_invalid_ids
# =========================
def test_clean_invalid_ids_removes_null(spark):
    df = _make_df(spark, [
        _job(job_id="1"),
        _job(job_id=None)
    ])

    result = clean_invalid_ids(df)

    assert result.count() == 1


# =========================
# deduplicate_jobs
# =========================
def test_deduplicate_jobs(spark):
    df = _make_df(spark, [
        _job(job_id="1"),
        _job(job_id="1"),
        _job(job_id="2"),
    ])

    result = deduplicate_jobs(df)

    assert result.count() == 2


# =========================
# standardize_contract_fields
# =========================
def test_standardize_contract_fields_uppercase(spark):
    df = _make_df(spark, [
        _job(contract_time=" full_time ", contract_type="permanent")
    ])

    result = standardize_contract_fields(df)
    row = result.collect()[0]

    assert row["contract_time"] == "FULL_TIME"
    assert row["contract_type"] == "PERMANENT"


def test_standardize_contract_fields_null(spark):
    df = _make_df(spark, [
        _job(contract_time=None, contract_type=None)
    ])

    result = standardize_contract_fields(df)
    row = result.collect()[0]

    assert row["contract_time"] == "UNKNOWN"
    assert row["contract_type"] == "UNKNOWN"


# =========================
# normalize_salary
# =========================
def test_normalize_salary_scale(spark):
    df = _make_df(spark, [
        _job(salary_min=30, salary_max=50)
    ])

    result = normalize_salary(df)
    row = result.collect()[0]

    assert row["salary_min"] == 30000
    assert row["salary_max"] == 50000


def test_normalize_salary_keep(spark):
    df = _make_df(spark, [
        _job(salary_min=30000, salary_max=50000)
    ])

    result = normalize_salary(df)
    row = result.collect()[0]

    assert row["salary_min"] == 30000
    assert row["salary_max"] == 50000