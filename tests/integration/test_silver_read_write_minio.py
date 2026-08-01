from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing.silver.build_silver import process_silver, write_jobs_silver
from processing.silver.read_silver import read_silver

TEST_DATE = "9999/01/01"

SILVER_PATH = f"s3a://data-lake/silver/jobs/dt={TEST_DATE}"

ADZUNA_BRONZE_PATH = f"s3a://data-lake/bronze/adzuna/{TEST_DATE}"
JOOBLE_BRONZE_PATH = f"s3a://data-lake/bronze/jooble/{TEST_DATE}"


ADZUNA_JOB_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("contract_time", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("created", LongType(), True),
    StructField("salary_min", LongType(), True),
    StructField("salary_max", LongType(), True),
    StructField("salary_is_predicted", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("redirect_url", StringType(), True),
    StructField("category", StructType([
        StructField("label", StringType(), True),
        StructField("tag", StringType(), True),
    ]), True),
    StructField("company", StructType([
        StructField("display_name", StringType(), True),
    ]), True),
    StructField("location", StructType([
        StructField("display_name", StringType(), True),
    ]), True),
])

ADZUNA_BRONZE_SCHEMA = StructType([
    StructField("records", ArrayType(ADZUNA_JOB_SCHEMA), True),
    StructField("batch_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


JOOBLE_JOB_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("company", StringType(), True),
    StructField("location", StringType(), True),
    StructField("salary", StringType(), True),
    StructField("type", StringType(), True),
    StructField("link", StringType(), True),
    StructField("updated", StringType(), True),
])

JOOBLE_BRONZE_SCHEMA = StructType([
    StructField("records", ArrayType(JOOBLE_JOB_SCHEMA), True),
    StructField("batch_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


def _adzuna_job(**overrides):
    job = {
        "id": "101",
        "title": "Data Engineer",
        "contract_time": "full_time",
        "contract_type": "permanent",
        "created": 1234567890,
        "salary_min": 30000,
        "salary_max": 50000,
        "salary_is_predicted": "0",
        "latitude": 10.0,
        "longitude": 106.0,
        "redirect_url": "https://adzuna.example/job/101",
        "category": {"label": "IT", "tag": "it-jobs"},
        "company": {"display_name": "ABC Corp"},
        "location": {"display_name": "Ho Chi Minh City"},
    }
    job.update(overrides)
    return job


def _jooble_job(**overrides):
    job = {
        "id": "201",
        "title": "Backend Developer",
        "company": "XYZ Ltd",
        "location": "Hanoi",
        "salary": "45k - 65k",
        "type": "Full-time, Permanent",
        "link": "https://jooble.example/job/201",
        "updated": "2026-06-01T00:00:00",
    }
    job.update(overrides)
    return job


def _write_bronze(spark, path, schema, jobs, batch_id="test-batch-1", source="test"):
    rows = [{
        "records": jobs,
        "batch_id": batch_id,
        "source": source,
        "ingested_at": "2026-01-01T00:00:00",
    }]
    spark.createDataFrame(rows, schema=schema).coalesce(1).write.mode("overwrite").json(path)


def test_process_silver_unifies_adzuna_and_jooble(spark, cleanup_s3a_paths):
    _write_bronze(spark, ADZUNA_BRONZE_PATH, ADZUNA_BRONZE_SCHEMA,
                  [_adzuna_job(), _adzuna_job(id="102")], source="adzuna")
    cleanup_s3a_paths.append(ADZUNA_BRONZE_PATH)

    _write_bronze(spark, JOOBLE_BRONZE_PATH, JOOBLE_BRONZE_SCHEMA,
                  [_jooble_job()], source="jooble")
    cleanup_s3a_paths.append(JOOBLE_BRONZE_PATH)

    jobs_df = process_silver(spark, sources=["adzuna", "jooble"], date_path=TEST_DATE)
    write_jobs_silver(jobs_df, TEST_DATE)
    cleanup_s3a_paths.append(SILVER_PATH)

    result = read_silver(spark, TEST_DATE)
    rows = {row["job_id"]: row.asDict() for row in result.collect()}

    assert result.count() == 3
    assert set(rows.keys()) == {"adzuna_101", "adzuna_102", "jooble_201"}

    adzuna_row = rows["adzuna_101"]
    assert adzuna_row["company_name"] == "ABC Corp"
    assert adzuna_row["category_label"] == "IT"
    assert adzuna_row["contract_time"] == "FULL_TIME"
    assert adzuna_row["contract_type"] == "PERMANENT"
    assert adzuna_row["salary_min"] == 30000
    assert adzuna_row["salary_max"] == 50000

    jooble_row = rows["jooble_201"]
    assert jooble_row["company_name"] == "XYZ Ltd"
    assert jooble_row["category_label"] == "Uncategorized"
    assert jooble_row["salary_min"] == 45000.0
    assert jooble_row["salary_max"] == 65000.0
    assert jooble_row["contract_time"] == "FULL_TIME"
    assert jooble_row["contract_type"] == "PERMANENT"

    for row in rows.values():
        assert row["category_id"] is not None
        assert row["company_id"] is not None
        assert row["location_id"] is not None


def test_process_silver_deduplicates_by_job_id_within_source(spark, cleanup_s3a_paths):
    _write_bronze(spark, ADZUNA_BRONZE_PATH, ADZUNA_BRONZE_SCHEMA,
                  [_adzuna_job(id="101")], batch_id="b1", source="adzuna")
    (
        spark.createDataFrame(
            [{"records": [_adzuna_job(id="101", title="Data Engineer (updated)")],
              "batch_id": "b2", "source": "adzuna", "ingested_at": "2026-01-01T01:00:00"}],
            schema=ADZUNA_BRONZE_SCHEMA,
        )
        .coalesce(1)
        .write.mode("append")
        .json(ADZUNA_BRONZE_PATH)
    )
    cleanup_s3a_paths.append(ADZUNA_BRONZE_PATH)

    jobs_df = process_silver(spark, sources=["adzuna"], date_path=TEST_DATE)
    write_jobs_silver(jobs_df, TEST_DATE)
    cleanup_s3a_paths.append(SILVER_PATH)

    result = read_silver(spark, TEST_DATE)

    assert result.count() == 1
    assert result.collect()[0]["job_id"] == "adzuna_101"