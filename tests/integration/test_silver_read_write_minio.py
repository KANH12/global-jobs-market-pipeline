from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing.silver.read_silver import read_adzuna_silver
from processing.silver.build_silver import (
    process_silver,
    transform_bronze_to_silver,
    write_jobs_silver,
)

TEST_DATE = "9999/01/01"
SILVER_PATH = f"s3a://data-lake/silver/adzuna/jobs/dt={TEST_DATE}"


JOB_SCHEMA = StructType([
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
    StructField("category", StructType([
        StructField("label", StringType(), True),
        StructField("tag", StringType(), True),
    ]), True),
    StructField("company", StructType([
        StructField("display_name", StringType(), True),
    ]), True),
    StructField("location", StructType([
        StructField("display_name", StringType(), True),
        StructField("area", ArrayType(StringType()), True),
    ]), True),
])

BRONZE_SCHEMA = StructType([
    StructField("records", ArrayType(JOB_SCHEMA), True),
])


def _bronze_job(**overrides):
    job = {
        "id": "1",
        "title": "Data Engineer",
        "contract_time": "full_time",
        "contract_type": "permanent",
        "created": 1234567890,
        "salary_min": 30,
        "salary_max": 50,
        "salary_is_predicted": "0",
        "latitude": 10.0,
        "longitude": 106.0,
        "category": {"label": "IT", "tag": "it-jobs"},
        "company": {"display_name": "ABC"},
        "location": {"display_name": "HCM", "area": ["VN"]}
    }
    job.update(overrides)
    return job


def test_write_and_read_silver_round_trip(spark, cleanup_s3a_paths):
    rows = [{"records": [_bronze_job(), _bronze_job(id="2")]}]
    df_bronze = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    df = transform_bronze_to_silver(df_bronze)

    jobs_df = process_silver(df, TEST_DATE)
    write_jobs_silver(jobs_df, TEST_DATE)
    cleanup_s3a_paths.append(SILVER_PATH)

    result = read_adzuna_silver(spark, TEST_DATE)

    assert result.count() == 2
    assert "job_id" in result.columns
    assert set(result.select("job_id").rdd.flatMap(lambda r: r).collect()) == {"1", "2"}