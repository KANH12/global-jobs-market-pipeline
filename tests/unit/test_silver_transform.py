from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing.silver.build_silver import (
    clean_invalid_ids,
    deduplicate_jobs,
    normalize_salary,
    process_silver,
    standardize_contract_fields,
    transform_bronze_to_silver,
)

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

JOB_LEVEL_SCHEMA = JOB_SCHEMA


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


def _make_bronze_df(spark, records_list):
    """records_list: list các list job (mỗi phần tử là 1 dòng bronze, chứa list job bên trong)"""
    rows = [{"records": records} for records in records_list]
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


def _make_job_level_df(spark, jobs):
    return spark.createDataFrame(jobs, schema=JOB_LEVEL_SCHEMA)


# =========================
# transform_bronze_to_silver
# =========================
def test_transform_bronze_to_silver_explodes_records(spark):
    df_bronze = _make_bronze_df(spark, [[_bronze_job(), _bronze_job(id="2")]])

    df = transform_bronze_to_silver(df_bronze)

    assert df.count() == 2
    assert "id" in df.columns


def test_transform_bronze_to_silver_skips_null_records(spark):
    df_bronze = _make_bronze_df(spark, [[_bronze_job()], None])

    df = transform_bronze_to_silver(df_bronze)

    assert df.count() == 1


# =========================
# clean_invalid_ids
# =========================
def test_clean_invalid_ids_removes_null_id(spark):
    df = _make_job_level_df(spark, [_bronze_job(id="1"), _bronze_job(id=None)])

    result = clean_invalid_ids(df)

    assert result.count() == 1
    assert result.collect()[0]["id"] == "1"


# =========================
# deduplicate_jobs
# =========================
def test_deduplicate_jobs_removes_duplicate_id(spark):
    df = _make_job_level_df(
        spark, [_bronze_job(id="1"), _bronze_job(id="1"), _bronze_job(id="2")]
    )

    result = deduplicate_jobs(df)

    assert result.count() == 2


# =========================
# standardize_contract_fields
# =========================
def test_standardize_contract_fields_uppercases_and_trims(spark):
    df = _make_job_level_df(
        spark, [_bronze_job(contract_time=" full_time ", contract_type="permanent")]
    )

    result = standardize_contract_fields(df)
    row = result.collect()[0]

    assert row["contract_time"] == "FULL_TIME"
    assert row["contract_type"] == "PERMANENT"


def test_standardize_contract_fields_defaults_null_to_unknown(spark):
    df = _make_job_level_df(
        spark, [_bronze_job(contract_time=None, contract_type=None)]
    )

    result = standardize_contract_fields(df)
    row = result.collect()[0]

    assert row["contract_time"] == "UNKNOWN"
    assert row["contract_type"] == "UNKNOWN"


# =========================
# normalize_salary
# =========================
def test_normalize_salary_scales_values_under_1000(spark):
    df = _make_job_level_df(spark, [_bronze_job(salary_min=30, salary_max=50)])

    result = normalize_salary(df)
    row = result.collect()[0]

    assert row["salary_min"] == 30000
    assert row["salary_max"] == 50000


def test_normalize_salary_keeps_values_over_1000_unchanged(spark):
    df = _make_job_level_df(spark, [_bronze_job(salary_min=30000, salary_max=50000)])

    result = normalize_salary(df)
    row = result.collect()[0]

    assert row["salary_min"] == 30000
    assert row["salary_max"] == 50000


# =========================
# process_silver
# =========================
def test_process_silver_produces_expected_columns(spark):
    df_bronze = _make_bronze_df(spark, [[_bronze_job()]])
    df = transform_bronze_to_silver(df_bronze)

    result = process_silver(df, "2026/05/20")
    row = result.collect()[0]

    assert row["job_id"] == "1"
    assert row["category_label"] == "IT"
    assert row["company_name"] == "ABC"
    assert row["location_name"] == "HCM"
    assert row["ingestion_date"] == "2026/05/20"
    assert row["salary_is_predicted"] is False  # "0" -> False


def test_process_silver_removes_duplicates_end_to_end(spark):
    df_bronze = _make_bronze_df(
        spark, [[_bronze_job(id="1"), _bronze_job(id="1"), _bronze_job(id="2")]]
    )
    df = transform_bronze_to_silver(df_bronze)

    result = process_silver(df, "2026/05/20")

    assert result.count() == 2