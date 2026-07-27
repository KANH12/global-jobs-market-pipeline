from pyspark.sql.types import LongType, StringType, StructField, StructType

from quality.silver_quality import (
    check_contract_fields,
    check_duplicates,
    check_required_fields,
    check_salary,
    run_silver_quality_checks,
)

SILVER_SCHEMA = StructType([
    StructField("job_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("salary_min", LongType(), True),
    StructField("salary_max", LongType(), True),
    StructField("contract_time", StringType(), True),
    StructField("contract_type", StringType(), True),
])


def _silver_job(**overrides):
    job = {
        "job_id": "1",
        "title": "Data Engineer",
        "company_name": "ABC",
        "salary_min": 1000,
        "salary_max": 2000,
        "contract_time": "FULL_TIME",
        "contract_type": "PERMANENT",
    }
    job.update(overrides)
    return job


def _make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


def test_check_required_fields_does_not_crash_with_null(spark):
    df = _make_df(spark, [_silver_job(company_name=None)])
    check_required_fields(df)  # không raise là pass


def test_check_required_fields_warns_on_null(spark, caplog):
    df = _make_df(spark, [_silver_job(company_name=None)])

    with caplog.at_level("WARNING"):
        check_required_fields(df)

    assert any("company_name" in record.message for record in caplog.records)


# =========================
# check_duplicates
# =========================
# def test_check_duplicates_no_warning_when_unique(spark, caplog):
#     df = _make_df(spark, [_silver_job(job_id="1"), _silver_job(job_id="2")])

#     with caplog.at_level("INFO"):
#         check_duplicates(df)

#     assert not any("duplicate" in record.message.lower() for record in caplog.records)
def test_check_duplicates_no_warning_when_unique(spark, caplog):
    df = _make_df(spark, [_silver_job(job_id="1"), _silver_job(job_id="2")])

    with caplog.at_level("INFO"):
        check_duplicates(df)

    assert not any(r.levelname == "WARNING" for r in caplog.records)


def test_check_duplicates_warns_when_duplicate_found(spark, caplog):
    df = _make_df(spark, [_silver_job(job_id="1"), _silver_job(job_id="1")])

    with caplog.at_level("WARNING"):
        check_duplicates(df)

    assert any("duplicate" in record.message.lower() for record in caplog.records)


# =========================
# check_salary
# =========================
def test_check_salary_no_warning_when_valid(spark, caplog):
    df = _make_df(spark, [_silver_job(salary_min=1000, salary_max=2000)])

    with caplog.at_level("WARNING"):
        check_salary(df)

    assert len(caplog.records) == 0


def test_check_salary_warns_when_min_greater_than_max(spark, caplog):
    df = _make_df(spark, [_silver_job(salary_min=3000, salary_max=1000)])

    with caplog.at_level("WARNING"):
        check_salary(df)

    assert any("invalid salary" in record.message.lower() for record in caplog.records)


# =========================
# check_contract_fields
# =========================
def test_check_contract_fields_no_warning_when_valid(spark, caplog):
    df = _make_df(spark, [_silver_job(contract_time="FULL_TIME", contract_type="PERMANENT")])

    with caplog.at_level("WARNING"):
        check_contract_fields(df)

    assert len(caplog.records) == 0


def test_check_contract_fields_warns_on_invalid_contract_type(spark, caplog):
    df = _make_df(spark, [_silver_job(contract_type="RANDOM", contract_time="FULL_TIME")])

    with caplog.at_level("WARNING"):
        check_contract_fields(df)

    assert any("contract_type" in record.message for record in caplog.records)


def test_check_contract_fields_warns_on_invalid_contract_time(spark, caplog):
    df = _make_df(spark, [_silver_job(contract_time="INVALID_TYPE", contract_type="PERMANENT")])

    with caplog.at_level("WARNING"):
        check_contract_fields(df)

    assert any("contract_time" in record.message for record in caplog.records)


# =========================
# run_silver_quality_checks 
# =========================
def test_run_silver_quality_checks_does_not_crash_on_bad_data(spark):
    df = _make_df(spark, [_silver_job(
        job_id="1",
        company_name=None,
        salary_min=3000,
        salary_max=1000,
        contract_type="RANDOM"
    )])
    run_silver_quality_checks(df)