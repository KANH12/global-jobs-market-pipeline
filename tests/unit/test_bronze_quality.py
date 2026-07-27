import logging

import pytest
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from quality.bronze_quality import (
    check_record_count,
    check_records_structure,
    check_required_columns,
    run_bronze_quality_checks,
)

test_logger = logging.getLogger("test-bronze-quality")

JOB_ITEM_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
])

BRONZE_SCHEMA = StructType([
    StructField("records", ArrayType(JOB_ITEM_SCHEMA), True),
    StructField("batch_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


def _valid_bronze_record(**overrides):
    record = {
        "records": [{"id": "1", "title": "Data Engineer"}],
        "batch_id": "batch-1",
        "source": "adzuna",
        "ingested_at": "2026-05-20T00:00:00"
    }
    record.update(overrides)
    return record


def _make_df(spark, rows, schema=BRONZE_SCHEMA):
    return spark.createDataFrame(rows, schema=schema)


# =========================
# check_record_count
# =========================
def test_check_record_count_passes_when_not_empty(spark):
    df = _make_df(spark, [_valid_bronze_record()])
    check_record_count(df, test_logger)


def test_check_record_count_raises_when_empty(spark):
    df = _make_df(spark, [_valid_bronze_record()]).filter("1 = 0")

    with pytest.raises(Exception, match="Bronze data ingestion failed"):
        check_record_count(df, test_logger)


# =========================
# check_required_columns
# =========================
def test_check_required_columns_passes_when_all_present(spark):
    df = _make_df(spark, [_valid_bronze_record()])
    check_required_columns(df, test_logger)


def test_check_required_columns_raises_when_missing(spark):
    partial_schema = StructType([
        StructField("records", ArrayType(JOB_ITEM_SCHEMA), True),
        StructField("source", StringType(), True),
    ])
    data = [{"records": [{"id": "1", "title": "x"}], "source": "adzuna"}]
    df = _make_df(spark, data, schema=partial_schema)

    with pytest.raises(Exception, match="Bronze schema validation failed"):
        check_required_columns(df, test_logger)


# =========================
# check_records_structure
# =========================
def test_check_records_structure_passes_when_no_null(spark):
    df = _make_df(spark, [_valid_bronze_record()])
    check_records_structure(df, test_logger)


def test_check_records_structure_raises_when_null_records(spark):
    data = [
        _valid_bronze_record(),
        _valid_bronze_record(records=None),
    ]
    df = _make_df(spark, data)

    with pytest.raises(Exception, match="Bronze records corrupted"):
        check_records_structure(df, test_logger)


# =========================
# run_bronze_quality_checks (full pipeline)
# =========================
def test_run_bronze_quality_checks_passes_for_valid_data(spark):
    df = _make_df(spark, [_valid_bronze_record(), _valid_bronze_record()])
    run_bronze_quality_checks(df, test_logger)


def test_run_bronze_quality_checks_raises_on_first_failing_check(spark):
    df = _make_df(spark, [_valid_bronze_record()]).filter("1 = 0")

    with pytest.raises(Exception, match="Bronze data ingestion failed"):
        run_bronze_quality_checks(df, test_logger)