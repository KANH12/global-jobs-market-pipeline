import pytest

from processing.bronze.read_bronze import read_bronze

TEST_DATE = "9999/01/01"


@pytest.mark.parametrize("source", ["adzuna", "jooble"])
def test_read_bronze_returns_seeded_data(spark, cleanup_s3a_paths, source):
    path = f"s3a://data-lake/bronze/{source}/{TEST_DATE}"

    data = [{
        "records": [{"id": "1", "title": "Data Engineer"}],
        "batch_id": "test-batch-1",
        "source": source,
        "ingested_at": "2026-01-01T00:00:00"
    }]

    spark.createDataFrame(data).coalesce(1).write.mode("overwrite").json(path)
    cleanup_s3a_paths.append(path)

    df = read_bronze(spark, source, TEST_DATE)

    assert df.count() == 1
    assert "records" in df.columns
    assert df.collect()[0]["batch_id"] == "test-batch-1"


@pytest.mark.parametrize("source", ["adzuna", "jooble"])
def test_read_bronze_multiple_batches_combined(spark, cleanup_s3a_paths, source):
    path = f"s3a://data-lake/bronze/{source}/{TEST_DATE}"

    data1 = [{"records": [{"id": "1", "title": "A"}], "batch_id": "b1",
              "source": source, "ingested_at": "2026-01-01T00:00:00"}]
    data2 = [{"records": [{"id": "2", "title": "B"}], "batch_id": "b2",
              "source": source, "ingested_at": "2026-01-01T01:00:00"}]

    spark.createDataFrame(data1).coalesce(1).write.mode("overwrite").json(path)
    spark.createDataFrame(data2).coalesce(1).write.mode("append").json(path)
    cleanup_s3a_paths.append(path)

    df = read_bronze(spark, source, TEST_DATE)

    assert df.count() == 2
    batch_ids = {row["batch_id"] for row in df.collect()}
    assert batch_ids == {"b1", "b2"}