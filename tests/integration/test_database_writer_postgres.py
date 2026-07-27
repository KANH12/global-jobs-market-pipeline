from pyspark.sql.types import LongType, StringType, StructField, StructType

from storage.postgres_writer import load_config, write_to_postgres

TEST_TABLE = "test_jobs_summary"

SCHEMA = StructType([
    StructField("category_label", StringType(), True),
    StructField("total_jobs", LongType(), True),
])


def test_write_to_postgres_and_read_back(spark, cleanup_postgres_tables):
    data = [
        {"category_label": "IT", "total_jobs": 10},
        {"category_label": "Sales", "total_jobs": 5},
    ]
    df = spark.createDataFrame(data, schema=SCHEMA)

    write_to_postgres(df, TEST_TABLE, mode="overwrite")
    cleanup_postgres_tables.append(TEST_TABLE)

    db = load_config()["postgres"]
    jdbc_url = f"jdbc:postgresql://{db['host']}:{db['port']}/{db['database']}"

    result = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", TEST_TABLE)
        .option("user", db["user"])
        .option("password", db["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    assert result.count() == 2
    rows = {row["category_label"]: row["total_jobs"] for row in result.collect()}
    assert rows["IT"] == 10
    assert rows["Sales"] == 5