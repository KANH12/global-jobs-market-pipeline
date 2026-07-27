import os
import sys

import pytest

from core.spark_session import create_spark_session

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

TEST_DATE = "9999/01/01"

@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session(enable_s3a=True)
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def _delete_s3a_path(spark, path: str):
    """Xóa file/folder trên MinIO qua Hadoop FileSystem API - không cần boto3."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(hadoop_conf)
    if fs.exists(jvm_path):
        fs.delete(jvm_path, True)


@pytest.fixture
def cleanup_s3a_paths(spark):
    """
    Đăng ký các S3A path cần xóa sau khi test xong:

        def test_x(spark, cleanup_s3a_paths):
            path = "s3a://data-lake/bronze/adzuna/9999/01/01"
            ...write data...
            cleanup_s3a_paths.append(path)
    """
    paths_to_clean = []
    yield paths_to_clean
    for path in paths_to_clean:
        _delete_s3a_path(spark, path)


@pytest.fixture
def cleanup_postgres_tables():
    """
    Đăng ký các table Postgres cần DROP sau khi test xong:

        def test_y(cleanup_postgres_tables):
            cleanup_postgres_tables.append("test_jobs_summary")
            ...write_to_postgres(df, "test_jobs_summary")...
    """
    import psycopg2

    from storage.postgres_writer import load_config

    tables_to_drop = []
    yield tables_to_drop

    if not tables_to_drop:
        return

    db = load_config()["postgres"]
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["database"],
        user=db["user"],
        password=db["password"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        for table in tables_to_drop:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.close()