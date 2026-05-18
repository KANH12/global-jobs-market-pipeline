from pathlib import Path
from pyspark.sql import SparkSession
import yaml
import os
from dotenv import load_dotenv

load_dotenv()

def create_spark_session(config_path=None):
    if config_path is None:
        BASE_DIR = Path(__file__).resolve().parents[1]
        config_path = BASE_DIR / "config" / "spark.yaml"

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    builder = (
        SparkSession.builder
        .appName(cfg["app"]["name"])
        .config("spark.executor.memory", cfg["resources"]["executor"]["memory"])
        .config("spark.executor.cores", cfg["resources"]["executor"]["cores"])
        .config("spark.driver.memory", cfg["resources"]["driver"]["memory"])
        .config("spark.sql.shuffle.partitions", cfg["tuning"]["shuffle_partitions"])
        .config("spark.default.parallelism", cfg["tuning"]["parallelism"])

        #S3A config
        .config("spark.hadoop.fs.s3a.endpoint", cfg["s3a"]["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", cfg["s3a"]["path_style_access"])
        .config("spark.hadoop.fs.s3a.impl", cfg["s3a"]["impl"])
    )

    return builder.getOrCreate()
