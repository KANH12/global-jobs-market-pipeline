import glob
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

def create_spark_session(config_path=None, enable_s3a=True):
    BASE_DIR = Path(__file__).resolve().parents[1]

    if config_path is None:
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
    )

    if enable_s3a:
        jar_dir = BASE_DIR / "jars"
        jar_files = ",".join(glob.glob(str(jar_dir / "*.jar")))

        builder = (
            builder
            .config("spark.jars", jar_files)
            .config("spark.hadoop.fs.s3a.endpoint", cfg["s3a"]["endpoint"])
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.path.style.access", cfg["s3a"]["path_style_access"])
            .config("spark.hadoop.fs.s3a.impl", cfg["s3a"]["impl"])
        )

    return builder.getOrCreate()