import logging
import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# =========================
# Spark session 
# =========================
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest-unit")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()

@pytest.fixture(autouse=True)
def enable_log_propagation():
    loggers_to_fix = [
        logging.getLogger("bronze.adzuna_bronze_quality"),
        logging.getLogger("silver.adzuna_silver_quality"),
        logging.getLogger("bronze.adzuna_bronze_reader"),
        logging.getLogger("silver.adzuna_jobs_silver_writer"),
        logging.getLogger("gold.adzuna_jobs_gold_summary"),
        logging.getLogger("gold.adzuna_gold_salary_analysis"),
    ]

    original_states = {lg: lg.propagate for lg in loggers_to_fix}

    for lg in loggers_to_fix:
        lg.propagate = True

    yield

    for lg, original in original_states.items():
        lg.propagate = original