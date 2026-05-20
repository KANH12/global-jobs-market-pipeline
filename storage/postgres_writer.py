import yaml
import os
from dotenv import load_dotenv
from core.logger import get_job_logger
from pyspark.sql import DataFrame
from pathlib import Path

logger = get_job_logger(
    job_name="adzuna_jobs_postgres_writer",
    component="database"
)

load_dotenv()

# =========================
# Load config
# =========================
def load_config(path=None):
    if path is None:
        BASE_DIR = Path(__file__).resolve().parents[1]
        path = BASE_DIR / "config" / "postgres.yaml"

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    config["postgres"]["user"] = os.getenv("POSTGRES_USER")
    config["postgres"]["password"] = os.getenv("POSTGRES_PASSWORD")
    #config["postgres"]["jdbc_url"] = os.getenv("URL_POSTGRES")

    return config
# =========================
# Write to PostgreSQL
# =========================
def write_to_postgres(
    df: DataFrame,
    table_name: str,
    mode: str = "append"
):
    config = load_config()
    db = config["postgres"]

    # build JDBC URL
    jdbc_url = f"jdbc:postgresql://{db['host']}:{db['port']}/{db['database']}"

    logger.info(f"[START] Writing to PostgreSQL table: {table_name}")

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", db["user"])
        .option("password", db["password"])
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )

    logger.info("[SUCCESS] Write to PostgreSQL completed")