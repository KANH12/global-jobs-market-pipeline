import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def get_postgres_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "jobs_market_db")

    if not user or not password:
        raise ValueError("Missing POSTGRES_USER or POSTGRES_PASSWORD")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def load_jobs_summary():
    engine = get_postgres_engine()

    query = """
        SELECT *
        FROM jobs_summary
    """

    return pd.read_sql(query, engine)


def load_salary_analysis():
    engine = get_postgres_engine()

    query = """
        SELECT *
        FROM salary_analysis
    """

    return pd.read_sql(query, engine)

def load_jobs_detail():
    engine = get_postgres_engine()

    query = """
        SELECT *
        FROM jobs_detail
    """

    return pd.read_sql(query, engine)