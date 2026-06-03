# Global Jobs Market Data Pipeline

An end-to-end data engineering project that collects job market data from the Adzuna API, processes it through a Bronze/Silver/Gold data lake architecture, loads serving-ready datasets into PostgreSQL, orchestrates the workflow with Apache Airflow, and visualizes insights through a Streamlit dashboard.

---

## Overview

This project simulates a modern batch data pipeline for job market analytics and job-level exploration.

The pipeline extracts job postings from the Adzuna API, stores raw data in MinIO, transforms data using Apache Spark, loads curated datasets into PostgreSQL, and serves an interactive Streamlit dashboard.

---

## Architecture

![Pipeline Architecture](docs/diagram_pic.png)

```text
Adzuna API
   ↓
Python Ingestion
   ↓
MinIO Data Lake
   ├── Bronze: Raw JSON
   ├── Silver: Cleaned job-level data
   └── Gold: Serving-ready datasets
   ↓
PostgreSQL
   ├── jobs_detail
   ├── jobs_summary
   └── salary_analysis
   ↓
Streamlit Dashboard

Airflow orchestrates:
ingestion → processing → database loading
```

---

## Tech Stack

| Layer            | Tools                  |
| ---------------- | ---------------------- |
| Data Source      | Adzuna Jobs API        |
| Ingestion        | Python, Requests       |
| Processing       | Apache Spark, PySpark  |
| Data Lake        | MinIO, S3A             |
| Storage Format   | JSON, Parquet          |
| Serving Database | PostgreSQL             |
| Orchestration    | Apache Airflow         |
| Dashboard        | Streamlit, Plotly      |
| Infrastructure   | Docker, Docker Compose |

---

## Key Features

* API-based batch data ingestion
* Bronze/Silver/Gold Medallion Architecture
* Spark-based data transformation
* Data quality checks for Bronze and Silver layers
* MinIO object storage with S3A integration
* PostgreSQL serving layer
* Airflow orchestration
* Streamlit dashboard with analytics and job exploration
* Fully containerized local data platform

---

## Data Pipeline

### Bronze Layer

Stores raw API responses in JSON format.

```text
s3a://data-lake/bronze/adzuna/YYYY/MM/DD/
```

### Silver Layer

Stores cleaned and normalized job-level data in Parquet format.

```text
s3a://data-lake/silver/adzuna/jobs/dt=YYYY/MM/DD/
```

### Gold Layer

Stores serving-ready datasets for analytics and dashboard usage.

| Dataset           | Purpose                               |
| ----------------- | ------------------------------------- |
| `jobs_detail`     | Job-level data for Job Explorer       |
| `jobs_summary`    | Aggregated job statistics by category |
| `salary_analysis` | Salary statistics by contract type    |

```text
s3a://data-lake/gold/adzuna/jobs_detail/dt=YYYY/MM/DD/
s3a://data-lake/gold/adzuna/jobs_summary/dt=YYYY/MM/DD/
s3a://data-lake/gold/adzuna/salary_analysis/dt=YYYY/MM/DD/
```

---

## Dashboard

![Streamlit Dashboard](docs/streamlit_dashboard.png)

The Streamlit dashboard includes:

* Market Overview
* Job Explorer
* Salary Analysis
* Data Explorer
* Search and filters
* Job cards and data tables
* CSV download

---

## Project Structure

```text
global-jobs-market-pipeline/
├── airflow/
├── app/
├── config/
├── core/
├── docker/
├── ingestion/
├── processing/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── quality/
├── storage/
├── requirements/
├── docs/
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Environment Variables

Create a `.env` file in the project root.

```env
# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Adzuna API
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_app_key

# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=jobs_market_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

Do not commit the real `.env` file.

---

## How to Run

Build and start all services:

```bash
docker compose up -d --build
```

Expected services:

```text
minio
spark
postgres
airflow-webserver
airflow-scheduler
streamlit
```

Service URLs:

| Service       | URL                   |
| ------------- | --------------------- |
| Airflow       | http://localhost:8080 |
| Streamlit     | http://localhost:8501 |
| MinIO Console | http://localhost:9001 |
| PostgreSQL    | http://localhost:5432 |

It only runs when Docker is enabled!
---

## Airflow Setup

Initialize Airflow metadata database:

```bash
docker exec -it airflow-webserver airflow db migrate
```

Create an admin user:

```bash
docker exec -it airflow-webserver airflow users create --username admin --password admin --firstname Khang --lastname Le --role Admin --email admin@example.com
```

Trigger the DAG manually:

```bash
docker exec -it airflow-webserver airflow dags test adzuna_jobs_market_pipeline 2026-06-02
```

---

## Future Improvements

* Crawl Vietnamese IT job websites
* Extract technical skills from job descriptions
* Add historical trend analysis
* Add Discord or email notifications
* Deploy to cloud or VPS
* Add dashboard authentication
* Improve incremental loading strategy
* Add database indexes for faster job search

---

## Author

**Khang Le**
Data Engineering Portfolio Project
