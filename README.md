# 🌍 Global Jobs Market Data Platform

An end-to-end **modular data pipeline system** designed to collect, process, validate, and serve global job market data.

This project simulates a **real-world data engineering workflow**, combining batch processing, data quality validation, orchestration, and analytics delivery.

---

## Overview

This project builds a complete data platform to analyze global job market trends, including:

- Job demand distribution
- Salary insights
- Hiring patterns

The system follows a **Medallion Architecture (Bronze → Silver → Gold)** and integrates multiple components:

- API ingestion
- Distributed data processing (PySpark)
- Data quality validation
- Data warehouse serving layer
- Dashboard visualization

---

## Why This Project

In real-world data systems, raw data is often inconsistent and unreliable.
This project focuses on building a robust pipeline that ensures data quality, scalability, and maintainability.

It demonstrates how to design a production-like data platform that can be extended to real-time processing and cloud environments.

---

## Architecture

[![Pipeline Architecture](docs/diagram.png)](docs/diagram.png)

*Click the diagram to view full resolution.*

```text
Adzuna API
    ↓
Ingestion Layer (Python)
    ↓
Data Lake (MinIO - S3)
├── Bronze: Raw JSON
├── Silver: Cleaned & validated data
└── Gold: Business-ready datasets
    ↓
Serving Layer (PostgreSQL)
    ↓
Streamlit Dashboard

Orchestration: Apache Airflow
```

---

## System Design Highlights

- **Modular pipeline architecture**
  Separate ingestion, processing, and quality layers

- **Configuration-driven system**
  YAML-based configuration for flexible pipeline management

- **Structured logging system**
  Per-module logs (API, Bronze, Silver, Gold, Database, Pipeline) tracking both execution and data quality events

- **Data quality validation with severity levels**
  Fail-fast (Bronze) vs monitoring-based (Silver) logic

- **Partitioned data storage**
  Optimized for scalability and query performance

- **Containerized environment**
  Fully reproducible using Docker

- **Automated test suite**
  Unit tests for transform/quality logic, integration tests for real MinIO/Postgres I/O — see [Testing](#testing)

---

## Tech Stack

| Layer            | Tools                  |
|------------------|------------------------|
| Data Source      | Adzuna API            |
| Ingestion        | Python, Requests      |
| Processing       | PySpark               |
| Data Lake        | MinIO (S3A)           |
| Storage Format   | JSON, Parquet         |
| Serving DB       | PostgreSQL            |
| Orchestration    | Airflow               |
| Visualization    | Streamlit, Plotly     |
| Infrastructure   | Docker                |
| Testing          | Pytest (unit + integration) |

---

## Key Features

- End-to-end batch data pipeline
- Medallion Architecture (Bronze / Silver / Gold)
- PySpark-based data transformation
- Data quality validation at multiple layers
- Structured, per-module logging system
- Airflow-based orchestration
- Partitioned data storage (by date)
- PostgreSQL serving layer
- Interactive Streamlit dashboard
- Fully containerized system
- Unit + integration test suite (Pytest)

---

## Data Flow Summary

```text
Adzuna API
-> Bronze (Raw JSON)
-> Silver (Cleaned & validated data)
-> Gold (Aggregated datasets)
-> PostgreSQL
-> Dashboard
```

---

## Data Quality

### 🔹 Bronze Layer (Strict Validation)

- Schema validation
- Required fields check
- Empty data prevention

👉 **Fail-fast mechanism applied** — any failed check raises an exception and stops the pipeline immediately.

---

### 🔹 Silver Layer (Monitoring & Validation)

- Duplicate detection (`job_id`)
- Null checks on critical fields
- Salary validation (`salary_min ≤ salary_max`)
- Domain validation (`contract_time`, `contract_type`)

👉 **Severity-based logic:**

- Warning → log and continue
- Critical → stop pipeline

---

### Example Execution Log (Silver Layer Validation)

Here is a snippet from the Spark executor log demonstrating how the pipeline successfully catches data anomalies (such as an invalid salary range) and logs a warning without crashing the entire batch:

<details>
<summary><b>Click to expand Spark Silver Pipeline Log</b></summary>

```text
2026-06-03 08:33:51,480 | INFO | silver.adzuna_silver_quality | [START] START Silver quality checks
2026-06-03 08:33:55,411 | INFO | silver.adzuna_silver_quality | [CHECKED] No duplicates found
2026-06-03 08:33:55,937 | WARNING | silver.adzuna_silver_quality | [WARNING] Found 1 invalid salary ranges
2026-06-03 08:33:56,459 | INFO | silver.adzuna_silver_quality | [CHECKED] No invalid contract_type found
2026-06-03 08:33:56,801 | INFO | silver.adzuna_silver_quality | [SUCCESS] Silver quality checks done | rows=246 | cols=18
```
</details>

---

## Data Storage Design

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

| Dataset           | Description |
|------------------|-------------|
| jobs_detail      | Job-level dataset |
| jobs_summary     | Aggregated statistics |
| salary_analysis  | Salary insights |

```text
s3a://data-lake/gold/adzuna/jobs_detail/dt=YYYY/MM/DD/
s3a://data-lake/gold/adzuna/jobs_summary/dt=YYYY/MM/DD/
s3a://data-lake/gold/adzuna/salary_analysis/dt=YYYY/MM/DD/
```

---

## Dashboard

![Streamlit Dashboard](docs/streamlit_dashboard.png)

The dashboard enables users to explore job market trends, analyze salary distribution, and search job listings interactively.

The Streamlit dashboard includes:

* Market Overview
* Job Explorer
* Salary Analysis
* Data Explorer
* Search and filters
* Job cards and data tables
* CSV download

---

## Testing

The pipeline is covered by an automated Pytest suite, split into two layers:

| Layer | What it checks | Dependencies | Docs |
|---|---|---|---|
| **Unit** | Transform + quality check logic (bronze/silver/gold), fully in-memory | None (local Spark session only) | [docs/unit-tests.md](docs/unit-tests.md) |
| **Integration** | Real read/write against MinIO (S3A) and PostgreSQL (JDBC) | Docker network (MinIO, Postgres containers running) | [docs/integration-tests.md](docs/integration-tests.md) |

```bash
# Unit tests (run on host or in container, no infra needed)
pytest tests/unit -v

# Integration tests (must run inside the spark container - needs Docker network)
docker compose exec spark pytest tests/integration -v
```

Integration test data is seeded and cleaned up automatically under a fixed dummy date (`9999/01/01`), so the suite is reproducible on a fresh clone without depending on any pre-existing pipeline run.

The docs above also include the real environment debugging journey (Windows PySpark setup, JAR version conflicts, PySpark/Spark runtime mismatch) and a data-quality bug found and fixed via unit testing.

---

## Project Structure

```text
global-jobs-market-pipeline/
├── airflow/                  # Workflow orchestration
├── app/                      # Streamlit dashboard
├── config/                   # YAML configurations
├── core/                     # Shared utilities (logger, Spark session)
├── docker/                   # Container setup
├── ingestion/                # API data ingestion
├── jars/                     # Extra JDBC/Hadoop-AWS jars (S3A, Postgres)
├── processing/               # PySpark transformations
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── quality/                  # Data validation logic
├── storage/                  # Data lake and database interaction (MinIO, PostgreSQL)
├── requirements/             # Service-based dependencies (Airflow, Spark, Streamlit, Tests)
├── docs/                     # Diagrams, images, test documentation
├── scripts/                  # Pipeline runners / manual jobs
├── tests/
│   ├── unit/                 # Logic-only tests, in-memory Spark
│   └── integration/          # Real MinIO/Postgres I/O tests
├── docker-compose.yml        # Multi-service container orchestration
├── .env.example              # Environment variables template
├── .gitignore
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

Ensure Docker is installed and running before starting the services.

---

## Airflow DAG

```bash
docker exec -it airflow-webserver airflow dags test adzuna_jobs_market_pipeline 2026-06-02
```

---

## Future Improvements

- Near real-time pipeline (Kafka)
- Incremental data processing
- Data observability metrics
- Alerting system (Slack/Discord)
- Cloud deployment (AWS/GCP)

---

## Key Learnings

- Designed and implemented a Medallion Architecture (Bronze/Silver/Gold)
- Built scalable batch data pipelines using Apache Spark
- Managed object storage with MinIO and S3-compatible APIs
- Orchestrated workflows using Apache Airflow
- Ensured data quality across multiple pipeline stages
- Delivered end-to-end data systems from ingestion to visualization
- Built and debugged a full Pytest suite (unit + integration), including a real data-quality bug found via testing and a full Docker/JVM/PySpark environment debugging journey

---
## Author

**Khang Le**
Data Engineering Portfolio Project