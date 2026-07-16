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

![Pipeline Architecture](docs/diagram_pic.png)


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
  Tracks pipeline execution and debugging  

- **Data quality validation with severity levels**  
  Warning vs fail-fast logic  

- **Partitioned data storage**  
  Optimized for scalability and query performance  

- **Containerized environment**  
  Fully reproducible using Docker

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

---

## Key Features

- End-to-end batch data pipeline  
- Medallion Architecture (Bronze / Silver / Gold)  
- PySpark-based data transformation  
- Data quality validation at multiple layers  
- Structured logging for monitoring  
- Airflow-based orchestration  
- Partitioned data storage (by date)  
- PostgreSQL serving layer  
- Interactive Streamlit dashboard  
- Fully containerized system

---
## Data Flow Summary

Adzuna API
-> Bronze (Raw JSON)
-> Silver (Cleaned & validated data)
-> Gold (Aggregated datasets)
-> PostgreSQL
-> Dashboard

---

## Data Quality

### 🔹 Bronze Layer (Strict Validation)

- Schema validation  
- Required fields check  
- Empty data prevention  

👉 **Fail-fast mechanism applied**

---

### 🔹 Silver Layer (Monitoring & Validation)

- Duplicate detection (`job_id`)  
- Null checks on critical fields  
- Salary validation (`salary_min ≤ salary_max`)  
- Domain validation  

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

## Project Structure

```text
global-jobs-market-pipeline/
├── airflow/                  # Workflow orchestration
├── app/                      # Streamlit dashboard
├── config/                   # YAML configurations
├── core/                     # Shared utilities (logger, config loader)
├── docker/                   # Container setup
├── ingestion/                # API data ingestion
├── processing/               # PySpark transformations
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── quality/                  # Data validation logic
├── storage/                  # Database interaction
├── requirements/             # Service-based dependencies (Airflow, Spark, Streamlit)
├── docs/                     # Diagrams, images
├── scripts/                  # Pipeline runners / manual jobs
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

---
## Author

**Khang Le**
Data Engineering Portfolio Project
