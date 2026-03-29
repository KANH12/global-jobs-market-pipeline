# Global Jobs Market Data Pipeline

## Project Status

Current progress:

- ✅ API Ingestion  
- ✅ Bronze Layer (raw data ingestion)  
- ✅ Bronze Data Quality Checks  
- ✅ Silver Layer transformation  
- ✅ Silver Data Quality Checks  
- ✅ Gold Layer (jobs_summary, salary_analysis)  

- 🚧 Orchestration with Airflow  
- 🚧 Data visualization  

---

## Architecture

![Pipeline Architecture](docs/diagram_pic.png)

---

## Tech Stack

- Python  
- Apache Spark  
- Docker  
- MinIO (S3-compatible object storage)  
- Structured Logging  
- Data Quality Validation  

---

## Project Structure

```
global-jobs-market-pipeline/

core/
spark_session.py
logger.py

ingestion/
fetcher.py
batch_builder.py
writer.py
run_ingestion.py

processing/
bronze/
read_bronze_data.py

silver/
write_jobs_silver_parquet.py

gold/
jobs_summary.py
salary_analysis.py

quality/
bronze_quality.py
silver_quality.py

logs/
api/
bronze/
silver/
gold/

docs/
architecture diagrams

docker/
container setup
```

---

## Data Architecture

The project uses a **Medallion Architecture** to organize the data pipeline.

### Bronze Layer

The Bronze layer stores **raw ingested data** with minimal processing.

**Characteristics:**

- Raw API responses  
- Stored as JSON  
- Partitioned by ingestion date  
- Includes metadata such as batch_id and ingestion timestamp  

**Example storage path:**

```
s3a://data-lake/bronze/adzuna/YYYY/MM/DD/
```


Bronze data is not transformed but validated to ensure ingestion integrity.

---

### Bronze Data Quality Checks

Before transforming the data further, a **data quality validation step** runs on the Bronze dataset.

**Current checks include:**

- Record count validation  
- Required column validation  
- Records structure validation  

**Example validations:**

- Dataset must not be empty  
- Required fields must exist  
- Raw records must not be NULL  

These checks help detect **ingestion failures and schema issues early in the pipeline**.

---

### Silver Layer

The Silver layer contains **cleaned and structured datasets** derived from Bronze data.

**Transformations performed:**

- Flatten nested job records  
- Remove invalid records (NULL ids)  
- Deduplicate job records  
- Standardize categorical fields (e.g. contract_type)  
- Normalize job attributes  
- Extract important fields such as title, salary, company, and location  
- Store structured datasets in Parquet format  

**Example output:**

```
s3a://data-lake/silver/adzuna/jobs/dt=YYYY/MM/DD/
```


Silver data is optimized for **analytics and downstream processing**.

---

### Silver Data Quality Checks

After transformation, the Silver dataset is validated to ensure data consistency and correctness.

**Current checks include:**

- Duplicate job_id detection  
- Invalid categorical values (e.g. contract_type)  
- Record count monitoring  

These checks help identify **data issues introduced during transformation**.

---

### Gold Layer

The Gold layer contains **aggregated datasets designed for analytics and reporting**.

Unlike Silver (row-level data), Gold datasets are grouped and summarized to provide business insights.

#### jobs_summary

Provides an overview of the job market across categories.

**Aggregations include:**

- Total number of jobs per category  
- Average, minimum, and maximum salary  
- Median salary estimation  

**Example output:**

```
s3a://data-lake/gold/adzuna/jobs_summary/dt=YYYY/MM/DD/
```


#### salary_analysis

Analyzes salary distribution across job types.

**Aggregations include:**

- Job count by contract_type  
- Average salary (min and max)  
- Median salary  
- Maximum salary  

**Example output:**

```
s3a://data-lake/gold/adzuna/salary_analysis/dt=YYYY/MM/DD/
```

---

## Logging

The pipeline uses **structured logging** to track job execution.

**Logs are organized by pipeline component:**


```
logs/
  api/
  bronze/
  silver/
  gold/
```

Each pipeline run writes logs containing:

- job start and completion  
- dataset statistics  
- data quality results  
- Spark execution events  

This helps with **debugging pipeline failures and monitoring data processing**.

---

## Example Pipeline Flow

1. API ingestion collects job data  
2. Raw data stored in Bronze layer  
3. Bronze quality checks validate ingestion  
4. Spark transforms data into Silver layer  
5. Silver quality checks validate cleaned data  
6. Aggregated datasets generated in Gold layer  

---

## Future Improvements

Planned features for the pipeline:

- Workflow orchestration with Apache Airflow  
- Schema drift detection  
- Automated data quality reporting  
- Dashboard visualization for job market insights  
- Additional Gold datasets (location, company insights, trends)  

---

## Learning Goals

This project demonstrates key data engineering concepts:

- Building batch data pipelines  
- Implementing Medallion architecture  
- Using Apache Spark for data transformation  
- Designing data quality validation layers  
- Building analytical data models (Gold layer)  
- Implementing structured logging for pipelines  

---

## Author

**Khang**  
Data Engineering Portfolio Project 🚀

