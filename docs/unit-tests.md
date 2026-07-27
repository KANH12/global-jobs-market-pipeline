# Unit Tests — `tests/unit/`

## Purpose

Unit tests in this project cover **pure business logic** (transform, validation, aggregation) running on a local Spark session (`local[*]`), entirely in memory — **no** MinIO, Postgres, or Docker network required. Input data is created manually via `spark.createDataFrame(data, schema=...)`.

Tests that require real I/O (reading JSON from MinIO, writing Parquet to S3A, writing to Postgres) belong to `tests/integration/` (run inside Docker), not this layer.

## Structure

```
tests/
├── conftest.py                     # shared fixtures (sample data)
└── unit/
    ├── conftest.py                  # local Spark session + PYSPARK_PYTHON fix
    ├── test_bronze_quality.py       # check_record_count, check_required_columns, check_records_structure
    ├── test_silver_transform.py     # clean_invalid_ids, deduplicate_jobs, standardize_contract_fields, normalize_salary, process_silver
    ├── test_silver_quality.py       # check_required_fields, check_duplicates, check_salary, check_contract_fields
    ├── test_gold_summary.py         # build_jobs_summary (aggregation by ingestion_date + category_label)
    ├── test_gold_salary_analysis.py # build_salary_analysis (aggregation by contract_time)
    └── test_gold_jobs_detail.py     # build_jobs_detail (transform, no aggregation)
```

## How to run

```bash
python -m pytest tests/unit -v
```

Run from the project root (where `pytest.ini` lives), not from inside `tests/`.

## Environment setup (Windows)

Running PySpark locally on Windows requires 2 conditions. Missing either one crashes any real action (`count()`, `collect()`...), even though `createDataFrame` (declaration only) still works fine:

1. **Hadoop native binaries (`winutils.exe`, `hadoop.dll`)** — Spark needs these to handle temp files/shuffle even when running fully local, with nothing to do with MinIO/S3.
   - Download the version matching the Hadoop build bundled with PySpark (repo `cdarlint/winutils`).
   - Place them at `D:\Hadoop\bin\winutils.exe` + `hadoop.dll`.
   - Set the environment variable `HADOOP_HOME=D:\Hadoop`, add `%HADOOP_HOME%\bin` to `PATH`.
   - Open a brand-new terminal so the environment variables are picked up.

2. **`PYSPARK_PYTHON`** — by default Spark launches the Python worker with the `python3` command, which doesn't exist on Windows (only `python`). Set it in `tests/unit/conftest.py`:
   ```python
   os.environ["PYSPARK_PYTHON"] = sys.executable
   os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
   ```
   Using `sys.executable` automatically points to the correct `venv\Scripts\python.exe` currently running, no hardcoded path needed.

## Why explicit schema instead of letting Spark infer

Initially, tests created DataFrames with `spark.createDataFrame(data)` without a schema, causing `CANNOT_INFER_TYPE_FOR_FIELD` / `CANNOT_DETERMINE_TYPE` errors in these cases:
- A column with only `None` values across all test rows (e.g. `company_name=None` as the single row).
- Nested structs (`category`, `company`, `location` inside the bronze record).

Solution: declare an explicit `StructType` for each test dataset, passed via `createDataFrame(data, schema=SCHEMA)`. This also makes tests more stable, since they no longer depend on the order/values of the first row that Spark samples to infer types.

## Bug found via unit testing

**`check_contract_fields` (silver_quality.py) — early return hides the contract_time check**

Original code:
```python
if invalid_contract_type_count == 0:
    logger.info("[CHECKED] No invalid contract_type found")
    return   # exits early, skipping the entire contract_time check below
```

When `contract_type` is valid but `contract_time` is invalid, the function returns right after the `contract_type` check, so the `contract_time` check (below it) **never runs**. The test `test_check_contract_fields_warns_on_invalid_contract_time` (valid `contract_type` + invalid `contract_time`) caught this behavior.

**Fix:** split the two checks into independent `if/else` blocks instead of an early `return`, so both are always evaluated regardless of the other's result.

## Test design notes

- `bronze_quality.py` follows a **fail-fast** strategy: the `check_*` functions raise an `Exception` as soon as an issue is found → tests use `pytest.raises(...)`.
- `silver_quality.py` follows a **monitoring-based** strategy: the `check_*` functions only `logger.warning(...)`, never raise, never return a value → tests use `caplog` to verify the actual logged content, rather than just checking "it didn't crash".
- `caplog` requires `logger.propagate = True` to capture logs — the custom logger in `core/logger.py` sets `propagate = False` by default (to avoid duplicate logs in production), so `tests/unit/conftest.py` has an autouse fixture `enable_log_propagation` that temporarily re-enables propagation during tests, restoring the original state afterward.