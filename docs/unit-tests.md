# Unit Tests — `tests/unit/`

## Purpose

Unit tests validate **pure data processing logic** (transformations, validations, aggregations) using a local Spark session (`local[*]`) entirely in memory.

These tests do **not interact with external systems** such as MinIO, PostgreSQL, or network resources.

All input data is manually constructed using:
```python
spark.createDataFrame(data, schema=...)
```

Tests requiring real I/O are implemented separately in `tests/integration/`.

## Design Philosophy

The testing strategy follows a layered approach:

- Unit tests validate transformation logic in isolation using Spark local mode
- Integration tests validate infrastructure connectivity (MinIO, PostgreSQL)

This separation ensures:

- Fast feedback during development
- High confidence in production behavior

## Coverage Scope

Unit tests cover:

- Bronze layer — data validation (fail-fast)
- Silver layer — data transformation and normalization
- Gold layer — business-level transformations and aggregations

They explicitly exclude:

- External I/O (MinIO, PostgreSQL)
- Network calls
- File system operations

## Structure

```
tests/
├── conftest.py
└── unit/
    ├── conftest.py
    ├── test_bronze_quality.py
    ├── test_silver_transform.py
    ├── test_silver_quality.py
    ├── test_gold_summary.py
    ├── test_gold_salary_analysis.py
    └── test_gold_jobs_detail.py
```

## How to Run

```bash
python -m pytest tests/unit -v
```

Run from project root.

## Environment Setup (Windows)

Running PySpark locally requires:

1. Hadoop native binaries (`winutils.exe`, `hadoop.dll`)
2. Python binding configuration (`PYSPARK_PYTHON`)

These are configured in:

```
tests/unit/conftest.py
```

## Why Explicit Schema is Required

Spark schema inference can fail when:

- Columns contain only `None`
- Nested structures are present

Solution:

```python
spark.createDataFrame(data, schema=SCHEMA)
```

Benefits:

- Stable tests
- Deterministic behavior
- No dependency on data ordering

## Bug Discovery via Testing

### Issue: Early Return in `check_contract_fields`

Original implementation:

```python
if invalid_contract_type_count == 0:
    return
```

Impact:

- `contract_time` validation was skipped entirely

Fix:

- Removed early return
- Evaluated both validations independently

## Testing Strategy

- Bronze layer → fail-fast validation (`pytest.raises`)
- Silver layer → monitoring-based (`logger.warning`, verified via `caplog`)
- Gold layer → business correctness validation

Logging behavior is captured using `caplog`, with log propagation temporarily enabled during tests.

## Key Takeaways

- PySpark unit testing can be fully isolated using local mode
- Explicit schemas are critical for reliability
- Separation between unit and integration tests improves maintainability
- Real-world bugs can be effectively caught at the unit test level