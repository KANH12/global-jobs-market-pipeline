# Integration Tests — `tests/integration/`

## Purpose

Integration tests cover the **real I/O boundary** — reading/writing MinIO (S3A), writing to PostgreSQL via JDBC — unlike unit tests which only handle in-memory data. These tests verify the pipeline connects correctly to real infrastructure (MinIO, Postgres); they don't re-test transform logic (already covered by unit tests).

Must run **inside the Spark container** (not on the Windows host), since it needs the internal Docker network to resolve `minio:9000`, `postgres:5432`.

## Design Philosophy

The testing strategy follows a layered approach:

- Unit tests validate transformation logic in isolation using Spark local mode
- Integration tests validate infrastructure connectivity (MinIO, PostgreSQL) and the real read/write boundary between pipeline layers

This separation ensures:

- Fast feedback during development (unit tests)
- High confidence in production behavior (integration tests)

## Coverage Scope

Integration tests cover:

- Bronze layer — reading real JSON from MinIO, per source (`adzuna`, `jooble`)
- Silver layer — full transform + write + read round-trip, **unified across all sources**
- Gold layer — write Parquet + read back round-trip
- Postgres — write via JDBC + read back to verify

They explicitly exclude:

- Transform/business logic correctness (already covered by unit tests — see `unit-tests.md`)
- Any source that isn't wired into `NORMALIZERS` in `build_silver.py`

## Structure

```
tests/integration/
├── conftest.py                        # Spark session with S3A + MinIO/Postgres cleanup fixtures
├── test_bronze_read_write_minio.py    # read real bronze JSON from MinIO, parametrized by source
├── test_silver_read_write_minio.py    # full pipeline: seed bronze per source -> process_silver -> write -> read back unified
├── test_gold_read_write_minio.py      # write Gold Parquet + read back (round-trip)
└── test_database_writer_postgres.py   # write to Postgres via JDBC + read back to verify
```

## How to run

```bash
docker compose exec spark pytest tests/integration -v
```

Current run: **8 passed in ~354s (5m54s)**.

## Multi-source architecture — what changed when `jooble` was added

The pipeline was refactored from a single-source (`adzuna`-only) design to a multi-source one. This has a direct impact on how each layer is tested:

- **Bronze stays per-source.** Each source is still written to its own path (`s3a://data-lake/bronze/{source}/{date}/*.json`) and read via the generic `read_bronze(spark, source, date_path)`. The bronze test is simply **parametrized** over `["adzuna", "jooble"]` — the same test body covers both sources, since the read logic itself doesn't care which source it is.
- **Silver and Gold are unified, not per-source.** `process_silver(spark, sources: list, date_path)` reads bronze for every source in the list, normalizes each one through its own normalizer (`adzuna_normalizer.normalize`, `jooble_normalizer.normalize`), unions them into one common schema, then dedups/standardizes/enriches across the whole set. The output path dropped its `/adzuna/` segment entirely: `s3a://data-lake/silver/jobs/dt=...` and `s3a://data-lake/gold/jobs_summary/dt=...`. `read_silver`/gold reads no longer take a `source` argument.
- **Each normalizer has its own raw bronze schema.** Adzuna's raw job is nested (`category.label`, `company.display_name`, `location.display_name`) while Jooble's is flat (`company`, `location` as plain strings) and needs extra parsing (`salary` as a free-text string parsed into `salary_min`/`salary_max`, `type` mapped into `contract_time`/`contract_type`). The silver integration test seeds both shapes separately before unioning.
- **Cross-source dedup relies on the `{source}_{id}` prefix.** Since `job_id` is built as `f"{source}_{raw_id}"` in each normalizer, two different sources can never collide on `job_id` even if their raw ids overlap. A dedicated test still checks that duplicate `job_id`s *within* the same source (e.g. the same job re-ingested in two batches) collapse to one row.

**Takeaway for the next new source:** adding a source means adding a parametrize case to the bronze test, a new raw-schema + seed helper to the silver test, and one more entry in `NORMALIZERS` — the unified silver/gold path and test structure don't need to change at all.

## Test data strategy: automatic seed + cleanup

Each test seeds its own fake data into MinIO/Postgres **before** it runs, and **cleans it up** afterward — it doesn't depend on real data already present from a previous pipeline run.

Why this approach over relying on existing real data:
- The suite is reproducible on another machine (fresh clone, `docker compose up` from scratch, empty MinIO — tests still pass since they create the data they need).
- It doesn't break when real data gets deleted/overwritten by another Airflow run.
- It scales well when new data sources are added later — confirmed in practice when `jooble` was added: the bronze test only needed a `@pytest.mark.parametrize("source", [...])`, and the silver test only needed a second seed helper (`_jooble_job`) with its own raw schema.

All test data uses a fixed dummy date `9999/01/01` (never overlapping with the pipeline's real production dates), to prevent the cleanup fixture from ever accidentally deleting real data if a bug slips in.

```python
# tests/integration/conftest.py
@pytest.fixture
def cleanup_s3a_paths(spark):
    paths_to_clean = []
    yield paths_to_clean
    for path in paths_to_clean:
        _delete_s3a_path(spark, path)   # deleted via the Hadoop FileSystem API, no boto3 needed
```

Postgres test tables are also named with a `test_` prefix (e.g. `test_jobs_summary`) and get `DROP TABLE`'d automatically once the test finishes.

## Environment setup — the real debugging journey

This part took the most time, since each layer of error was hiding the next one. The actual order things were debugged in:

### 1. `docker compose restart` doesn't apply `docker-compose.yml` changes
Added volume mounts (`./tests:/home/jovyan/tests`, `./jars:/home/jovyan/jars`) but used `restart` instead of `up -d` → the existing container kept its old config. `restart` only restarts the current container as-is; `docker compose up -d` is required for Docker to diff the config and recreate the container with the new settings.

### 2. `pytest: executable file not found`
The Spark image didn't have `pytest`/`pytest-mock`/`psycopg2-binary` installed. The Dockerfile only `COPY`s + `pip install`s specific requirements files (`spark.txt`, `airflow.txt`...) — adding `requirements/tests.txt` to the codebase doesn't auto-install it; it needs a matching `COPY` + `RUN pip install` block in the Dockerfile, or a manual install into the running container for a quick test.

### 3. `TypeError: 'JavaPackage' object is not callable` (round 1 — missing JARs)
Reading S3A requires `hadoop-aws` + `aws-java-sdk-bundle` on the classpath. The `jars/` folder initially only had `postgresql-*.jar` — the other two were missing.

### 4. `TypeError: 'JavaPackage' object is not callable` (round 2 — conflicting JAR versions)
After adding `hadoop-aws-3.3.4.jar` to `spark.jars`, the error persisted — because the Spark 3.5.0 image already ships `hadoop-aws-3.3.2.jar` in `$SPARK_HOME/jars/` (auto-loaded by default). Adding version `3.3.4` on top via `spark.jars` put **two versions of the same class on the classpath at once** → conflict, and the JVM couldn't resolve `SparkSession`.

**Fix:** removed the manually downloaded `hadoop-aws`/`aws-java-sdk-bundle`, keeping only `postgresql-*.jar` in `jars/` (the JDBC driver isn't bundled with the image, so it's the only JAR that genuinely needs to be added via `spark.jars`).

```bash
docker compose exec spark sh -c "find / -iname 'hadoop-aws*.jar' 2>/dev/null"
# /usr/local/spark-3.5.0-bin-hadoop3/jars/hadoop-aws-3.3.2.jar   <- already present
# /home/jovyan/jars/hadoop-aws-3.3.4.jar                          <- redundant, caused the conflict
```

### 5. `TypeError: 'JavaPackage' object is not callable` (round 3 — PySpark version mismatch)
After cleaning up the JARs the error was still there, this time crashing right inside `SparkSession.__init__` — before even touching the S3A config. Checking `pyspark.__version__` revealed **4.2.0**, while the Spark bundled in the image was **3.5.0** — caused by `requirements/tests.txt` not pinning a version, so `pip install pyspark` pulled the latest release from PyPI, a full major version off from the actual JVM running in the container.

**Fix:** pinned `pyspark==3.5.0` in `requirements/tests.txt`.

### 6. `Permission denied` when reinstalling `pyspark==3.5.0`
The user running the command inside the container (`jovyan`) didn't have permission to overwrite a package installed by a different user at image build time. Fixed by running as root:
```bash
docker compose exec -u root spark pip install pyspark==3.5.0 --force-reinstall
```

### 7. `AnalysisException: Can only star expand struct data types`
Once every environment layer was solid, the last failure was a **test logic** issue — creating the bronze test DataFrame with `spark.createDataFrame(data)` without a schema caused Spark to mis-infer the nested struct types (`category`, `company`, `location`), so `explode(...).select("job.*")` failed because the column after explode was inferred as `Array` instead of `Struct`.

**Fix:** declared an explicit `StructType` for the bronze job schema (same approach used in the unit tests), passed via `spark.createDataFrame(data, schema=...)`.

## Testing Strategy

- Bronze layer → read-only round-trip, parametrized per source (`adzuna`, `jooble`)
- Silver layer → full pipeline round-trip: seed multi-source bronze → `process_silver` → write → read back unified, plus a dedicated dedup test
- Gold layer → write/read Parquet round-trip on the unified path
- Postgres → write/read JDBC round-trip, source-agnostic

## Key Takeaways

- **PySpark on Windows needs 3 foundational things** before logic even comes into play: `winutils.exe`/`hadoop.dll` (native Hadoop binaries), `PYSPARK_PYTHON` pointing to the right `python.exe` (Windows has no `python3`), and extra JARs that don't conflict in version with the ones already shipped in the Spark image.
- **`docker compose restart` ≠ `docker compose up -d`** — any change in `docker-compose.yml` (volumes, ports, env) needs `up -d` for the container to be recreated with the new config.
- **Not pinning a dependency version (`pyspark` without `==`) is a real risk**, not just a theoretical one — it caused a major version mismatch between PySpark (Python side) and the Spark runtime (JVM) already running in the container, producing an error that gave no hint it was a version mismatch.
- **Explicit schemas when building test DataFrames** don't just prevent simple type errors (`CANNOT_INFER_TYPE`) — they also prevent deeper execution-time failures (struct star-expansion) when nested fields are involved.
- **Unifying Silver/Gold across sources simplified the test surface**, not complicated it: adding `jooble` only meant parametrizing bronze and adding one seed helper for silver — the write/read paths and Gold layer needed no source-specific test at all.