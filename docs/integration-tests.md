# Integration Tests — `tests/integration/`

## Purpose

Integration tests cover the **real I/O boundary** — reading/writing MinIO (S3A), writing to PostgreSQL via JDBC — unlike unit tests which only handle in-memory data. These tests verify the pipeline connects correctly to real infrastructure (MinIO, Postgres); they don't re-test transform logic (already covered by unit tests).

Must run **inside the Spark container** (not on the Windows host), since it needs the internal Docker network to resolve `minio:9000`, `postgres:5432`.

## Structure

```
tests/integration/
├── conftest.py                        # Spark session with S3A + MinIO/Postgres cleanup fixtures
├── test_bronze_read_write_minio.py    # read real bronze JSON from MinIO
├── test_silver_read_write_minio.py    # write Silver Parquet + read back (round-trip)
├── test_gold_read_write_minio.py      # write Gold Parquet + read back (round-trip)
└── test_database_writer_postgres.py   # write to Postgres via JDBC + read back to verify
```

## How to run

```bash
docker compose exec spark pytest tests/integration -v
```

## Test data strategy: automatic seed + cleanup

Each test seeds its own fake data into MinIO/Postgres **before** it runs, and **cleans it up** afterward — it doesn't depend on real data already present from a previous pipeline run.

Why this approach over relying on existing real data:
- The suite is reproducible on another machine (fresh clone, `docker compose up` from scratch, empty MinIO — tests still pass since they create the data they need).
- It doesn't break when real data gets deleted/overwritten by another Airflow run.
- It scales well when new data sources are added later (just add a dedicated seed fixture for that source).

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

## Takeaways

- **PySpark on Windows needs 3 foundational things** before logic even comes into play: `winutils.exe`/`hadoop.dll` (native Hadoop binaries), `PYSPARK_PYTHON` pointing to the right `python.exe` (Windows has no `python3`), and extra JARs that don't conflict in version with the ones already shipped in the Spark image.
- **`docker compose restart` ≠ `docker compose up -d`** — any change in `docker-compose.yml` (volumes, ports, env) needs `up -d` for the container to be recreated with the new config.
- **Not pinning a dependency version (`pyspark` without `==`) is a real risk**, not just a theoretical one — it caused a major version mismatch between PySpark (Python side) and the Spark runtime (JVM) already running in the container, producing an error that gave no hint it was a version mismatch.
- **Explicit schemas when building test DataFrames** don't just prevent simple type errors (`CANNOT_INFER_TYPE`) — they also prevent deeper execution-time failures (struct star-expansion) when nested fields are involved.