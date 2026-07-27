# Integration Tests — `tests/integration/`

## Mục đích

Integration test kiểm tra **boundary I/O thật** — đọc/ghi MinIO (S3A), ghi Postgres qua JDBC — khác với unit test chỉ xử lý data in-memory. Các test này verify pipeline kết nối đúng với hạ tầng thật (MinIO, Postgres), không lặp lại việc test logic transform (đã được unit test cover).

Bắt buộc chạy **bên trong container Spark** (không chạy trên Windows host), vì cần network nội bộ Docker để resolve đúng `minio:9000`, `postgres:5432`.

## Cấu trúc

```
tests/integration/
├── conftest.py                        # Spark session có S3A + fixture cleanup MinIO/Postgres
├── test_bronze_read_write_minio.py    # đọc bronze JSON thật từ MinIO
├── test_silver_read_write_minio.py    # ghi Silver Parquet + đọc lại (round-trip)
├── test_gold_read_write_minio.py      # ghi Gold Parquet + đọc lại (round-trip)
└── test_database_writer_postgres.py   # ghi Postgres qua JDBC + đọc lại verify
```

## Cách chạy

```bash
docker compose exec spark pytest tests/integration -v
```

## Chiến lược dữ liệu test: seed + cleanup tự động

Mỗi test tự **seed dữ liệu giả vào MinIO/Postgres trước khi chạy**, và **tự dọn dẹp sau khi xong** — không phụ thuộc vào dữ liệu thật đã có sẵn từ lần chạy pipeline trước đó.

Lý do chọn hướng này thay vì dùng data thật có sẵn:
- Test tái tạo được trên máy khác (clone repo, `docker compose up` từ đầu, MinIO trống — test vẫn chạy được vì tự tạo data cần).
- Không vỡ khi dữ liệu thật bị xóa/ghi đè bởi lần chạy Airflow khác.
- Mở rộng tốt khi có thêm nguồn dữ liệu mới (chỉ cần thêm fixture seed riêng cho nguồn đó).

Toàn bộ dữ liệu test dùng ngày giả cố định `9999/01/01` (không bao giờ trùng ngày thật của pipeline production) để tránh fixture cleanup lỡ xóa nhầm data thật nếu có bug.

```python
# tests/integration/conftest.py
@pytest.fixture
def cleanup_s3a_paths(spark):
    paths_to_clean = []
    yield paths_to_clean
    for path in paths_to_clean:
        _delete_s3a_path(spark, path)   # xóa qua Hadoop FileSystem API, không cần boto3
```

Bảng Postgres test cũng đặt tên có prefix `test_` (ví dụ `test_jobs_summary`) và tự `DROP TABLE` sau khi test xong.

## Setup môi trường — hành trình debug thực tế

Phần này tốn nhiều thời gian nhất vì mỗi lớp lỗi che khuất lớp lỗi tiếp theo. Thứ tự debug thực tế:

### 1. `docker compose restart` không áp dụng thay đổi `docker-compose.yml`
Thêm volume mount (`./tests:/home/jovyan/tests`, `./jars:/home/jovyan/jars`) nhưng dùng `restart` thay vì `up -d` → container cũ vẫn giữ config cũ. `restart` chỉ khởi động lại container hiện có; phải dùng `docker compose up -d` để Docker so sánh và recreate container theo config mới.

### 2. `pytest: executable file not found`
Image Spark không cài `pytest`/`pytest-mock`/`psycopg2-binary`. Dockerfile chỉ `COPY` + `pip install` cho từng file requirements cụ thể (`spark.txt`, `airflow.txt`...) — thêm `requirements/tests.txt` vào code không tự động cài, cần thêm dòng `COPY` + `RUN pip install` tương ứng trong Dockerfile, hoặc cài tay vào container đang chạy để test nhanh.

### 3. `TypeError: 'JavaPackage' object is not callable` (lần 1 — do thiếu JAR)
Đọc S3A cần `hadoop-aws` + `aws-java-sdk-bundle` trên classpath. Folder `jars/` ban đầu rỗng (chỉ có `postgresql-*.jar`) → thiếu 2 JAR còn lại.

### 4. `TypeError: 'JavaPackage' object is not callable` (lần 2 — do version JAR xung đột)
Sau khi tải `hadoop-aws-3.3.4.jar` thêm vào `spark.jars`, lỗi vẫn còn — vì image Spark 3.5.0 đã có sẵn `hadoop-aws-3.3.2.jar` trong `$SPARK_HOME/jars/` (tự nạp mặc định). Nạp thêm bản `3.3.4` qua `spark.jars` gây **2 version cùng 1 class cùng lúc trên classpath** → xung đột, JVM không resolve được `SparkSession`.

**Fix:** xóa JAR `hadoop-aws`/`aws-java-sdk-bundle` tự tải, chỉ giữ lại `postgresql-*.jar` trong `jars/` (JDBC driver không có sẵn trong image, JAR duy nhất thực sự cần bổ sung qua `spark.jars`).

```bash
docker compose exec spark sh -c "find / -iname 'hadoop-aws*.jar' 2>/dev/null"
# /usr/local/spark-3.5.0-bin-hadoop3/jars/hadoop-aws-3.3.2.jar   <- đã có sẵn
# /home/jovyan/jars/hadoop-aws-3.3.4.jar                          <- dư thừa, gây xung đột
```

### 5. `TypeError: 'JavaPackage' object is not callable` (lần 3 — do version PySpark)
Dọn JAR xong vẫn lỗi y hệt, lần này crash ngay tại `SparkSession.__init__` — trước cả khi chạm config S3A. Kiểm tra `pyspark.__version__` phát hiện **4.2.0**, trong khi Spark cài sẵn trong image là **3.5.0** — do `requirements/tests.txt` không ghim version, `pip install pyspark` kéo về bản mới nhất từ PyPI, lệch hẳn 1 major version với JVM thật trong container.

**Fix:** ghim cứng `pyspark==3.5.0` trong `requirements/tests.txt`.

### 6. `Permission denied` khi cài lại `pyspark==3.5.0`
User chạy lệnh trong container (`jovyan`) không có quyền ghi đè package đã cài bởi user khác lúc build image. Fix bằng cách chạy với quyền root:
```bash
docker compose exec -u root spark pip install pyspark==3.5.0 --force-reinstall
```

### 7. `AnalysisException: Can only star expand struct data types`
Sau khi mọi lớp môi trường đã ổn, lỗi cuối cùng là **logic test** — tạo bronze DataFrame test bằng `spark.createDataFrame(data)` không kèm schema, khiến Spark suy luận sai kiểu nested struct (`category`, `company`, `location`), làm `explode(...).select("job.*")` thất bại vì cột sau explode bị hiểu thành `Array` thay vì `Struct`.

**Fix:** khai báo `StructType` tường minh cho bronze job schema (giống cách đã áp dụng ở unit test), truyền vào `spark.createDataFrame(data, schema=...)`.

## Bài học rút ra

- **Windows + PySpark local cần 3 điều kiện nền tảng** trước khi bàn tới logic: `winutils.exe`/`hadoop.dll` (native Hadoop binary), `PYSPARK_PYTHON` trỏ đúng `python.exe` (Windows không có `python3`), và JAR bổ sung không được trùng version với JAR có sẵn trong Spark image.
- **`docker compose restart` ≠ `docker compose up -d`** — bất kỳ thay đổi nào trong `docker-compose.yml` (volume, port, env) đều cần `up -d` để container được recreate đúng config mới.
- **Không ghim version dependency (`pyspark` không kèm `==`) là rủi ro thực sự**, không chỉ lý thuyết — gây lệch major version giữa PySpark (Python) và Spark runtime (JVM) đang chạy sẵn trong container, dẫn đến lỗi khó chẩn đoán vì traceback không nói thẳng "version mismatch".
- **Explicit schema khi tạo test DataFrame** không chỉ tránh lỗi kiểu dữ liệu đơn giản (`CANNOT_INFER_TYPE`), mà còn tránh lỗi sâu hơn ở tầng thực thi (star-expand struct) khi có nested field.
