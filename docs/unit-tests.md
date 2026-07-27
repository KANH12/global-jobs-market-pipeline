# Unit Tests — `tests/unit/`

## Mục đích

Unit test trong project này kiểm tra **logic nghiệp vụ thuần túy** (transform, validation, aggregation) chạy trên Spark session local (`local[*]`), hoàn toàn trong RAM — **không** cần MinIO, Postgres, hay Docker network. Dữ liệu đầu vào được tạo thủ công bằng `spark.createDataFrame(data, schema=...)`.

Test cần I/O thật (đọc JSON từ MinIO, ghi Parquet lên S3A, ghi Postgres) thuộc phạm vi `tests/integration/` (chạy trong Docker), không nằm trong phần này.

## Cấu trúc

```
tests/
├── conftest.py                     # fixture dùng chung (sample data)
└── unit/
    ├── conftest.py                  # Spark session local + fix PYSPARK_PYTHON
    ├── test_bronze_quality.py       # check_record_count, check_required_columns, check_records_structure
    ├── test_silver_transform.py     # clean_invalid_ids, deduplicate_jobs, standardize_contract_fields, normalize_salary, process_silver
    ├── test_silver_quality.py       # check_required_fields, check_duplicates, check_salary, check_contract_fields
    ├── test_gold_summary.py         # build_jobs_summary (aggregation theo ingestion_date + category_label)
    ├── test_gold_salary_analysis.py # build_salary_analysis (aggregation theo contract_time)
    └── test_gold_jobs_detail.py     # build_jobs_detail (transform, không aggregate)
```

## Cách chạy

```bash
python -m pytest tests/unit -v
```

Chạy từ thư mục gốc project (nơi có `pytest.ini`), không cd vào `tests/`.

## Setup môi trường (Windows)

Chạy PySpark local trên Windows cần 2 điều kiện, thiếu 1 trong 2 sẽ crash ngay khi gọi bất kỳ action nào (`count()`, `collect()`...), dù `createDataFrame` (chỉ khai báo) vẫn chạy được bình thường:

1. **Hadoop native binaries (`winutils.exe`, `hadoop.dll`)** — Spark cần các file này để thao tác file tạm/shuffle ngay cả khi chạy hoàn toàn local, không liên quan gì đến MinIO/S3.
   - Tải đúng version khớp Hadoop bundle trong PySpark (repo `cdarlint/winutils`).
   - Đặt vào `D:\Hadoop\bin\winutils.exe` + `hadoop.dll`.
   - Set biến môi trường `HADOOP_HOME=D:\Hadoop`, thêm `%HADOOP_HOME%\bin` vào `PATH`.
   - Mở lại terminal mới hoàn toàn để nhận biến môi trường.

2. **`PYSPARK_PYTHON`** — mặc định Spark launch Python worker bằng lệnh `python3`, không tồn tại trên Windows (chỉ có `python`). Set trong `tests/unit/conftest.py`:
   ```python
   os.environ["PYSPARK_PYTHON"] = sys.executable
   os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
   ```
   Dùng `sys.executable` để tự động trỏ đúng `venv\Scripts\python.exe` đang chạy, không cần hardcode đường dẫn.

## Vì sao dùng explicit schema thay vì để Spark tự suy luận

Ban đầu test tạo DataFrame bằng `spark.createDataFrame(data)` không kèm schema, gây lỗi `CANNOT_INFER_TYPE_FOR_FIELD` / `CANNOT_DETERMINE_TYPE` trong các trường hợp:
- Một cột toàn giá trị `None` trong tất cả các dòng test (ví dụ `company_name=None` là dòng duy nhất).
- Nested struct (`category`, `company`, `location` trong bronze record).

Giải pháp: khai báo `StructType` tường minh cho mỗi bộ test data, truyền vào `createDataFrame(data, schema=SCHEMA)`. Cách này cũng giúp test ổn định hơn, không phụ thuộc vào thứ tự/giá trị ngẫu nhiên của dòng đầu tiên khi Spark sample để suy luận kiểu.

## Bug phát hiện qua unit test

**`check_contract_fields` (silver_quality.py) — early return che khuất lỗi contract_time**

Code gốc:
```python
if invalid_contract_type_count == 0:
    logger.info("[CHECKED] No invalid contract_type found")
    return   # thoát sớm, bỏ qua toàn bộ phần check contract_time bên dưới
```

Khi `contract_type` hợp lệ nhưng `contract_time` không hợp lệ, hàm trả về ngay sau khi check `contract_type`, khiến phần check `contract_time` (nằm phía dưới) **không bao giờ được chạy**. Test `test_check_contract_fields_warns_on_invalid_contract_time` (đưa `contract_type` hợp lệ + `contract_time` không hợp lệ) phát hiện ra hành vi này.

**Fix:** tách 2 khối check độc lập bằng `if/else`, không dùng `return` sớm, để cả hai luôn được đánh giá bất kể kết quả của khối còn lại.

## Ghi chú thiết kế test

- `bronze_quality.py` dùng chiến lược **fail-fast**: các hàm `check_*` raise `Exception` ngay khi phát hiện lỗi → test dùng `pytest.raises(...)`.
- `silver_quality.py` dùng chiến lược **monitoring-based**: các hàm `check_*` chỉ `logger.warning(...)`, không raise, không return giá trị → test dùng `caplog` để verify đúng nội dung log được ghi, thay vì chỉ kiểm tra "không crash".
- `caplog` cần `logger.propagate = True` để bắt được log — logger custom trong `core/logger.py` mặc định set `propagate = False` (tránh duplicate log khi chạy thật), nên `tests/unit/conftest.py` có fixture `enable_log_propagation` (autouse) tạm bật lại propagate trong lúc test, trả về trạng thái cũ sau khi test xong.
