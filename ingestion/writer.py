import json
from datetime import datetime
from io import BytesIO
from minio import Minio
from minio.error import S3Error


# -------------------------
# MinIO client
# -------------------------
def get_minio_client():
    return Minio(
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )


# -------------------------
# Ensure bucket exists
# -------------------------
def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


# -------------------------
# Write batch to Data Lake
# -------------------------
def write_batch(batch: dict, bucket: str = "data-lake"):
    client = get_minio_client()
    ensure_bucket(client, bucket)

    date_path = datetime.now().strftime("%Y/%m/%d")

    object_name = (
        f"bronze/"
        f"{batch['source']}/"
        f"{date_path}/"
        f"batch_{batch['batch_id']}.json"
    )

    payload_bytes = json.dumps(batch, ensure_ascii=False).encode("utf-8")
    payload_stream = BytesIO(payload_bytes)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=payload_stream,
        length=len(payload_bytes),
        content_type="application/json"
    )

    print(f"✅ Written batch to MinIO: {object_name}")
