# ingestion/batch_builder.py

from datetime import datetime, timezone
from typing import List, Dict
import uuid


def build_batches(
    records: List[Dict],
    batch_size: int = 100,
    source: str = "adzuna"
) -> List[Dict]:
    """
    Build ingestion batches from raw records.

    Each batch contains:
    - batch_id
    - source
    - ingested_at
    - record_count
    - records (raw payload)

    Returns a list of batch dicts.
    """

    if not records:
        return []

    batches = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for i in range(0, len(records), batch_size):
        chunk = records[i:i + batch_size]

        batch = {
            "batch_id": str(uuid.uuid4()),
            "source": source,
            "ingested_at": ingested_at,
            "record_count": len(chunk),
            "records": chunk
        }

        batches.append(batch)

    return batches
