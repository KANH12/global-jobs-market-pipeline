# ingestion/run_ingestion.py

from fetcher import fetch_all_jobs
from batch_builder import build_batches
from writer import write_batch


def main():
    raw_records = fetch_all_jobs()

    batches = build_batches(
        records=raw_records,
        batch_size=100,
        source="adzuna"
    )

    for batch in batches:
        write_batch(batch)


if __name__ == "__main__":
    main()
