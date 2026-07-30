# run_ingestion.py
from ingestion.batch_builder import build_batches
from ingestion.sources import adzuna_source, jooble_source
from storage.writer_minio import write_batch

SOURCES = {
    "adzuna": adzuna_source.fetch_all_jobs,
    "jooble": jooble_source.fetch_all_jobs,
}

def main():
    for source_name, fetch_fn in SOURCES.items():
        raw_records = fetch_fn()
        batches = build_batches(records=raw_records, batch_size=100, source=source_name)
        for batch in batches:
            write_batch(batch)

if __name__ == "__main__":
    main()