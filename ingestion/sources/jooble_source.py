import os
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

from core.logger import get_job_logger

load_dotenv()

# -------------------------
# Load API key
# -------------------------
JOOBLE_API_KEY = os.getenv("JOOBLE_API_KEY")

if not JOOBLE_API_KEY:
    raise Exception("Missing Jooble API key")

# -------------------------
# Load API config
# -------------------------
api_logger = get_job_logger(
    job_name="jooble_fetch",
    component="api"
)

#CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "jooble_api.yaml"
BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "config" / "jooble_api.yaml"

with open(CONFIG_PATH, "r") as f:
    API_CONFIG = yaml.safe_load(f)

BASE_URL = API_CONFIG["base_url"]  # https://jooble.org/api
KEYWORDS = API_CONFIG.get("keywords", "")
LOCATION = API_CONFIG.get("location", "")
RESULTS_PER_PAGE = API_CONFIG.get("results_per_page", 20)
MAX_PAGES = API_CONFIG.get("max_pages", 10)
TIMEOUT = API_CONFIG.get("timeout", 30)


def fetch_page(page: int, query_params: dict = None) -> dict:
    url = f"{BASE_URL}/{JOOBLE_API_KEY}"

    body = {
        "keywords": KEYWORDS,
        "location": LOCATION,
        "page": page,
        "ResultOnPage": RESULTS_PER_PAGE,
    }

    if query_params:
        body.update(query_params)

    api_logger.info(
        f"START_FETCH | source=jooble | page={page} | params={query_params}"
    )
    try:
        response = requests.post(
            url,
            json=body,
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()

        data = response.json()
        record_count = len(data.get("jobs", []))

        api_logger.info(
            f"FETCH_SUCCESS | source=jooble | page={page} | records={record_count}"
        )

        return data
    except Exception as e:
        api_logger.error(
            f"FETCH_FAILED | source=jooble | page={page} | error={str(e)}"
        )
        raise


def fetch_all_jobs(query_params: dict = None) -> list:
    all_jobs = []

    api_logger.info("API_JOB_START | source=jooble")

    for page in range(1, MAX_PAGES + 1):
        data = fetch_page(page, query_params)
        jobs = data.get("jobs", [])

        all_jobs.extend(jobs)

        if not jobs:
            api_logger.info(
                f"NO_MORE_DATA | source=jooble | stop_page={page}"
            )
            break

    api_logger.info(
        f"API_JOB_FINISH | source=jooble | total_record={len(all_jobs)}"
    )

    return all_jobs


# TEST
if __name__ == "__main__":
    data = fetch_all_jobs()
    print(f"Records fetched: {len(data)}")
    if data:
        print(data[0])