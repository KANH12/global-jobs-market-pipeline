import requests
import yaml
from pathlib import Path
import os
from core.logger import get_job_logger
from dotenv import load_dotenv

load_dotenv()
# -------------------------
# Load API id and keys
# -------------------------
app_id = os.getenv("ADZUNA_APP_ID")
app_key = os.getenv("ADZUNA_APP_KEY")

if not app_id or not app_key:
    raise Exception("Missing Adzuna API credentials")

# -------------------------
# Load API config
# -------------------------
api_logger = get_job_logger(
    job_name="adzuna_fetch",
    component="api"
)
#CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "adzuna_api.yaml"
BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "config" / "adzuna_api.yaml"

with open(CONFIG_PATH, "r") as f:
    API_CONFIG = yaml.safe_load(f)

BASE_URL = API_CONFIG["base_url"]

RESULTS_PER_PAGE = API_CONFIG["results_per_page"]
MAX_PAGES = API_CONFIG["max_pages"]
TIMEOUT = API_CONFIG.get("timeout", 30)
CATEGORY = API_CONFIG.get("category") 

if "countries" in API_CONFIG:
    COUNTRIES = API_CONFIG["countries"]
elif "country" in API_CONFIG:
    COUNTRIES = [API_CONFIG["country"]]
else:
    raise KeyError(
        "config/api.yaml phải có 'country' (1 quốc gia) hoặc 'countries' (danh sách)"
    )
 
def fetch_page(country: str, page: int, query_params: dict = None) -> dict:
    url = f"{BASE_URL}/jobs/{country}/search/{page}"
 
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": RESULTS_PER_PAGE,
    }
 
    if CATEGORY:
        params["category"] = CATEGORY
 
    if query_params:
        params.update(query_params)
 
    api_logger.info(
        f"START_FETCH | source=adzuna | country={country} | page={page} | params={query_params}"
    )
    try:
        response = requests.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
 
        data = response.json()
        record_count = len(data.get("results", []))
 
        api_logger.info(
            f"FETCH_SUCCESS | source=adzuna | country={country} | page={page} | records={record_count}"
        )
 
        return data
    except Exception as e:
        api_logger.error(
            f"FETCH_FAILED | source=adzuna | country={country} | page={page} | error={str(e)}"
        )
        raise
 
 
def fetch_all_jobs(query_params: dict = None) -> list:
    all_jobs = []
 
    api_logger.info(f"API_JOB_START | source=adzuna | countries={COUNTRIES}")
 
    for country in COUNTRIES:
        for page in range(1, MAX_PAGES + 1):
            data = fetch_page(country, page, query_params)
            jobs = data.get("results", [])
 
            for job in jobs:
                job["_source_country"] = country
 
            all_jobs.extend(jobs)
 
            if not jobs:
                api_logger.info(
                    f"NO_MORE_DATA | source=adzuna | country={country} | stop_page={page}"
                )
                break
 
    api_logger.info(
        f"API_JOB_FINISH | source=adzuna | total_record={len(all_jobs)}"
    )
 
    return all_jobs
 
 
# TEST
if __name__ == "__main__":
    data = fetch_all_jobs()
    print(f"Records fetched: {len(data)}")
    if data:
        print(data[0])