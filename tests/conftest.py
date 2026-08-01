import pytest


@pytest.fixture
def sample_adzuna_job():
    return {
        "id": "1",
        "title": "Data Engineer",
        "contract_time": "full_time",
        "contract_type": "permanent",
        "created": 1234567890,
        "salary_min": 30,
        "salary_max": 50,
        "salary_is_predicted": "0",
        "latitude": 10.0,
        "longitude": 106.0,
        "category": {"label": "IT", "tag": "it-jobs"},
        "company": {"display_name": "ABC"},
        "location": {"display_name": "HCM", "area": ["VN"]}
    }


@pytest.fixture
def sample_jooble_job():
    """Raw single job dict, shaped exactly like one element of Jooble's
    bronze 'jobs' array (flat, no nested struct)."""
    return {
        "id": 12345,
        "title": "Data Engineer",
        "company": "ABC Corp",
        "location": "Remote",
        "salary": "$70,000 - $90,000 a year",
        "snippet": "Looking for a data engineer...",
        "source": "LinkedIn",
        "type": "Full-time",
        "link": "https://jooble.org/job/12345",
        "updated": "2026-05-20T00:00:00",
    }