import pytest


@pytest.fixture
def sample_adzuna_record():
    return {
        "records": [{
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
        }]
    }