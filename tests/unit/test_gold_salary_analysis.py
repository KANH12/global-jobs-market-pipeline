from processing.gold.salary_analysis import build_salary_analysis


def test_salary_analysis_groups_by_contract_time(spark):
    data = [
        {"contract_time": "FULL_TIME", "salary_min": 1000, "salary_max": 2000},
        {"contract_time": "FULL_TIME", "salary_min": 1500, "salary_max": 2500},
        {"contract_time": "PART_TIME", "salary_min": 500, "salary_max": 800},
    ]
    df = spark.createDataFrame(data)

    result = build_salary_analysis(df)

    assert result.count() == 2

    full_time_row = result.filter(result.contract_time == "FULL_TIME").collect()[0]
    assert full_time_row["job_count"] == 2
    assert full_time_row["avg_salary_min"] == 1250
    assert full_time_row["avg_salary_max"] == 2250
    assert full_time_row["max_salary"] == 2500
    assert full_time_row["min_salary"] == 1000


def test_salary_analysis_ordered_by_avg_salary_max_desc(spark):
    data = [
        {"contract_time": "PART_TIME", "salary_min": 500, "salary_max": 800},
        {"contract_time": "FULL_TIME", "salary_min": 1000, "salary_max": 3000},
    ]
    df = spark.createDataFrame(data)

    result = build_salary_analysis(df)
    rows = result.collect()

    # FULL_TIME có avg_salary_max cao hơn -> phải đứng đầu (orderBy desc)
    assert rows[0]["contract_time"] == "FULL_TIME"
    assert rows[1]["contract_time"] == "PART_TIME"
