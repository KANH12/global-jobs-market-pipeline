import streamlit as st
import pandas as pd
import plotly.express as px

from db import load_jobs_summary, load_salary_analysis, load_jobs_detail


# =========================
# Page config
# =========================
st.set_page_config(
    page_title="Global Jobs Market Dashboard",
    page_icon="📊",
    layout="wide"
)


# =========================
# Custom CSS
# =========================
st.markdown(
    """
    <style>
        .main-title {
            font-size: 42px;
            font-weight: 800;
            margin-bottom: 0px;
        }

        .subtitle {
            color: #6b7280;
            font-size: 16px;
            margin-bottom: 25px;
        }

        .section-title {
            font-size: 24px;
            font-weight: 700;
            margin-top: 25px;
            margin-bottom: 15px;
        }

        .metric-card {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 16px;
            border: 1px solid #e5e7eb;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        }

        .metric-label {
            color: #6b7280;
            font-size: 14px;
            margin-bottom: 8px;
        }

        .metric-value {
            color: #111827;
            font-size: 32px;
            font-weight: 800;
        }

        .status-badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 999px;
            background-color: #dcfce7;
            color: #166534;
            font-weight: 700;
            font-size: 13px;
        }

        .job-card {
            background-color: #ffffff;
            padding: 18px;
            border-radius: 14px;
            border: 1px solid #e5e7eb;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            margin-bottom: 14px;
        }

        .job-title {
            font-size: 19px;
            font-weight: 800;
            color: #111827;
            margin-bottom: 6px;
        }

        .job-meta {
            color: #4b5563;
            font-size: 14px;
            margin-bottom: 4px;
        }

        .pill {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            background-color: #f3f4f6;
            color: #374151;
            font-size: 12px;
            font-weight: 600;
            margin-right: 6px;
            margin-top: 8px;
        }
    </style>
    """,
    unsafe_allow_html=True
)


# =========================
# Load data
# =========================
@st.cache_data(ttl=300)
def get_data():
    jobs_summary = load_jobs_summary()
    salary_analysis = load_salary_analysis()
    jobs_detail = load_jobs_detail()
    return jobs_summary, salary_analysis, jobs_detail


try:
    jobs_summary, salary_analysis, jobs_detail = get_data()

except Exception as e:
    st.error("Failed to load data from PostgreSQL.")
    st.exception(e)
    st.stop()


# =========================
# Header
# =========================
st.markdown(
    '<div class="main-title">Global Jobs Market Dashboard</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="subtitle">Adzuna jobs data pipeline: API → MinIO → Spark → PostgreSQL → Streamlit</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<span class="status-badge">Pipeline Connected</span>',
    unsafe_allow_html=True
)

st.divider()


# =========================
# Sidebar
# =========================
st.sidebar.title("Dashboard Filters")
st.sidebar.caption("Filter jobs market data")

filtered_summary = jobs_summary.copy()
filtered_salary = salary_analysis.copy()
filtered_detail = jobs_detail.copy()


# Ingestion date filter
date_source = filtered_summary

if "ingestion_date" in date_source.columns:
    available_dates = sorted(date_source["ingestion_date"].dropna().unique())

    selected_dates = st.sidebar.multiselect(
        "Ingestion Date",
        options=available_dates,
        default=available_dates
    )

    if selected_dates:
        if "ingestion_date" in filtered_summary.columns:
            filtered_summary = filtered_summary[
                filtered_summary["ingestion_date"].isin(selected_dates)
            ]

        if "ingestion_date" in filtered_detail.columns:
            filtered_detail = filtered_detail[
                filtered_detail["ingestion_date"].isin(selected_dates)
            ]


# Category filter
if "category_label" in filtered_summary.columns:
    available_categories = sorted(
        filtered_summary["category_label"].dropna().unique()
    )

    selected_categories = st.sidebar.multiselect(
        "Category",
        options=available_categories,
        default=available_categories
    )

    if selected_categories:
        filtered_summary = filtered_summary[
            filtered_summary["category_label"].isin(selected_categories)
        ]

        if "category_label" in filtered_detail.columns:
            filtered_detail = filtered_detail[
                filtered_detail["category_label"].isin(selected_categories)
            ]


# Contract time filter
if "contract_time" in filtered_salary.columns:
    available_contracts = sorted(
        filtered_salary["contract_time"].dropna().unique()
    )

    selected_contracts = st.sidebar.multiselect(
        "Contract Time",
        options=available_contracts,
        default=available_contracts
    )

    if selected_contracts:
        filtered_salary = filtered_salary[
            filtered_salary["contract_time"].isin(selected_contracts)
        ]

        if "contract_time" in filtered_detail.columns:
            filtered_detail = filtered_detail[
                filtered_detail["contract_time"].isin(selected_contracts)
            ]


# =========================
# Helper values
# =========================
total_jobs = (
    int(filtered_summary["total_jobs"].sum())
    if "total_jobs" in filtered_summary.columns and not filtered_summary.empty
    else len(filtered_detail)
)

total_categories = (
    int(filtered_summary["category_label"].nunique())
    if "category_label" in filtered_summary.columns and not filtered_summary.empty
    else 0
)

avg_salary = (
    filtered_summary["avg_salary"].mean()
    if "avg_salary" in filtered_summary.columns and not filtered_summary.empty
    else 0
)

median_salary = (
    filtered_summary["median_salary"].mean()
    if "median_salary" in filtered_summary.columns and not filtered_summary.empty
    else 0
)


# =========================
# KPI Cards
# =========================
st.markdown('<div class="section-title">Overview</div>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">Total Jobs</div>
            <div class="metric-value">{total_jobs:,}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col2:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">Categories</div>
            <div class="metric-value">{total_categories:,}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col3:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">Avg Salary</div>
            <div class="metric-value">{avg_salary:,.0f}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col4:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">Median Salary</div>
            <div class="metric-value">{median_salary:,.0f}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


# =========================
# Tabs
# =========================
tab_overview, tab_explorer, tab_salary, tab_data = st.tabs(
    ["Market Overview", "Job Explorer", "Salary Analysis", "Data Explorer"]
)


# =========================
# Tab 1: Market Overview
# =========================
with tab_overview:
    st.markdown(
        '<div class="section-title">Jobs Distribution</div>',
        unsafe_allow_html=True
    )

    col_left, col_right = st.columns(2)

    with col_left:
        if {"category_label", "total_jobs"}.issubset(filtered_summary.columns):
            category_chart = (
                filtered_summary
                .groupby("category_label", as_index=False)["total_jobs"]
                .sum()
                .sort_values("total_jobs", ascending=False)
                .head(10)
            )

            fig = px.bar(
                category_chart,
                x="total_jobs",
                y="category_label",
                orientation="h",
                title="Top 10 Job Categories",
                labels={
                    "total_jobs": "Total Jobs",
                    "category_label": "Category"
                }
            )

            fig.update_layout(
                yaxis={"categoryorder": "total ascending"},
                height=500
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Missing category_label or total_jobs column.")

    with col_right:
        contract_cols = {
            "total_jobs",
            "pct_full_time",
            "pct_part_time",
            "pct_unknown"
        }

        if contract_cols.issubset(filtered_summary.columns):
            contract_distribution = pd.DataFrame({
                "contract_type": ["FULL_TIME", "PART_TIME", "UNKNOWN"],
                "estimated_jobs": [
                    (
                        filtered_summary["total_jobs"]
                        * filtered_summary["pct_full_time"]
                    ).sum(),
                    (
                        filtered_summary["total_jobs"]
                        * filtered_summary["pct_part_time"]
                    ).sum(),
                    (
                        filtered_summary["total_jobs"]
                        * filtered_summary["pct_unknown"]
                    ).sum(),
                ]
            })

            fig = px.pie(
                contract_distribution,
                names="contract_type",
                values="estimated_jobs",
                title="Estimated Jobs by Contract Type",
                hole=0.45
            )

            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Missing contract percentage columns.")

    st.markdown(
        '<div class="section-title">Category Breakdown</div>',
        unsafe_allow_html=True
    )

    if {"category_label", "total_jobs"}.issubset(filtered_summary.columns):
        agg_dict = {
            "total_jobs": ("total_jobs", "sum")
        }

        if "avg_salary" in filtered_summary.columns:
            agg_dict["avg_salary"] = ("avg_salary", "mean")

        if "min_salary" in filtered_summary.columns:
            agg_dict["min_salary"] = ("min_salary", "min")

        if "max_salary" in filtered_summary.columns:
            agg_dict["max_salary"] = ("max_salary", "max")

        if "median_salary" in filtered_summary.columns:
            agg_dict["median_salary"] = ("median_salary", "mean")

        category_table = (
            filtered_summary
            .groupby("category_label", as_index=False)
            .agg(**agg_dict)
            .sort_values("total_jobs", ascending=False)
        )

        st.dataframe(
            category_table,
            use_container_width=True,
            hide_index=True
        )


# =========================
# Tab 2: Job Explorer
# =========================
with tab_explorer:
    st.markdown(
        '<div class="section-title">Job Explorer</div>',
        unsafe_allow_html=True
    )

    if filtered_detail.empty:
        st.info("No job detail data available.")
    else:
        col_search, col_salary = st.columns([2, 1])

        with col_search:
            keyword = st.text_input(
                "Search jobs",
                placeholder="Search by title, company, category, location..."
            )

        with col_salary:
            salary_filter_enabled = st.checkbox("Filter by salary", value=False)

        explorer_df = filtered_detail.copy()

        if keyword:
            keyword_lower = keyword.lower()

            if "job_search_text" in explorer_df.columns:
                explorer_df = explorer_df[
                    explorer_df["job_search_text"]
                    .astype(str)
                    .str.contains(keyword_lower, case=False, na=False)
                ]
            else:
                search_cols = [
                    col for col in [
                        "title",
                        "company_name",
                        "category_label",
                        "location_name",
                        "contract_time",
                        "contract_type"
                    ]
                    if col in explorer_df.columns
                ]

                if search_cols:
                    search_series = (
                        explorer_df[search_cols]
                        .fillna("")
                        .astype(str)
                        .agg(" ".join, axis=1)
                        .str.lower()
                    )

                    explorer_df = explorer_df[
                        search_series.str.contains(keyword_lower, na=False)
                    ]

        if salary_filter_enabled and "salary_avg" in explorer_df.columns:
            salary_values = explorer_df["salary_avg"].dropna()

            if not salary_values.empty:
                min_salary = float(salary_values.min())
                max_salary = float(salary_values.max())

                selected_salary_range = st.slider(
                    "Salary Average Range",
                    min_value=min_salary,
                    max_value=max_salary,
                    value=(min_salary, max_salary)
                )

                explorer_df = explorer_df[
                    explorer_df["salary_avg"].between(
                        selected_salary_range[0],
                        selected_salary_range[1]
                    )
                ]

        st.caption(f"Showing {len(explorer_df):,} jobs")

        sort_options = []

        if "created_date" in explorer_df.columns:
            sort_options.append("Newest")

        if "salary_avg" in explorer_df.columns:
            sort_options.extend(["Highest Salary", "Lowest Salary"])

        sort_options.append("Default")

        selected_sort = st.selectbox(
            "Sort by",
            options=sort_options,
            index=0
        )

        if selected_sort == "Newest" and "created_date" in explorer_df.columns:
            explorer_df = explorer_df.sort_values("created_date", ascending=False)

        elif selected_sort == "Highest Salary" and "salary_avg" in explorer_df.columns:
            explorer_df = explorer_df.sort_values("salary_avg", ascending=False)

        elif selected_sort == "Lowest Salary" and "salary_avg" in explorer_df.columns:
            explorer_df = explorer_df.sort_values("salary_avg", ascending=True)

        display_mode = st.radio(
            "Display mode",
            options=["Cards", "Table"],
            horizontal=True
        )

        if display_mode == "Cards":
            max_cards = st.slider(
                "Number of job cards",
                min_value=5,
                max_value=50,
                value=10,
                step=5
            )

            for _, row in explorer_df.head(max_cards).iterrows():
                title = row.get("title", "Unknown title")
                company = row.get("company_name", "Unknown company")
                location = row.get("location_name", "Unknown location")
                category = row.get("category_label", "Unknown category")
                contract_time = row.get("contract_time", "UNKNOWN")
                contract_type = row.get("contract_type", "UNKNOWN")
                created_date = row.get("created_date", row.get("created", "Unknown"))
                salary_range = row.get("salary_range", "Not available")

                st.markdown(
                    f"""
                    <div class="job-card">
                        <div class="job-title">{title}</div>
                        <div class="job-meta"><b>Company:</b> {company}</div>
                        <div class="job-meta"><b>Location:</b> {location}</div>
                        <div class="job-meta"><b>Category:</b> {category}</div>
                        <div class="job-meta"><b>Salary:</b> {salary_range}</div>
                        <div class="job-meta"><b>Created:</b> {created_date}</div>
                        <span class="pill">{contract_time}</span>
                        <span class="pill">{contract_type}</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        else:
            job_cols = [
                "job_id",
                "title",
                "company_name",
                "category_label",
                "location_name",
                "contract_time",
                "contract_type",
                "created_date",
                "salary_min",
                "salary_max",
                "salary_avg",
                "salary_range",
                "salary_is_predicted",
                "ingestion_date"
            ]

            existing_job_cols = [
                col for col in job_cols
                if col in explorer_df.columns
            ]

            st.dataframe(
                explorer_df[existing_job_cols],
                use_container_width=True,
                hide_index=True
            )


# =========================
# Tab 3: Salary Analysis
# =========================
with tab_salary:
    st.markdown(
        '<div class="section-title">Salary Analysis by Contract Time</div>',
        unsafe_allow_html=True
    )

    if filtered_salary.empty:
        st.info("No salary analysis data available.")
    else:
        col_left, col_right = st.columns(2)

        with col_left:
            required_cols = {
                "contract_time",
                "avg_salary_min",
                "avg_salary_max"
            }

            if required_cols.issubset(filtered_salary.columns):
                salary_long = filtered_salary.melt(
                    id_vars="contract_time",
                    value_vars=["avg_salary_min", "avg_salary_max"],
                    var_name="salary_type",
                    value_name="salary"
                )

                fig = px.bar(
                    salary_long,
                    x="contract_time",
                    y="salary",
                    color="salary_type",
                    barmode="group",
                    title="Average Salary Range by Contract Time",
                    labels={
                        "contract_time": "Contract Time",
                        "salary": "Salary",
                        "salary_type": "Salary Type"
                    }
                )

                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(
                    "Missing contract_time, avg_salary_min or avg_salary_max columns."
                )

        with col_right:
            required_cols = {
                "contract_time",
                "median_salary"
            }

            if required_cols.issubset(filtered_salary.columns):
                fig = px.bar(
                    filtered_salary.sort_values("median_salary", ascending=False),
                    x="contract_time",
                    y="median_salary",
                    title="Median Salary by Contract Time",
                    labels={
                        "contract_time": "Contract Time",
                        "median_salary": "Median Salary"
                    }
                )

                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Missing contract_time or median_salary columns.")

        st.markdown(
            '<div class="section-title">Jobs by Contract Time</div>',
            unsafe_allow_html=True
        )

        if {"contract_time", "job_count"}.issubset(filtered_salary.columns):
            fig = px.pie(
                filtered_salary,
                names="contract_time",
                values="job_count",
                title="Job Count by Contract Time",
                hole=0.45
            )

            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Missing contract_time or job_count columns.")

        st.markdown(
            '<div class="section-title">Salary Analysis Table</div>',
            unsafe_allow_html=True
        )

        salary_cols = [
            "contract_time",
            "job_count",
            "avg_salary_min",
            "avg_salary_max",
            "median_salary",
            "max_salary",
            "min_salary"
        ]

        existing_salary_cols = [
            col for col in salary_cols
            if col in filtered_salary.columns
        ]

        st.dataframe(
            filtered_salary[existing_salary_cols],
            use_container_width=True,
            hide_index=True
        )

    st.markdown(
        '<div class="section-title">Salary by Job Category</div>',
        unsafe_allow_html=True
    )

    category_salary_cols = {
        "category_label",
        "avg_salary",
        "median_salary",
        "total_jobs"
    }

    if category_salary_cols.issubset(filtered_summary.columns):
        salary_category = (
            filtered_summary
            .groupby("category_label", as_index=False)
            .agg(
                avg_salary=("avg_salary", "mean"),
                median_salary=("median_salary", "mean"),
                total_jobs=("total_jobs", "sum")
            )
            .sort_values("total_jobs", ascending=False)
            .head(10)
        )

        salary_category_long = salary_category.melt(
            id_vars="category_label",
            value_vars=["avg_salary", "median_salary"],
            var_name="salary_type",
            value_name="salary"
        )

        fig = px.bar(
            salary_category_long,
            x="category_label",
            y="salary",
            color="salary_type",
            barmode="group",
            title="Average vs Median Salary by Top Categories",
            labels={
                "category_label": "Category",
                "salary": "Salary",
                "salary_type": "Salary Type"
            }
        )

        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Missing category salary columns from jobs_summary.")


# =========================
# Tab 4: Data Explorer
# =========================
with tab_data:
    st.markdown(
        '<div class="section-title">Jobs Detail Data</div>',
        unsafe_allow_html=True
    )

    st.dataframe(
        filtered_detail,
        use_container_width=True,
        hide_index=True
    )

    detail_csv = filtered_detail.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download jobs detail as CSV",
        data=detail_csv,
        file_name="jobs_detail_filtered.csv",
        mime="text/csv"
    )

    st.markdown(
        '<div class="section-title">Jobs Summary Data</div>',
        unsafe_allow_html=True
    )

    st.dataframe(
        filtered_summary,
        use_container_width=True,
        hide_index=True
    )

    summary_csv = filtered_summary.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download jobs summary as CSV",
        data=summary_csv,
        file_name="jobs_summary_filtered.csv",
        mime="text/csv"
    )

    st.markdown(
        '<div class="section-title">Salary Analysis Data</div>',
        unsafe_allow_html=True
    )

    st.dataframe(
        filtered_salary,
        use_container_width=True,
        hide_index=True
    )

    salary_csv = filtered_salary.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download salary analysis as CSV",
        data=salary_csv,
        file_name="salary_analysis_filtered.csv",
        mime="text/csv"
    )


# =========================
# Footer
# =========================
st.divider()

st.caption(
    "Built with Airflow, Spark, MinIO, PostgreSQL and Streamlit. "
    "This dashboard is powered by an automated jobs market data pipeline."
)