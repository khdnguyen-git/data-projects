import streamlit as st
    
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Snowflake Usage Dashboard", layout="wide")

session = get_active_session()

DB_SCHEMA = "VING_PRD_TREND_DB.TMP_1M"


def run_query(sql):
    return session.sql(sql).to_pandas()


# -- sidebar navigation
page = st.sidebar.radio("Page", ["Storage", "Query Activity", "Cortex AI Usage"])


# =====================================================
# PAGE 1: STORAGE
# =====================================================
if page == "Storage":
    st.title("Storage Overview")

    df = run_query(f"""
        select
            table_schema
            , count(*) as tables
            , sum(row_count) as total_rows
            , round(sum(gb), 2) as total_gb
            , round(sum(estimated_monthly_cost_usd), 2) as est_monthly_cost
        from {DB_SCHEMA}.kn_pbi_storage_snapshot
        where table_type = 'BASE TABLE'
        group by 1
        order by total_gb desc nulls last
    """)

    # metric cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Size (TB)", f"{df['TOTAL_GB'].sum() / 1024:.1f}")
    c2.metric("Total Tables", f"{df['TABLES'].sum():,.0f}")
    c3.metric("Schemas", f"{len(df)}")
    c4.metric("Est. Monthly Cost", f"${df['EST_MONTHLY_COST'].sum():,.0f}")

    st.divider()

    # bar chart: GB by schema
    st.subheader("Storage by Schema (GB)")
    top = df.head(10).copy()
    st.bar_chart(top, x="TABLE_SCHEMA", y="TOTAL_GB")

    # detail table
    st.subheader("Schema Breakdown")
    st.dataframe(
        df,
        column_config={
            "TABLE_SCHEMA": "Schema",
            "TABLES": st.column_config.NumberColumn("Tables", format="%d"),
            "TOTAL_ROWS": st.column_config.NumberColumn("Rows", format="%d"),
            "TOTAL_GB": st.column_config.NumberColumn("GB", format="%.2f"),
            "EST_MONTHLY_COST": st.column_config.NumberColumn("Est. $/mo", format="$%.2f"),
        },
        hide_index=True,
        use_container_width=True,
    )

    # table-level detail
    with st.expander("Table-level detail"):
        schema_filter = st.selectbox(
            "Filter by schema",
            ["All"] + sorted(df["TABLE_SCHEMA"].tolist()),
        )
        where = "" if schema_filter == "All" else f"and table_schema = '{schema_filter}'"
        tbl = run_query(f"""
            select
                table_schema
                , table_name
                , table_owner
                , row_count
                , gb
                , estimated_monthly_cost_usd as est_cost
                , created
                , last_altered
            from {DB_SCHEMA}.kn_pbi_storage_snapshot
            where table_type = 'BASE TABLE' {where}
            order by gb desc nulls last
            limit 100
        """)
        st.dataframe(tbl, hide_index=True, use_container_width=True)


# =====================================================
# PAGE 2: QUERY ACTIVITY
# =====================================================
elif page == "Query Activity":
    st.title("Query Activity")

    # date range filter
    col_a, col_b = st.columns(2)
    lookback = col_a.selectbox("Time range", ["7 days", "14 days", "30 days", "90 days"], index=1)
    days = int(lookback.split()[0])

    summary = run_query(f"""
        select
            count(*) as total_queries
            , count(distinct user_name) as unique_users
            , round(avg(elapsed_sec), 2) as avg_elapsed
            , round(sum(credits_used_cloud_services), 4) as total_credits
        from {DB_SCHEMA}.kn_pbi_query_activity
        where query_date >= dateadd(day, -{days}, current_date())
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Queries", f"{summary['TOTAL_QUERIES'].iloc[0]:,}")
    c2.metric("Unique Users", f"{summary['UNIQUE_USERS'].iloc[0]}")
    c3.metric("Avg Elapsed (sec)", f"{summary['AVG_ELAPSED'].iloc[0]:.2f}")
    c4.metric("Cloud Credits", f"{summary['TOTAL_CREDITS'].iloc[0]:.4f}")

    st.divider()

    # daily query count
    st.subheader("Daily Queries")
    daily = run_query(f"""
        select query_date, count(*) as queries
        from {DB_SCHEMA}.kn_pbi_query_activity
        where query_date >= dateadd(day, -{days}, current_date())
        group by 1 order by 1
    """)
    st.line_chart(daily, x="QUERY_DATE", y="QUERIES")

    # queries by user
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Queries by User")
        by_user = run_query(f"""
            select user_name, count(*) as queries
            from {DB_SCHEMA}.kn_pbi_query_activity
            where query_date >= dateadd(day, -{days}, current_date())
            group by 1 order by 2 desc limit 10
        """)
        st.bar_chart(by_user, x="USER_NAME", y="QUERIES")

    with col2:
        st.subheader("Queries by Schema")
        by_schema = run_query(f"""
            select coalesce(schema_name, 'N/A') as schema_name, count(*) as queries
            from {DB_SCHEMA}.kn_pbi_query_activity
            where query_date >= dateadd(day, -{days}, current_date())
            group by 1 order by 2 desc limit 10
        """)
        st.bar_chart(by_schema, x="SCHEMA_NAME", y="QUERIES")

    # queries by type
    st.subheader("Queries by Type")
    by_type = run_query(f"""
        select query_type, count(*) as queries, round(avg(elapsed_sec), 2) as avg_sec
        from {DB_SCHEMA}.kn_pbi_query_activity
        where query_date >= dateadd(day, -{days}, current_date())
        group by 1 order by 2 desc
    """)
    st.dataframe(by_type, hide_index=True, use_container_width=True)

    # recent queries
    with st.expander("Recent Queries"):
        recent = run_query(f"""
            select
                start_time
                , user_name
                , query_type
                , query_preview
                , elapsed_sec
                , gb_scanned
                , execution_status
            from {DB_SCHEMA}.kn_pbi_query_activity
            where query_date >= dateadd(day, -{days}, current_date())
            order by start_time desc
            limit 50
        """)
        st.dataframe(recent, hide_index=True, use_container_width=True)


# =====================================================
# PAGE 3: CORTEX AI USAGE
# =====================================================
elif page == "Cortex AI Usage":
    st.title("Cortex AI Usage")

    lookback = st.selectbox("Time range", ["7 days", "14 days", "30 days", "90 days", "All time"], index=2)

    if lookback == "All time":
        date_clause = "1=1"
    else:
        days = int(lookback.split()[0])
        date_clause = f"usage_date >= dateadd(day, -{days}, current_date())"

    tbl = f"{DB_SCHEMA}.kn_pbi_coco_usage"

    summary = run_query(f"""
        select
            round(sum(token_credits), 4) as total_credits
            , count(distinct user_name) as unique_users
            , count(*) as total_requests
            , sum(tokens) as total_tokens
        from {tbl}
        where {date_clause}
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Credits", f"{summary['TOTAL_CREDITS'].iloc[0]:.4f}")
    c2.metric("Unique Users", f"{summary['UNIQUE_USERS'].iloc[0]}")
    c3.metric("Total Requests", f"{summary['TOTAL_REQUESTS'].iloc[0]:,}")
    c4.metric("Total Tokens", f"{summary['TOTAL_TOKENS'].iloc[0]:,}")

    st.divider()

    # daily credits
    st.subheader("Daily Credits")
    daily = run_query(f"""
        select usage_date, round(sum(token_credits), 4) as credits
        from {tbl}
        where {date_clause}
        group by 1 order by 1
    """)
    st.line_chart(daily, x="USAGE_DATE", y="CREDITS")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Credits by User")
        by_user = run_query(f"""
            select user_name, round(sum(token_credits), 4) as credits
            from {tbl}
            where {date_clause}
            group by 1 order by 2 desc limit 10
        """)
        st.bar_chart(by_user, x="USER_NAME", y="CREDITS")

    with col2:
        st.subheader("Credits by Model")
        by_model = run_query(f"""
            select model_name, round(sum(token_credits), 4) as credits
            from {tbl}
            where {date_clause}
            group by 1 order by 2 desc limit 10
        """)
        st.bar_chart(by_model, x="MODEL_NAME", y="CREDITS")

    # detailed log
    with st.expander("Usage Detail"):
        detail = run_query(f"""
            select
                usage_date
                , user_name
                , model_name
                , token_credits
                , tokens_input
                , tokens_output
                , tokens_cache_read
            from {tbl}
            where {date_clause}
            order by usage_date desc
            limit 100
        """)
        st.dataframe(detail, hide_index=True, use_container_width=True)
