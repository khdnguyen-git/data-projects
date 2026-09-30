-- ============================================================
-- Snowflake Usage Dashboard: materialized tables + nightly task
-- ============================================================


-- ============================================================
-- STEP 1: Initial table creation (run once for backfill)
-- ============================================================

-- Set lookback to 30 days for faster initial backfill
SET SDRP_LOOKBACK_DAYS = 30;

-- 1a. Storage snapshot (point-in-time from INFORMATION_SCHEMA.TABLES)
create or replace table ving_prd_trend_db.tmp_1m.kn_pbi_storage_snapshot as
select
    table_schema
    , table_name
    , table_owner
    , table_type
    , row_count
    , bytes
    , round(bytes / power(1024, 3), 4) as gb
    , round(bytes / power(1024, 4) * 23, 2) as estimated_monthly_cost_usd
    , is_transient
    , is_iceberg
    , is_dynamic
    , is_hybrid
    , clustering_key
    , retention_time
    , created
    , last_altered
    , last_ddl
    , last_ddl_by
    , comment
    , current_date() as snapshot_date
from ving_prd_trend_db.information_schema.tables
where table_schema != 'INFORMATION_SCHEMA';


drop view tmp_1m.kn_pbi_query_activity;

-- 1b. Query activity (slimmed columns, filtered to our DB/WH)
create or replace table ving_prd_trend_db.tmp_1m.kn_pbi_query_activity as
select
    query_id
    , query_text
    , left(query_text, 200) as query_preview
    , database_name
    , schema_name
    , query_type
    , session_id
    , user_name
    , role_name
    , warehouse_name
    , warehouse_size
    , query_tag
    , execution_status
    , error_code
    , error_message
    , start_time
    , end_time
    , date(start_time) as query_date
    , dayname(start_time) as day_of_week
    , hour(start_time) as query_hour
    , date_trunc('week', start_time)::date as query_week
    , date_trunc('month', start_time)::date as query_month
    , round(total_elapsed_time / 1000, 2) as elapsed_sec
    , round(compilation_time / 1000, 2) as compilation_sec
    , round(execution_time / 1000, 2) as execution_sec
    , round(queued_provisioning_time / 1000, 2) as queued_provisioning_sec
    , round(queued_overload_time / 1000, 2) as queued_overload_sec
    , round(transaction_blocked_time / 1000, 2) as blocked_sec
    , bytes_scanned
    , round(bytes_scanned / power(1024, 3), 4) as gb_scanned
    , percentage_scanned_from_cache as cache_hit_pct
    , rows_produced
    , rows_inserted
    , rows_updated
    , rows_deleted
    , round(bytes_spilled_to_local_storage / power(1024, 3), 4) as gb_spilled_local
    , round(bytes_spilled_to_remote_storage / power(1024, 3), 4) as gb_spilled_remote
    , credits_used_cloud_services
from dws_env_devops_db.dws_env_public_gen2_sc.info_db_query_history
where database_name = 'VING_PRD_TREND_DB'
    and warehouse_name = 'VING_PRD_MNR_HCE_DATAINFRA_WH';


drop view tmp_1m.kn_pbi_coco_usage;
-- 1c. CoCo Snowsight usage
create or replace table ving_prd_trend_db.tmp_1m.kn_pbi_coco_usage as
select
    user_id
    , user_name
    , request_id
    , parent_request_id
    , usage_time
    , date(usage_time) as usage_date
    , dayname(usage_time) as day_of_week
    , hour(usage_time) as usage_hour
    , date_trunc('week', usage_time)::date as usage_week
    , date_trunc('month', usage_time)::date as usage_month
    , token_credits
    , tokens
    , coalesce(
        get(object_keys(credits_granular), 0)::string
        , 'unknown'
    ) as model_name
    , coalesce(
        get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'input')::number
        , 0
    ) as tokens_input
    , coalesce(
        get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'output')::number
        , 0
    ) as tokens_output
    , coalesce(
        get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'cache_read_input')::number
        , 0
    ) as tokens_cache_read
    , coalesce(
        get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'cache_write_input')::number
        , 0
    ) as tokens_cache_write
from dws_env_devops_db.dws_env_public_gen2_sc.info_cortex_code_snowsight_usage_history;


-- ============================================================
-- STEP 2: Stage for Streamlit files
-- ============================================================

create or replace stage ving_prd_trend_db.tmp_1m.kn_pbi_usage_stage
    directory = (enable = true);


-- ============================================================
-- STEP 3: Nightly incremental refresh task (midnight CT)
-- ============================================================

create or replace task ving_prd_trend_db.tmp_1m.kn_pbi_refresh_task
    warehouse = 'VING_PRD_MNR_HCE_DATAINFRA_WH'
    schedule = 'USING CRON 0 0 * * * America/Chicago'
as
begin
    -- Storage snapshot: full replace (fast, no DWS dependency)
    create or replace table ving_prd_trend_db.tmp_1m.kn_pbi_storage_snapshot as
    select
        table_schema
        , table_name
        , table_owner
        , table_type
        , row_count
        , bytes
        , round(bytes / power(1024, 3), 4) as gb
        , round(bytes / power(1024, 4) * 23, 2) as estimated_monthly_cost_usd
        , is_transient
        , is_iceberg
        , is_dynamic
        , is_hybrid
        , clustering_key
        , retention_time
        , created
        , last_altered
        , last_ddl
        , last_ddl_by
        , comment
        , current_date() as snapshot_date
    from ving_prd_trend_db.information_schema.tables
    where table_schema != 'INFORMATION_SCHEMA';

    -- Query activity: incremental append (2-day lookback)
    insert into ving_prd_trend_db.tmp_1m.kn_pbi_query_activity
    select
        query_id
        , query_text
        , left(query_text, 200) as query_preview
        , database_name
        , schema_name
        , query_type
        , session_id
        , user_name
        , role_name
        , warehouse_name
        , warehouse_size
        , query_tag
        , execution_status
        , error_code
        , error_message
        , start_time
        , end_time
        , date(start_time) as query_date
        , dayname(start_time) as day_of_week
        , hour(start_time) as query_hour
        , date_trunc('week', start_time)::date as query_week
        , date_trunc('month', start_time)::date as query_month
        , round(total_elapsed_time / 1000, 2) as elapsed_sec
        , round(compilation_time / 1000, 2) as compilation_sec
        , round(execution_time / 1000, 2) as execution_sec
        , round(queued_provisioning_time / 1000, 2) as queued_provisioning_sec
        , round(queued_overload_time / 1000, 2) as queued_overload_sec
        , round(transaction_blocked_time / 1000, 2) as blocked_sec
        , bytes_scanned
        , round(bytes_scanned / power(1024, 3), 4) as gb_scanned
        , percentage_scanned_from_cache as cache_hit_pct
        , rows_produced
        , rows_inserted
        , rows_updated
        , rows_deleted
        , round(bytes_spilled_to_local_storage / power(1024, 3), 4) as gb_spilled_local
        , round(bytes_spilled_to_remote_storage / power(1024, 3), 4) as gb_spilled_remote
        , credits_used_cloud_services
    from dws_env_devops_db.dws_env_public_gen2_sc.info_db_query_history as src
    where src.database_name = 'VING_PRD_TREND_DB'
        and src.warehouse_name = 'VING_PRD_MNR_HCE_DATAINFRA_WH'
        and src.start_time >= dateadd(day, -2, current_timestamp())
        and not exists (
            select 1
            from ving_prd_trend_db.tmp_1m.kn_pbi_query_activity as tgt
            where tgt.query_id = src.query_id
        );

    -- CoCo Snowsight: incremental append (2-day lookback)
    insert into ving_prd_trend_db.tmp_1m.kn_pbi_coco_usage
    select
        user_id
        , user_name
        , request_id
        , parent_request_id
        , usage_time
        , date(usage_time) as usage_date
        , dayname(usage_time) as day_of_week
        , hour(usage_time) as usage_hour
        , date_trunc('week', usage_time)::date as usage_week
        , date_trunc('month', usage_time)::date as usage_month
        , token_credits
        , tokens
        , coalesce(
            get(object_keys(credits_granular), 0)::string
            , 'unknown'
        ) as model_name
        , coalesce(
            get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'input')::number
            , 0
        ) as tokens_input
        , coalesce(
            get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'output')::number
            , 0
        ) as tokens_output
        , coalesce(
            get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'cache_read_input')::number
            , 0
        ) as tokens_cache_read
        , coalesce(
            get(get(tokens_granular, coalesce(get(object_keys(tokens_granular), 0)::string, '')), 'cache_write_input')::number
            , 0
        ) as tokens_cache_write
    from dws_env_devops_db.dws_env_public_gen2_sc.info_cortex_code_snowsight_usage_history as src
    where src.usage_time >= dateadd(day, -2, current_timestamp())
        and not exists (
            select 1
            from ving_prd_trend_db.tmp_1m.kn_pbi_coco_usage as tgt
            where tgt.request_id = src.request_id
        );
end;


-- ============================================================
-- STEP 4: Resume the task
-- ============================================================

alter task ving_prd_trend_db.tmp_1m.kn_pbi_refresh_task resume;


select * from tmp_1m.esb_rpm_cosmos_facets_details