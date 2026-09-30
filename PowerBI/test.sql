-- ============================================================
-- Materialize DWS views as tables for Power BI
-- Initial backfill (run once), then nightly incremental append
-- ============================================================

-- Step 1: Initial table creation (run once, full 90-day backfill)

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
    , role_type
    , warehouse_name
    , warehouse_size
    , warehouse_type
    , cluster_number
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
    , bytes_written
    , bytes_written_to_result
    , rows_produced
    , rows_inserted
    , rows_updated
    , rows_deleted
    , partitions_scanned
    , partitions_total
    , case
        when partitions_total > 0 then round(partitions_scanned / partitions_total * 100, 1)
        else null
    end as partition_scan_pct
    , bytes_spilled_to_local_storage
    , bytes_spilled_to_remote_storage
    , round(bytes_spilled_to_local_storage / power(1024, 3), 4) as gb_spilled_local
    , round(bytes_spilled_to_remote_storage / power(1024, 3), 4) as gb_spilled_remote
    , credits_used_cloud_services
    , is_client_generated_statement
    , query_acceleration_bytes_scanned
    , query_acceleration_partitions_scanned
    , query_acceleration_upper_limit_scale_factor
    , bytes_sent_over_the_network
    , query_retry_time
    , query_retry_cause
    , transaction_id
    , query_hash
    , query_parameterized_hash
    , release_version
from dws_env_devops_db.dws_env_public_gen2_sc.info_db_query_history
where database_name = 'VING_PRD_TREND_DB'
    and warehouse_name = 'VING_PRD_MNR_HCE_DATAINFRA_WH';


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


-- Step 2: Nightly incremental append task (midnight CT)
-- Uses 2-day lookback with NOT EXISTS to catch stragglers without duplicates

create or replace task ving_prd_trend_db.tmp_1m.kn_pbi_refresh_task
    warehouse = 'VING_PRD_MNR_HCE_DATAINFRA_WH'
    schedule = 'USING CRON 0 0 * * * America/Chicago'
as
begin
    -- Append new query activity rows from last 2 days
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
        , role_type
        , warehouse_name
        , warehouse_size
        , warehouse_type
        , cluster_number
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
        , bytes_written
        , bytes_written_to_result
        , rows_produced
        , rows_inserted
        , rows_updated
        , rows_deleted
        , partitions_scanned
        , partitions_total
        , case
            when partitions_total > 0 then round(partitions_scanned / partitions_total * 100, 1)
            else null
        end as partition_scan_pct
        , bytes_spilled_to_local_storage
        , bytes_spilled_to_remote_storage
        , round(bytes_spilled_to_local_storage / power(1024, 3), 4) as gb_spilled_local
        , round(bytes_spilled_to_remote_storage / power(1024, 3), 4) as gb_spilled_remote
        , credits_used_cloud_services
        , is_client_generated_statement
        , query_acceleration_bytes_scanned
        , query_acceleration_partitions_scanned
        , query_acceleration_upper_limit_scale_factor
        , bytes_sent_over_the_network
        , query_retry_time
        , query_retry_cause
        , transaction_id
        , query_hash
        , query_parameterized_hash
        , release_version
    from dws_env_devops_db.dws_env_public_gen2_sc.info_db_query_history as src
    where src.database_name = 'VING_PRD_TREND_DB'
        and src.warehouse_name = 'VING_PRD_MNR_HCE_DATAINFRA_WH'
        and src.start_time >= dateadd(day, -2, current_timestamp())
        and not exists (
            select 1
            from ving_prd_trend_db.tmp_1m.kn_pbi_query_activity as tgt
            where tgt.query_id = src.query_id
        );

    -- Append new CoCo usage rows from last 2 days
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


-- Step 3: Resume the task
alter task ving_prd_trend_db.tmp_1m.kn_pbi_refresh_task resume;
