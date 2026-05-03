/*==============================================================================
 * OPR Auth Import — Refactored
 *
 * Source:  tmp_1m.opr_mr_auth_202409_20251031_v2
 * Output:  tmp_1m.kn_opr_auth_request_check_v11  (episode-level, for Excel)
 *
 * Steps:
 *   1. Format raw import columns
 *   2. Dedup + aggregate to episode-level (check_v11)
 *==============================================================================*/



/*==============================================================================
 * Step 1: Format raw import columns
 *==============================================================================*/
create or replace table tmp_1m.opr_mr_auth_202409_20251031_v2_formatted as
select distinct
    authnumber as auth_id
    , cast(to_date(datereceived, 'mm/dd/yyyy hh24:mi') as date) as date_received_dt
    , to_char(to_date(datereceived, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as date_received_mth
    , cast(to_date(datereviewed, 'mm/dd/yyyy hh24:mi') as date) as date_reviewed_dt
    , to_char(to_date(datereviewed, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as date_reviewed_mth
    , cast(to_date(reqstart, 'mm/dd/yyyy hh24:mi') as date) as req_start_dt
    , to_char(to_date(reqstart, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as req_start_mth
    , cast(to_date(reqend, 'mm/dd/yyyy hh24:mi') as date) as req_end_dt
    , to_char(to_date(reqend, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as req_end_mth
    , cast(to_date(authstart, 'mm/dd/yyyy hh24:mi') as date) as auth_start_dt
    , to_char(to_date(authstart, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as auth_start_mth
    , cast(to_date(authend, 'mm/dd/yyyy hh24:mi') as date) as auth_end_dt
    , to_char(to_date(authend, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as auth_end_mth
    , tinnumber as prov_tin
    , lpad(tinnumber, 10, '0') as prov_tin_10digit
    , providerspecialty as prov_specialty
    , regexp_replace(healthplanpatientid, '00$', '') as patient_id
    , patientstate as state
    , split_part(groupnumber, '-', 1) as group_number
    , split_part(groupnumber, '-', 2) as site_cd
    , visitreq as visit_req
    , iff(visitreq >= 18, '18+', to_char(visitreq)) as visit_req_cat
    , visitauth as visit_auth
    , iff(visitauth >= 18, '18+', to_char(visitauth)) as visit_auth_cat
    , reviewdecision as review_decision
    , auto_approval as auto_approval_flag
    , upper(diagcode) as diagcode
    , diagcodedesc as diagcode_desc
from tmp_1m.opr_mr_auth_202409_20251031_v2
;

-- validation
select count(*) as row_cnt
    , count(distinct patient_id) as n_patients
from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
;



/*==============================================================================
 * Step 2: Dedup + aggregate to episode level (check_v11)
 *
 * Episode key = patient_id + prov_tin + prov_specialty
 *
 * Logic:
 *   - Dedup exact-duplicate auth rows (same patient/tin/specialty/received/start/end)
 *   - Pull diagcode, review_decision, auto_approval from the earliest row per episode
 *   - Aggregate dates (first/last), visit counts (sum), auth_id counts (distinct)
 *==============================================================================*/
create or replace table tmp_1m.kn_opr_auth_request_check_v11 as
with base as (
    select
        patient_id
        , prov_tin
        , prov_specialty
        , auth_id
        , date_received_dt
        , date_reviewed_dt
        , req_start_dt
        , req_end_dt
        , auth_start_dt
        , auth_end_dt
        , visit_req
        , visit_auth
        , date_received_mth
        , date_reviewed_mth
        , req_start_mth
        , req_end_mth
        , auth_start_mth
        , auth_end_mth
        , state
        , group_number
        , site_cd
        , review_decision
        , auto_approval_flag
        , visit_req_cat
        , visit_auth_cat
        , diagcode
        , diagcode_desc
        , concat_ws('_', patient_id, prov_tin, prov_specialty) as request_episode_v3
    from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
    qualify row_number() over (
        partition by patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, req_end_dt
        order by date_received_dt, req_start_dt
    ) = 1
)
, first_rows as (
    select
        request_episode_v3
        , diagcode
        , diagcode_desc
        , review_decision
        , auto_approval_flag
    from (
        select
            request_episode_v3
            , diagcode
            , diagcode_desc
            , review_decision
            , auto_approval_flag
            , row_number() over (
                partition by request_episode_v3
                order by date_received_dt
            ) as rn
        from base
    )
    where rn = 1
)
, aggregated as (
    select
        a.request_episode_v3
        , min(a.patient_id) as patient_id
        , min(a.prov_tin) as prov_tin
        , min(a.prov_specialty) as prov_specialty
        , min(a.date_received_dt) as first_date_received_dt
        , max(a.date_received_dt) as last_date_received_dt
        , min(a.date_reviewed_dt) as first_date_reviewed_dt
        , max(a.date_reviewed_dt) as last_date_reviewed_dt
        , min(a.req_start_dt) as first_req_start_dt
        , max(a.req_end_dt) as last_req_end_dt
        , min(a.auth_start_dt) as first_auth_start_dt
        , max(a.auth_end_dt) as last_auth_end_dt
        , count(*) as total_requests
        , count(distinct auth_id) as total_auth_ids
        , sum(a.visit_req) as total_visit_req
        , sum(a.visit_auth) as total_visit_auth
        , min(a.state) as state
        , min(a.group_number) as group_number
        , min(a.site_cd) as site_cd
        , b.review_decision
        , b.auto_approval_flag
        , b.diagcode
        , b.diagcode_desc
    from base as a
    left join first_rows as b
        on a.request_episode_v3 = b.request_episode_v3
    group by a.request_episode_v3
        , b.review_decision
        , b.auto_approval_flag
        , b.diagcode
        , b.diagcode_desc
)
select
    a.patient_id
    , a.prov_tin
    , a.prov_specialty
    , a.request_episode_v3
    , a.first_date_received_dt
    , a.last_date_received_dt
    , datediff(day, a.first_date_received_dt, a.last_date_received_dt) as episode_duration
    , a.first_date_reviewed_dt
    , a.last_date_reviewed_dt
    , a.first_req_start_dt
    , a.last_req_end_dt
    , a.first_auth_start_dt
    , a.last_auth_end_dt
    , a.total_requests
    , a.total_auth_ids
    , a.total_visit_req
    , a.total_visit_auth
    , to_char(a.first_date_received_dt, 'yyyymm') as first_date_received_mth
    , to_char(a.last_date_received_dt, 'yyyymm') as last_date_received_mth
    , a.state
    , a.group_number
    , a.site_cd
    , a.review_decision
    , a.auto_approval_flag
    , a.diagcode
    , a.diagcode_desc
from aggregated as a
order by
    a.patient_id
    , a.prov_tin
    , a.first_date_received_dt
;

-- validation
select count(*) as row_cnt
    , count(distinct request_episode_v3) as n_episodes
    , sum(total_requests) as sum_requests
from tmp_1m.kn_opr_auth_request_check_v11
;
