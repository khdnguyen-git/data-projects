/*==============================================================================
 * Checking imported row count
 *==============================================================================*/


select count(*) from tmp_1q.opr_mr_auth_202409_20260505;

select * from tmp_1q.opr_mr_auth_202409_20260505
limit 5;

/*==============================================================================
 * Format variables
 *==============================================================================*/
create or replace table tmp_1q.opr_mr_auth_202409_20260505_formatted as
select distinct
    authnumber as auth_id
    , cast(to_date(datereceived, 'DDMONYYYY":" HH24:MI:SS.FF3') as date) as date_received_dt
    , to_char(to_date(datereceived, 'DDMONYYYY":" HH24:MI:SS.FF3'), 'yyyymm') as date_received_mth
    , cast(to_date(datereviewed, 'DDMONYYYY":" HH24:MI:SS.FF3') as date) as date_reviewed_dt
    , to_char(to_date(datereviewed, 'DDMONYYYY":" HH24:MI:SS.FF3'), 'yyyymm') as date_reviewed_mth
    , cast(to_date(reqstart, 'DDMONYYYY":" HH24:MI:SS.FF3') as date) as req_start_dt
    , to_char(to_date(reqstart, 'DDMONYYYY":" HH24:MI:SS.FF3'), 'yyyymm') as req_start_mth
    , cast(to_date(reqend, 'DDMONYYYY":" HH24:MI:SS.FF3') as date) as req_end_dt
    , to_char(to_date(reqend, 'DDMONYYYY":" HH24:MI:SS.FF3'), 'yyyymm') as req_end_mth
    , cast(to_date(authstart, 'DDMONYYYY":" HH24:MI:SS.FF3') as date) as auth_start_dt
    , to_char(to_date(authstart, 'DDMONYYYY":" HH24:MI:SS.FF3'), 'yyyymm') as auth_start_mth
    , cast(to_date(authend, 'DDMONYYYY":" HH24:MI:SS.FF3') as date) as auth_end_dt
    , to_char(to_date(authend, 'DDMONYYYY":" HH24:MI:SS.FF3'), 'yyyymm') as auth_end_mth
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
from tmp_1q.opr_mr_auth_202409_20260505
;

-- row count + distinct patient count
select count(*), count(distinct patient_id) from tmp_1q.opr_mr_auth_202409_20260505_formatted;

-- null check on key columns
select count(*) from tmp_1q.opr_mr_auth_202409_20260505_formatted where patient_id is null or prov_tin is null;


/*==============================================================================
 * Request episode dedup and aggregation (v11 pattern)
 *==============================================================================*/
create or replace table tmp_1q.kn_opr_auth_request_check_v11 as
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
    from tmp_1q.opr_mr_auth_202409_20260505_formatted
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
select count(*), count(distinct patient_id) from tmp_1q.kn_opr_auth_request_check_v11;


/*==============================================================================
 * Join with TRE
 *==============================================================================*/
create or replace table tmp_1q.opr_mr_auth_202409_20260505_join_tre as
with joined as (
select distinct
    a.*
    , case when b.gal_sbscr_nbr is null then 'No Match'
        else 'Match'
    end as match_flag
    , case when b.global_cap = 'NA' or nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN') then 'FFS'
        else 'Not FFS'
    end as ffs_flag
    , b.fin_mbi_hicn_fnl as mbi
    , b.fin_inc_month
    , b.fin_inc_year
    , b.migration_source
    , b.fin_brand
    , b.fin_source_name
    , b.sgr_source_name
    , b.nce_tadm_dec_risk_type
    , b.tfm_include_flag
    , b.global_cap
    , b.fin_market
    , b.fin_plan_level_2
    , b.fin_product_level_3
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_member_cnt
    , case when b.fin_brand = 'M&R' and b.global_cap = 'NA' and b.sgr_source_name = 'COSMOS' and b.fin_product_level_3 != 'INSTITUTIONAL' and b.tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
    , case when b.fin_brand = 'M&R' and b.sgr_source_name = 'NICE' and b.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
    , case when (b.fin_brand = 'M&R' and b.global_cap = 'NA' and b.sgr_source_name = 'COSMOS' and b.fin_product_level_3 != 'INSTITUTIONAL' and b.tfm_include_flag = 1)
        or (b.fin_brand = 'M&R' and b.sgr_source_name = 'NICE' and b.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')) then 1 else 0 end as MnR_FFS_FLAG
    , case when b.fin_brand = 'M&R' and b.fin_product_level_3 = 'DUAL' then 1 else 0 end as MnR_Dual_flag
    , case when b.sgr_source_name in ('COSMOS', 'CSP')
        and b.global_cap = 'NA'
        and (
            (b.fin_brand = 'C&S' and b.migration_source != 'OAH' and b.fin_product_level_3 = 'DUAL')
            or (b.fin_brand = 'C&S' and b.fin_inc_year = '2024' and b.migration_source = 'OAH' and b.fin_state = 'MD')
            or (b.fin_brand != 'C&S' and b.fin_inc_year = '2024' and b.migration_source = 'OAH' and b.fin_market = 'MD')
        ) then 1 else 0 end as CnS_Dual_flag
    , case when b.migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
    , case when b.fin_brand = 'M&R' and b.fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as ISNP_flag
from tmp_1q.opr_mr_auth_202409_20260505_formatted as a
left join fichsrv.tre_membership as b
    on a.patient_id = substring(b.gal_sbscr_nbr, 3)
    and a.auth_start_mth = b.fin_inc_month
)
select
    *
    , case when MnR_COSMOS_FFS_Flag = 1 then 'MnR FFS'
        when MnR_NICE_FFS_Flag = 1 then 'MnR FFS'
        when MnR_FFS_FLAG = 1 then 'MnR FFS'
        when MnR_Dual_flag = 1 then 'MnR DSNP'
        when CnS_Dual_flag = 1 then 'CnS DSNP'
        when total_OAH_flag = 'OAH' then 'OAH'
        when ISNP_flag = 1 then 'ISNP'
    end as population
    , count(auth_id) as n_auth_id
    , count(distinct auth_id) as n_distinct_auth_id
from joined
group by all
;


/*==============================================================================
 * Validation
 *==============================================================================*/
select count(*) from tmp_1q.opr_mr_auth_202409_20260505_join_tre;

select match_flag, count(*) from tmp_1q.opr_mr_auth_202409_20260505_join_tre
group by 1;

select ffs_flag, count(*) from tmp_1q.opr_mr_auth_202409_20260505_join_tre
group by 1;

select match_flag, ffs_flag, sum(n_distinct_auth_id) from tmp_1q.opr_mr_auth_202409_20260505_join_tre
group by 1, 2;


select fin_contract_nbr, fin_contractpbp from hce_ops_fnl.hce_adr_avtar_like_25_26_f 
where fin_contract_nbr != fin_contractpbp

select *
from fichsrv.tre_membership
limit 5