/*==============================================================================
 * OPR Auth Import — Refactored
 *
 * Source:  tmp_1q.opr_mr_auth_20240901_20260505
 * Output:  tmp_1q.kn_opr_auth_request_check_v11  (episode-level, for Excel)
 *
 * Steps:
 *   1. Format raw import columns
 *   2. Dedup + aggregate to episode-level (check_v11)
 *==============================================================================*/



/*==============================================================================
 * Step 1a: Format raw import columns
 *==============================================================================*/
create or replace table tmp_1q.opr_mr_auth_20240901_20260505_formatted as
select distinct
    authnumber as auth_id
    , to_date(datereceived, 'DDMONYYYY:HH24:MI:SS.FF3') as date_received_dt
    , to_char(to_date(datereceived, 'DDMONYYYY:HH24:MI:SS.FF3'), 'yyyymm') as date_received_mth
    , to_date(datereviewed, 'DDMONYYYY:HH24:MI:SS.FF3') as date_reviewed_dt
    , to_char(to_date(datereviewed, 'DDMONYYYY:HH24:MI:SS.FF3'), 'yyyymm') as date_reviewed_mth
    , to_date(reqstart, 'DDMONYYYY:HH24:MI:SS.FF3') as req_start_dt
    , to_char(to_date(reqstart, 'DDMONYYYY:HH24:MI:SS.FF3'), 'yyyymm') as req_start_mth
    , to_date(reqend, 'DDMONYYYY:HH24:MI:SS.FF3') as req_end_dt
    , to_char(to_date(reqend, 'DDMONYYYY:HH24:MI:SS.FF3'), 'yyyymm') as req_end_mth
    , to_date(authstart, 'DDMONYYYY:HH24:MI:SS.FF3') as auth_start_dt
    , to_char(to_date(authstart, 'DDMONYYYY:HH24:MI:SS.FF3'), 'yyyymm') as auth_start_mth
    , to_date(authend, 'DDMONYYYY:HH24:MI:SS.FF3') as auth_end_dt
    , to_char(to_date(authend, 'DDMONYYYY:HH24:MI:SS.FF3'), 'yyyymm') as auth_end_mth
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
from tmp_1q.opr_mr_auth_20240901_20260505
;



select * from 

-- validation
select count(*) as row_cnt
    , count(distinct patient_id) as n_patients
from tmp_1q.opr_mr_auth_20240901_20260505_formatted
;
create or replace table tmp_1q.OPR_MR_AUTH_20240901_20251031_formatted as
select 
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
from tmp_1q.OPR_MR_AUTH_20240901_20251031
;

-- validation
select count(*) as row_cnt
    , count(distinct patient_id) as n_patients
from tmp_1q.OPR_MR_AUTH_20240901_20251031_formatted
;
/*==============================================================================
 * Step 1b: Compare new vs old data
 *==============================================================================*/
select
        coalesce(a.date_received_mth, b.date_received_mth)
  as date_received_mth
        , a.auto_y as auto_approval_y_20251031
        , b.auto_y as auto_approval_y_20260505
        , (b.auto_y - a.auto_y) as diff
        , ((b.auto_y - a.auto_y) / nullif(a.auto_y, 0)) as reldiff
        , c.distinct_auth_ids
        , round((b.auto_y - a.auto_y) / nullif(c.distinct_auth_ids, 0), 4) as diff_over_total_auth
    from (
        select
            date_received_mth
            , count(case when auto_approval_flag = 'Y' then
  1 end) as auto_y
        from tmp_1q.opr_mr_auth_20240901_20251031_formatted
        group by date_received_mth
    ) as a
    full outer join (
        select
            date_received_mth
            , count(case when auto_approval_flag = 'Y' then
  1 end) as auto_y
        from tmp_1q.opr_mr_auth_20240901_20260505_formatted
        group by date_received_mth
    ) as b
        on a.date_received_mth = b.date_received_mth
    left join (
        select
            date_received_mth
            , count(distinct auth_id) as distinct_auth_ids
        from tmp_1q.opr_mr_auth_20240901_20260505_formatted
        group by date_received_mth
    ) as c
        on coalesce(a.date_received_mth,
  b.date_received_mth) = c.date_received_mth
    order by coalesce(a.date_received_mth,
  b.date_received_mth)
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
    from tmp_1q.opr_mr_auth_20240901_20260505_formatted
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
from tmp_1q.kn_opr_auth_request_check_v11
;
/*==============================================================================
 * Step 3: Join with TRE
 *==============================================================================*/

create or replace table tmp_1q.opr_mr_auth_20240901_20260505_join_tre as
with joined as (
select distinct
	a.*
	, case when b.gal_sbscr_nbr is null then 'No Match'
		else 'Match'
	end as match_flag
	, case when b.global_cap = 'NA' or nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 'FFS'
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
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_product_level_3
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_member_cnt
from tmp_1q.opr_mr_auth_20240901_20260505_formatted as a
left join fichsrv.tre_membership as b
  on a.patient_id = substring(b.gal_sbscr_nbr, 3)
  and a.auth_start_mth = b.fin_inc_month
)
select
  *
  , case
      when migration_source = 'OAH'
          and not (fin_brand = 'C&S' and fin_inc_year = '2024' and fin_state = 'MD')
          and not (fin_brand != 'C&S' and fin_inc_year = '2024' and fin_market = 'MD')
      then 'OAH'
      when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
      when sgr_source_name in ('COSMOS', 'NICE')
          and fin_brand = 'M&R'
          and global_cap = 'NA'
          and fin_product_level_3 not in ('DUAL', 'INSTITUTIONAL')
          and tfm_include_flag = 1
      then 'M&R FFS (excl. DSNP)'
      when sgr_source_name in ('COSMOS', 'CSP')
          and global_cap = 'NA'
          and (
              (fin_brand = 'C&S' and migration_source != 'OAH' and fin_product_level_3 = 'DUAL')
              or (fin_brand = 'C&S' and fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')
              or (fin_brand != 'C&S' and fin_inc_year = '2024' and migration_source = 'OAH' and fin_market = 'MD')
          )
      then 'C&S DSNP'
      when fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 'M&R DSNP'
      else 'N/A'
    end as population
	, count(auth_id) as n_auth_id
	, count(distinct auth_id) as n_distinct_auth_id
from joined
where match_flag = 'Match'
group by
	all
;

select match_flag, sum(n_distinct_auth_id) 
from tmp_1q.opr_mr_auth_20240901_20260505_join_tre
group by 1
;

/*==============================================================================
 * Step 4: Summary table
 *==============================================================================*/
create or replace table tmp_1q.opr_mr_auth_20240901_20260505_join_tre_sum as
select
    date_received_mth
    , date_reviewed_mth
    , req_start_mth
    , req_end_mth
    , auth_start_mth
    , auth_end_mth
    , prov_specialty
    , visit_req
    , visit_req_cat
    , visit_auth
    , visit_auth_cat
    , review_decision
    , auto_approval_flag
    , fin_market
    , population
    , sum(fin_member_cnt) as total_member_cnt
    , sum(n_auth_id) as total_auth_id
    , sum(n_distinct_auth_id) as total_distinct_auth_id
from tmp_1q.opr_mr_auth_20240901_20260505_join_tre
group by
    date_received_mth
    , date_reviewed_mth
    , req_start_mth
    , req_end_mth
    , auth_start_mth
    , auth_end_mth
    , prov_specialty
    , visit_req
    , visit_req_cat
    , visit_auth
    , visit_auth_cat
    , review_decision
    , auto_approval_flag
    , fin_market
    , population
;

select
    date_received_mth
    , prov_specialty
    , sum(total_distinct_auth_id) 
from tmp_1q.opr_mr_auth_20240901_20260505_join_tre_sum
group by 1, 2
order by 1, 2
;


select
    date_received_mth
    , sum(case when prov_specialty = 'DC' then total_distinct_auth_id else 0 end)
   as DC
    , sum(case when prov_specialty = 'OT' then total_distinct_auth_id else 0 end)
   as OT
    , sum(case when prov_specialty = 'PT' then total_distinct_auth_id else 0 end)
   as PT
    , sum(case when prov_specialty = 'ST' then total_distinct_auth_id else 0 end)
   as ST
   , sum(total_distinct_auth_id)
from tmp_1q.opr_mr_auth_20240901_20260505_join_tre_sum
group by 1
order by 1
;

select
date_received_mth
, prov_specialty
, count(distinct auth_id)
from tmp_1q.OPR_MR_AUTH_20240901_20251031_formatted
group by 1,2
;



select
    date_received_mth
    , sum(case when prov_specialty = 'DC' then total_distinct_auth_id else 0 end)
   as DC
    , sum(case when prov_specialty = 'OT' then total_distinct_auth_id else 0 end)
   as OT
    , sum(case when prov_specialty = 'PT' then total_distinct_auth_id else 0 end)
   as PT
    , sum(case when prov_specialty = 'ST' then total_distinct_auth_id else 0 end)
   as ST
   , sum(total_distinct_auth_id)
from tmp_1q.opr_mr_auth_20240901_20260505_join_tre_sum
group by 1
order by 1
;


select * from tmp_1q.opr_mr_auth_20240901_20260505_join_tre_sum
limit 100
;


select count(*) from tmp_1q.opr_mr_auth_20240901_20260505_join_tre;


select *
    from (
        select
            date_received_mth
            , prov_specialty
            , auth_id
        from tmp_1q.OPR_MR_AUTH_20240901_20251031_formatted
    )
    pivot (
        count(distinct auth_id)
        for prov_specialty in (any order by prov_specialty)
    )
    order by date_received_mth
;

select *
    from (
        select
            date_received_mth
            , prov_specialty
            , auth_id
        from tmp_1q.OPR_MR_AUTH_20240901_20251031_formatted
    )
    pivot (
        count(distinct auth_id)
        for prov_specialty in (any order by prov_specialty)
    )
    order by date_received_mth
    ;


select *
    from (
        select distinct
            date_received_mth
            , prov_specialty
            , auth_id
        from tmp_1q.OPR_MR_AUTH_20240901_20251031_formatted
    )
    pivot (
        count(auth_id)
        for prov_specialty in (any order by prov_specialty)
    )
    order by date_received_mth
    ;