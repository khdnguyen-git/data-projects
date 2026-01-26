/*==============================================================================
 * Checking imported row count
 *==============================================================================*/

select count(*) from tmp_1m.OPR_MR_AUTH_202409_20251031
-- 2576187
-- 655346 + 700451 + 780612 + 439778 = 2576187



select * from tmp_1m.OPR_MR_AUTH_202409_20251031
limit 5;

/*==============================================================================
 * Format variables
 *==============================================================================*/
create or replace table tmp_1m.opr_mr_auth_202409_20251031_formatted as
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
	, upper(diagcode) as diagcode
	, diagcodedesc as diagcode_desc
from tmp_1m.opr_mr_auth_202409_20251031
;


/*==============================================================================
* Checking imported row count for v2
*==============================================================================*/

select count(*) from tmp_1m.OPR_MR_AUTH_202409_20251031

select count(*) from tmp_1m.OPR_MR_AUTH_202409_20251031_v2
-- 2571817
-- 655351 + 700451 + 780471 + 435544 = 2571817


select * from tmp_1m.OPR_MR_AUTH_202409_20251031_v2
limit 5;

/*==============================================================================
 * Format variables
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

select count(*) from tmp_1m.OPR_MR_AUTH_202409_20251031_v2
-- 2571817

select count(*) from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted


select distinct reviewdecision from tmp_1m.OPR_MR_AUTH_202409_20251031_v2













--------------------

create or replace table tmp_1m.kn_opr_auth_request_check as 
with base as (
    select
        patient_id
        , prov_tin
        , req_start_dt
        , req_end_dt
        , auth_start_dt
        , auth_end_dt
        , date_received_dt
        , auth_id
        , group_number
        , site_cd
        , visit_req
        , visit_auth
        , concat_ws('_', patient_id, prov_tin, req_start_dt, visit_req, visit_auth)
        as request_episode
    from tmp_1m.opr_mr_auth_202409_20251031_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode
            order by date_received_dt
        ) as request_rank
    from base
)
, episode_counts as (
    select
        request_episode
        , count(*) as total_requests_in_episode
    from ranked
    group by request_episode
)
select
    r.*
    , e.total_requests_in_episode
from ranked as r
left join episode_counts as e
    on r.request_episode = e.request_episode
order by
    r.patient_id
    , r.prov_tin
    , r.req_start_dt
    , r.auth_start_dt
    , r.date_received_dt
    , r.request_rank
;

create or replace table tmp_1m.kn_opr_auth_request_check_sample as 
select * from tmp_1m.kn_opr_auth_request_check
limit 10000
;




create or replace table tmp_1m.kn_opr_auth_request_check_v2 as
with base as (
    select
        patient_id
        , prov_tin
        , req_start_dt
        , req_end_dt
        , auth_start_dt
        , auth_end_dt
        , date_received_dt
        , auth_id
        , group_number
        , site_cd
        , visit_req
        , visit_auth
        , diagcode
        , review_decision
        , concat_ws('_', patient_id, prov_tin, req_start_dt, visit_req, visit_auth)
            as request_episode
    from tmp_1m.opr_mr_auth_202409_20251031_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode
            order by date_received_dt
        ) as request_rank
    from base
)
, episode_counts as (
    select
        request_episode
        , count(*) as total_requests_in_episode
    from ranked
    group by request_episode
    having count(*) > 1
)
select
    r.*
    , e.total_requests_in_episode
from ranked as r
join episode_counts as e
    on r.request_episode = e.request_episode
order by
    r.patient_id
    , r.prov_tin
    , r.req_start_dt
    , r.auth_start_dt
    , r.date_received_dt
    , r.request_rank
;

select count(*) from tmp_1m.kn_opr_auth_request_check_v2

create or replace table tmp_1m.kn_opr_auth_request_check_v2_sample as 
select * from tmp_1m.kn_opr_auth_request_check_v2
limit 50000
;

select * from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
limit 10

create or replace table tmp_1m.kn_opr_auth_request_check_v2 as
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
 		, visit_req_cat
 		, visit_auth_cat
 		, diagcode
 		, diagcode_desc
        , concat_ws('_', patient_id, prov_tin, date_received_dt)
            as request_episode
    from tmp_1m.opr_mr_auth_202409_20251031_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode
            order by date_received_dt
        ) as request_rank
    from base
)
, episode_counts as (
    select
        request_episode
        , count(*) as total_requests_in_episode
    from ranked
    group by request_episode
)
select
    r.*
    , e.total_requests_in_episode
from ranked as r
join episode_counts as e
    on r.request_episode = e.request_episode
order by
    r.patient_id
    , r.prov_tin
    , r.auth_id
    , r.date_received_dt
    , r.req_start_dt
    , r.auth_start_dt
    , r.request_rank
;


create or replace table tmp_1m.kn_opr_auth_request_check_v3 as
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
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt)
            as request_episode
    from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode
            order by auth_id, date_received_dt, req_start_dt
        ) as request_rank
    from base
)
, episode_counts as (
    select
        request_episode
        , count(*) as total_requests_in_episode
    from ranked
    group by request_episode
)
select
    r.*
    , e.total_requests_in_episode
from ranked as r
join episode_counts as e
    on r.request_episode = e.request_episode
order by
    r.patient_id
    , r.prov_tin
    , r.auth_id
    , r.date_received_dt
    , r.req_start_dt
    , r.auth_start_dt
    , r.request_rank
;


select 
    patient_id
    , prov_tin
    , prov_specialty
    , auth_id
    , date_received_dt
    , req_start_dt
    , visit_req
    , visit_auth
    , request_rank
	, total_requests_in_episode
    , diagcode
from tmp_1m.kn_opr_auth_request_check_v3
where patient_id = '964562929' and prov_tin = '263694972'
and auth_id like '303%'
order by total_requests_in_episode desc, request_rank

select 
    patient_id
    , prov_tin
    , prov_specialty
    , auth_id
    , date_received_dt
    , req_start_dt
    , visit_req
    , visit_auth
    , request_rank
	, total_requests_in_episode
    , diagcode
from tmp_1m.kn_opr_auth_request_check_v3
where patient_id = '986907273' and prov_tin = '371378417'
and auth_id like '299%'
order by total_requests_in_episode desc, request_rank






create or replace table tmp_1m.kn_opr_auth_request_check_v3_sample as 
select * from tmp_1m.kn_opr_auth_request_check_v3
limit 950000
;

select count(distinct request_episode) from tmp_1m.kn_opr_auth_request_check_v2
where total_requests_in_episode > 1
;
-- 340,622

select count(distinct request_episode) from tmp_1m.kn_opr_auth_request_check_v3
where total_requests_in_episode > 1
;
-- 319,010

select count(distinct request_episode) from tmp_1m.kn_opr_auth_request_check_v4
where total_requests_in_episode > 1
;


select 
    patient_id
    , prov_tin
    , prov_specialty
    , auth_id
    , date_received_dt
    , req_start_dt
    , auth_start_dt
    , visit_req
    , visit_auth
    , auto_approval_flag
    , request_rank
	, total_requests_in_episode
    , diagcode
from tmp_1m.kn_opr_auth_request_check_v3
where total_requests_in_episode > 4
order by patient_id, auth_id, request_rank, req_start_dt, auth_start_dt
;



select
    patient_id
    , prov_tin
    , req_start_dt
    , date_received_dt
    , auth_start_dt
	, req_end_dt
    , auth_end_dt
    , auth_id
    , group_number
    , site_cd
    , visit_req
    , visit_auth
    , diagcode
    , review_decision
from tmp_1m.opr_mr_auth_202409_20251031_formatted
where patient_id in ('948644842', '985320159') and prov_tin in ('204178816', '200950417')
order by
    patient_id
    , prov_tin
    , req_start_dt
    , date_received_dt
    , auth_start_dt




create or replace table tmp_1m.kn_opr_auth_request_check_v4 as
with base as (
    select
        patient_id
        , prov_tin
        , prov_specialty
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
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, auth_start_dt)
            as request_episode
    from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode
            order by date_received_dt, req_start_dt
        ) as request_rank
    from base
)
, episode_counts as (
    select
        request_episode
        , count(*) as total_requests_in_episode
    from ranked
    group by request_episode
)
select
    r.*
    , e.total_requests_in_episode
from ranked as r
join episode_counts as e
    on r.request_episode = e.request_episode
order by
    r.patient_id
    , r.prov_tin
    , r.date_received_dt
    , r.req_start_dt
    , r.auth_start_dt
    , r.request_rank
;

create or replace table tmp_1m.kn_opr_auth_request_check_v4_rpt as 
select * from tmp_1m.kn_opr_auth_request_check_v4
where total_requests_in_episode > 1 
;

create or replace table tmp_1m.kn_opr_auth_request_check_v5 as
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
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, auth_start_dt) as request_episode_v1
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt) as request_episode_v2
    from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode_v1
            order by date_received_dt, req_start_dt
        ) as request_rank
    from base
)
, episode_counts as (
    select
        request_episode_v1
        , count(*) as total_requests_in_episode_v1
        , count(*) over (partition by request_episode_v2) as total_requests_in_episode_v2
    from ranked
    group by request_episode_v1, request_episode_v2
)
select
    r.*
    , e.total_requests_in_episode_v1
    , e.total_requests_in_episode_v2
from ranked as r
join episode_counts as e
    on r.request_episode_v1 = e.request_episode_v1
order by
    r.patient_id
    , r.prov_tin
    , r.date_received_dt
    , r.req_start_dt
    , r.auth_start_dt
    , r.request_rank
;



select count(distinct request_episode_v1) from tmp_1m.kn_opr_auth_request_check_v5
where total_requests_in_episode_v1 > 1
union all
select count(distinct request_episode_v2) from tmp_1m.kn_opr_auth_request_check_v5
where total_requests_in_episode_v2 > 1


create or replace table tmp_1m.kn_opr_auth_request_check_v5_rpt as 
select * from tmp_1m.kn_opr_auth_request_check_v5
where total_requests_in_episode_v1 > 1 or total_requests_in_episode_v2 > 1
;

select count(*) from tmp_1m.kn_opr_auth_request_check_v5_rpt




create or replace table tmp_1m.kn_opr_auth_request_check_v6 as
with base as (
    select
        patient_id
        , prov_tin
        , auth_id
        , prov_specialty
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
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, auth_start_dt) as request_episode_v1
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt) as request_episode_v2
    from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode_v1
            order by date_received_dt, req_start_dt
        ) as request_rank_v1
        , row_number() over (
            partition by request_episode_v2
            order by date_received_dt, req_start_dt
        ) as request_rank_v2
        , count(*) over (partition by request_episode_v2) as total_requests_in_episode_v2
        , sum(visit_req) over (partition by request_episode_v2) as total_visit_req_in_episode_v2
        , sum(visit_auth) over (partition by request_episode_v2) as total_visit_auth_in_episode_v2
    from base
)
select
    *
from ranked
order by
    patient_id
    , prov_tin
    , date_received_dt
    , req_start_dt
    , auth_start_dt
    , request_rank_v1
    , request_rank_v2
;


create or replace table tmp_1m.kn_opr_auth_request_check_v6_sample as
select
	patient_id
	, prov_tin
	, auth_id
	, prov_specialty
	, date_received_dt
	, date_reviewed_dt
	, req_start_dt
	, req_end_dt
	, auth_start_dt
	, auth_end_dt
	, visit_req
	, visit_auth
	, request_episode_v1
	, request_rank_v1
	, request_episode_v2
	, total_requests_in_episode_v2
	, total_visit_req_in_episode_v2
	, total_visit_auth_in_episode_v2
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
from tmp_1m.kn_opr_auth_request_check_v6 
limit 500000
;




create or replace table tmp_1m.kn_opr_auth_request_check_v6_dedup as
select * from tmp_1m.kn_opr_auth_request_check_v6
where total_requests_in_episode_v1 = 1





select count(*) from tmp_1m.kn_opr_auth_request_check_v6_dedup


select * from tmp_1m.kn_opr_auth_request_check_v6
where patient_id = '964306015' and prov_tin = '272627934'

	

create or replace table tmp_1m.kn_opr_auth_request_check_v6_sample_send as
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
    , state
    , group_number
    , site_cd
    , review_decision
    , auto_approval_flag
    , diagcode
    , diagcode_desc
from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
where patient_id in ('980622950', '983097627', '980669387', '980003504', '980856437')
order by patient_id, auth_id, date_received_dt, req_start_dt, auth_start_dt
;

select * from tmp_1m.kn_opr_auth_request_check_v6_sample_send






select * from tmp_1m.kn_opr_auth_request_check_v6
where patient_id = '980003504'


select count(distinct auth_id) from tmp_1m.kn_opr_auth_request_check_v6
where total_requests_in_episode_v1 > 1
;

select count(distinct auth_id) from tmp_1m.kn_opr_auth_request_check_v6
where total_requests_in_episode_v2 > 1
;
select count(distinct auth_id) from tmp_1m.kn_opr_auth_request_check_v6

select count(distinct request_episode_v1) from tmp_1m.kn_opr_auth_request_check_v6
-- 



select count(*) from tmp_1m.kn_opr_auth_request_check_v4
where total_requests_in_episode_v1 > 1
-- 2462 


select * from tmp_1m.kn_opr_auth_request_check_v6


create or replace table tmp_1m.kn_opr_auth_request_check_v7 as
with base as (
    select
        patient_id
        , prov_tin
        , auth_id
        , prov_specialty
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
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, auth_start_dt) as request_episode_v1
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt) as request_episode_v2
        , concat_ws('_', patient_id, prov_tin, prov_specialty) as request_episode_v3
    from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
)
, ranked as (
    select
        *
        , row_number() over (
            partition by request_episode_v1
            order by date_received_dt, req_start_dt
        ) as request_rank_v1
        , row_number() over (
            partition by request_episode_v2
            order by date_received_dt, req_start_dt
        ) as request_rank_v2
        , row_number() over (
            partition by request_episode_v3
            order by date_received_dt, req_start_dt
        ) as request_rank_v3
        , count(*) over (partition by request_episode_v2) as total_requests_in_episode_v2
        , count(*) over (partition by request_episode_v3) as total_requests_in_episode_v3
        , sum(visit_req) over (partition by request_episode_v2) as total_visit_req_in_episode_v2
        , sum(visit_req) over (partition by request_episode_v3) as total_visit_req_in_episode_v3
        , sum(visit_auth) over (partition by request_episode_v2) as total_visit_auth_in_episode_v2
        , sum(visit_auth) over (partition by request_episode_v3) as total_visit_auth_in_episode_v3
    from base
)
select
    *
from ranked
where request_rank_v1 = 1
order by
    patient_id
    , prov_tin
    , date_received_dt
    , req_start_dt
    , auth_start_dt
    , request_rank_v2
    , request_rank_v3
;


select count(*) from tmp_1m.kn_opr_auth_request_check_v7


select count(*) from tmp_1m.kn_opr_auth_request_check_v6
where request_rank_v1 = 1



create or replace table tmp_1m.kn_opr_auth_request_check_v7_sample as
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
	, request_episode_v2
	, request_rank_v2
	, total_requests_in_episode_v2
	, total_visit_req_in_episode_v2
	, total_visit_auth_in_episode_v2
	, request_episode_v3
	, request_rank_v3
	, total_requests_in_episode_v3
	, total_visit_req_in_episode_v3
	, total_visit_auth_in_episode_v3
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
from tmp_1m.kn_opr_auth_request_check_v7
limit 500000
;




select * from tmp_1m.kn_opr_auth_request_check_v7 
where patient_id = '008423144' and prov_tin = '841320338'

select * from tmp_1m.kn_opr_auth_request_check_v7 
where patient_id = '004571599' and prov_tin = '930386793'

select * from tmp_1m.kn_opr_auth_request_check_v7 
where patient_id = '007155208' and prov_tin = '823481322'

create or replace table tmp_1m.kn_opr_auth_request_check_v8 as
with base as (
    select
        patient_id
        , prov_tin
        , auth_id
        , prov_specialty
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
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, auth_start_dt) as request_episode_v1
        , concat_ws('_', patient_id, prov_tin, prov_specialty, date_received_dt) as request_episode_v2
        , concat_ws('_', patient_id, prov_tin, prov_specialty) as request_episode_v3
    from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
)
, ranked as (
    select
        *
        , row_number() over (partition by request_episode_v1 order by date_received_dt, req_start_dt) 
        as request_rank_v1
        , row_number() over (partition by request_episode_v2 order by date_received_dt, req_start_dt) 
        as request_rank_v2
        , row_number() over (partition by request_episode_v3 order by date_received_dt, req_start_dt) 
        as request_rank_v3
        , count(*) over (partition by request_episode_v2) as total_requests_in_episode_v2
        , count(*) over (partition by request_episode_v3) as total_requests_in_episode_v3
        , sum(visit_req) over (partition by request_episode_v2) 
        as total_visit_req_in_episode_v2
        , sum(visit_req) over (partition by request_episode_v3) 
        as total_visit_req_in_episode_v3
        , sum(visit_auth) over (partition by request_episode_v2) 
        as total_visit_auth_in_episode_v2
        , sum(visit_auth) over (partition by request_episode_v3) 
        as total_visit_auth_in_episode_v3
        , coalesce(datediff(day, lag(date_received_dt) over (partition by request_episode_v3 order by date_received_dt), date_received_dt), 0)
        as days_since_prev_request_episode_v3
        , coalesce(round(datediff(day, lag(date_received_dt) over (partition by request_episode_v3 order by date_received_dt), date_received_dt) / 30.436875, 1), 0)
        as months_since_prev_request_episode_v3
    from base
)
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
	, request_episode_v2
	, request_rank_v2
	, total_requests_in_episode_v2
	, total_visit_req_in_episode_v2
	, total_visit_auth_in_episode_v2
	, request_episode_v3
	, request_rank_v3
	, total_requests_in_episode_v3
	, total_visit_req_in_episode_v3
	, total_visit_auth_in_episode_v3
	, days_since_prev_request_episode_v3
	, months_since_prev_request_episode_v3
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
from ranked
where request_rank_v1 = 1
order by
    patient_id
    , prov_tin
    , date_received_dt
    , req_start_dt
    , auth_start_dt
    , request_rank_v2
    , request_rank_v3
;


create or replace table tmp_1m.kn_opr_auth_request_check_v8_sample as
select *
from tmp_1m.kn_opr_auth_request_check_v8
limit 252000


select * from tmp_1m.kn_opr_auth_request_check_v8
where patient_id = '008423144' and prov_tin = '841320338'
;
select * from tmp_1m.kn_opr_auth_request_check_v8 
where patient_id = '004571599' and prov_tin = '930386793'
;
select * from tmp_1m.kn_opr_auth_request_check_v8 
where patient_id = '007155208' and prov_tin = '823481322'





select * from tmp_1m.kn_opr_auth_request_check_v9
where patient_id = '901107221' and prov_tin = '571098556'

 
select * from tmp_1m.kn_opr_auth_request_check_v9
where patient_id = '983098456' and prov_tin = '830404371'

select * from tmp_1m.kn_opr_auth_request_check_v9
where patient_id = '901107267' and prov_tin = '522368011'

 

select count(*) from tmp_1m.kn_opr_auth_request_check_v9


create or replace table tmp_1m.kn_opr_auth_request_check_v10_sample as
select * from tmp_1m.kn_opr_auth_request_check_v10
where patient_id like '9011%'
;

create or replace table tmp_1m.kn_opr_auth_request_check_v9_sample as
select * from tmp_1m.kn_opr_auth_request_check_v9
where patient_id like '9011%'
;

create or replace table tmp_1m.kn_opr_auth_request_check_v8_sample as
select * from tmp_1m.kn_opr_auth_request_check_v8
where patient_id like '9011%'
;
select count(*) from tmp_1m.kn_opr_auth_request_check_v9_sample;
select count(*) from tmp_1m.kn_opr_auth_request_check_v8_sample
;

select * from tmp_1m.kn_opr_auth_request_check_v9_sample

select * from tmp_1m.kn_opr_auth_request_check_v8_sample


to_char(to_date(reqstart, 'mm/dd/yyyy hh24:mi'), 'yyyymm')


create or replace table tmp_1m.kn_opr_auth_request_check_v9 as
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
    qualify row_number() over (partition by patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, req_end_dt order by date_received_dt, req_start_dt) = 1
)
, aggregated as (
    select
        request_episode_v3
        , min(patient_id) as patient_id
        , min(prov_tin) as prov_tin
        , min(prov_specialty) as prov_specialty
        , min(date_received_dt) as first_date_received_dt
        , max(date_received_dt) as last_date_received_dt
        , min(date_reviewed_dt) as first_date_reviewed_dt
        , max(date_reviewed_dt) as last_date_reviewed_dt
        , min(req_start_dt) as first_req_start_dt
        , max(req_end_dt) as last_req_end_dt
        , min(auth_start_dt) as first_auth_start_dt
        , max(auth_end_dt) as last_auth_end_dt
        , count(*) as total_requests
        , sum(visit_req) as total_visit_req
        , sum(visit_auth) as total_visit_auth
        , min(state) as state
        , min(group_number) as group_number
        , min(site_cd) as site_cd
        , min(review_decision) as review_decision
        , min(auto_approval_flag) as auto_approval_flag
        , min(diagcode) as diagcode
        , min(diagcode_desc) as diagcode_desc
    from base
    group by request_episode_v3
)
select
    a.patient_id
    , a.prov_tin
    , a.prov_specialty
    , a.first_date_received_dt
    , a.first_date_reviewed_dt
    , a.first_req_start_dt
    , a.last_req_end_dt
    , a.first_auth_start_dt
    , a.last_auth_end_dt
    , a.total_requests
    , a.total_visit_req
    , a.total_visit_auth
    , to_char(a.first_date_received_dt, 'yyyymm') as first_date_received_mth
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



create or replace table tmp_1m.kn_opr_auth_request_check_v10 as
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


select sum(total_requests) from tmp_1m.kn_opr_auth_request_check_v10
;
select sum(total_requests) from tmp_1m.kn_opr_auth_request_check_v9
;

with ordered as (
    select patient_id
         , prov_tin
         , prov_specialty
         , visit_req
         , date_received_dt
         , visit_auth
         , auto_approval_flag
         , lead(visit_req) over (
               partition by patient_id, prov_tin, prov_specialty
               order by date_received_dt, req_start_dt
           ) as next_visit_req
         , datediff(day, date_received_dt, lead(date_received_dt) over (
               partition by patient_id, prov_tin, prov_specialty
               order by date_received_dt, req_start_dt
           )) as gap_date_received_dt
   	from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
    qualify row_number() over (
        partition by patient_id, prov_tin, prov_specialty, date_received_dt, req_start_dt, req_end_dt
        order by date_received_dt, req_start_dt
    ) = 1
)
, comb as (
    select visit_req
         , next_visit_req
         , min(date_received_dt) as first_date_received_dt
         , visit_auth
         , auto_approval_flag
         , count(*) as comb_count
         , round(avg(gap_date_received_dt),0) as avg_gap
    from ordered
    where next_visit_req is not null
    group by visit_req, next_visit_req, visit_auth, auto_approval_flag
)
select visit_req
     , next_visit_req
     , visit_auth
     , first_date_received_mth
     , auto_approval_flag
     , avg_gap
     , comb_count
     , rnk
from (
    select visit_req
         , next_visit_req
         , visit_auth
         , to_char(first_date_received_dt, 'YYYYMM') as first_date_received_mth
         , auto_approval_flag
         , avg_gap
         , comb_count
         , rank() over (
               partition by visit_req
               order by comb_count desc
           ) as rnk
    from comb
) as ranked
where visit_req > 1
  and rnk < 5
order by avg_gap, visit_req, rnk;




select * from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted where visit_req = 49


















/*==============================================================================
 * Join with TRE
 *==============================================================================*/
create or replace table tmp_1m.opr_mr_auth_202409_20251031_join_tre as
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
    , b.fin_plan_level_2
    , b.fin_product_level_3
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_member_cnt
	, case when b.fin_brand = 'M&R' and b.global_cap = 'NA' and b.sgr_source_name = 'COSMOS' and b.fin_product_level_3 <> 'INSTITUTIONAL' and b.tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
	, case when b.fin_brand = 'M&R' and b.sgr_source_name = 'NICE' and b.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (b.fin_brand = 'M&R' and b.global_cap = 'NA' and b.sgr_source_name = 'COSMOS' and b.fin_product_level_3 <> 'INSTITUTIONAL' and b.tfm_include_flag = 1) 
	    or (b.fin_brand = 'M&R' and b.sgr_source_name = 'NICE' and b.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_FFS_FLAG
	, case when b.fin_brand = 'M&R' and b.fin_product_level_3 = 'DUAL' then 1 else 0 end as MnR_Dual_flag
	, case when ((b.fin_brand in ('C&S') and b.migration_source <> 'OAH' and b.global_cap = 'NA' and b.fin_product_level_3 = 'DUAL' and 
	    b.sgr_source_name in ('COSMOS','CSP') and b.fin_state not in ('OK','NC','NM','NV','OH','TX')) or (b.fin_inc_year = '2024' and b.fin_brand in ('C&S')
	    and b.global_cap = 'NA' and b.sgr_source_name in ('COSMOS','CSP') and b.migration_source = 'OAH' and b.fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when b.migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when b.fin_brand = 'M&R' and b.fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as ISNP_flag
from tmp_1m.opr_mr_auth_202409_20251031_formatted as a
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
group by
	all
;
		   

select count(*) from tmp_1m.opr_mr_auth_202409_20251031_join_tre;
-- 2,576,187

select match_flag, count(*) from tmp_1m.opr_mr_auth_202409_20251031_join_tre
group by 1
--Match	2548412
--No Match	27775

select match_flag, sum(n_distinct_event) from tmp_1m.opr_mr_auth_202409_20251031_join_tre
group by 1
--No Match	27766
--Match	2548145

select ffs_flag, count(*) from tmp_1m.opr_mr_auth_202409_20251031_join_tre
group by 1
--FFS	2545833
--Not FFS	30354

select match_flag, ffs_flag, sum(n_distinct_event) from tmp_1m.opr_mr_auth_202409_20251031_join_tre
group by 1,2

--No Match	Not FFS	27766
--Match	Not FFS	2576
--Match	FFS	2545569
;



/*==============================================================================
 * Join with TRE
 *==============================================================================*/
create or replace table tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2 as
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
    , b.fin_plan_level_2
    , b.fin_product_level_3
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_member_cnt
	, case when b.fin_brand = 'M&R' and b.global_cap = 'NA' and b.sgr_source_name = 'COSMOS' and b.fin_product_level_3 <> 'INSTITUTIONAL' and b.tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
	, case when b.fin_brand = 'M&R' and b.sgr_source_name = 'NICE' and b.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (b.fin_brand = 'M&R' and b.global_cap = 'NA' and b.sgr_source_name = 'COSMOS' and b.fin_product_level_3 <> 'INSTITUTIONAL' and b.tfm_include_flag = 1) 
	    or (b.fin_brand = 'M&R' and b.sgr_source_name = 'NICE' and b.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_FFS_FLAG
	, case when b.fin_brand = 'M&R' and b.fin_product_level_3 = 'DUAL' then 1 else 0 end as MnR_Dual_flag
	, case when ((b.fin_brand in ('C&S') and b.migration_source <> 'OAH' and b.global_cap = 'NA' and b.fin_product_level_3 = 'DUAL' and 
	    b.sgr_source_name in ('COSMOS','CSP') and b.fin_state not in ('OK','NC','NM','NV','OH','TX')) or (b.fin_inc_year = '2024' and b.fin_brand in ('C&S')
	    and b.global_cap = 'NA' and b.sgr_source_name in ('COSMOS','CSP') and b.migration_source = 'OAH' and b.fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when b.migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when b.fin_brand = 'M&R' and b.fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as ISNP_flag
from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted as a
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
group by
	all
;

select count(*) from tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2;
-- 2,576,187

select match_flag, count(*) from tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2
group by 1
--Match	2544119
--No Match	27698

select match_flag, sum(n_distinct_auth_id) from tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2
group by 1
--Match	2544119
--No Match	27698

select ffs_flag, count(*) from tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2
group by 1
--FFS	2541541
--Not FFS	30276

select match_flag, ffs_flag, sum(n_distinct_auth_id) from tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2
group by 1,2

--No Match	Not FFS	27698
--Match	FFS	2541541
--Match	Not FFS	2578
;


create or replace table tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2_aa as
select
	*
from tmp_1m.opr_mr_auth_202409_20251031_join_tre_v2
where auto_approval_flag = 'Y'



select 
	*
from hce_ops_fnl.hce_adr_avtar_like_2023_f_1
where subscriber_id like '%93555679%'












                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  