/*============================================================================================================
 * VpE Episodes Ad-Hoc Analysis
 * Extracted from Therapy_PMPM+VpE_202607_prod.sql
 * Prerequisite: Run the main PMPM+VpE script first (needs kn_mbm_cosmos_csp_nice_claims_vpe_3_202607)
 *===========================================================================================================*/


/*==============================================================================
 * Episodes-level summary
 * For ad-hoc requests to look at prov_tin at the episodes level 
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_episodes_agg_test_202607 as
with first_tin as (
select
	mbi_key
	, national_pilot_flag
	, ep_num
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, row_number() over (partition by mbi_key, national_pilot_flag, ep_num
						 order by visit_id, fst_srvc_dt)
	as rn
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_3_202607
)
,
ep_agg as (
select
	a.mbi_key
	, a.national_pilot_flag
	, a.ep_num
	, a.prov_tin
	, a.market_fnl
	, a.ep_start_dt
	, to_char(a.ep_start_dt, 'yyyymm') as ep_start_month
	, a.mbm_category
	, a.therapy_category
	, a.population
	, b.prov_tin as first_tin
	, b.optum_tin_flag
	, b.rural_tin_flag
	, a.ahrq_diag_dtl_catgy_desc
	, count(distinct concat(a.visit_id, a.fst_srvc_dt)) as n_visits
	, sum(a.allowed) as allowed
	, count(distinct a.prov_tin) as tins_in_episodes
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_3_202607 as a
join first_tin as b
	on a.mbi_key = b.mbi_key
	and a.national_pilot_flag = b.national_pilot_flag
	and a.ep_num = b.ep_num
	and b.rn = 1
group by 
	a.mbi_key
	, a.national_pilot_flag
	, a.ep_num
	, a.prov_tin
	, a.market_fnl
	, a.ep_start_dt
	, to_char(a.ep_start_dt, 'yyyymm')
	, a.mbm_category
	, a.therapy_category
	, a.population
	, b.prov_tin
	, b.optum_tin_flag
	, b.rural_tin_flag
	, a.ahrq_diag_dtl_catgy_desc
)
select * from ep_agg
;

create or replace table tmp_1q.kn_mbm_outlier_202607 as
with ep_2025 as (
select
	population
	, mbi_key
	, cast(first_tin as varchar) as prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, therapy_category as category
	, market_fnl
	, n_visits
	, allowed
from tmp_1q.kn_mbm_episodes_agg_test_202607
where population != 'N/A'
	and ep_start_month >= '202501'
	and therapy_category != 'Other'
)
select
	population
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, category
	, market_fnl
	, count(distinct mbi_key) as unique_member_count
	, count(*) as episode_count
	, sum(n_visits) as visit_count
	, sum(allowed) as allowed
from ep_2025
group by 1,2,3,4,5,6,7
;


/*==============================================================================
 * VpE Tiers in Episodes
 * Separate analysis for Tim, unofficial
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_vpe_aggregated_202607 as
with vpe as (
select
	mbi_key
	, ep_num
	, ep_start_dt
	, ep_hcta_paid_dt
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt) as ep_start_qtr
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
	, sum(allowed) as allowed
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_3_202607
group by
	mbi_key
	, ep_num
	, ep_start_dt
	, ep_hcta_paid_dt
	, to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt)
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
)
select * from vpe;


-- VpE in episodes with percentile categories
create or replace table tmp_1q.kn_mbm_vpe_aggregated_category_mnr_202607 as
with pct_mnr as (
    select
        *
        , percentile_cont(0.25) within group (order by n_visits)
            over (partition by national_pilot_flag, mbm_category) as p25
        , percentile_cont(0.50) within group (order by n_visits)
            over (partition by national_pilot_flag, mbm_category) as p50
        , percentile_cont(0.75) within group (order by n_visits)
            over (partition by national_pilot_flag, mbm_category) as p75
    from tmp_1q.kn_mbm_vpe_aggregated_202607
    where population = 'M&R FFS (excl. DSNP)'
)
select
	*
	, case when n_visits between 0 and 6 then '1 - 6'
		   when n_visits between 7 and 12 then '7 - 12'
		   when n_visits between 13 and 24 then '13 - 24'
		   when n_visits between 25 and 35 then '25 - 35'
		   when n_visits between 36 and 45 then '36 - 45'
		   when n_visits >= 46 then '46+'
		   else ''
	end as vpe_cat1
    , case when n_visits between 1 and 10 then '1 - 10'
           when n_visits between 11 and 20 then '11 - 20'
           when n_visits between 21 and 30 then '21 - 30'
           when n_visits >= 31 then '31+'
           else ''
    end as vpe_cat2
	, case when n_visits > (p75 + 3 * (p75 - p25)) then 'Extreme Outlier'
		   when n_visits > (p75 + 1.5 * (p75 - p25)) then 'Mild Outlier'
		   when n_visits > p75 then 'Above Average'
		   when n_visits > p25 then 'Normal'
		   else 'Below Average'
	end as vpe_cat3
	, 1 as n_episodes
from pct_mnr
;


-- Episodes summary for stacking, to count only episodes
create or replace table tmp_1q.kn_mbm_vpe_with_runout_episodes_202607 as
select
	'EPISODES' as data_type
	, ep_start_month
	, cast(null as varchar) as visit_month
	, cast(null as varchar) as visit_paid_month
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat2 as vpe_buckets_10
	, vpe_cat3 as vpe_buckets_stat
	, 0 as visit_runout_month
	, 0 as visit_ep_runout_month
	, sum(n_episodes) as n_episodes
	, 0 as n_visits
	, 0 as allowed
	, 0 as mm
from tmp_1q.kn_mbm_vpe_aggregated_category_mnr_202607
group by
	ep_start_month
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat2
	, vpe_cat3
;

-- Visit summary for stacking
create or replace table tmp_1q.kn_mbm_vpe_with_runout_visits_202607 as
with visits_dedup as (
select
	mbi_key
	, visit_id
	, fst_srvc_dt
	, fst_srvc_month
	, min(min_hcta_paid_dt) as min_hcta_paid_dt
	, ep_start_dt
	, national_pilot_flag
	, ep_num
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, sum(allowed) as allowed
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_3_202607
where population = 'M&R FFS (excl. DSNP)'
group by
	mbi_key
	, visit_id
	, fst_srvc_dt
	, fst_srvc_month
	, ep_start_dt
	, national_pilot_flag
	, ep_num
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
)
select
	'VISITS' as data_type
	, to_char(a.ep_start_dt, 'yyyyMM') as ep_start_month
	, a.fst_srvc_month as visit_month
	, a.min_hcta_paid_dt as visit_paid_month
	, a.national_pilot_flag
	, a.mbm_category
	, a.therapy_category
	, a.prov_tin
	, a.optum_tin_flag
	, a.rural_tin_flag
	, a.ahrq_diag_dtl_catgy_desc
	, a.market_fnl
	, a.population
	, b.vpe_cat2 as vpe_buckets_10
	, b.vpe_cat3 as vpe_buckets_stat
    , floor((datediff('day', a.fst_srvc_dt, a.min_hcta_paid_dt) + 20) / 30.5) as visit_runout_month
    , floor(datediff('day', a.ep_start_dt, a.fst_srvc_dt) / 30.5) as visit_ep_runout_month
	, 0 as n_episodes
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
	, sum(a.allowed) as allowed
	, count(distinct a.mbi_key) as mm
from visits_dedup as a
join tmp_1q.kn_mbm_vpe_aggregated_category_mnr_202607 as b
	on a.mbi_key = b.mbi_key 
	and a.national_pilot_flag = b.national_pilot_flag
	and a.ep_num = b.ep_num
where a.population = 'M&R FFS (excl. DSNP)'
group by
	to_char(a.ep_start_dt, 'yyyyMM')
	, a.fst_srvc_month
	, a.min_hcta_paid_dt
	, a.national_pilot_flag
	, a.mbm_category
	, a.therapy_category
	, a.prov_tin
	, a.optum_tin_flag
	, a.rural_tin_flag
	, a.ahrq_diag_dtl_catgy_desc
	, a.market_fnl
	, a.population
	, b.vpe_cat2
	, b.vpe_cat3
    , floor((datediff('day', a.fst_srvc_dt, a.min_hcta_paid_dt) + 20) / 30.5)
    , floor(datediff('day', a.ep_start_dt, a.fst_srvc_dt) / 30.5)
;


create or replace table tmp_1q.kn_mbm_vpe_with_runout_visits_episodes_stacked_202607 as
select * from tmp_1q.kn_mbm_vpe_with_runout_visits_202607
union all
select * from tmp_1q.kn_mbm_vpe_with_runout_episodes_202607
;


create or replace table tmp_1q.kn_mbm_vpe_with_runout_summary_202607 as
select
	ep_start_month
	, visit_month
	, visit_paid_month
	, visit_ep_runout_month
	, visit_runout_month
	, vpe_buckets_10
	, vpe_buckets_stat
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, sum(n_visits) as n_visits
	, sum(n_episodes) as n_episodes
	, sum(allowed) as allowed
	, sum(mm) as mm
from tmp_1q.kn_mbm_vpe_with_runout_visits_episodes_stacked_202607
group by
	ep_start_month
	, visit_month
	, visit_paid_month
	, visit_ep_runout_month
	, visit_runout_month
	, vpe_buckets_10
	, vpe_buckets_stat
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
;


create or replace table tmp_1q.kn_mbm_vpe_category_mnr_summmary_202607 as
select	
	ep_start_qtr
	, ep_start_month
	, ep_hcta_paid_dt
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat1
	, vpe_cat2
	, vpe_cat3
	, sum(n_episodes) as n_episodes
	, sum(n_visits) as n_visits
	, sum(allowed) as allowed
from tmp_1q.kn_mbm_vpe_aggregated_category_mnr_202607
group by
	ep_start_qtr
	, ep_start_month
	, ep_hcta_paid_dt
	, national_pilot_flag
	, mbm_category
	, therapy_category
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat1
	, vpe_cat2
	, vpe_cat3
;
