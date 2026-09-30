/*==============================================================================
 * OPTIONAL / UNOFFICIAL — not part of the core RPM pipeline
 *
 * Ad-hoc analyses and one-off requests (Stars, grandfathered members, etc.)
 * built on top of kn_rpm_202608_detail. None of these tables feed downstream
 * fraud analysis or any deliverable. Safe to ignore entirely.
 *
 * Output:
 *   tmp_1m.kn_rpm_202608_summary             -- aggregated by provider/period
 *   tmp_1m.kn_rpm_202608_stars               -- Stars team combined details
 *   tmp_1m.kn_rpm_202608_mbr_totals          -- member event totals
 *   tmp_1m.kn_rpm_202608_tin_totals          -- TIN-level totals
 *   tmp_1m.kn_rpm_202608_1h_2026             -- 1H 2026 member spend
 *   tmp_1m.kn_rpm_202608_2h_2025             -- 2H 2025 member spend
 *   tmp_1m.kn_rpm_202608_new_2026            -- new RPM members in 2026
 *   tmp_1m.kn_rpm_202608_continuous_to_2026  -- continuous members into 2026
 *==============================================================================*/


/*------------------------------------------------------------------------------
 * Summary table
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_rpm_202608_summary as
select
    entity
    , component
    , population
    , prov_tin
    , srvc_prov_npi_fnl
    , tadm_prov_spec_cat
    , hospital_group
    , fin_state
    , fst_srvc_year
    , fst_srvc_month
    , fst_srvc_qtr
    , proc_cd
    , dx_flag_final
    , sum(tadm_units) as units
    , sum(sbmt_chrg_amt) as submitted
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as net
    , count(*) as line_count
    , count(distinct mbi) as distinct_mbrs
from tmp_1m.kn_rpm_202608_detail
group by
    entity
    , component
    , population
    , prov_tin
    , srvc_prov_npi_fnl
    , tadm_prov_spec_cat
    , hospital_group
    , fin_state
    , fst_srvc_year
    , fst_srvc_month
    , fst_srvc_qtr
    , proc_cd
    , dx_flag_final
;


/*------------------------------------------------------------------------------
 * Stars combined details (HF-only DX flag)
 * Excludes ISNP, groups to event level
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_rpm_202608_stars as
select
    population
    , visit_id
    , mbi
    , prov_prtcp_sts_cd
    , prov_tin
    , srvc_prov_npi_fnl
    , tadm_prov_spec_cat
    , full_nm
    , market_fnl
    , fst_srvc_year
    , fst_srvc_month
    , fst_srvc_qtr
    , proc_cd
    , primary_diag_cd
    , case when dx_flag_final = 'Heart Failure' then 'Heart Failure'
           else 'NOT A VALID DX FOR RPM'
      end as dx_final
    , sum(tadm_units) as units
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as net
from tmp_1m.kn_rpm_202608_detail
where population not in ('M&R ISNP', 'N/A')
group by
    population
    , visit_id
    , mbi
    , prov_prtcp_sts_cd
    , prov_tin
    , srvc_prov_npi_fnl
    , tadm_prov_spec_cat
    , full_nm
    , market_fnl
    , fst_srvc_year
    , fst_srvc_month
    , fst_srvc_qtr
    , proc_cd
    , primary_diag_cd
    , case when dx_flag_final = 'Heart Failure' then 'Heart Failure'
           else 'NOT A VALID DX FOR RPM'
      end
;


/*------------------------------------------------------------------------------
 * Member event totals
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_rpm_202608_mbr_totals as
select
    population
    , dx_final
    , fst_srvc_year
    , prov_prtcp_sts_cd
    , proc_cd
    , count(distinct visit_id) as events
    , sum(allowed) as tot_allowed
from tmp_1m.kn_rpm_202608_stars
where fst_srvc_qtr not in ('2026Q3', '2026Q4')
group by
    population
    , dx_final
    , fst_srvc_year
    , prov_prtcp_sts_cd
    , proc_cd
;


/*------------------------------------------------------------------------------
 * TIN-level totals
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_rpm_202608_tin_totals as
select
    population
    , prov_tin
    , full_nm
    , market_fnl
    , fst_srvc_year
    , dx_final
    , sum(units) as tot_units
    , sum(allowed) as tot_allowed
    , sum(net) as tot_net
from tmp_1m.kn_rpm_202608_stars
group by
    population
    , prov_tin
    , full_nm
    , market_fnl
    , fst_srvc_year
    , dx_final
;


/*------------------------------------------------------------------------------
 * Grandfathered member analysis
 *------------------------------------------------------------------------------*/

-- 1H 2026 member spend
create or replace table tmp_1m.kn_rpm_202608_1h_2026 as
select
    population
    , mbi
    , sum(allowed) as total_allowed
from tmp_1m.kn_rpm_202608_stars
where fst_srvc_qtr in ('2026Q1', '2026Q2')
group by
    population
    , mbi
;

-- 2H 2025 member spend
create or replace table tmp_1m.kn_rpm_202608_2h_2025 as
select
    population
    , mbi
    , sum(allowed) as total_allowed
from tmp_1m.kn_rpm_202608_stars
where fst_srvc_qtr in ('2025Q3', '2025Q4')
group by
    population
    , mbi
;

-- New RPM members in 2026 (in 1H 2026 but not in 2H 2025)
create or replace table tmp_1m.kn_rpm_202608_new_2026 as
select
    a.population
    , a.mbi
    , a.total_allowed
from tmp_1m.kn_rpm_202608_1h_2026 as a
left join tmp_1m.kn_rpm_202608_2h_2025 as b
    on a.mbi = b.mbi
where b.mbi is null
;

-- Continuous members into 2026 (in both 1H 2026 and 2H 2025)
create or replace table tmp_1m.kn_rpm_202608_continuous_to_2026 as
select
    a.population
    , a.mbi
    , a.total_allowed as h1_total
    , b.total_allowed as h2_total
from tmp_1m.kn_rpm_202608_1h_2026 as a
inner join tmp_1m.kn_rpm_202608_2h_2025 as b
    on a.mbi = b.mbi
;


/*------------------------------------------------------------------------------
 * QA
 *------------------------------------------------------------------------------*/

select count(*) as row_count, count(distinct mbi) as distinct_mbrs
from tmp_1m.kn_rpm_202608_stars;

select 'new_2026' as cohort, count(*) as mbrs, sum(total_allowed) as allowed
from tmp_1m.kn_rpm_202608_new_2026
union all
select 'continuous', count(*), sum(h1_total) + sum(h2_total)
from tmp_1m.kn_rpm_202608_continuous_to_2026;

select entity, component, sum(allowed) as allowed, sum(net) as net, sum(units) as units
from tmp_1m.kn_rpm_202608_summary
where fst_srvc_month = '202604'
    and component = 'PR'
    and entity in ('COSMOS', 'CSP')
group by 1, 2
order by 1;
