-- Cadence vendor-involvement baseline: feature construction
-- Source: RPM claims (M&R FFS), tre_membership, fraud_scores
-- Output: tmp_1m.knd_rpm_cadence_baseline (7,254 TINs, 38 features + is_cadence flag)

create or replace table tmp_1m.knd_rpm_cadence_baseline as

with latest_membership as (
    select
        fin_mbi_hicn_fnl
        , datediff(
            'month'
            , to_date(orig_eff_month || '01', 'YYYYMMDD')
            , current_date()
        ) as plan_tenure_months
    from fichsrv.tre_membership
    where population = 'M&R FFS'
    qualify row_number() over (
        partition by fin_mbi_hicn_fnl
        order by fin_inc_month desc
    ) = 1
)

, member_tin as (
    select
        d.prov_tin
        , d.mbi
        , datediff('year', max(d.bth_dt), min(d.fst_srvc_dt)) as member_age
        , max(d.gdr_cd) as gdr_cd
        , datediff('month', min(d.fst_srvc_dt), max(d.fst_srvc_dt)) + 1 as rpm_tenure_months
        , count(distinct d.fst_srvc_month) as active_months
        , count(*) as claim_lines
        , sum(d.allw_amt_fnl) as total_allowed
        , max(case when d.dx_flag_final = 'Hypertension' then 1 else 0 end) as has_htn
        , max(case when d.dx_flag_final = 'Diabetes Mellitus' then 1 else 0 end) as has_dm
        , max(case when d.dx_flag_final = 'Heart Failure' then 1 else 0 end) as has_hf
        , max(case when d.dx_flag_final = 'NOT A VALID DX' then 1 else 0 end) as has_invalid_dx
        , sum(case when d.rpm_proc_cat = 'Treatment and Management' then 1 else 0 end) as mgmt_lines
        , sum(case when d.rpm_proc_cat = 'Device Supply' then 1 else 0 end) as device_lines
        , sum(case when d.rpm_proc_cat = 'Education and Setup' then 1 else 0 end) as setup_lines
        , sum(case when d.proc_cd = '99458' then 1 else 0 end) as lines_99458
        , max(m.plan_tenure_months) as plan_tenure_months
    from tmp_1m.kn_rpm_202608_detail as d
    left join latest_membership as m
        on d.mbi = m.fin_mbi_hicn_fnl
    where d.population = 'M&R FFS'
    group by
        d.prov_tin
        , d.mbi
)

, tin_member_agg as (
    select
        prov_tin
        , avg(member_age) as avg_member_age
        , stddev(member_age) as std_member_age
        , avg(case when gdr_cd = 'F' then 1.0 else 0.0 end) as pct_female
        , avg(has_htn * 1.0) as pct_htn
        , avg(has_dm * 1.0) as pct_dm
        , avg(has_hf * 1.0) as pct_hf
        , avg(has_invalid_dx * 1.0) as pct_invalid_dx
        , avg(plan_tenure_months) as avg_plan_tenure_months
        , count(*) as total_mbrs
        , avg(rpm_tenure_months * 1.0) as avg_rpm_tenure_months
        , avg(case when active_months = 1 then 1.0 else 0.0 end) as single_month_churn_rate
        , sum(mgmt_lines) * 1.0 / nullif(sum(claim_lines), 0) as pct_mgmt_lines
        , sum(device_lines) * 1.0 / nullif(sum(claim_lines), 0) as pct_device_lines
        , sum(setup_lines) * 1.0 / nullif(sum(claim_lines), 0) as pct_setup_lines
        , sum(lines_99458) * 1.0 / nullif(sum(claim_lines), 0) as pct_99458
        , sum(total_allowed) / nullif(sum(claim_lines), 0) as avg_allowed_per_line
    from member_tin
    group by prov_tin
)

, monthly_counts as (
    select
        prov_tin
        , fst_srvc_month
        , count(distinct mbi) as mbrs_in_month
    from tmp_1m.kn_rpm_202608_detail
    where population = 'M&R FFS'
    group by
        prov_tin
        , fst_srvc_month
)

, monthly_growth as (
    select
        prov_tin
        , mbrs_in_month
        , lag(mbrs_in_month) over (partition by prov_tin order by fst_srvc_month) as prev_mbrs
    from monthly_counts
)

, tin_growth as (
    select
        prov_tin
        , max(
            case
                when prev_mbrs > 0
                then (mbrs_in_month - prev_mbrs) * 100.0 / prev_mbrs
                else 0
            end
        ) as peak_mom_growth_pct
    from monthly_growth
    group by prov_tin
)

, dom_counts as (
    select
        prov_tin
        , day(fst_srvc_dt) as dom
        , count(*) as dom_cnt
    from tmp_1m.kn_rpm_202608_detail
    where population = 'M&R FFS'
    group by
        prov_tin
        , day(fst_srvc_dt)
)

, dom_shares as (
    select
        prov_tin
        , dom_cnt * 1.0 / sum(dom_cnt) over (partition by prov_tin) as dom_share
    from dom_counts
)

, tin_dom as (
    select
        prov_tin
        , max(dom_share) as top_dom_pct
    from dom_shares
    group by prov_tin
)

, proc_counts as (
    select
        prov_tin
        , proc_cd
        , count(*) as proc_cnt
    from tmp_1m.kn_rpm_202608_detail
    where population = 'M&R FFS'
    group by
        prov_tin
        , proc_cd
)

, proc_shares as (
    select
        prov_tin
        , proc_cnt * 1.0 / sum(proc_cnt) over (partition by prov_tin) as share
    from proc_counts
)

, tin_hhi as (
    select
        prov_tin
        , sum(power(share, 2)) as proc_hhi
    from proc_shares
    group by prov_tin
)

, tin_claim_features as (
    select
        prov_tin
        , count(distinct fst_srvc_month) as months_active
        , count(*) as total_lines
        , avg(case when dayofweekiso(fst_srvc_dt) in (6, 7) then 1.0 else 0.0 end) as weekend_pct
        , avg(datediff('day', fst_srvc_dt, adjd_dt)) as avg_submission_lag
        , count(distinct srvc_prov_npi_fnl) as distinct_srvc_npis
        , avg(case
            when srvc_prov_npi_fnl is null or bil_prov_npi_nbr is null then null
            when srvc_prov_npi_fnl != bil_prov_npi_nbr then 1.0
            else 0.0
        end) as pct_srvc_ne_bil
    from tmp_1m.kn_rpm_202608_detail
    where population = 'M&R FFS'
    group by prov_tin
)

select
    a.prov_tin
    -- provider identification
    , s.provider_name
    , s.provider_group_name
    , s.hospital_group
    , s.tadm_prov_spec_cat
    , s.provider_state
    , s.provider_zip
    -- target
    , case when c.prov_tin is not null then 1 else 0 end as is_cadence
    -- A. member profile from claims (7)
    , a.avg_member_age
    , a.std_member_age
    , a.pct_female
    , a.pct_htn
    , a.pct_dm
    , a.pct_hf
    , a.pct_invalid_dx
    -- B. membership (1)
    , a.avg_plan_tenure_months
    -- C. enrollment dynamics (5)
    , a.total_mbrs
    , cf.months_active
    , a.avg_rpm_tenure_months
    , a.single_month_churn_rate
    , coalesce(g.peak_mom_growth_pct, 0) as peak_mom_growth_pct
    -- D. billing behavior (10)
    , a.pct_mgmt_lines
    , a.pct_device_lines
    , a.pct_setup_lines
    , a.pct_99458
    , h.proc_hhi
    , a.avg_allowed_per_line
    , cf.total_lines * 1.0 / nullif(a.total_mbrs, 0) as avg_lines_per_mbr
    , d.top_dom_pct
    , cf.weekend_pct
    , cf.avg_submission_lag
    -- E. provider structure (3)
    , cf.distinct_srvc_npis
    , a.total_mbrs * 1.0 / nullif(cf.distinct_srvc_npis, 0) as mbrs_per_srvc_npi
    , cf.pct_srvc_ne_bil
    -- F. continuous metrics from fraud_scores (5)
    , s.pct_no_prior_rel
    , s.pct_no_treatment_mgmt
    , s.pct_multi_practice_overlap
    , coalesce(s.max_jaccard, 0) as max_jaccard
    , coalesce(s.pct_shared_npis, 0) as pct_shared_npis
    -- G. binary signals from fraud_scores (7)
    , s.sig_ramp
    , s.sig_no_prior
    , s.sig_no_treat
    , s.sig_mbr_multi_tin
    , s.sig_billing_conc
    , s.sig_churn
    , s.sig_tin_network
from tin_member_agg as a
inner join tin_claim_features as cf
    on a.prov_tin = cf.prov_tin
left join tin_growth as g
    on a.prov_tin = g.prov_tin
left join tin_dom as d
    on a.prov_tin = d.prov_tin
left join tin_hhi as h
    on a.prov_tin = h.prov_tin
inner join tmp_1m.kn_rpm_202608_fraud_scores as s
    on a.prov_tin = s.prov_tin
left join tmp_1m.kn_rpm_cadence_tin as c
    on a.prov_tin = c.prov_tin
;
