/*==============================================================================
 * RPM Third-Party Vendor Billing Fraud Analysis
 * Reads from: tmp_1m.kn_rpm_202608_detail
 *
 * Output:
 *   tmp_1m.kn_rpm_202608_fraud_profiles   -- per-TIN behavioral profile (~30 features)
 *   tmp_1m.kn_rpm_202608_fraud_prior_rel  -- member-TIN prior relationship
 *   tmp_1m.kn_rpm_202608_fraud_linkage    -- TIN linkage features
 *==============================================================================*/


/*------------------------------------------------------------------------------
 * Step 1: Provider profile base table
 *------------------------------------------------------------------------------*/

--select * from tmp_1m.kn_rpm_202608_fraud_profiles;

create or replace table tmp_1m.kn_rpm_202608_fraud_profiles as

-- Vendor identification -------------------------------------------------------
-- soft-flag: name contains RPM/telehealth keywords, not evidence of vendor involvement
with rpm_keyword_tins as (
    select distinct tin as prov_tin
    from fichsrv.tadm_glxy_provider
    where clin_nm is not null
        and (
            lower(clin_nm) like any ('%remote patient%', '%rpm%', '%telehealth%', '%telemedicine%', '%virtual care%', '%digital health%', '%connected care%', '%health monitor%')
            or lower(full_nm) like any ('%remote patient%', '%rpm%', '%telehealth%', '%telemedicine%')
        )
)

, prov_dim as (
    select prov_tin, clin_nm, provider_state, provider_zip
    from (
        select
            tin as prov_tin
            , clin_nm
            , st_abbr_cd as provider_state
            , zip_cd as provider_zip
            , count(*) as cnt
        from fichsrv.tadm_glxy_provider
        group by 1, 2, 3, 4
        qualify row_number() over (partition by tin order by cnt desc) = 1
    )
)

-- Monthly time series and ramp ------------------------------------------------
-- Sum mm, bill line, allowed per month per TIN
, monthly as (
    select
        prov_tin
        , fst_srvc_month
        , count(distinct mbi) as mbrs
        , count(*) as lines
        , sum(allw_amt_fnl) as allowed
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2
)
-- Calculate avg mm per month
, monthly_agg as (
    select
        prov_tin
        , avg(mbrs) as avg_mbrs_per_month
        , max(mbrs) as max_mbrs_in_month
    from monthly
    group by 1
)
-- MoM diff in mm, line, allowed per month per TIN
, mom as (
    select
        prov_tin
        , fst_srvc_month
        , mbrs
        , mbrs - lag(mbrs) over (partition by prov_tin order by fst_srvc_month) as net_new
        , case when lag(mbrs) over (partition by prov_tin order by fst_srvc_month) > 0
              then (mbrs - lag(mbrs) over (partition by prov_tin order by fst_srvc_month)) * 100.0
                   / lag(mbrs) over (partition by prov_tin order by fst_srvc_month)
          end as mom_growth_pct
    from monthly
)
-- Find big spikes among MoM growth
, ramp as (
    select
        prov_tin
        , max(mom_growth_pct) as peak_mom_growth_pct
        , max(net_new) as peak_mom_net_new
    from mom
    group by 1
)
-- Rank each TIN's metrics to compute growth ratio
, month_ranked as (
    select
        prov_tin
        , fst_srvc_month
        , mbrs
        , row_number() over (partition by prov_tin order by fst_srvc_month) as rn_asc
        , row_number() over (partition by prov_tin order by fst_srvc_month desc) as rn_desc
        , count(*) over (partition by prov_tin) as total_months
    from monthly
)
-- Compare avg TIN's metrics of last 3 months vs first 3 months 
-- Calculate how much mm grew over their life cycle
, tin_growth as (
    select
        prov_tin
        , avg(case when rn_desc <= 3 then mbrs end)
          / nullif(avg(case when rn_asc <= 3 then mbrs end), 0) as growth_ratio
    from month_ranked
    where total_months >= 6
    group by 1
)
-- Billing patterns ------------------------------------------------------------
-- Find any batch billing (provider usually bills throughout the month)
, dom_dist as (
    select
        prov_tin
        , dayofmonth(fst_srvc_dt) as dom
        , count(*) as dom_lines
        , sum(count(*)) over (partition by prov_tin) as tin_total_lines
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2
)
--
, dom_features as (
    select
        prov_tin
        , max(dom_lines * 1.0 / tin_total_lines) as top_dom_pct
        , -sum((dom_lines * 1.0 / tin_total_lines) * ln(dom_lines * 1.0 / tin_total_lines)) as dom_entropy
    from dom_dist
    group by 1
)

, mbr_months as (
    select
        prov_tin
        , mbi
        , fst_srvc_month
        , count(*) as claims
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2, 3
)

, claims_intensity as (
    select
        prov_tin
        , avg(claims) as avg_claims_per_mbr_month
    from mbr_months
    group by 1
)

, weekend as (
    select
        prov_tin
        , count(case when dayname(fst_srvc_dt) in ('Sat', 'Sun') then 1 end) * 1.0 / count(*) as weekend_pct
    from tmp_1m.kn_rpm_202608_detail
    group by 1
)

-- Coding patterns -------------------------------------------------------------

, coding as (
    select
        prov_tin
        , sum(case when proc_cd = '99458' then 1 else 0 end) * 1.0
          / nullif(sum(case when proc_cd = '99457' then 1 else 0 end), 0) as pct_99458_addon
        , sum(case when proc_cd = '99453' then 1 else 0 end) * 1.0
          / nullif(count(distinct mbi), 0) as setup_ratio
    from tmp_1m.kn_rpm_202608_detail
    group by 1
)

, bundle_base as (
    select
        prov_tin
        , mbi
        , fst_srvc_month
        , count(distinct proc_cd) as n_procs
    from tmp_1m.kn_rpm_202608_detail
    where proc_cd in ('99453', '99454', '99457', '99458')
    group by 1, 2, 3
)

, bundle as (
    select
        prov_tin
        , count(case when n_procs >= 3 then 1 end) * 1.0 / count(*) as pct_full_bundle
    from bundle_base
    group by 1
)

, proc_dist as (
    select
        prov_tin
        , proc_cd
        , count(*) as proc_lines
        , sum(count(*)) over (partition by prov_tin) as tin_total_lines
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2
)

, proc_hhi as (
    select
        prov_tin
        , sum(power(proc_lines * 1.0 / tin_total_lines, 2)) as proc_hhi
    from proc_dist
    group by 1
)

-- NPI features ----------------------------------------------------------------

, npi as (
    select
        prov_tin
        , count(case when srvc_prov_npi_nbr is not null
                      and bil_prov_npi_nbr is not null
                      and srvc_prov_npi_nbr != bil_prov_npi_nbr then 1 end) * 1.0
          / nullif(count(case when srvc_prov_npi_nbr is not null
                               and bil_prov_npi_nbr is not null then 1 end), 0) as pct_srvc_ne_bil
        , count(distinct srvc_prov_npi_nbr) as distinct_srvc_npis
        , count(distinct bil_prov_npi_nbr) as distinct_bil_npis
    from tmp_1m.kn_rpm_202608_detail
    group by 1
)

-- Member churn ----------------------------------------------------------------

, mbr_tenure as (
    select
        prov_tin
        , mbi
        , count(distinct fst_srvc_month) as active_months
        , datediff('month', min(fst_srvc_dt), max(fst_srvc_dt)) as tenure_months
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2
)

, churn as (
    select
        prov_tin
        , median(tenure_months) as median_member_tenure_months
        , avg(tenure_months) as mean_member_tenure_months
        , count(case when active_months = 1 then 1 end) * 1.0 / count(*) as single_month_churn_rate
    from mbr_tenure
    group by 1
)

-- Geographic features ---------------------------------------------------------

, tin_mbrs_with_state as (
    select prov_tin, count(distinct mbi) as total_mbrs_state
    from tmp_1m.kn_rpm_202608_detail
    where fin_state is not null
    group by 1
)

, state_dist as (
    select
        s.prov_tin
        , s.fin_state
        , count(distinct s.mbi) as mbrs_in_state
        , t.total_mbrs_state
    from tmp_1m.kn_rpm_202608_detail as s
    inner join tin_mbrs_with_state as t on s.prov_tin = t.prov_tin
    where s.fin_state is not null
    group by 1, 2, 4
)

, geo_entropy as (
    select
        prov_tin
        , -sum((mbrs_in_state * 1.0 / total_mbrs_state) * ln(mbrs_in_state * 1.0 / total_mbrs_state)) as member_state_entropy
    from state_dist
    group by 1
)

, state_mismatch as (
    select
        s.prov_tin
        , case when p.provider_state is null then null
               else 1.0 - sum(case when s.fin_state = p.provider_state then s.mbrs_in_state else 0 end) * 1.0
                         / nullif(max(s.total_mbrs_state), 0)
          end as pct_member_state_mismatch
    from state_dist as s
    inner join prov_dim as p on s.prov_tin = p.prov_tin
    group by 1, p.provider_state
)

-- Proc sequencing -------------------------------------------------------------

, proc_dates as (
    select
        prov_tin
        , mbi
        , min(case when proc_cd = '99453' then fst_srvc_dt end) as first_setup_dt
        , min(case when proc_cd = '99457' then fst_srvc_dt end) as first_mgmt_dt
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2
)

, sequencing as (
    select
        prov_tin
        , count(case when first_setup_dt is not null and first_mgmt_dt is not null
                      and first_setup_dt = first_mgmt_dt then 1 end) * 1.0
          / nullif(count(case when first_setup_dt is not null
                               and first_mgmt_dt is not null then 1 end), 0) as pct_sameday_setup_mgmt
        , count(case when first_setup_dt is not null and first_mgmt_dt is not null
                      and first_mgmt_dt < first_setup_dt then 1 end) * 1.0
          / nullif(count(case when first_setup_dt is not null
                               and first_mgmt_dt is not null then 1 end), 0) as pct_mgmt_before_setup
    from proc_dates
    group by 1
)

-- Provider names (mode) -------------------------------------------------------

, provider_name_mode as (
    select prov_tin, full_nm as provider_name
    from (
        select prov_tin, full_nm, count(*) as cnt
        from tmp_1m.kn_rpm_202608_detail
        group by 1, 2
        qualify row_number() over (partition by prov_tin order by cnt desc) = 1
    )
)

, provider_group_mode as (
    select prov_tin, provider_group_name
    from (
        select prov_tin, provider_group_name, count(*) as cnt
        from tmp_1m.kn_rpm_202608_detail
        where provider_group_name is not null and provider_group_name != ''
        group by 1, 2
        qualify row_number() over (partition by prov_tin order by cnt desc) = 1
    )
)

-- Base aggregates -------------------------------------------------------------

, base as (
    select
        prov_tin
        , min(hospital_group) as hospital_group
        , min(delegated_entity) as sample_delegated_entity
        , count(distinct mbi) as total_mbrs
        , count(*) as total_lines
        , sum(allw_amt_fnl) as total_allowed
        , sum(net_pd_amt_fnl) as total_net
        , count(distinct fst_srvc_month) as months_active
        , min(fst_srvc_dt) as first_claim_dt
        , max(fst_srvc_dt) as last_claim_dt
        , sum(allw_amt_fnl) / nullif(count(*), 0) as avg_allowed_per_line
    from tmp_1m.kn_rpm_202608_detail
    group by 1
)

, specialty as (
    select prov_tin, cos_prov_spcl_cd as primary_specialty
    from (
        select prov_tin, cos_prov_spcl_cd, count(*) as cnt
        from tmp_1m.kn_rpm_202608_detail
        where cos_prov_spcl_cd is not null
        group by 1, 2
        qualify row_number() over (partition by prov_tin order by cnt desc) = 1
    )
)

, spec_cat as (
    select prov_tin, tadm_prov_spec_cat
    from (
        select prov_tin, tadm_prov_spec_cat, count(*) as cnt
        from tmp_1m.kn_rpm_202608_detail
        where tadm_prov_spec_cat is not null
            and tadm_prov_spec_cat != ''
        group by 1, 2
        qualify row_number() over (partition by prov_tin order by cnt desc) = 1
    )
)

-- Assembly --------------------------------------------------------------------

select
    -- identification
    b.prov_tin
    , pn.provider_name
    , pg.provider_group_name
    , pd.clin_nm
    , b.hospital_group
    , b.sample_delegated_entity
    , case when v.prov_tin is not null then 1 else 0 end as rpm_keyword_flag
    , sp.primary_specialty
    , sc.tadm_prov_spec_cat
    , pd.provider_state
    , pd.provider_zip

    -- volume
    , b.total_mbrs
    , b.total_lines
    , b.total_allowed
    , b.total_net
    , b.months_active
    , ma.avg_mbrs_per_month
    , ma.max_mbrs_in_month
    , b.first_claim_dt
    , b.last_claim_dt

    -- ramp
    , r.peak_mom_growth_pct
    , r.peak_mom_net_new
    , gr.growth_ratio

    -- billing patterns
    , df.top_dom_pct
    , df.dom_entropy
    , ci.avg_claims_per_mbr_month
    , w.weekend_pct

    -- coding patterns
    , cd.pct_99458_addon
    , cd.setup_ratio
    , bu.pct_full_bundle
    , b.avg_allowed_per_line
    , ph.proc_hhi

    -- NPI
    , n.pct_srvc_ne_bil
    , n.distinct_srvc_npis
    , n.distinct_bil_npis
    , b.total_mbrs * 1.0 / nullif(n.distinct_srvc_npis, 0) as mbrs_per_srvc_npi

    -- member churn
    , ch.median_member_tenure_months
    , ch.mean_member_tenure_months
    , ch.single_month_churn_rate

    -- geographic
    , ge.member_state_entropy
    , sm.pct_member_state_mismatch

    -- proc sequencing
    , sq.pct_sameday_setup_mgmt
    , sq.pct_mgmt_before_setup

from base as b
left join provider_name_mode as pn on b.prov_tin = pn.prov_tin
left join provider_group_mode as pg on b.prov_tin = pg.prov_tin
left join rpm_keyword_tins as v on b.prov_tin = v.prov_tin
left join prov_dim as pd on b.prov_tin = pd.prov_tin
left join specialty as sp on b.prov_tin = sp.prov_tin
left join spec_cat as sc on b.prov_tin = sc.prov_tin
left join monthly_agg as ma on b.prov_tin = ma.prov_tin
left join ramp as r on b.prov_tin = r.prov_tin
left join tin_growth as gr on b.prov_tin = gr.prov_tin
left join dom_features as df on b.prov_tin = df.prov_tin
left join claims_intensity as ci on b.prov_tin = ci.prov_tin
left join weekend as w on b.prov_tin = w.prov_tin
left join coding as cd on b.prov_tin = cd.prov_tin
left join bundle as bu on b.prov_tin = bu.prov_tin
left join proc_hhi as ph on b.prov_tin = ph.prov_tin
left join npi as n on b.prov_tin = n.prov_tin
left join churn as ch on b.prov_tin = ch.prov_tin
left join geo_entropy as ge on b.prov_tin = ge.prov_tin
left join state_mismatch as sm on b.prov_tin = sm.prov_tin
left join sequencing as sq on b.prov_tin = sq.prov_tin
;


/*------------------------------------------------------------------------------
 * Step 2: Prior relationship table
 * For each member's first RPM claim at a TIN, check if they had any prior
 * E&M or telehealth claim at the same TIN since Jan 2021.
 * E&M = 99201-99215 (office), 99381-99397 (preventive), 99341-99350 (home)
 * Telehealth = 99441-99443 (phone), 99421-99423 (online)
 * Entities: GLXY_OP_F, GLXY_PR_F, DCSP_OP_F, DCSP_PR_F (no NCE)
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_rpm_202608_fraud_prior_rel as

with first_rpm as (
    select prov_tin, mbi, min(fst_srvc_dt) as first_rpm_dt
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2
)

select
    f.prov_tin
    , f.mbi
    , f.first_rpm_dt
    , case when exists (
        select 1 from fichsrv.glxy_op_f as g
        where g.prov_tin = f.prov_tin and g.gal_mbi_hicn_fnl = f.mbi
            and g.fst_srvc_dt >= '2021-01-01'
            and g.fst_srvc_dt < f.first_rpm_dt
            and (g.proc_cd between '99201' and '99215'
                or g.proc_cd between '99381' and '99397'
                or g.proc_cd between '99341' and '99350'
                or g.proc_cd between '99441' and '99443'
                or g.proc_cd between '99421' and '99423')
    ) or exists (
        select 1 from fichsrv.glxy_pr_f as g
        where g.prov_tin = f.prov_tin and g.gal_mbi_hicn_fnl = f.mbi
            and g.fst_srvc_dt >= '2021-01-01'
            and g.fst_srvc_dt < f.first_rpm_dt
            and (g.proc_cd between '99201' and '99215'
                or g.proc_cd between '99381' and '99397'
                or g.proc_cd between '99341' and '99350'
                or g.proc_cd between '99441' and '99443'
                or g.proc_cd between '99421' and '99423')
    ) or exists (
        select 1 from fichsrv.dcsp_op_f as g
        where g.tin = f.prov_tin and g.gal_mbi_hicn_fnl = f.mbi
            and g.fst_srvc_dt >= '2021-01-01'
            and g.fst_srvc_dt < f.first_rpm_dt
            and (g.proc_cd between '99201' and '99215'
                or g.proc_cd between '99381' and '99397'
                or g.proc_cd between '99341' and '99350'
                or g.proc_cd between '99441' and '99443'
                or g.proc_cd between '99421' and '99423')
    ) or exists (
        select 1 from fichsrv.dcsp_pr_f as g
        where g.tin = f.prov_tin and g.gal_mbi_hicn_fnl = f.mbi
            and g.fst_srvc_dt >= '2021-01-01'
            and g.fst_srvc_dt < f.first_rpm_dt
            and (g.proc_cd between '99201' and '99215'
                or g.proc_cd between '99381' and '99397'
                or g.proc_cd between '99341' and '99350'
                or g.proc_cd between '99441' and '99443'
                or g.proc_cd between '99421' and '99423')
    ) then 1 else 0 end as has_prior_em_claim
from first_rpm as f
;

-- Join pct_no_prior_rel back to profile table
alter table tmp_1m.kn_rpm_202608_fraud_profiles add column pct_no_prior_rel float;

merge into tmp_1m.kn_rpm_202608_fraud_profiles as f
using (
    select prov_tin, 1.0 - sum(has_prior_em_claim) * 1.0 / count(*) as pct_no_prior_rel
    from tmp_1m.kn_rpm_202608_fraud_prior_rel
    group by 1
) as p
on f.prov_tin = p.prov_tin
when matched then update set f.pct_no_prior_rel = p.pct_no_prior_rel;


-- Treatment management: % of TIN's RPM enrollees who never received treatment
-- management (99457/99458/99470/99091/G0322) from ANY TIN during the period.
alter table tmp_1m.kn_rpm_202608_fraud_profiles add column pct_no_treatment_mgmt float;

merge into tmp_1m.kn_rpm_202608_fraud_profiles as f
using (
    with mbr_tin as (
        select distinct prov_tin, mbi
        from tmp_1m.kn_rpm_202608_detail
    )
    , mbr_has_mgmt as (
        select distinct mbi
        from tmp_1m.kn_rpm_202608_detail
        where rpm_proc_cat = 'Treatment and Management'
    )
    select
        mt.prov_tin
        , sum(case when m.mbi is null then 1 else 0 end) * 1.0 / count(*) as pct_no_treatment_mgmt
    from mbr_tin as mt
    left join mbr_has_mgmt as m on mt.mbi = m.mbi
    group by 1
) as t
on f.prov_tin = t.prov_tin
when matched then update set f.pct_no_treatment_mgmt = t.pct_no_treatment_mgmt;


-- Multi-practice overlap: % of TIN's enrollees who also received RPM from 2+ other TINs.
alter table tmp_1m.kn_rpm_202608_fraud_profiles add column pct_multi_practice_overlap float;

merge into tmp_1m.kn_rpm_202608_fraud_profiles as f
using (
    with mbr_tin_count as (
        select mbi, count(distinct prov_tin) as n_tins
        from tmp_1m.kn_rpm_202608_detail
        group by 1
    )
    , mbr_tin as (
        select distinct prov_tin, mbi
        from tmp_1m.kn_rpm_202608_detail
    )
    select
        mt.prov_tin
        , sum(case when mc.n_tins >= 3 then 1 else 0 end) * 1.0 / count(*) as pct_multi_practice_overlap
    from mbr_tin as mt
    inner join mbr_tin_count as mc on mt.mbi = mc.mbi
    group by 1
) as t
on f.prov_tin = t.prov_tin
when matched then update set f.pct_multi_practice_overlap = t.pct_multi_practice_overlap;


/*------------------------------------------------------------------------------
 * Step 3: TIN linkage features
 * Detect "same vendor, multiple TINs" via NPI sharing, address clustering,
 * and member overlap (Jaccard similarity).
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_rpm_202608_fraud_linkage as

with npi_tin_map as (
    select
        srvc_prov_npi_nbr
        , prov_tin
        , count(distinct mbi) as mbrs
    from tmp_1m.kn_rpm_202608_detail
    where srvc_prov_npi_nbr is not null
    group by 1, 2
)

, npi_tin_count as (
    select srvc_prov_npi_nbr, count(distinct prov_tin) as n_tins
    from npi_tin_map
    group by 1
)

, npi_sharing as (
    select
        m.prov_tin
        , count(distinct m.srvc_prov_npi_nbr) as total_npis_at_tin
        , count(distinct case when c.n_tins >= 2 then m.srvc_prov_npi_nbr end) as shared_npis
        , max(c.n_tins) as max_npi_tin_count
    from npi_tin_map as m
    inner join npi_tin_count as c on m.srvc_prov_npi_nbr = c.srvc_prov_npi_nbr
    group by 1
)

, tin_zips as (
    select prov_tin, provider_zip, total_mbrs
    from tmp_1m.kn_rpm_202608_fraud_profiles
    where provider_zip is not null
)

, zip_tin_count as (
    select
        provider_zip
        , count(distinct prov_tin) as tins_at_zip
        , sum(total_mbrs) as combined_mbrs_at_zip
    from tin_zips
    group by 1
)

, address_cluster as (
    select
        z.prov_tin
        , zc.tins_at_zip - 1 as colocated_tin_count
        , zc.combined_mbrs_at_zip - z.total_mbrs as colocated_tin_combined_mbrs
    from tin_zips as z
    inner join zip_tin_count as zc on z.provider_zip = zc.provider_zip
)

, tin_members as (
    select prov_tin, mbi
    from tmp_1m.kn_rpm_202608_detail
    group by 1, 2
)

, tin_mbr_count as (
    select prov_tin, count(*) as mbr_count
    from tin_members
    group by 1
)

, overlap_pairs as (
    select
        a.prov_tin as tin_a
        , b.prov_tin as tin_b
        , count(*) as shared_mbrs
    from tin_members as a
    inner join tin_members as b
        on a.mbi = b.mbi
        and a.prov_tin < b.prov_tin
    group by 1, 2
    having count(*) >= 5
)

, jaccard as (
    select
        o.tin_a
        , o.tin_b
        , o.shared_mbrs
        , o.shared_mbrs * 1.0 / (ca.mbr_count + cb.mbr_count - o.shared_mbrs) as jaccard_sim
    from overlap_pairs as o
    inner join tin_mbr_count as ca on o.tin_a = ca.prov_tin
    inner join tin_mbr_count as cb on o.tin_b = cb.prov_tin
)

, mbr_overlap as (
    select
        prov_tin
        , max(jaccard_sim) as max_jaccard
        , count(*) as linked_tin_count
        , sum(partner_mbrs) as linked_tin_combined_mbrs
    from (
        select tin_a as prov_tin, jaccard_sim, cb.mbr_count as partner_mbrs
        from jaccard as j
        inner join tin_mbr_count as cb on j.tin_b = cb.prov_tin
        where jaccard_sim >= 0.01

        union all

        select tin_b as prov_tin, jaccard_sim, ca.mbr_count as partner_mbrs
        from jaccard as j
        inner join tin_mbr_count as ca on j.tin_a = ca.prov_tin
        where jaccard_sim >= 0.01
    )
    group by 1
)

select
    f.prov_tin
    , f.tadm_prov_spec_cat
    , ns.total_npis_at_tin
    , ns.shared_npis
    , ns.shared_npis * 1.0 / nullif(ns.total_npis_at_tin, 0) as pct_shared_npis
    , ns.max_npi_tin_count
    , coalesce(ac.colocated_tin_count, 0) as colocated_tin_count
    , coalesce(ac.colocated_tin_combined_mbrs, 0) as colocated_tin_combined_mbrs
    , mo.max_jaccard
    , coalesce(mo.linked_tin_count, 0) as linked_tin_count
    , coalesce(mo.linked_tin_combined_mbrs, 0) as linked_tin_combined_mbrs
from tmp_1m.kn_rpm_202608_fraud_profiles as f
left join npi_sharing as ns on f.prov_tin = ns.prov_tin
left join address_cluster as ac on f.prov_tin = ac.prov_tin
left join mbr_overlap as mo on f.prov_tin = mo.prov_tin
;


/*------------------------------------------------------------------------------
 * Step 4: Composite investigation-priority scoring
 * Thresholds: p95 computed from TINs with 25+ members only.
 * 9 signal categories, tier by count: High (3+), Medium (2), Low (1), Clean (0).
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_rpm_202608_fraud_scores as

with thresholds as (
    select
        percentile_cont(0.95) within group (order by pct_no_prior_rel) as p95_no_prior
        , percentile_cont(0.95) within group (order by peak_mom_growth_pct) as p95_ramp
        , percentile_cont(0.95) within group (order by peak_mom_net_new) as p95_ramp_net
        , percentile_cont(0.95) within group (order by top_dom_pct) as p95_dom
        , percentile_cont(0.95) within group (order by weekend_pct) as p95_weekend
        , percentile_cont(0.95) within group (order by pct_99458_addon) as p95_addon
        , percentile_cont(0.95) within group (order by proc_hhi) as p95_hhi
        , percentile_cont(0.95) within group (order by single_month_churn_rate) as p95_churn
        , percentile_cont(0.95) within group (order by pct_sameday_setup_mgmt) as p95_sameday
        , percentile_cont(0.90) within group (order by pct_mgmt_before_setup) as p90_mgmt_before
        , percentile_cont(0.95) within group (order by pct_no_treatment_mgmt) as p95_no_treatment
        , percentile_cont(0.95) within group (order by pct_multi_practice_overlap) as p95_multi_practice
    from tmp_1m.kn_rpm_202608_fraud_profiles
    where total_mbrs >= 25
)

, link_thresholds as (
    select
        percentile_cont(0.95) within group (order by l.max_jaccard) as p95_jaccard
        , percentile_cont(0.95) within group (order by l.pct_shared_npis) as p95_shared
    from tmp_1m.kn_rpm_202608_fraud_linkage as l
    inner join tmp_1m.kn_rpm_202608_fraud_profiles as p on l.prov_tin = p.prov_tin
    where p.total_mbrs >= 25
)

, scored as (
    select
        p.*
        , l.total_npis_at_tin
        , l.shared_npis
        , l.pct_shared_npis
        , l.max_npi_tin_count
        , l.colocated_tin_count
        , l.colocated_tin_combined_mbrs
        , l.max_jaccard
        , l.linked_tin_count
        , l.linked_tin_combined_mbrs

        -- sig_no_prior: % of TIN's RPM members with no prior E&M/telehealth
        -- claim at this TIN since Jan 2021. Enrolling strangers, not existing patients.
        , case when p.pct_no_prior_rel >= t.p95_no_prior then 1 else 0 end as sig_no_prior

        -- sig_ramp: largest single month-over-month enrollment spike (pct or absolute).
        -- Sudden volume explosion = newly stood-up billing operation.
        , case when p.peak_mom_growth_pct >= t.p95_ramp
               or p.peak_mom_net_new >= t.p95_ramp_net then 1 else 0 end as sig_ramp

        -- sig_billing_conc: claims clustered on one calendar day OR heavy weekend billing.
        -- Batch/automated billing, not real clinical encounters.
        , case when p.top_dom_pct >= t.p95_dom or p.weekend_pct >= t.p95_weekend then 1 else 0 end as sig_billing_conc

        -- sig_cpt_bundle: high 99458 add-on rate relative to 99457 base, OR narrow
        -- procedure mix (HHI). Upcoding or templated billing (same codes for everyone).
        , case when p.pct_99458_addon >= t.p95_addon or p.proc_hhi >= t.p95_hhi then 1 else 0 end as sig_cpt_bundle

        -- sig_churn: most members active for only 1 month.
        -- Enroll, bill setup, move on. Not providing ongoing monitoring.
        , case when p.single_month_churn_rate >= t.p95_churn then 1 else 0 end as sig_churn

        -- sig_bill_order: setup (99453) and management (99457) billed same day,
        -- or management before setup. Clinically implausible: can't have 20 min
        -- of monitoring data on the day the device is set up.
        , case when p.pct_sameday_setup_mgmt >= t.p95_sameday
                 or coalesce(p.pct_mgmt_before_setup, 0) >= t.p90_mgmt_before then 1 else 0 end as sig_bill_order

        -- sig_tin_network: high Jaccard member overlap with another TIN, OR high %
        -- of shared servicing NPIs. Same vendor operating multiple TIN shells.
        , case when coalesce(l.max_jaccard, 0) >= lt.p95_jaccard
                 or coalesce(l.pct_shared_npis, 0) >= lt.p95_shared then 1 else 0 end as sig_tin_network

        -- sig_no_treat (OIG measure 3): % of TIN's RPM members who never
        -- received treatment management (99457/99458) from ANY TIN. Billing for
        -- device supply without clinical follow-through.
        , case when p.pct_no_treatment_mgmt >= t.p95_no_treatment then 1 else 0 end as sig_no_treat

        -- sig_mbr_multi_tin (OIG measure 4): % of TIN's members also
        -- receiving RPM from 2+ other TINs (n_tins >= 3). Same member billed
        -- by multiple practices simultaneously.
        , case when p.pct_multi_practice_overlap >= t.p95_multi_practice then 1 else 0 end as sig_mbr_multi_tin
    from tmp_1m.kn_rpm_202608_fraud_profiles as p
    left join tmp_1m.kn_rpm_202608_fraud_linkage as l on p.prov_tin = l.prov_tin
    cross join thresholds as t
    cross join link_thresholds as lt
)

, with_totals as (
    select
        s.*
        , (sig_no_prior + sig_ramp + sig_billing_conc + sig_cpt_bundle
           + sig_churn + sig_bill_order + sig_tin_network
           + sig_no_treat + sig_mbr_multi_tin) as total_signals

        -- 5 independent domains: correlated signals grouped so they don't inflate tier counts.
        -- e.g. billing regularity and coding concentration often co-occur = one concern, not two.
        -- dom_relationship: enrolling strangers or shared across 3+ TINs
        , case when sig_no_prior = 1 or sig_mbr_multi_tin = 1 then 1 else 0 end as dom_relationship
        -- dom_growth: sudden enrollment spike
        , sig_ramp as dom_growth
        -- dom_billing_coding: templated/upcoded/sequenced wrong/no treatment mgmt
        , case when sig_billing_conc = 1 or sig_cpt_bundle = 1 or sig_bill_order = 1 or sig_no_treat = 1 then 1 else 0 end as dom_billing_coding
        -- dom_retention: high churn
        , sig_churn as dom_retention
        -- dom_network: TIN linked to other TINs via shared NPIs or member overlap
        , sig_tin_network as dom_network
    from scored as s
)

, with_domains as (
    select
        w.*
        , (dom_relationship + dom_growth + dom_billing_coding
           + dom_retention + dom_network) as total_domains
    from with_totals as w
)

select
    d.*
    , case when d.total_signals >= 3 then '3+ flags'
           when d.total_signals = 2 then '2 flags'
           when d.total_signals = 1 then '1 flag'
           else '0 flags'
      end as flag_category
    , case when d.total_domains >= 4 then '4+ domains'
           when d.total_domains >= 2 then '2-3 domains'
           when d.total_domains = 1 then '1 domain'
           else '0 domains'
      end as flag_category_domain
from with_domains as d
;


/*------------------------------------------------------------------------------
 * QA
 *------------------------------------------------------------------------------*/

-- Profile table row count
select count(*) as row_count, sum(vendor_name_flag) as vendor_flagged
from tmp_1m.kn_rpm_202608_fraud_profiles;

-- Tier distribution
select priority_tier, count(*) as tins, sum(total_mbrs) as total_mbrs, round(sum(total_allowed)) as total_allowed
from tmp_1m.kn_rpm_202608_fraud_scores
group by 1
order by case priority_tier when 'High' then 1 when 'Medium' then 2 when 'Low' then 3 else 4 end;

-- Vendor calibration
select prov_tin, provider_name, provider_group_name, total_mbrs, total_signals, priority_tier
from tmp_1m.kn_rpm_202608_fraud_scores
where vendor_name_flag = 1
order by total_mbrs desc;


-- Does grouping by specialty reduce within-group variance
-- For each metric, compare global vs within-specialty variance
-- If avg within-specialty stddev ≈ global stddev, specialty isn't helping
select
    'pct_no_prior_rel' as metric
    , stddev(pct_no_prior_rel) as global_std
    , avg(grp_std) as avg_within_group_std
    , 1 - avg(grp_std) / nullif(stddev(pct_no_prior_rel), 0) as variance_reduction
from tmp_1m.kn_rpm_202608_fraud_scores as f
left join (
    select primary_specialty, stddev(pct_no_prior_rel) as grp_std
    from tmp_1m.kn_rpm_202608_fraud_scores
    where total_mbrs >= 25
    group by 1
    having count(*) >= 30
) as g on f.primary_specialty = g.primary_specialty
where total_mbrs >= 25
;