/*==============================================================================
 * LOC Anomaly Prep — Population-specific pre-aggregated rate tables
 *
 * Creates 3 separate tables, one per population, each with the same
 * 12 dimension blocks + global block. Python reads each table independently
 * and runs a separate Isolation Forest for each population.
 *
 * Run order: this SQL first, then loc_anomaly.py
 *==============================================================================*/


/*==============================================================================
 * M&R FFS  →  kn_loc_mnr_agg_04222026
 * Filter:  mnr_total_ffs_flag = 1
 *==============================================================================*/

select * from tmp_1m.kn_loc_mnr_agg_04222026
;

select *
    from tmp_1m.kn_loc_notif_04222026_od
;


create or replace table tmp_1m.kn_loc_mnr_agg_04222026 as

with base as (
    select *
    from tmp_1m.kn_loc_notif_04222026_od
    where ipa_pac_flag in ('IPA', 'MM')
        and loc_flag = 1
        and mnr_total_ffs_flag = 1
)

-- prov_tin -----------------------------------------------------------------
select
    'prov_tin'                                                          as _dimension
    , coalesce(cast(prov_tin as varchar), 'Unknown')                    as _dim_value
    , sum(case_count)                                                   as case_count
    , sum(membership)                                                   as membership
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)             as adr_rate
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)             as persistent_adr_rate
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)        as persistency
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)             as md_review_rate
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0) as appeal_rate
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0) as appeal_overturn_rate
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0) as p2p_rate
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0) as p2p_overturn_rate
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0) as mcr_reconsideration_rate
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0) as mcr_overturn_rate
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0) as member_appeal_rate
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0) as member_appeal_overturn_rate
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)             as pre_auth_rate
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)            as auth_per_k
from base
group by prov_tin
having sum(case_count) >= 30

union all

-- svc_setting --------------------------------------------------------------
select
    'svc_setting'
    , coalesce(cast(svc_setting as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by svc_setting
having sum(case_count) >= 30

union all

-- fin_market ---------------------------------------------------------------
select
    'fin_market'
    , coalesce(cast(fin_market as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by fin_market
having sum(case_count) >= 30

union all

-- hospital_group -----------------------------------------------------------
select
    'hospital_group'
    , coalesce(cast(hospital_group as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by hospital_group
having sum(case_count) >= 30

union all

-- los_categories -----------------------------------------------------------
select
    'los_categories'
    , coalesce(cast(los_categories as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by los_categories
having sum(case_count) >= 30

union all

-- admit_type ---------------------------------------------------------------
select
    'admit_type'
    , coalesce(cast(admit_type as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by admit_type
having sum(case_count) >= 30

union all

-- ipa_li_split -------------------------------------------------------------
select
    'ipa_li_split'
    , coalesce(cast(ipa_li_split as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by ipa_li_split
having sum(case_count) >= 30

union all

-- ip_type ------------------------------------------------------------------
select
    'ip_type'
    , coalesce(cast(ip_type as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by ip_type
having sum(case_count) >= 30

union all

-- global -------------------------------------------------------------------
select
    'global'
    , concat_ws(' | '
        , coalesce(cast(prov_tin as varchar), '')
        , coalesce(cast(svc_setting as varchar), '')
        , coalesce(cast(fin_market as varchar), '')
        , coalesce(cast(hospital_group as varchar), '')
        , coalesce(cast(los_categories as varchar), '')
        , coalesce(cast(admit_type as varchar), '')
        , coalesce(cast(ipa_li_split as varchar), '')
        , coalesce(cast(ip_type as varchar), '')
      )
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by
    prov_tin, svc_setting, fin_market, hospital_group, los_categories, admit_type, ipa_li_split, ip_type
having sum(case_count) >= 30
;


/*==============================================================================
 * C&S DSNP  →  kn_loc_cns_agg_04222026
 * Filter:  cns_dual_flag = 1
 * Dimensions: prov_tin, svc_setting, fin_market, hospital_group,
 *             los_categories, admit_type, ipa_li_split, ip_type, global
 *==============================================================================*/
create or replace table tmp_1m.kn_loc_cns_agg_04222026 as

with base as (
    select *
    from tmp_1m.kn_loc_notif_04222026_od
    where ipa_pac_flag in ('IPA', 'MM')
        and loc_flag = 1
        and cns_dual_flag = 1
)

-- prov_tin -----------------------------------------------------------------
select
    'prov_tin'                                                          as _dimension
    , coalesce(cast(prov_tin as varchar), 'Unknown')                    as _dim_value
    , sum(case_count)                                                   as case_count
    , sum(membership)                                                   as membership
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)             as adr_rate
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)             as persistent_adr_rate
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)        as persistency
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)             as md_review_rate
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0) as appeal_rate
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0) as appeal_overturn_rate
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0) as p2p_rate
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0) as p2p_overturn_rate
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0) as mcr_reconsideration_rate
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0) as mcr_overturn_rate
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0) as member_appeal_rate
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0) as member_appeal_overturn_rate
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)             as pre_auth_rate
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)            as auth_per_k
from base
group by prov_tin
having sum(case_count) >= 30

union all

-- svc_setting --------------------------------------------------------------
select
    'svc_setting'
    , coalesce(cast(svc_setting as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by svc_setting
having sum(case_count) >= 30

union all

-- fin_market ---------------------------------------------------------------
select
    'fin_market'
    , coalesce(cast(fin_market as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by fin_market
having sum(case_count) >= 30

union all

-- hospital_group -----------------------------------------------------------
select
    'hospital_group'
    , coalesce(cast(hospital_group as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by hospital_group
having sum(case_count) >= 30

union all

-- los_categories -----------------------------------------------------------
select
    'los_categories'
    , coalesce(cast(los_categories as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by los_categories
having sum(case_count) >= 30

union all

-- admit_type ---------------------------------------------------------------
select
    'admit_type'
    , coalesce(cast(admit_type as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by admit_type
having sum(case_count) >= 30

union all

-- ipa_li_split -------------------------------------------------------------
select
    'ipa_li_split'
    , coalesce(cast(ipa_li_split as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by ipa_li_split
having sum(case_count) >= 30

union all

-- ip_type ------------------------------------------------------------------
select
    'ip_type'
    , coalesce(cast(ip_type as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by ip_type
having sum(case_count) >= 30

union all

-- global -------------------------------------------------------------------
select
    'global'
    , concat_ws(' | '
        , coalesce(cast(prov_tin as varchar), '')
        , coalesce(cast(svc_setting as varchar), '')
        , coalesce(cast(fin_market as varchar), '')
        , coalesce(cast(hospital_group as varchar), '')
        , coalesce(cast(los_categories as varchar), '')
        , coalesce(cast(admit_type as varchar), '')
        , coalesce(cast(ipa_li_split as varchar), '')
        , coalesce(cast(ip_type as varchar), '')
      )
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by
    prov_tin, svc_setting, fin_market, hospital_group
    , los_categories, admit_type, ipa_li_split, ip_type
having sum(case_count) >= 30
;


/*==============================================================================
 * OAH  →  kn_loc_oah_agg_04222026
 * Filter:  total_oah_flag = 'OAH'
 * Dimensions: prov_tin, svc_setting, fin_market, hospital_group,
 *             los_categories, admit_type, ipa_li_split, ip_type, global
 *==============================================================================*/
create or replace table tmp_1m.kn_loc_oah_agg_04222026 as

with base as (
    select *
    from tmp_1m.kn_loc_notif_04222026_od
    where ipa_pac_flag in ('IPA', 'MM')
        and loc_flag = 1
        and total_oah_flag = 'OAH'
)

-- prov_tin -----------------------------------------------------------------
select
    'prov_tin'                                                          as _dimension
    , coalesce(cast(prov_tin as varchar), 'Unknown')                    as _dim_value
    , sum(case_count)                                                   as case_count
    , sum(membership)                                                   as membership
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)             as adr_rate
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)             as persistent_adr_rate
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)        as persistency
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)             as md_review_rate
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0) as appeal_rate
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0) as appeal_overturn_rate
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0) as p2p_rate
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0) as p2p_overturn_rate
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0) as mcr_reconsideration_rate
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0) as mcr_overturn_rate
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0) as member_appeal_rate
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0) as member_appeal_overturn_rate
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)             as pre_auth_rate
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)            as auth_per_k
from base
group by prov_tin
having sum(case_count) >= 30

union all

-- svc_setting --------------------------------------------------------------
select
    'svc_setting'
    , coalesce(cast(svc_setting as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by svc_setting
having sum(case_count) >= 30

union all

-- fin_market ---------------------------------------------------------------
select
    'fin_market'
    , coalesce(cast(fin_market as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by fin_market
having sum(case_count) >= 30

union all

-- hospital_group -----------------------------------------------------------
select
    'hospital_group'
    , coalesce(cast(hospital_group as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by hospital_group
having sum(case_count) >= 30

union all

-- los_categories -----------------------------------------------------------
select
    'los_categories'
    , coalesce(cast(los_categories as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by los_categories
having sum(case_count) >= 30

union all

-- admit_type ---------------------------------------------------------------
select
    'admit_type'
    , coalesce(cast(admit_type as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by admit_type
having sum(case_count) >= 30

union all

-- ipa_li_split -------------------------------------------------------------
select
    'ipa_li_split'
    , coalesce(cast(ipa_li_split as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by ipa_li_split
having sum(case_count) >= 30

union all

-- ip_type ------------------------------------------------------------------
select
    'ip_type'
    , coalesce(cast(ip_type as varchar), 'Unknown')
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by ip_type
having sum(case_count) >= 30

union all

-- global -------------------------------------------------------------------
select
    'global'
    , concat_ws(' | '
        , coalesce(cast(prov_tin as varchar), '')
        , coalesce(cast(svc_setting as varchar), '')
        , coalesce(cast(fin_market as varchar), '')
        , coalesce(cast(hospital_group as varchar), '')
        , coalesce(cast(los_categories as varchar), '')
        , coalesce(cast(admit_type as varchar), '')
        , coalesce(cast(ipa_li_split as varchar), '')
        , coalesce(cast(ip_type as varchar), '')
      )
    , sum(case_count), sum(membership)
    , sum(initial_adr_cnt)    / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(case_count), 0)
    , sum(persistent_adr_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(md_reviewed_cnt)    / nullif(sum(case_count), 0)
    , sum(appeal_case_cnt)              / nullif(sum(initial_adr_cnt), 0)
    , sum(appeal_ovrtn_case_cnt)        / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_case_cnt)                 / nullif(sum(initial_adr_cnt), 0)
    , sum(p2p_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_reconsideration_case_cnt) / nullif(sum(initial_adr_cnt), 0)
    , sum(mcr_ovrtn_case_cnt)           / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_cnt)            / nullif(sum(initial_adr_cnt), 0)
    , sum(member_appeal_ovtn_cnt)       / nullif(sum(initial_adr_cnt), 0)
    , sum(pre_auth_cases)     / nullif(sum(case_count), 0)
    , sum(case_count) * 1000.0 / nullif(sum(membership), 0)
from base
group by
    prov_tin, svc_setting, fin_market, hospital_group
    , los_categories, admit_type, ipa_li_split, ip_type
having sum(case_count) >= 30
;
