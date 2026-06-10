/*==============================================================================
 * Therapy Savings — Category Breakdown (PT-OT / ST / Chiro)
 * Monthly total allowed by therapy_category and population, from 2023
 * Source: tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_202603
 *==============================================================================*/

select
    fst_srvc_year
    , fst_srvc_month
    , category_1 as therapy_category
    , population
    , market_fnl
    , prov_tin
    , ahrq_diag_dtl_catgy_desc
    , sum(allw_amt_fnl) as total_allowed
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_202603
where population != 'N/A'
  and category_1 in ('PT-OT', 'ST', 'Chiro')
group by
    fst_srvc_year
    , fst_srvc_month
    , category_1
    , population
    , market_fnl
    , prov_tin
    , ahrq_diag_dtl_catgy_desc
order by
    fst_srvc_month
    , category_1
    , population
    ;



select * from tmp_1m.knd_mbm_vpe_summary_202603
limit 5
;


create or replace table tmp_1m.kn_mbm_category_summary_202603 as
select
    ep_start_year
    , population
    , market_fnl
    , category_2 as mbm_category
    , category_1 as therapy_category
    , sum(allowed) as allowed
    , sum(total_visits) as visits
    , sum(total_episodes) as episodes
    , sum(mbr_count) as mbr_count
from tmp_1m.knd_mbm_vpe_summary_202603
where population != 'N/A'
group by
    ep_start_year
    , population
    , market_fnl
    , category_2
    , category_1
order by
    ep_start_year
    , mbm_category