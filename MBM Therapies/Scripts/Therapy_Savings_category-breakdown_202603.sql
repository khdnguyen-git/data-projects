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
