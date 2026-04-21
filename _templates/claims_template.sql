/*==============================================================================
 * Claims template
 * Pull from all 6 fichsrv claims tables via union all.
 * Alias convention: main claims table = a, lookup/join tables = b, c, ...
 *==============================================================================*/

create or replace table <schema>.<table_name> as
select
    'COSMOS' as entity
    , 'OP' as component
    , a.eventkey as visit_id
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , a.prov_tin
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.glxy_op_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.clm_dnl_f not in ('D', 'Y')
    and a.fst_srvc_year >= '2023'

union all
select
    'COSMOS' as entity
    , 'PR' as component
    , a.eventkey as visit_id
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , a.prov_tin
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.glxy_pr_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.clm_dnl_f not in ('D', 'Y')
    and a.fst_srvc_year >= '2023'

union all
select
    'CSP' as entity
    , 'OP' as component
    , a.eventkey as visit_id
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , a.prov_tin
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.dcsp_op_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.clm_dnl_f not in ('D', 'Y')
    and a.fst_srvc_year >= '2023'

union all
select
    'CSP' as entity
    , 'PR' as component
    , a.eventkey as visit_id
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , a.prov_tin
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.dcsp_pr_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.clm_dnl_f not in ('D', 'Y')
    and a.fst_srvc_year >= '2023'

union all
select
    'NICE' as entity
    , 'OP' as component
    , a.eventkey as visit_id
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , a.prov_tin
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.nce_op_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.clm_dnl_f not in ('D', 'Y')
    and a.fst_srvc_year >= '2023'

union all
select
    'NICE' as entity
    , 'PR' as component
    , a.eventkey as visit_id
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , a.prov_tin
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.nce_pr_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.clm_dnl_f not in ('D', 'Y')
    and a.fst_srvc_year >= '2023'
;
