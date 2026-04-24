/*==============================================================================
 * Claims template
 * Pull from all 6 fichsrv claims tables via union all.
 * Alias convention: main claims table = a, lookup/join tables = b, c, ...
 *==============================================================================*/

create or replace table <schema>.<table_name> as
with claims as (
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
    , a.migration_source
    , a.tfm_include_flag
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
    , a.migration_source
    , a.tfm_include_flag
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
    , a.migration_source
    , a.tfm_include_flag
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
    , a.migration_source
    , a.tfm_include_flag
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
    , a.mbi_hicn_fnl as mbi              -- NICE: mbi_hicn_fnl (not gal_mbi_hicn_fnl)
    , a.proc_cd
    , a.tin as prov_tin                  -- NICE: tin (not prov_tin)
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , iff(a.clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap  -- NICE: derived from clm_cap_flag
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , 'NA' as migration_source           -- NICE: no migration_source field
    , a.tfm_include_flag
    , a.allw_amt as allw_amt_fnl         -- NICE OP: allw_amt
    , a.net_pd_amt as net_pd_amt_fnl     -- NICE OP: net_pd_amt
from fichsrv.nce_op_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.clm_cap_flag = 'FFS'           -- NICE: filter on clm_cap_flag, not global_cap
    and a.dnl_f not in ('D', 'Y')        -- NICE: dnl_f (not clm_dnl_f)
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
    , a.mbi_hicn_fnl as mbi              -- NICE: mbi_hicn_fnl (not gal_mbi_hicn_fnl)
    , a.proc_cd
    , a.tin as prov_tin                  -- NICE: tin (not prov_tin)
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , iff(a.clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap  -- NICE: derived from clm_cap_flag
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , 'NA' as migration_source           -- NICE: no migration_source field
    , a.tfm_include_flag
    , a.calc_allw as allw_amt_fnl        -- NICE PR: calc_allw (not allw_amt_fnl)
    , a.calc_net_pd as net_pd_amt_fnl    -- NICE PR: calc_net_pd (not net_pd_amt_fnl)
from fichsrv.nce_pr_f as a
where a.brand_fnl in ('M&R', 'C&S')
    and a.clm_cap_flag = 'FFS'           -- NICE: filter on clm_cap_flag, not global_cap
    and a.dnl_f not in ('D', 'Y')        -- NICE: dnl_f (not clm_dnl_f)
    and a.fst_srvc_year >= '2023'
)
select
    *
    , case
        when migration_source = 'OAH'
            and not (brand_fnl = 'C&S' and fst_srvc_year = '2024' and st_abbr_cd = 'MD')
            and not (brand_fnl != 'C&S' and fst_srvc_year = '2024' and market_fnl = 'MD')
        then 'OAH'
        when brand_fnl = 'M&R'
            and product_level_3_fnl = 'INSTITUTIONAL'
        then 'M&R ISNP'
        when entity in ('COSMOS', 'NICE')
            and brand_fnl = 'M&R'
            and global_cap = 'NA'
            and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL')
            and tfm_include_flag = 1
        then 'M&R FFS (excl. DSNP)'
        when entity in ('COSMOS', 'CSP')
            and global_cap = 'NA'
            and (
                (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
                or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD')
                or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
            )
        then 'C&S DSNP'
        when brand_fnl = 'M&R'
            and product_level_3_fnl = 'DUAL'
        then 'M&R DSNP'
        else 'N/A'
      end as population
from claims
;
