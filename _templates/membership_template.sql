/*==============================================================================
 * Membership template
 * Pull from fichsrv.tre_membership.
 *==============================================================================*/

create or replace table <schema>.<table_name> as
select
    a.fin_inc_month
    , a.sgr_source_name as entity
    , a.fin_mbi_hicn_fnl as mbi
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.fin_tfm_product_new as tfm_product_new_fnl
    , a.fin_product_level_3 as product_level_3_fnl
    , a.fin_member_cnt
from fichsrv.tre_membership as a
where a.sgr_source_name in ('COSMOS', 'CSP', 'NICE')
    and a.fin_inc_month >= '202301'
;
