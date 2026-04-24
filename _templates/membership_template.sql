/*==============================================================================
 * Membership template
 * Pull from fichsrv.tre_membership.
 *==============================================================================*/

create or replace table <schema>.<table_name> as
with mm as (
select
    sgr_source_name as entity
    , '' as component
    , '' as service_code
    , fin_inc_month as fst_srvc_month
    , fin_inc_year as fst_srvc_year
    , global_cap
    , nce_tadm_dec_risk_type
    , fin_market as market_fnl
    , fin_state as st_abbr_cd
    , fin_brand as brand_fnl
    , fin_g_i as group_ind_fnl
    , tfm_include_flag
    , migration_source
    , fin_tfm_product_new as tfm_product_new_fnl
    , fin_product_level_3 as product_level_3_fnl
    , fin_member_cnt
    , fin_mbi_hicn_fnl
from fichsrv.tre_membership
where
    sgr_source_name in ('COSMOS', 'CSP', 'NICE')
    and fin_inc_month >= '202301'
)
select
    *
    , case
        when migration_source = 'OAH'
            and not (brand_fnl = 'C&S' and fst_srvc_year = '2024' and st_abbr_cd = 'MD')
            and not (brand_fnl != 'C&S' and fst_srvc_year = '2024' and market_fnl = 'MD')
        then 'OAH'
        when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 'M&R ISNP'
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
        when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 'M&R DSNP'
        else 'N/A'
      end as population
from mm
;
