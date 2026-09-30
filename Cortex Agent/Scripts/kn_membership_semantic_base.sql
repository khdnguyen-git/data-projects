/*==============================================================================
 * MEMBERSHIP TABLES FOR CORTEX ANALYST SEMANTIC VIEWS
 * Source: gl_rstd_gpsgalnce_f_202606
 * Grain: population + month + market + contract
 * Time range: 2024+
 * Two versions: claims naming + IPA naming
 *==============================================================================*/


/*------------------------------------------------------------------------------
 * CLAIMS MEMBERSHIP: kn_claims_semantic_membership
 * Column naming matches claims conventions (brand_fnl, product_level_3_fnl, etc.)
 *------------------------------------------------------------------------------*/
create or replace table tmp_1m.kn_claims_semantic_membership as
with population_mapped as (
    select
        a.fin_inc_month as srvc_month
        , a.fin_brand as brand_fnl
        , a.fin_product_level_3 as product_level_3_fnl
        , a.fin_state as st_abbr_cd
        , a.global_cap
        , a.migration_source
        , a.tfm_include_flag
        , a.sgr_source_name as claim_platform
        , a.nce_tadm_dec_risk_type
        , case
            when a.migration_source = 'OAH'
                and not (a.fin_brand = 'C&S' and a.fin_inc_year = '2024' and a.fin_state = 'MD')
            then 'OAH'
            when a.fin_product_level_3 = 'INSTITUTIONAL' then 'INSTITUTIONAL'
            when (a.fin_brand = 'C&S' and a.migration_source != 'OAH'
                and a.global_cap = 'NA' and a.fin_product_level_3 = 'DUAL'
                and a.sgr_source_name in ('COSMOS','CSP'))
                or (a.fin_inc_year = '2024' and a.fin_brand = 'C&S'
                    and a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS','CSP')
                    and a.migration_source = 'OAH' and a.fin_state = 'MD')
                then 'C&S DSNP'
            when (a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
                and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
                or (a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
                    and a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN'))
                then 'M&R FFS'
            else 'Other'
        end as population
        , case
            when a.fin_brand = 'M&R' then a.fin_market
            when a.fin_brand = 'C&S' then a.fin_state
        end as market_fnl
        , a.fin_contract_nbr as contract_fnl
        , a.fin_member_cnt
    from hce_ops_archv.gl_rstd_gpsgalnce_f_202606 as a
    where a.fin_brand in ('M&R', 'C&S')
        and a.fin_inc_month >= '202401'
)
select
    srvc_month
    , population
    , market_fnl
    , brand_fnl
    , product_level_3_fnl
    , sum(fin_member_cnt) as member_months
from population_mapped
group by
    srvc_month
    , population
    , market_fnl
    , brand_fnl
    , product_level_3_fnl
;


/*------------------------------------------------------------------------------
 * IPA MEMBERSHIP: kn_ipa_semantic_membership
 * Column naming matches auths/IPA conventions (fin_brand, fin_product_level_3, etc.)
 *------------------------------------------------------------------------------*/
create or replace table tmp_1m.kn_ipa_semantic_membership as
with population_mapped as (
    select
        a.fin_inc_month as admit_act_month
        , a.fin_brand
        , a.fin_product_level_3
        , a.fin_state
        , a.global_cap
        , a.migration_source
        , a.tfm_include_flag
        , a.sgr_source_name
        , a.nce_tadm_dec_risk_type
        , case
            when a.migration_source = 'OAH'
                and not (a.fin_brand = 'C&S' and a.fin_inc_year = '2024' and a.fin_state = 'MD')
            then 'OAH'
            when a.fin_product_level_3 = 'INSTITUTIONAL' then 'INSTITUTIONAL'
            when (a.fin_brand = 'C&S' and a.migration_source != 'OAH'
                and a.global_cap = 'NA' and a.fin_product_level_3 = 'DUAL'
                and a.sgr_source_name in ('COSMOS','CSP'))
                or (a.fin_inc_year = '2024' and a.fin_brand = 'C&S'
                    and a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS','CSP')
                    and a.migration_source = 'OAH' and a.fin_state = 'MD')
                then 'C&S DSNP'
            when (a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
                and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
                or (a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
                    and a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN'))
                then 'M&R FFS'
            else 'Other'
        end as population
        , case
            when a.fin_brand = 'M&R' then a.fin_market
            when a.fin_brand = 'C&S' then a.fin_state
        end as fin_market
        , a.fin_contract_nbr
        , a.fin_member_cnt
    from hce_ops_archv.gl_rstd_gpsgalnce_f_202606 as a
    where a.fin_brand in ('M&R', 'C&S')
        and a.fin_inc_month >= '202401'
)
select
    admit_act_month
    , population
    , fin_market
    , fin_contract_nbr
    , fin_brand
    , fin_product_level_3
    , fin_state
    , global_cap
    , migration_source
    , tfm_include_flag
    , sgr_source_name
    , sum(fin_member_cnt) as member_months
from population_mapped
group by
    admit_act_month
    , population
    , fin_market
    , fin_contract_nbr
    , fin_brand
    , fin_product_level_3
    , fin_state
    , global_cap
    , migration_source
    , tfm_include_flag
    , sgr_source_name
;
