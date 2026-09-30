create or replace table tmp_1q.kn_auths_semantic_membership as
with population_mapped as (
    select
        a.fin_inc_month as admit_act_month
        , case
            when a.migration_source = 'OAH' then 'OAH'
            when a.fin_product_level_3 = 'INSTITUTIONAL' then 'INSTITUTIONAL'
            when (a.fin_brand = 'C&S' and a.migration_source != 'OAH'
                and a.global_cap = 'NA' and a.fin_product_level_3 = 'DUAL'
                and a.sgr_source_name in ('COSMOS','CSP'))
                or (a.fin_inc_year = '2024' and a.fin_brand = 'C&S'
                    and a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS','CSP')
                    and a.migration_source = 'OAH' and a.fin_state = 'MD')
                then 'CNS DSNP'
            when (a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
                and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
                or (a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
                    and a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN'))
                then 'MNR FFS'
            else 'OTHER'
        end as population
        , case
            when a.fin_brand = 'M&R' then a.fin_market
            when a.fin_brand = 'C&S' then a.fin_state
        end as fin_market
        , a.fin_contract_nbr
        , a.fin_member_cnt
    from hce_ops_archv.gl_rstd_gpsgalnce_f_202606 as a
    where a.fin_brand in ('M&R','C&S')
        and a.fin_inc_month >= '202401'
)
select
    admit_act_month
    , population
    , fin_market
    , fin_contract_nbr
    , sum(fin_member_cnt) as member_months
from population_mapped
group by
    admit_act_month
    , population
    , fin_market
    , fin_contract_nbr
;

select sum(member_months)
from tmp_1m.kn_auths_semantic_membership
where population = 'MNR FFS' and admit_act_month = '202502' 
;