create or replace table tmp_1m.kn_pa_semantic_base as

with raw as (
    select
        case_id, fin_mbi_hicn_fnl, notif_yrmonth, proc_cd, pa_program,
        case_init_decn_cd, case_decn_stat_cd, initialfulladr_cases, persistentfulladr_cases,
        entity, business_segment, fin_brand, fin_product_level_3, fin_market, fin_state,
        fin_g_i, fin_plan_level_2, fin_contract_nbr, sgr_source_name, migration_source,
        global_cap, tfm_include_flag, nce_tadm_dec_risk_type, group_number, group_name,
        avtar_mtch_ind
    from hce_ops_fnl.hce_adr_avtar_like_24_25_f
    where notif_yrmonth between '202401' and '202412'
    union all
    select
        case_id, fin_mbi_hicn_fnl, notif_yrmonth, proc_cd, pa_program,
        case_init_decn_cd, case_decn_stat_cd, initialfulladr_cases, persistentfulladr_cases,
        entity, business_segment, fin_brand, fin_product_level_3, fin_market, fin_state,
        fin_g_i, fin_plan_level_2, fin_contract_nbr, sgr_source_name, migration_source,
        global_cap, tfm_include_flag, nce_tadm_dec_risk_type, group_number, group_name,
        avtar_mtch_ind
    from hce_ops_fnl.hce_adr_avtar_like_25_26_f
    where notif_yrmonth >= '202501'
)

select
    case_id
    , fin_mbi_hicn_fnl
    , notif_yrmonth
    , substring(notif_yrmonth, 1, 4) as notif_year
    , substring(notif_yrmonth, 1, 4) || 'Q' || ceil(substring(notif_yrmonth, 5, 2)::int / 3.0)::int as notif_qtr
    , proc_cd
    , pa_program
    , case_init_decn_cd
    , case_decn_stat_cd
    , initialfulladr_cases
    , persistentfulladr_cases
    , entity
    , business_segment
    , fin_brand
    , fin_product_level_3
    , fin_market
    , fin_state
    , fin_g_i
    , fin_plan_level_2
    , fin_contract_nbr
    , sgr_source_name
    , migration_source
    , global_cap
    , tfm_include_flag
    , nce_tadm_dec_risk_type
    , group_number
    , group_name
    , case when proc_cd in (
        'T1019','S5125','S5170','S5130','S5161','S5150','T2033','S5135','T1005','T2031',
        'S5102','S5126','T2040','T2029','S5140','T1023','S5160','S5165','T1017','T2021',
        'T4541','T2025','T4527','S5121','T4526','T2016','T2030','T4535','S5190','S5101',
        'T4528','T4523','S5100','T2003','T4524','T1002','T4522','T1001','S5120','S5105',
        'T1505','T1020','S0317','T4525','S5185','S5151','T1003','T4537','T2038','T4544',
        'S5136','T4543','S0315','T2032','T2002','T1028','T1999','T4540','T5999','T4534',
        'T4521','T2039','T4542','T2024','S9470','S5199','T2042','T1004','T2017','S0316',
        'T2035','T4532'
      ) then 1 else 0 end as hcbs_flag
    , case
        when migration_source = 'OAH' then 'OAH'
        when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 'INSTITUTIONAL'
        when fin_brand = 'C&S' and fin_product_level_3 = 'DUAL'
            and global_cap = 'NA' and sgr_source_name in ('COSMOS', 'CSP') then 'C&S DSNP'
        when fin_brand = 'M&R' and global_cap = 'NA' and tfm_include_flag = 1
            and fin_product_level_3 != 'INSTITUTIONAL' then 'M&R FFS'
        when fin_brand = 'M&R' and sgr_source_name = 'NICE'
            and nce_tadm_dec_risk_type = 'FFS' then 'M&R FFS'
        else 'Other'
      end as population
from raw
where avtar_mtch_ind = 1
    and pa_program not in ('Not EPAL-Prime', 'Non-EPAL')
    and business_segment not in ('EnI', 'ERR', 'null')
    and fin_plan_level_2 != 'PFFS'
;
