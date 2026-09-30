-----------------------------------------------------------------------------------------------------------------------------
-- AVTAR-like source data for PA Model
-- Produces: kn_auths_normal_output (standard model), kn_auths_enhanced_output (with group detail)
-----------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------
-- STEP 1: Build population-flagged base table (CTE chain → single materialized intermediate)
-----------------------------------------------------------------------------------------------------------------------------
create or replace table tmp_1m.kn_auths_pop_flags as

with raw_2023 as (
    select
        a.business_segment
        , a.medicare_id
        , a.create_dt
        , a.notif_recd_dttm
        , a.notif_yrmonth
        , a.entity
        , a.proc_cd
        , a.pa_program
        , a.case_init_decn_cd
        , a.case_decn_stat_cd
        , a.case_id
        , a.migration_source
        , a.fin_brand
        , a.fin_source_name
        , a.sgr_source_name
        , a.nce_tadm_dec_risk_type
        , a.tfm_include_flag
        , a.global_cap
        , a.fin_market
        , a.fin_state
        , a.fin_plan_level_2
        , a.fin_product_level_3
        , a.fin_g_i
        , a.group_number
        , a.group_name
        , '' as hce_cohort
    from ving_prd_trend_db.hce_ops_fnl.hce_adr_avtar_like_2023_f_1 as a  --ARCHIVE TABLE, MAY NEED TO CHANGE NEXT REFRESH--
    where a.business_segment not in ('EnI', 'ERR', 'null')
        and a.avtar_mtch_ind = 1
        and a.pa_program not in ('Not EPAL-Prime', 'Non-EPAL')
        and a.notif_yrmonth between '202301' and '202312'
)

, raw_2024 as (
    select
        a.business_segment
        , a.medicare_id
        , a.create_dt
        , a.notif_recd_dttm
        , a.notif_yrmonth
        , a.entity
        , a.proc_cd
        , a.pa_program
        , a.case_init_decn_cd
        , a.case_decn_stat_cd
        , a.case_id
        , a.migration_source
        , a.fin_brand
        , a.fin_source_name
        , a.sgr_source_name
        , a.nce_tadm_dec_risk_type
        , a.tfm_include_flag
        , a.global_cap
        , a.fin_market
        , a.fin_state
        , a.fin_plan_level_2
        , a.fin_product_level_3
        , a.fin_g_i
        , a.group_number
        , a.group_name
        , b.hce_cohort
    from ving_prd_trend_db.hce_ops_fnl.hce_adr_avtar_like_24_25_f as a
    left join tmp_1y.hce_cohort_2023_2024_6 as b  --MAY NEED TO CHANGE NEXT REFRESH--
        on b.fin_mbi_hicn_fnl = a.medicare_id
    where a.business_segment not in ('EnI', 'ERR', 'null')
        and a.avtar_mtch_ind = 1
        and a.pa_program not in ('Not EPAL-Prime', 'Non-EPAL')
        and a.notif_yrmonth between '202401' and '202412'
)

, raw_2025 as (
    select
        a.business_segment
        , a.medicare_id
        , a.create_dt
        , a.notif_recd_dttm
        , a.notif_yrmonth
        , a.entity
        , a.proc_cd
        , a.pa_program
        , a.case_init_decn_cd
        , a.case_decn_stat_cd
        , a.case_id
        , a.migration_source
        , a.fin_brand
        , a.fin_source_name
        , a.sgr_source_name
        , a.nce_tadm_dec_risk_type
        , a.tfm_include_flag
        , a.global_cap
        , a.fin_market
        , a.fin_state
        , a.fin_plan_level_2
        , a.fin_product_level_3
        , a.fin_g_i
        , a.group_number
        , a.group_name
        , b.hce_cohort
    from ving_prd_trend_db.hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
    left join hce_ops_fnl.hceops_2024_2025_hce_cohort_6 as b  --MAY NEED TO CHANGE NEXT REFRESH--
        on b.fin_mbi_hicn_fnl = a.medicare_id
    where a.business_segment not in ('EnI', 'ERR', 'null')
        and a.avtar_mtch_ind = 1
        and a.pa_program not in ('Not EPAL-Prime', 'Non-EPAL')
        and a.notif_yrmonth between '202501' and '202512'  -------------CHANGE THIS DATE--------------
)

, raw_2026 as (
    select
        a.business_segment
        , a.medicare_id
        , a.create_dt
        , a.notif_recd_dttm
        , a.notif_yrmonth
        , a.entity
        , a.proc_cd
        , a.pa_program
        , a.case_init_decn_cd
        , a.case_decn_stat_cd
        , a.case_id
        , a.migration_source
        , a.fin_brand
        , a.fin_source_name
        , a.sgr_source_name
        , a.nce_tadm_dec_risk_type
        , a.tfm_include_flag
        , a.global_cap
        , a.fin_market
        , a.fin_state
        , a.fin_plan_level_2
        , a.fin_product_level_3
        , a.fin_g_i
        , a.group_number
        , a.group_name
        , b.hce_cohort
    from ving_prd_trend_db.hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
    left join hce_ops_fnl.hceops_2024_2025_hce_cohort_6 as b  --MAY NEED TO CHANGE NEXT REFRESH--
        on b.fin_mbi_hicn_fnl = a.medicare_id
    where a.business_segment not in ('EnI', 'ERR', 'null')
        and a.avtar_mtch_ind = 1
        and a.pa_program not in ('Not EPAL-Prime', 'Non-EPAL')
        and a.notif_yrmonth between '202601' and '202612'  -------------CHANGE THIS DATE--------------
)

, unioned as (
    select * from raw_2026
    union all
    select * from raw_2025
    union all
    select * from raw_2024
    union all
    select * from raw_2023
)

, flagged as (
    select
        notif_yrmonth
        , substring(notif_yrmonth, 1, 4) as notif_year
        , business_segment
        , case when entity = 'PHS' then 'NICE' else 'COSMOS' end as entityname
        , entity
        , migration_source
        , fin_brand
        , fin_source_name
        , sgr_source_name
        , nce_tadm_dec_risk_type
        , tfm_include_flag
        , global_cap
        , fin_market
        , fin_state
        , fin_plan_level_2
        , fin_product_level_3
        , fin_g_i
        , group_number
        , group_name
        , hce_cohort
        , proc_cd
        , case when case_init_decn_cd in ('AD - Fully Adverse Determination', 'FM - Fully Mixed', 'PM - Partially Mixed', 'PD - Partially Adverse Determination') then 1 else 0 end as initial_adverse
        , case when case_decn_stat_cd in ('FM - Fully Mixed', 'PD - Partially Adverse Determination', 'PM - Partially Mixed') then 1 else 0 end as partial_adverse
        , case when case_decn_stat_cd in ('AD-Fully Adverse Determination', 'AD - Fully Adverse Determination') then 1 else 0 end as fully_adverse
        , 1 as case_cnt
    from unioned
    where proc_cd != 'null'
        and fin_plan_level_2 != 'PFFS'
)

select
    notif_yrmonth
    , notif_year
    , business_segment
    , entityname
    , entity
    , migration_source
    , fin_brand
    , fin_source_name
    , sgr_source_name
    , nce_tadm_dec_risk_type
    , tfm_include_flag
    , global_cap
    , fin_market
    , fin_state
    , fin_plan_level_2
    , fin_product_level_3
    , fin_g_i
    , group_number
    , group_name
    , hce_cohort
    , proc_cd
    , case_cnt
    , initial_adverse
    , partial_adverse
    , fully_adverse
    , case when business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 != 'INSTITUTIONAL' then 1 else 0 end as mnr_cosmos_ffs_flag
    , case when business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type = 'FFS' then 1 else 0 end as mnr_nice_ffs_flag
    , case
        when (business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 != 'INSTITUTIONAL')
            or (business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type = 'FFS')
        then 1 else 0
      end as mnr_total_ffs_flag
    , case
        when (notif_year = '2024' and business_segment = 'CnS' and fin_brand in ('M&R', 'C&S') and global_cap = 'NA' and sgr_source_name in ('COSMOS', 'CSP') and migration_source = 'OAH' and fin_state = 'MD') then 0
        when (business_segment = 'MnR' and fin_brand = 'M&R' and migration_source = 'OAH')
            or (business_segment = 'CnS' and fin_brand = 'C&S' and migration_source = 'OAH')
        then 1 else 0
      end as oah_flag
    , case
        when (business_segment = 'CnS' and fin_brand in ('M&R', 'C&S') and migration_source != 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and sgr_source_name in ('COSMOS', 'CSP'))
            or (notif_year = '2024' and business_segment = 'CnS' and fin_brand in ('M&R', 'C&S') and global_cap = 'NA' and sgr_source_name in ('COSMOS', 'CSP') and migration_source = 'OAH' and fin_state = 'MD')
        then 1 else 0
      end as cns_dual_flag
    , case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 1 else 0 end as mnr_dual_flag
    , case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as isnp_flag
from flagged
;

-----------------------------------------------------------------------------------------------------------------------------
-- QA CHECKS on kn_auths_pop_flags
-----------------------------------------------------------------------------------------------------------------------------
-- select distinct pa_program from tmp_1m.kn_auths_pop_flags  -- not carried forward; validate at source if needed
-- select entity, count(*) from tmp_1m.kn_auths_pop_flags group by entity
-- Expected: COSMOS, NICE

-----------------------------------------------------------------------------------------------------------------------------
-- STEP 2: Normal PA model output (aggregated by proc/population)
-----------------------------------------------------------------------------------------------------------------------------
create or replace table tmp_1m.kn_auths_normal_output as

with labeled as (
    select
        notif_yrmonth
        , fin_g_i
        , case
            when mnr_cosmos_ffs_flag = 1 then 'MnR_COSMOS_FFS'
            when mnr_nice_ffs_flag = 1 then 'MnR_NICE_FFS'
            when cns_dual_flag = 1 then 'CnS_Dual'
            when isnp_flag = 1 then 'ISNP'
            when oah_flag = 1 then 'OAH'
            else 'REMOVE'
          end as population_flag
        , proc_cd
        , case_cnt
        , initial_adverse
        , partial_adverse
        , fully_adverse
    from tmp_1m.kn_auths_pop_flags
    where proc_cd not in (
        'T1019', 'S5125', 'S5170', 'S5130', 'S5161', 'S5150', 'T2033', 'S5135', 'T1005', 'T2031',
        'S5102', 'S5126', 'T2040', 'T2029', 'S5140', 'T1023', 'S5160', 'S5165', 'T1017', 'T2021',
        'T4541', 'T2025', 'T4527', 'S5121', 'T4526', 'T2016', 'T2030', 'T4535', 'S5190', 'S5101',
        'T4528', 'T4523', 'S5100', 'T2003', 'T4524', 'T1002', 'T4522', 'T1001', 'S5120', 'S5105',
        'T1505', 'T1020', 'S0317', 'T4525', 'S5185', 'S5151', 'T1003', 'T4537', 'T2038', 'T4544',
        'S5136', 'T4543', 'S0315', 'T2032', 'T2002', 'T1028', 'T1999', 'T4540', 'T5999', 'T4534',
        'T4521', 'T2039', 'T4542', 'T2024', 'S9470', 'S5199', 'T2042', 'T1004', 'T2017', 'S0316',
        'T2035', 'T4532'
    )  --PROCS NOT IN AVTAR FOR CNS_DUALS--
)

select
    notif_yrmonth
    , fin_g_i
    , population_flag
    , proc_cd
    , sum(case_cnt) as total_auths
    , sum(initial_adverse) as initial_deny
    , sum(partial_adverse) as partial_deny
    , sum(fully_adverse) as fully_deny
from labeled
where population_flag != 'REMOVE'
group by
    notif_yrmonth
    , fin_g_i
    , population_flag
    , proc_cd
;

-----------------------------------------------------------------------------------------------------------------------------
-- STEP 3: Enhanced PA model output (with group detail)
-----------------------------------------------------------------------------------------------------------------------------
create or replace table tmp_1m.kn_auths_enhanced_output as

with labeled as (
    select
        notif_yrmonth
        , fin_market
        , fin_g_i
        , group_number
        , group_name
        , case
            when mnr_cosmos_ffs_flag = 1 then 'MnR_COSMOS_FFS'
            when mnr_nice_ffs_flag = 1 then 'MnR_NICE_FFS'
            when cns_dual_flag = 1 then 'CnS_Dual'
            when isnp_flag = 1 then 'ISNP'
            when oah_flag = 1 then 'OAH'
            else 'REMOVE'
          end as population_flag
        , proc_cd
        , case_cnt
        , fully_adverse
    from tmp_1m.kn_auths_pop_flags
)

select
    notif_yrmonth
    , fin_market
    , fin_g_i
    , group_number
    , group_name
    , population_flag
    , proc_cd
    , sum(case_cnt) as total_auths
    , sum(fully_adverse) as fully_deny
from labeled
where population_flag != 'REMOVE'
group by
    notif_yrmonth
    , fin_market
    , fin_g_i
    , group_number
    , group_name
    , population_flag
    , proc_cd
;

-----------------------------------------------------------------------------------------------------------------------------
-- EVAL POPULATION FILTERS (diagnostic)
-----------------------------------------------------------------------------------------------------------------------------
create or replace table tmp_1m.kn_auths_pop_eval as

select
    business_segment
    , fin_brand
    , fin_source_name
    , migration_source
    , global_cap
    , tfm_include_flag
    , nce_tadm_dec_risk_type
    , fin_product_level_3
    , mnr_cosmos_ffs_flag
    , mnr_total_ffs_flag
    , oah_flag
    , cns_dual_flag
    , mnr_dual_flag
    , isnp_flag
    , sum(case_cnt) as total_cases
    , count(*) as row_cnt
from tmp_1m.kn_auths_pop_flags
where notif_yrmonth like '2023%'
group by
    business_segment
    , fin_brand
    , fin_source_name
    , migration_source
    , global_cap
    , tfm_include_flag
    , nce_tadm_dec_risk_type
    , fin_product_level_3
    , mnr_cosmos_ffs_flag
    , mnr_total_ffs_flag
    , oah_flag
    , cns_dual_flag
    , mnr_dual_flag
    , isnp_flag
;
