/*==============================================================================
 * People Health Auth Metrics — H1961, H4544
 * Pull from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F
 * Membership from HCE_OPS_ARCHV.GL_RSTD_GPSGALNCE_F_202604
 * Period: 2024–2026
 * Population: M&R FFS (includes DSNP — DSNP exclusion is Therapies-only)
 *==============================================================================*/

describe table HCE_OPS_STAGE.DAILY_MD_ESC_CASE_ECSSTRAT_V 
;

select distinct 
    substr(a.fa_prov_id, 2, 9) as prov_tin
    , a.fin_market
    --, b.collection
    , a.group_name
from hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
left join tmp_1y.tin_collection as b
where a.fin_contract_nbr in ('H1961', 'H4544')
    and a.fin_brand in ('M&R', 'C&S')
    and concat(
        year(coalesce(a.admit_dt_act, a.admit_dt_exp))
        , lpad(month(coalesce(a.admit_dt_act, a.admit_dt_exp)), 2, 0)
    ) >= '202401'
    and fin_market = 'LA'
;


select 
case_id 
from tmp_1m.kn_people_health_auths_202605
where admit_act_month >= '202501'
order by case_id
limit 50
;



create or replace table tmp_1q.people_health as
with auths as (
    select
        a.fin_mbi_hicn_fnl
        , a.sgr_source_name
        , a.entity
        , a.fin_brand
        , a.fin_market
        , a.fin_state
        , a.migration_source
        , a.fin_product_level_3
        , a.fin_tfm_product_new
        , a.fin_contractpbp
        , a.fin_contract_nbr
        , a.global_cap
        , a.tfm_include_flag
        , a.fin_g_i
        , a.nce_tadm_dec_risk_type
        , a.svc_setting
        , a.plc_of_svc_cd
        , a.admit_cat_cd
        , a.case_svc_init_decn_cd
        , a.case_cur_svc_cat_dtl_cd
        , a.admit_dt_act
        , a.admit_dt_exp
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , concat(
            year(coalesce(a.admit_dt_act, a.admit_dt_exp))
            , lpad(month(coalesce(a.admit_dt_act, a.admit_dt_exp)), 2, 0)
          ) as admit_act_month
        , a.case_id
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , case
            when a.svc_setting = 'Inpatient' and a.plc_of_svc_cd = '21 - Acute Hospital' and a.admit_cat_cd in ('17 - Medical') then 'Medical'
            when a.svc_setting = 'Inpatient' and a.plc_of_svc_cd = '21 - Acute Hospital' and a.admit_cat_cd in ('30 - Surgical') then 'Surgical'
            when a.plc_of_svc_cd != '12 - Home' and a.case_cur_svc_cat_dtl_cd != '51 - Custodial'
                and a.case_cur_svc_cat_dtl_cd in ('17 - Long Term Care', '42 - Long Term Acute Care') then 'LTAC'
            when a.plc_of_svc_cd != '12 - Home' and a.case_cur_svc_cat_dtl_cd != '51 - Custodial'
                and (a.case_cur_svc_cat_dtl_cd in ('31 - Skilled Nursing', '46 - PAT Skilled Nursing')
                    or substr(a.plc_of_svc_cd, 1, 2) in ('31', '16')) then 'SNF'
            when a.plc_of_svc_cd != '12 - Home' and a.case_cur_svc_cat_dtl_cd != '51 - Custodial'
                and a.case_cur_svc_cat_dtl_cd in ('35 - Therapy Services')
                and substr(a.plc_of_svc_cd, 1, 2) in ('61', '6') then 'AIR'
            else 'NA'
          end as ip_type
        , case 
            when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
            when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1
            else 0 
        end as loc_flag
    from hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
    left join tmp_1y.tin_collection as d
    on substr(a.fa_prov_id, 2, 9) = d.tin 
    where a.fin_contract_nbr in ('H1961', 'H4544')
        and a.fin_brand in ('M&R', 'C&S')
        and concat(
            year(coalesce(a.admit_dt_act, a.admit_dt_exp))
            , lpad(month(coalesce(a.admit_dt_act, a.admit_dt_exp)), 2, 0)
        ) >= '202401'
),
counts as (
select 
    admit_act_month
    , count(distinct case_id) as case_count
    , count(distinct (case when initialfulladr_cases = 1 then case_id end)) as Initial_ADR_cnt	
    , count(distinct (case when persistentfulladr_cases = 1 then case_id end)) as Persistent_ADR_cnt
from auths
where 
    admit_act_month >= '202401' 
    --and fin_market = 'LA' 
    and loc_flag = 1
    and fin_brand = 'M&R' 
    and global_cap = 'NA' 
    and fin_product_level_3 != 'INSTITUTIONAL'
group by 1
order by admit_act_month
)
select 
    admit_act_month
    , sum(case_count) as case_cnt
    , sum(Initial_ADR_cnt) as Initial_ADR_cnt
    , sum(Persistent_ADR_cnt) as Persistent_ADR_cnt
from counts
group by 1
order by 1
;




select * from tmp_1q.people_health;
;


------------------------------------------------------------------------------------------------------------


create or replace table tmp_1m.kn_people_health_auths_202605 as
with auths as (
    select
        a.fin_mbi_hicn_fnl as medicare_id
        , a.sgr_source_name as entity
        , a.fin_brand
        , a.fin_market
        , a.fin_state
        , a.migration_source
        , a.fin_product_level_3
        , a.fin_tfm_product_new
        , a.fin_contractpbp
        , a.fin_contract_nbr
        , a.global_cap
        , a.tfm_include_flag
        , a.fin_g_i
        , a.nce_tadm_dec_risk_type
        , a.svc_setting
        , a.plc_of_svc_cd
        , a.admit_cat_cd
        , a.case_cur_svc_cat_dtl_cd
        , a.admit_dt_act
        , a.admit_dt_exp
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , concat(
            year(coalesce(a.admit_dt_act, a.admit_dt_exp))
            , lpad(month(coalesce(a.admit_dt_act, a.admit_dt_exp)), 2, 0)
          ) as admit_act_month
        , a.case_id
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , case
            when a.svc_setting = 'Inpatient' and a.plc_of_svc_cd = '21 - Acute Hospital' and a.admit_cat_cd in ('17 - Medical') then 'Medical'
            when a.svc_setting = 'Inpatient' and a.plc_of_svc_cd = '21 - Acute Hospital' and a.admit_cat_cd in ('30 - Surgical') then 'Surgical'
            when a.plc_of_svc_cd != '12 - Home' and a.case_cur_svc_cat_dtl_cd != '51 - Custodial'
                and a.case_cur_svc_cat_dtl_cd in ('17 - Long Term Care', '42 - Long Term Acute Care') then 'LTAC'
            when a.plc_of_svc_cd != '12 - Home' and a.case_cur_svc_cat_dtl_cd != '51 - Custodial'
                and (a.case_cur_svc_cat_dtl_cd in ('31 - Skilled Nursing', '46 - PAT Skilled Nursing')
                    or substr(a.plc_of_svc_cd, 1, 2) in ('31', '16')) then 'SNF'
            when a.plc_of_svc_cd != '12 - Home' and a.case_cur_svc_cat_dtl_cd != '51 - Custodial'
                and a.case_cur_svc_cat_dtl_cd in ('35 - Therapy Services')
                and substr(a.plc_of_svc_cd, 1, 2) in ('61', '6') then 'AIR'
            else 'NA'
          end as ip_type
    from hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
    left join tmp_1y.tin_collection as d
    on substr(a.fa_prov_id, 2, 9) = d.tin 
    where a.fin_contract_nbr in ('H1961', 'H4544')
        and a.fin_brand in ('M&R', 'C&S')
        and concat(
            year(coalesce(a.admit_dt_act, a.admit_dt_exp))
            , lpad(month(coalesce(a.admit_dt_act, a.admit_dt_exp)), 2, 0)
        ) >= '202401'
)
, auths_pop as (
    select
        *
        , case
            when migration_source = 'OAH'
                and not (fin_brand = 'C&S' and to_varchar(coalesce(admit_dt_act, admit_dt_exp), 'yyyy') = '2024' and fin_state = 'MD')
                and not (fin_brand != 'C&S' and to_varchar(coalesce(admit_dt_act, admit_dt_exp), 'yyyy') = '2024' and fin_market = 'MD')
            then 'OAH'
            when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
            when entity in ('COSMOS', 'NICE')
                and fin_brand = 'M&R'
                and global_cap = 'NA'
                and fin_product_level_3 != 'INSTITUTIONAL'
                and tfm_include_flag = 1
            then 'M&R FFS'
            when entity in ('COSMOS', 'CSP')
                and global_cap = 'NA'
                and (
                    (fin_brand = 'C&S' and migration_source != 'OAH' and fin_product_level_3 = 'DUAL')
                    or (fin_brand = 'C&S' and to_varchar(coalesce(admit_dt_act, admit_dt_exp), 'yyyy') = '2024' and migration_source = 'OAH' and fin_state = 'MD')
                    or (fin_brand != 'C&S' and to_varchar(coalesce(admit_dt_act, admit_dt_exp), 'yyyy') = '2024' and migration_source = 'OAH' and fin_market = 'MD')
                )
            then 'C&S DSNP'
            else 'N/A'
          end as population
    from auths
)
, auth_agg as (
    select
        population
        , admit_act_month
        , fin_product_level_3
        , ip_type
        , fin_tfm_product_new
        , fin_contractpbp
        , prov_tin
        , count(distinct case_id) as case_count
        , count(distinct (case when initialfulladr_cases = 1 then case_id end)) as Initial_ADR_cnt	
        , count(distinct (case when persistentfulladr_cases = 1 then case_id end)) as Persistent_ADR_cnt
    from auths_pop
    group by
        population
        , admit_act_month
        , fin_product_level_3
        , ip_type
        , fin_tfm_product_new
        , fin_contractpbp
        , prov_tin
)

, mm as (
    select
        a.fin_inc_month
        , case
            when a.migration_source = 'OAH'
                and not (a.fin_brand = 'C&S' and a.fin_inc_year = '2024' and a.fin_state = 'MD')
                and not (a.fin_brand != 'C&S' and a.fin_inc_year = '2024' and a.fin_market = 'MD')
            then 'OAH'
            when a.fin_brand = 'M&R' and a.fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
            when a.sgr_source_name in ('COSMOS', 'NICE')
                and a.fin_brand = 'M&R'
                and a.global_cap = 'NA'
                and a.fin_product_level_3 != 'INSTITUTIONAL'
                and a.tfm_include_flag = 1
            then 'M&R FFS'
            when a.sgr_source_name in ('COSMOS', 'CSP')
                and a.global_cap = 'NA'
                and (
                    (a.fin_brand = 'C&S' and a.migration_source != 'OAH' and a.fin_product_level_3 = 'DUAL')
                    or (a.fin_brand = 'C&S' and a.fin_inc_year = '2024' and a.migration_source = 'OAH' and a.fin_state = 'MD')
                    or (a.fin_brand != 'C&S' and a.fin_inc_year = '2024' and a.migration_source = 'OAH' and a.fin_market = 'MD')
                )
            then 'C&S DSNP'
            else 'N/A'
          end as population
        , sum(a.fin_member_cnt) as membership
    from hce_ops_archv.gl_rstd_gpsgalnce_f_202604 as a
    where a.fin_contract_nbr in ('H1961', 'H4544')
        and a.fin_inc_year in ('2024', '2025', '2026')
    group by
        a.fin_inc_month
        , case
            when a.migration_source = 'OAH'
                and not (a.fin_brand = 'C&S' and a.fin_inc_year = '2024' and a.fin_state = 'MD')
                and not (a.fin_brand != 'C&S' and a.fin_inc_year = '2024' and a.fin_market = 'MD')
            then 'OAH'
            when a.fin_brand = 'M&R' and a.fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
            when a.sgr_source_name in ('COSMOS', 'NICE')
                and a.fin_brand = 'M&R'
                and a.global_cap = 'NA'
                and a.fin_product_level_3 != 'INSTITUTIONAL'
                and a.tfm_include_flag = 1
            then 'M&R FFS'
            when a.sgr_source_name in ('COSMOS', 'CSP')
                and a.global_cap = 'NA'
                and (
                    (a.fin_brand = 'C&S' and a.migration_source != 'OAH' and a.fin_product_level_3 = 'DUAL')
                    or (a.fin_brand = 'C&S' and a.fin_inc_year = '2024' and a.migration_source = 'OAH' and a.fin_state = 'MD')
                    or (a.fin_brand != 'C&S' and a.fin_inc_year = '2024' and a.migration_source = 'OAH' and a.fin_market = 'MD')
                )
            then 'C&S DSNP'
            else 'N/A'
          end
)

select
    a.population
    , a.admit_act_month
    , a.fin_product_level_3
    , a.ip_type
    , a.fin_tfm_product_new
    , a.fin_contractpbp
    , a.prov_tin
    , a.initial_adr_cnt
    , a.persistent_adr_cnt
    , a.case_count
    , b.membership
    , a.case_count * 1000.0 / nullif(b.membership, 0) as auth_per_k
from auth_agg as a
left join mm as b
    on a.admit_act_month = b.fin_inc_month
    and a.population = b.population
;

select * from tmp_1m.kn_people_health_auths_202605
;

select
    admit_act_month
    , 100*(sum(initial_adr_cnt)/sum(case_count)) as Initial_ADR
    , 100*(sum(persistent_adr_cnt)/sum(case_count)) as Persistent_ADR
    , sum(case_count)
    , sum(membership), 
from tmp_1m.kn_people_health_auths_202605
where population = 'M&R FFS'
group by 1
order by 1