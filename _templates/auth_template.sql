/*==============================================================================
 * Auth template
 * Pull from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F.
 * Excludes TRS/Transplant (transplant_flag = 'Y').
 * Alias convention: main auth table = a, lookup/join tables = b, c, ...
 *==============================================================================*/

create or replace table <schema>.<table_name> as
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
    , a.global_cap
    , a.tfm_include_flag
    , a.fin_g_i
    , a.nce_tadm_dec_risk_type
    , a.svc_setting
    , a.plc_of_svc_cd
    , a.admit_cat_cd
    , a.case_cur_svc_cat_dtl_cd
    , a.admit_dt_act
    , a.dschg_dt_act
    , a.admit_dt_exp
    , a.dschg_dt_exp
    , a.case_status_cd
    , a.case_init_decn_cd
    , a.case_svc_init_decn_cd
    , substr(a.fa_prov_id, 2, 9) as prov_tin
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
        when a.svc_setting = 'Inpatient' and a.plc_of_svc_cd = '21 - Acute Hospital' and a.admit_cat_cd in ('17 - Medical') then 1
        when a.svc_setting = 'Inpatient' and a.plc_of_svc_cd = '21 - Acute Hospital' and a.admit_cat_cd in ('30 - Surgical') then 1
        else 0
      end as loc_flag
from hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
where a.fin_brand in ('M&R', 'C&S')
    and (a.transplant_flag != 'Y' or a.transplant_flag is null)
)
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
            and fin_product_level_3 not in ('DUAL', 'INSTITUTIONAL')
            and tfm_include_flag = 1
        then 'M&R FFS (excl. DSNP)'
        when entity in ('COSMOS', 'CSP')
            and global_cap = 'NA'
            and (
                (fin_brand = 'C&S' and migration_source != 'OAH' and fin_product_level_3 = 'DUAL')
                or (fin_brand = 'C&S' and to_varchar(coalesce(admit_dt_act, admit_dt_exp), 'yyyy') = '2024' and migration_source = 'OAH' and fin_state = 'MD')
                or (fin_brand != 'C&S' and to_varchar(coalesce(admit_dt_act, admit_dt_exp), 'yyyy') = '2024' and migration_source = 'OAH' and fin_market = 'MD')
            )
        then 'C&S DSNP'
        when fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 'M&R DSNP'
        else 'N/A'
      end as population
from auths
;
