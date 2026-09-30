-- Shift-Share ADR% Decomposition: Data Pull
-- Factors: ip_type, market, ahrq_dx, los_bucket, hospital_group
-- Population: M&R FFS (hard filter)
-- Period: 202301-202605 excluding 202403-202404
-- Dependencies: tmp_1m.knd_ahrq_crosswalk, tmp_1y.tin_collection


-- Step 1: Top 50 hospital groups (precomputed list used in CASE below)
-- See LOC_shiftshare_pull.sql header for reference; the top-50 list is baked into the query.


-- Step 2: Case-level base from all 3 source tables
create or replace table tmp_1m.knd_loc_shiftshare_base as
with top50_groups as (
    select collection as hospital_group
    from (
        select
            c.collection
            , count(*) as vol
        from (
            select substr(fa_prov_id, 2, 9) as prov_tin from hce_ops_archv.hce_adr_avtar_like_2023_f where fin_brand = 'M&R'
            union all
            select substr(fa_prov_id, 2, 9) from hce_ops_archv.hce_adr_avtar_like_2024_f where fin_brand = 'M&R'
            union all
            select substr(fa_prov_id, 2, 9) from hce_ops_fnl.hce_adr_avtar_like_25_26_f where fin_brand = 'M&R'
        ) as t
        left join tmp_1y.tin_collection as c on t.prov_tin = c.tin
        where c.collection is not null and trim(c.collection) != ''
        group by 1
        order by vol desc
        limit 50
    )
)

, src_2023 as (
    select
        a.case_id
        , a.svc_setting
        , a.plc_of_svc_cd
        , a.admit_cat_cd
        , a.case_cur_svc_cat_dtl_cd
        , a.fin_market
        , a.prim_diag_cd
        , coalesce(a.prim_diag_ahrq_genl_catgy_desc, xw.prim_diag_ahrq_genl_catgy_desc, 'Unmapped') as ahrq_genl_catgy
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , a.admit_dt_act
        , a.admit_dt_exp
        , a.dschg_dt_act
        , a.dschg_dt_exp
        , a.los
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , a.transplant_flag
        , a.admission_date
        , a.transplantdate
    from hce_ops_archv.hce_adr_avtar_like_2023_f as a
    left join tmp_1m.knd_ahrq_crosswalk as xw on a.prim_diag_cd = xw.prim_diag_cd
    where a.fin_brand = 'M&R'
        and (
            (a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP')
             and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
            or
            (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
)

, src_2024 as (
    select
        a.case_id
        , a.svc_setting
        , a.plc_of_svc_cd
        , a.admit_cat_cd
        , a.case_cur_svc_cat_dtl_cd
        , a.fin_market
        , a.prim_diag_cd
        , coalesce(nullif(trim(a.prim_diag_ahrq_genl_catgy_desc), ''), xw.prim_diag_ahrq_genl_catgy_desc, 'Unmapped') as ahrq_genl_catgy
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , a.admit_dt_act
        , a.admit_dt_exp
        , a.dschg_dt_act
        , a.dschg_dt_exp
        , a.los
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , a.transplant_flag
        , a.admission_date
        , a.transplantdate
    from hce_ops_archv.hce_adr_avtar_like_2024_f as a
    left join tmp_1m.knd_ahrq_crosswalk as xw on a.prim_diag_cd = xw.prim_diag_cd
    where a.fin_brand = 'M&R'
        and (
            (a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP')
             and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
            or
            (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
)

, src_2526 as (
    select
        a.case_id
        , a.svc_setting
        , a.plc_of_svc_cd
        , a.admit_cat_cd
        , a.case_cur_svc_cat_dtl_cd
        , a.fin_market
        , a.prim_diag_cd
        , coalesce(nullif(trim(a.prim_diag_ahrq_genl_catgy_desc), ''), xw.prim_diag_ahrq_genl_catgy_desc, 'Unmapped') as ahrq_genl_catgy
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , a.admit_dt_act
        , a.admit_dt_exp
        , a.dschg_dt_act
        , a.dschg_dt_exp
        , a.los
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , a.transplant_flag
        , a.admission_date
        , a.transplantdate
    from hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
    left join tmp_1m.knd_ahrq_crosswalk as xw on a.prim_diag_cd = xw.prim_diag_cd
    where a.fin_brand = 'M&R'
        and (
            (a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP')
             and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
            or
            (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
)

, all_cases as (
    select * from src_2023
    union all
    select * from src_2024
    union all
    select * from src_2526
)

, with_derived as (
    select
        a.case_id, a.svc_setting, a.plc_of_svc_cd, a.admit_cat_cd, a.case_cur_svc_cat_dtl_cd
        , a.fin_market, a.ahrq_genl_catgy, a.prov_tin
        , a.admit_dt_act, a.admit_dt_exp, a.los
        , a.initialfulladr_cases, a.persistentfulladr_cases
        , a.transplant_flag, a.admission_date, a.transplantdate
        , case
            when a.transplant_flag = 'Y' then 'Transplant'
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
            when a.transplant_flag = 'Y' and a.admit_dt_act is not null then a.admit_dt_act
            when a.transplant_flag = 'Y' and a.admit_dt_act is null and a.admission_date is not null then try_to_date(a.admission_date)
            when a.transplant_flag = 'Y' and a.admit_dt_act is null and a.admission_date is null then try_to_date(a.transplantdate)
            when a.svc_setting = 'Inpatient' and a.plc_of_svc_cd = '21 - Acute Hospital' then a.admit_dt_act
            when a.admit_dt_act is not null then a.admit_dt_act
            else a.admit_dt_exp
        end as hcedt
        , case
            when a.los <= 1 then '1'
            when a.los = 2 then '2'
            when a.los = 3 then '3'
            when a.los between 4 and 5 then '4-5'
            when a.los between 6 and 10 then '6-10'
            when a.los between 11 and 30 then '11-30'
            when a.los > 30 then '31+'
            else 'NA'
        end as los_bucket
        , coalesce(tc.collection, 'Other') as hospital_group_raw
    from all_cases as a
    left join tmp_1y.tin_collection as tc on a.prov_tin = tc.tin
)

, with_month as (
    select
        d.*
        , concat(lpad(year(d.hcedt), 4, '0'), lpad(month(d.hcedt), 2, '0')) as hce_admit_month
        , case
            when t.hospital_group is not null then d.hospital_group_raw
            else 'Other'
        end as hospital_group
    from with_derived as d
    left join top50_groups as t on d.hospital_group_raw = t.hospital_group
    where d.ip_type != 'NA'
        and (
            (d.ip_type in ('Medical', 'Surgical', 'Transplant') and to_varchar(d.admit_dt_act, 'MM/dd/yyyy') is not null)
            or d.ip_type in ('LTAC', 'SNF', 'AIR')
        )
        and d.hcedt is not null
        and concat(lpad(year(d.hcedt), 4, '0'), lpad(month(d.hcedt), 2, '0')) between '202301' and '202605'
        and concat(lpad(year(d.hcedt), 4, '0'), lpad(month(d.hcedt), 2, '0')) not in ('202403', '202404')
)

, agg_ip_type as (
    select
        'ip_type' as factor_name
        , ip_type as factor_value
        , hce_admit_month
        , count(distinct case_id) as case_count
        , count(distinct case when initialfulladr_cases = 1 then case_id end) as initial_adr_cnt
        , count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt
    from with_month
    group by 1, 2, 3
)

, agg_market as (
    select
        'market' as factor_name
        , fin_market as factor_value
        , hce_admit_month
        , count(distinct case_id) as case_count
        , count(distinct case when initialfulladr_cases = 1 then case_id end) as initial_adr_cnt
        , count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt
    from with_month
    group by 1, 2, 3
)

, agg_ahrq as (
    select
        'ahrq_dx' as factor_name
        , ahrq_genl_catgy as factor_value
        , hce_admit_month
        , count(distinct case_id) as case_count
        , count(distinct case when initialfulladr_cases = 1 then case_id end) as initial_adr_cnt
        , count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt
    from with_month
    group by 1, 2, 3
)

, agg_los as (
    select
        'los_bucket' as factor_name
        , los_bucket as factor_value
        , hce_admit_month
        , count(distinct case_id) as case_count
        , count(distinct case when initialfulladr_cases = 1 then case_id end) as initial_adr_cnt
        , count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt
    from with_month
    group by 1, 2, 3
)

, agg_hospital as (
    select
        'hospital_group' as factor_name
        , hospital_group as factor_value
        , hce_admit_month
        , count(distinct case_id) as case_count
        , count(distinct case when initialfulladr_cases = 1 then case_id end) as initial_adr_cnt
        , count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt
    from with_month
    group by 1, 2, 3
)

select * from agg_ip_type
union all
select * from agg_market
union all
select * from agg_ahrq
union all
select * from agg_los
union all
select * from agg_hospital
;