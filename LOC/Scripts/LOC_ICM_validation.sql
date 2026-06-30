-- ICM by Hospital Group and Year

create or replace table tmp_1m.loc_hospitals_icm_2025_2026 as
with base as (
    select
        a.case_id
        , year(coalesce(a.admit_dt_act, a.admit_dt_exp)) as admit_yr
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , a.admit_cat_cd
        , a.icm_ever_owned
        , a.icm_md_reviewed_ind
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
    from ving_prd_trend_db.tmp_7d.hce_adr_avtar_like_25_26_f as a
    where a.svc_setting = 'Inpatient'
        and a.plc_of_svc_cd = '21 - Acute Hospital'
        and a.admit_cat_cd in ('17 - Medical', '30 - Surgical')
        and (
            (a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
                and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
            or (a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
                and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
)
select
    b.admit_yr
    , c.collection as hospital_group
    , b.admit_cat_cd
    , b.icm_ever_owned
    , case when (
        c.collection ilike '%advocate%health%hospital%il%'
        or c.collection ilike '%advocate%northside%'
        or c.collection ilike '%ascension%mi%'
        or c.collection ilike '%health care authority%ann%'
        or c.collection ilike '%mayo%'
        or c.collection ilike '%mosaic%'
        or c.collection ilike '%self regional%'
        or c.collection ilike '%university of pennsylvania%'
        or c.collection ilike '%catholic healthcare west%'
    ) then 1 else 0 end as dan_list
    , count(distinct b.case_id) as case_count
    , sum(b.icm_md_reviewed_ind) as md_reviewed_cnt
    , sum(b.initialfulladr_cases) as initial_adr_cnt
    , sum(b.persistentfulladr_cases) as persistent_adr_cnt
from base as b
left join ving_prd_trend_db.tmp_1y.tin_collection as c
    on b.prov_tin = c.tin
group by
    b.admit_yr
    , c.collection
    , b.admit_cat_cd
    , b.icm_ever_owned
    , dan_list
order by
    b.admit_yr
    , c.collection
    , b.admit_cat_cd
    , b.icm_ever_owned
;



select * from tmp_1m.loc_hospitals_icm_2025_2026

select 
    hospital_group
    , icm_ever_owned
    , admit_cat_cd
    , admit_yr
    , sum(case_count) as case_count
    , sum(md_reviewed_cnt) as md_escalation_cnt 
from tmp_1m.loc_hospitals_icm_2025_2026
where icm_ever_owned = 1 and hospital_group ilike '%advocate%' and hospital_group not ilike '%pmo%'
group by 1, 2, 3, 4

-- HOSPITAL_GROUP	ICM_EVER_OWNED	ADMIT_CAT_CD	ADMIT_YR	CASE_COUNT	MD_ESCALATION_CNT
-- Mosaic Life Care	1.0000000000	17 - Medical	2025	75	39
-- Mosaic Life Care	1.0000000000	30 - Surgical	2025	2	2
-- Mosaic Life Care	1.0000000000	17 - Medical	2026	23	17

-- Mosaic - 1
-- Advocate -- 1




select 
    hospital_group
    , icm_ever_owned
    , admit_cat_cd
    , admit_yr
    , sum(case_count) as case_count
    , sum(md_reviewed_cnt) as md_escalation_cnt 
from tmp_1m.loc_hospitals_icm_2025_2026
where hospital_group ilike '%self regional%' and hospital_group not ilike '%pmo%'
group by 1, 2, 3, 4
order by admit_yr

select 
    hospital_group
    , icm_ever_owned
    , admit_cat_cd
    , admit_yr
    , sum(case_count) as case_count
    , sum(md_reviewed_cnt) as md_escalation_cnt 
from tmp_1m.loc_hospitals_icm_2025_2026
where hospital_group ilike '%self regional%' and hospital_group not ilike '%pmo%'
group by 1, 2, 3, 4
order by admit_yr


-- All target hospitals with dan_list flag
select
    hospital_group
    , icm_ever_owned
    , admit_cat_cd
    , admit_yr
    , case when (
        hospital_group ilike '%advocate%health%hospital%il%'
        or hospital_group ilike '%advocate%northside%'
        or hospital_group ilike '%ascension%mi%'
        or hospital_group ilike '%health care authority%ann%'
        or hospital_group ilike '%mayo%'
        or hospital_group ilike '%mosaic%'
        or hospital_group ilike '%self regional%'
        or hospital_group ilike '%university of pennsylvania%'
        or hospital_group ilike '%catholic healthcare west%'
    ) then 1 else 0 end as dan_list
    , sum(case_count) as case_count
    , sum(md_reviewed_cnt) as md_escalation_cnt
from tmp_1m.loc_hospitals_icm_2025_2026
group by 1, 2, 3, 4, 5
order by dan_list desc, hospital_group, admit_yr, admit_cat_cd
;

-- Ratio of icm_ever_owned = 0 to total by year
with yearly as (
    select
        year(coalesce(admit_dt_act, admit_dt_exp)) as admit_yr
        , count(distinct case_id) as total_cases
        , count(distinct case when icm_ever_owned = 0 then case_id end) as icm_zero_cases
    from ving_prd_trend_db.tmp_7d.hce_adr_avtar_like_25_26_f
    where svc_setting = 'Inpatient'
        and plc_of_svc_cd = '21 - Acute Hospital'
        and admit_cat_cd in ('17 - Medical', '30 - Surgical')
        and (
            (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS'
                and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
            or (fin_brand = 'M&R' and sgr_source_name = 'NICE'
                and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
    group by admit_yr
)
select
    admit_yr
    , icm_zero_cases
    , total_cases
    , round(icm_zero_cases / nullif(total_cases, 0), 4) as icm_zero_ratio
from yearly
order by admit_yr
;