-- OAH DSNP ADR Rate Comparison
-- Compare ADR rates: OAH DSNP vs M&R/C&S DSNP
-- 2024-2026 actual admit dates, LOC definition
-- Auth source: hce_ops_fnl.hce_adr_avtar_like_24_25_f (2024)
--             hce_ops_fnl.hce_adr_avtar_like_25_26_f (2025-2026)
-- Membership source: fichsrv.tre_membership
-- NOTE: H2406052 (OAH, GA) is tagged plan_pop = 'M&R' in PPT but
--       fin_brand = 'C&S' in the data. Kept as 'M&R' 

create or replace table tmp_1m.kn_oah_adr_comparison_202604 as
with plan_list as (
    select column1 as plan_id, column2 as study_group, column3 as mr_flag
    from values
        -- 45 OAH study PBPs
        ('H0432009','OAH',1), ('H0432013','OAH',1), ('H1889009','OAH',1), ('H2802044','OAH',1), ('H0271023','OAH',1), ('H0271024','OAH',1), ('H0271045','OAH',0), ('H0271046','OAH',0), ('H0624001','OAH',0)
        , ('H0271014','OAH',1), ('H0271059','OAH',1), ('H3113011','OAH',0), ('H3113013','OAH',0), ('H2228044','OAH',1), ('H2406052','OAH',1), ('H3256001','OAH',0), ('H3256002','OAH',0), ('H5322030','OAH',0)
        , ('H0169001','OAH',0), ('H0169002','OAH',0), ('H0169008','OAH',0), ('H0271029','OAH',0), ('H5253041','OAH',1), ('H5253116','OAH',1), ('H1889005','OAH',0), ('H0271060','OAH',0), ('H3387014','OAH',0)
        , ('H3387015','OAH',0), ('H0271055','OAH',0), ('H5253059','OAH',0), ('H5253122','OAH',0), ('H5322028','OAH',0), ('H5322034','OAH',0), ('H1889007','OAH',0), ('H3113009','OAH',0), ('H3113014','OAH',0)
        , ('H0271016','OAH',1), ('H0271056','OAH',1), ('H0271044','OAH',0), ('H5008002','OAH',0), ('H5008015','OAH',0), ('H0294027','OAH',0), ('H3794002','OAH',0), ('H3794004','OAH',0), ('H5253024','OAH',0)

        -- 48 UHC study PBPs
        , ('H0321002','UHC',0), ('H0321004','UHC',0), ('H1045012','UHC',1), ('H1045038','UHC',1), ('H1045039','UHC',0), ('H1889002','UHC',0), ('H2509001','UHC',0), ('H5420006','UHC',1), ('H2228043','UHC',0)
        , ('H0271005','UHC',1), ('H0271054','UHC',1), ('H0169004','UHC',0), ('H5322029','UHC',0), ('H1889008','UHC',0), ('H6595003','UHC',0), ('H6595004','UHC',0), ('H1889010','UHC',0), ('H1961003','UHC',0)
        , ('H1961019','UHC',0), ('H5008010','UHC',0), ('H0271006','UHC',1), ('H0271020','UHC',1), ('H0271028','UHC',0), ('H2247001','UHC',0), ('H2247003','UHC',0), ('H1889011','UHC',0), ('H5008011','UHC',0)
        , ('H5008016','UHC',0), ('H0271030','UHC',1), ('H0169003','UHC',0), ('H0169006','UHC',0), ('H0271050','UHC',0), ('H2802053','UHC',0), ('H3113005','UHC',0), ('H0271053','UHC',0), ('H5322031','UHC',0)
        , ('H5322033','UHC',0), ('H0764001','UHC',0), ('H3113010','UHC',0), ('H0271037','UHC',1), ('H0251002','UHC',0), ('H0251004','UHC',0), ('H1889006','UHC',0), ('H7464001','UHC',0), ('H7464005','UHC',0)
        , ('H7464006','UHC',0), ('H7464007','UHC',0), ('H0271013','UHC',1), ('H0271058','UHC',1), ('H0271042','UHC',1)
)

, auth_base as (
    select
        a.case_id
        , a.admit_dt_act
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , a.md_escalation_ind
        , coalesce(p.study_group, 'All') as study_group
        , p.mr_flag
        -- population segmentation (MD C&S carve-out excluded from OAH)
        , case
            when a.migration_source = 'OAH'
                and a.fin_product_level_3 = 'DUAL'
                and not (a.fin_brand = 'C&S' and a.fin_state = 'MD')
            then 'OAH DSNP'
            when a.fin_product_level_3 = 'DUAL'
                and a.global_cap = 'NA'
                and a.migration_source != 'OAH'
                and (
                    (a.fin_brand = 'M&R' and a.migration_source = 'NA')
                    or (a.fin_brand = 'C&S')
                )
            then 'M&R/C&S DSNP'
        end as gen_pop
    from (
        -- 2024 auths
        select
            case_id
            , admit_dt_act
            , initialfulladr_cases
            , persistentfulladr_cases
            , md_escalation_ind
            , migration_source
            , fin_product_level_3
            , fin_brand
            , fin_state
            , global_cap
            , fin_contractpbp
            , svc_setting
            , plc_of_svc_cd
            , admit_cat_cd
        from hce_ops_fnl.hce_adr_avtar_like_24_25_f
        where admit_dt_act between '2024-01-01' and '2024-12-31'
        union all
        -- 2025-2026 auths
        select
            case_id
            , admit_dt_act
            , initialfulladr_cases
            , persistentfulladr_cases
            , md_escalation_ind
            , migration_source
            , fin_product_level_3
            , fin_brand
            , fin_state
            , global_cap
            , fin_contractpbp
            , svc_setting
            , plc_of_svc_cd
            , admit_cat_cd
        from hce_ops_fnl.hce_adr_avtar_like_25_26_f
        where admit_dt_act between '2025-01-01' and '2026-04-30'
    ) as a
    -- join above plan_id with fin_contractpbp H####-###-### format
    left join plan_list as p
        on left(replace(a.fin_contractpbp, '-', ''), 8) = p.plan_id
    where a.fin_product_level_3 = 'DUAL'
        -- LOC definition: IP Acute Med/Surg only
        and a.svc_setting = 'Inpatient'
        and a.plc_of_svc_cd = '21 - Acute Hospital'
        and a.admit_cat_cd in ('17 - Medical', '30 - Surgical')
)
select
    'AUTH' as source
    , gen_pop
    , study_group
    , mr_flag
    , to_varchar(admit_dt_act, 'yyyyMM') as month
    , count(distinct case_id) as case_count
    , count(distinct case when initialfulladr_cases = 1 then case_id end) as initial_adr_cnt
    , count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt
    , count(distinct case when md_escalation_ind = 1 then case_id end) as md_escalation_cnt
    , null as member_months
from auth_base
where gen_pop is not null
group by gen_pop, study_group, mr_flag, to_varchar(admit_dt_act, 'yyyyMM')
union all
select 
    source
    , gen_pop
    , study_group
    , mr_flag
    , month
    , null -- case_count
    , null -- initial_adr_cnt
    , null -- persistent_adr_cnt
    , null -- md_escalation_cnt
    , member_months
from (
    select
        'MM' as source
        , case
            when a.migration_source = 'OAH'
                and a.fin_product_level_3 = 'DUAL'
                and not (a.fin_brand = 'C&S' and a.fin_state = 'MD')
            then 'OAH DSNP'
            when a.fin_product_level_3 = 'DUAL'
                and a.global_cap = 'NA'
                and a.migration_source != 'OAH'
                and (
                    (a.fin_brand = 'M&R' and a.migration_source = 'NA')
                    or (a.fin_brand = 'C&S')
                )
            then 'M&R/C&S DSNP'
        end as gen_pop
        , coalesce(p.study_group, 'All') as study_group
        , p.mr_flag
        , a.fin_inc_month as month
        , sum(a.fin_member_cnt) as member_months
    from fichsrv.tre_membership as a
    left join plan_list as p
        on left(replace(a.fin_contractpbp, '-', ''), 8) = p.plan_id
    where a.fin_inc_month between '202401' and '202604'
        and a.fin_brand in ('M&R', 'C&S')
        and a.fin_product_level_3 = 'DUAL'
    group by 1, 2, 3, 4, 5
) as mm
where gen_pop is not null
;

select * from tmp_1m.kn_oah_adr_comparison_202604;
-- 456