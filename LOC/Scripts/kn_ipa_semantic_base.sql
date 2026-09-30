create or replace table tmp_1m.kn_ipa_semantic_base as
with base as (
    select
        a.case_id
        , a.fin_contract_nbr
        , a.svc_setting
        , a.prim_diag_cd
        , a.proc_cd
        , a.prim_proc_ind
        , a.admit_act_qtr
        , a.hce_admit_month as admit_act_month
        , left(a.hce_admit_month, 4) as admit_year
        , a.admit_week
        , a.icm_ever_owned
        , a.loc_flag
        , a.fin_market
        , a.fin_brand
        , a.fin_product_level_3
        , a.fin_state
        , a.global_cap
        , a.sgr_source_name
        , a.migration_source
        , a.tfm_include_flag
        , a.nce_tadm_dec_risk_type
        , a.group_name
        , tc.collection as hospital_group
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , a.mnr_hce_drv_par_status as par_nonpar
        -- los categories
        , case
            when a.los <= 1 then '1'
            when a.los = 2 then '2'
            when a.los = 3 then '3'
            when a.los between 4 and 5 then '4-5'
            when a.los between 6 and 10 then '6-10'
            when a.los between 11 and 30 then '11-30'
            when a.los > 30 then '31+'
            else 'NA'
        end as los_categories
        -- respiratory flag
        , case
            when a.prim_diag_cd in ('B97.29','J02.48','U07.1','J12.82','J12.81') then 'COVID-19'
            when a.prim_diag_cd in (
                '079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486',
                '487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1',
                '488.11','488.12','488.19','490','780.6','780.60','786.2','B97.10','B97.89',
                'H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09',
                'J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10','J10.0','J10.00','J10.01',
                'J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11',
                'J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83',
                'J11.89','J12.0','J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8',
                'J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8',
                'J20.9','J22','J40','J41.0','J41.1','J41.8','J80','J98.8','R05','R05.1',
                'R05.2','R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83',
                'A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90'
            ) then 'ILI'
            else 'NA'
        end as respiratory_flag
        -- population (consolidated from flags)
        , case
            when a.migration_source = 'OAH'
                and not (a.fin_brand = 'C&S' and year(a.hce_dt) = 2024 and a.fin_state = 'MD')
            then 'OAH'
            when a.fin_product_level_3 = 'INSTITUTIONAL' then 'INSTITUTIONAL'
            when (a.business_segment = 'CnS' and a.fin_brand = 'C&S' and a.migration_source != 'OAH'
                and a.global_cap = 'NA' and a.fin_product_level_3 = 'DUAL'
                and a.sgr_source_name in ('COSMOS','CSP'))
                or (year(a.hce_dt) = 2024 and a.business_segment = 'CnS' and a.fin_brand = 'C&S'
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
        -- ipa/pac flag
        , case
            when sw.tin is not null then 'PAC'
            when a.ip_type in ('LTAC','SNF','AIR') then 'PAC'
            when a.ip_type in ('Medical','Surgical','Transplant') then 'IPA'
            else 'NA'
        end as ipa_pac_flag
        -- ipa li split
        , case
            when a.ip_type = 'Transplant' then 'Transplant'
            when a.prim_diag_cd in ('B97.29','J02.48','U07.1','J12.82','J12.81') then 'COVID-19'
            when a.prim_diag_cd in (
                '079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486',
                '487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1',
                '488.11','488.12','488.19','490','780.6','780.60','786.2','B97.10','B97.89',
                'H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09',
                'J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10','J10.0','J10.00','J10.01',
                'J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11',
                'J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83',
                'J11.89','J12.0','J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8',
                'J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8',
                'J20.9','J22','J40','J41.0','J41.1','J41.8','J80','J98.8','R05','R05.1',
                'R05.2','R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83',
                'A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90'
            ) then 'ILI'
            when a.admit_cat_cd = '17 - Medical' then 'Medical'
            when a.admit_cat_cd = '30 - Surgical' then 'Surgical'
            when a.ip_type = 'LTAC' then 'LTAC'
            when a.ip_type = 'SNF' then 'SNF'
            when a.ip_type = 'AIR' then 'AIR'
            else 'Medical'
        end as ipa_li_split
        -- metric indicators (0/1)
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , a.icm_md_reviewed_ind
        , a.p2p_full_evertouched_cnt
        , a.p2p_full_ovtn
        , a.appeal_ind
        , a.appeal_ovrtn_ind
        , a.mcr_reconsideration_ind
        , a.mcr_ovtrn_ind
        , a.member_appeal_ind
        , a.member_appeal_ovtn_ind
    from tmp_1m.ec_avtar_25_26_3_od as a
    left join tmp_1y.tin_collection as tc
        on substr(a.fa_prov_id, 2, 9) = tc.tin
    left join tmp_1y.hk_swingbed_2026 as sw
        on substr(a.fa_prov_id, 2, 9) = sw.tin
)
select *
from base
where admit_act_month >= '202401'
;