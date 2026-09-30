-- P2P overturn by hospital_group x month
-- Source: kn_ip_dataset_loc_08192026_od, M&R FFS, 202501+
-- Output: kn_loc_p2p_hospital_monthly_08192026

create or replace table tmp_1m.kn_loc_p2p_hospital_monthly_08192026 as
with agg as (
    select
        hospital_group
        , prov_tin
        , fin_market
        , admit_act_month
        , sum(case_count) as case_count
        , sum(initial_adr_cnt) as initial_adr_cnt
        , sum(persistent_adr_cnt) as persistent_adr_cnt
        , sum(p2p_case_cnt) as p2p_case_cnt
        , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
    from tmp_1m.kn_ip_dataset_loc_08192026_od
    where population = 'M&R FFS'
        and admit_act_month >= '202501'
        and hospital_group is not null
    group by hospital_group, prov_tin, fin_market, admit_act_month
)
, prov_nm as (
    select prov_tin, max(full_nm) as full_nm
    from fichsrv.glxy_ip_admit_f
    where prov_tin is not null
    group by prov_tin
)
select
    a.hospital_group
    , a.prov_tin
    , p.full_nm
    , a.fin_market
    , a.admit_act_month
    , a.case_count
    , a.initial_adr_cnt
    , a.persistent_adr_cnt
    , a.p2p_case_cnt
    , a.p2p_ovrtn_case_cnt
from agg as a
left join prov_nm as p
    on a.prov_tin = p.prov_tin
order by a.hospital_group, a.prov_tin, a.admit_act_month
;


-- V2: AVTAR source with fa_prov_id
-- Source: 25_26 AVTAR (202501+), M&R FFS, LOC, icm_ever_owned = 1
-- Output: kn_loc_p2p_hospital_monthly_v2_08192026

create or replace table tmp_1m.kn_loc_p2p_hospital_monthly_v2_08192026 as
with base as (
    select
        a.fa_prov_id
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , a.fin_market
        , to_char(a.admit_dt_act, 'YYYYMM') as admit_act_month
        , a.case_id
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , a.p2p_full_evertouched_cnt
        , a.p2p_full_ovtn
    from hce_ops_fnl.hce_adr_avtar_like_25_26_f as a
    where a.svc_setting = 'Inpatient'
        and a.plc_of_svc_cd = '21 - Acute Hospital'
        and a.admit_cat_cd in ('17 - Medical', '30 - Surgical')
        and a.admit_dt_act >= '2025-01-01'
        and a.global_cap = 'NA'
        and a.fin_brand = 'M&R'
        and a.tfm_include_flag = 1
        and a.sgr_source_name in ('COSMOS', 'CSP')
        and a.fin_product_level_3 != 'INSTITUTIONAL'
        and a.icm_ever_owned = 1
)
, agg as (
    select
        b.fa_prov_id
        , b.prov_tin
        , b.fin_market
        , b.admit_act_month
        , count(distinct b.case_id) as case_count
        , count(distinct case when b.initialfulladr_cases = 1 then b.case_id end) as initial_adr_cnt
        , count(distinct case when b.persistentfulladr_cases = 1 then b.case_id end) as persistent_adr_cnt
        , count(distinct case when b.p2p_full_evertouched_cnt = 1 then b.case_id end) as p2p_case_cnt
        , count(distinct case when b.p2p_full_ovtn = 1 then b.case_id end) as p2p_ovrtn_case_cnt
    from base as b
    group by b.fa_prov_id, b.prov_tin, b.fin_market, b.admit_act_month
)
, prov_nm as (
    select prov_tin, max(full_nm) as full_nm
    from fichsrv.glxy_ip_admit_f
    where prov_tin is not null
    group by prov_tin
)
select
    tc.collection as hospital_group
    , a.fa_prov_id
    , a.prov_tin
    , p.full_nm
    , a.fin_market
    , a.admit_act_month
    , a.case_count
    , a.initial_adr_cnt
    , a.persistent_adr_cnt
    , a.p2p_case_cnt
    , a.p2p_ovrtn_case_cnt
from agg as a
left join tmp_1y.tin_collection as tc
    on a.prov_tin = tc.tin
left join prov_nm as p
    on a.prov_tin = p.prov_tin
where tc.collection is not null
order by tc.collection, a.fa_prov_id, a.admit_act_month
;
