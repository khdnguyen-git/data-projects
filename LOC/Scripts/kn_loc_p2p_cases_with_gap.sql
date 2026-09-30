create or replace table tmp_1m.kn_loc_p2p_cases_with_gap as
with base as (
    select distinct
        fin_mbi_hicn_fnl
        , case_id
        , p2p_full_evertouched_cnt
        , p2p_full_ovtn
        , p2p_match_ind
        , initialfulladr_cases
        , persistentfulladr_cases
        , initial_dnl_decn_dttm
        , latest_dnl_decn_dttm
        , notif_recd_dttm
        , admit_dt_act
        , admit_dt_exp
        , dschg_dt_act
        , dschg_dt_exp
        , icm_ever_owned
        , icm_md_reviewed_ind
        , fa_prov_id
    from hce_ops_fnl.hce_adr_avtar_like_25_26_f
    where initial_dnl_decn_dttm is not null
        and admit_dt_act >= '2025-06-01'
        -- LOC
        and svc_setting = 'Inpatient'
        and plc_of_svc_cd = '21 - Acute Hospital'
        and admit_cat_cd in ('17 - Medical', '30 - Surgical')
        -- M&R FFS
        and global_cap = 'NA'
        and fin_brand = 'M&R'
        and tfm_include_flag = 1
        and sgr_source_name in ('COSMOS', 'CSP')
        and fin_product_level_3 != 'INSTITUTIONAL'
)
select
    a.fin_mbi_hicn_fnl as medicare_id
    , a.case_id
    , a.p2p_full_evertouched_cnt
    , a.p2p_full_ovtn
    , a.p2p_match_ind
    , substr(a.fa_prov_id, 2, 9) as prov_tin
    , a.initialfulladr_cases
    , a.persistentfulladr_cases
    , a.initial_dnl_decn_dttm
    , a.latest_dnl_decn_dttm
    , a.notif_recd_dttm as notif_dt
    , to_char(a.admit_dt_act, 'YYYYMM') as admit_month
    , a.admit_dt_act
    , a.admit_dt_exp
    , a.dschg_dt_act
    , a.dschg_dt_exp
    , a.icm_ever_owned
    , a.icm_md_reviewed_ind
    , datediff('day', a.notif_recd_dttm, a.admit_dt_act) as days_notif_to_admit
    , datediff('day', a.notif_recd_dttm, a.initial_dnl_decn_dttm) as days_notif_to_init_decn
    , datediff('day', a.notif_recd_dttm, a.latest_dnl_decn_dttm) as days_notif_to_last_decn
    , datediff('day', a.initial_dnl_decn_dttm, a.latest_dnl_decn_dttm) as days_init_decn_to_last_decn
from base as a
;


select count(*) from tmp_1m.kn_loc_p2p_cases_with_gap;
-- 446,479

-- -- Separate query: join first approval date from service decision table
-- select
--     g.*
--     , fa.first_approval_dttm
--     , datediff('day', g.initial_dnl_decn_dttm, fa.first_approval_dttm) as days_denial_to_approval
-- from tmp_1m.kn_loc_p2p_cases_with_gap as g
-- left join (
--     select
--         d.case_key
--         , min(d.decn_dttm) as first_approval_dttm
--     from hce_ops_stage.hceops_sd_adr_trans_serv_decn_curr_1 as d
--     inner join tmp_1m.kn_loc_p2p_cases_with_gap as b
--         on concat('HSR-', b.case_id) = d.case_key
--     where d.decn_otcm_cd = '1'
--         and d.decn_dttm > b.initial_dnl_decn_dttm
--         and d.decn_dttm < '2026-07-01'
--     group by
--         d.case_key
-- ) as fa
--     on concat('HSR-', g.case_id) = fa.case_key
-- ;
