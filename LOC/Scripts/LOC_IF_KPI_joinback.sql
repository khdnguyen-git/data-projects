-- =============================================================================
-- LOC IF KPI JOIN-BACK
-- Case-level detail for flagged outlier hospitals.
-- Aggregation (weekly/monthly/yearly) done in Excel PivotTable.
-- KPIs are 0/1 indicators — sum them in pivot to get counts.
-- Source: TMP_1M.KN_LOC_IF_OUTLIER_HOSPITALS_MNR (written by LOC_IF_shap.ipynb)
-- =============================================================================

create or replace table ving_prd_trend_db.tmp_1m.kn_loc_if_outlier_kpis_mnr as

with outliers as (
    select distinct
        hospital_group
        , fin_market
        , mad_z_driver
        , shap_driver
        , anomaly_rank
        , anomaly_score
    from ving_prd_trend_db.tmp_1m.kn_loc_if_outlier_hospitals_mnr
)

, auth_base as (
    select
        d.collection as hospital_group
        , substr(a.fa_prov_id, 2, 9) as prov_tin
        , a.fin_market
        , a.fin_submarket
        , a.fin_region
        , a.fin_state
        , a.fin_contractpbp
        , a.fin_plan_level_2
        , a.fin_g_i
        , a.entity
        , a.business_segment
        , a.case_id
        , a.case_category_cd
        , a.svc_setting
        , a.ip_type
        , a.loc_flag
        , a.hce_admit_month
        , a.admit_week
        , left(a.hce_admit_month, 4) as admit_year
        , a.hce_dt
        , a.admit_dt_act
        , a.dschg_dt_act
        , a.los
        , a.notif_recd_dttm
        , a.fa_prov_clm_id
        , a.prim_diag_cd
        , a.prim_diag_ahrq_genl_catgy_desc
        , a.prim_diag_ahrq_diag_dtl_catgy_desc
        , a.proc_cd
        , a.auth_typ_cd
        , a.channel_cd
        , a.case_init_decn_cd
        , a.case_svc_init_decn_cd
        , a.case_decn_stat_cd
        , a.case_svc_decn_stat_cd
        , a.initialfulladr_cases
        , a.persistentfulladr_cases
        , a.p2p_full_ovtn
        , a.appeal_ind
        , a.appeal_ovrtn_ind
        , a.mcr_ovtrn_ind
        , a.mcr_reconsideration_ind
        , a.icm_md_reviewed_ind
        , a.md_escalation_ind
        , a.member_appeal_ind
        , a.member_appeal_ovtn_ind
        , a.rvsl_ind
        , a.oth_ovrtn_ind
        , a.p2p_match_ind
        , a.fin_mbi_hicn_fnl
        , a.member_id
        , a.member_state
        , a.initial_dnl_decn_userid
        , a.initial_dnl_decn_user_role
        , a.initial_dnl_decn_dttm
        , a.latest_dnl_decn_userid
        , a.latest_dnl_decn_user_role
        , a.latest_dnl_decn_dttm
    from ving_prd_trend_db.tmp_1m.ec_avtar_25_26_3_od as a
    left join ving_prd_trend_db.tmp_1y.tin_collection as d
        on substr(a.fa_prov_id, 2, 9) = d.tin
    where a.fin_brand = 'M&R'
        and a.loc_flag = 1
        and (
            (a.ip_type in ('Medical', 'Surgical', 'Transplant')
             and to_varchar(a.admit_dt_act, 'MM/dd/yyyy') is not null)
            or a.ip_type in ('LTAC', 'SNF', 'AIR')
        )
        and (
            (a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
             and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
            or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
        and d.collection is not null
)

select
    b.*
    , o.mad_z_driver
    , o.shap_driver
    , o.anomaly_rank
    , o.anomaly_score
from auth_base as b
inner join outliers as o
    on b.hospital_group = o.hospital_group
    and b.fin_market = o.fin_market
order by o.anomaly_rank, b.hce_admit_month, b.admit_week
;
