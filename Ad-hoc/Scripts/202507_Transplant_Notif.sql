drop table if exists tmp_1m.kn_transplant_auth;
create table tmp_1m.kn_transplant_auth as
select
    b.medicare_id
    , b.case_id
    , a.cpt as proc_cd
    , b.admit_dt_act
    , b.dschg_dt_act
    , b.admit_dt_exp
    , b.dschg_dt_exp
    , b.initialfulladr_cases
    , b.persistentfulladr_cases
    , b.notif_recd_dttm
    , b.prim_diag_ahrq_genl_catgy_desc
    , b.transplant_flag
    , b.trans_cat_count
    , b.transplantdate
    , b.transplant_type
    , b.medsurg_overlap_ind
    , b.fin_source_name
    , b.migration_source
    , b.fin_product_level_3
    , b.tfm_include_flag
    , b.global_cap
    , b.nce_tadm_dec_risk_type
    , b.fin_contractpbp
    , b.fin_contract_nbr
    , b.fin_pbp
    , b.fin_submarket
    , b.fin_market
    , b.fin_region
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_g_i
    , b.fin_brand
    , b.group_number
    , b.group_name
from tmp_1m.kn_transplant_cpt as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
    on  a.cpt = b.proc_cd
where year(b.notif_recd_dttm) >= 2024
    and b.fin_product_level_3 != 'INSTITUTIONAL'
    and 
    (
	    (b.global_cap = 'NA' and b.fin_source_name = 'COSMOS')
	    or 
	    (b.fin_source_name = 'NICE' and b.nce_tadm_dec_risk_type = 'FFS') 
    )
;
select count(*) from tmp_1m.kn_transplant_auth; -- 89,617

select count(distinct medicare_id) from tmp_1m.kn_transplant_auth; -- 47,618


select count(*) from tmp_1m.kn_transplant_auth
where year(notif_recd_dttm) = 2024; -- 48,472

select count(distinct medicare_id) from tmp_1m.kn_transplant_auth
where year(notif_recd_dttm) = 2024; -- 27,626

select count(distinct case_id) from tmp_1m.kn_transplant_auth
where year(notif_recd_dttm) = 2024; -- 34,334

select count(distinct proc_cd) from tmp_1m.kn_transplant_auth; -- 52
select count(cpt) from tmp_1m.kn_transplant_cpt; -- 82
select count(distinct cpt) from tmp_1m.kn_transplant_cpt; -- 82

