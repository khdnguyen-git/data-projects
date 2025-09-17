/*==============================================================================
 * Extract transplant Auths
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_auth;
create table tmp_1m.kn_transplant_auth as
select distinct
    b.medicare_id
    , b.case_id
    , a.cpt as proc_cd
    , b.admit_dt_act
    , b.dschg_dt_act
    , b.admit_dt_exp
    , b.dschg_dt_exp
    , case when b.svc_setting = 'Inpatient' then b.persistentfulladr_cases
   		else b.case_decn_stat_cd
   	end as case_status
    , b.initialfulladr_cases -- TBR
    , b.persistentfulladr_cases -- TBR
    , b.case_decn_stat_cd -- TBR
    , b.notif_recd_dttm
    , year(b.notif_recd_dttm) as notif_year
    , b.prim_diag_ahrq_genl_catgy_desc
    , b.transplant_flag -- TBR
    , b.trans_cat_count -- TBR
    , b.transplantdate -- TBR
    , b.transplant_type -- TBR
    , b.medsurg_overlap_ind -- TBR
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
    --, b.svc_seq_id
    , b.svc_setting
from tmp_1m.kn_transplant_cpt as a
join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
    on  a.cpt = b.proc_cd
	and year(b.notif_recd_dttm) = 2024
    and b.fin_product_level_3 != 'INSTITUTIONAL'
    and b.business_segment = 'MnR'
    and 
    (
	    (b.global_cap = 'NA' and b.fin_source_name = 'COSMOS')
	    or 
	    (b.fin_source_name = 'NICE' and b.nce_tadm_dec_risk_type = 'FFS') 
    )
;

/*==============================================================================
 * Extract OP transplant claims
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_cosmos_claims_op;
create table tmp_1m.kn_transplant_cosmos_claims_op as
select
    a.cpt as proc_cd
    , b.component
    , b.gal_mbi_hicn_fnl as mbi
    , b.site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.adj_srvc_unit_cnt
    -- , b.service_catg ?
    , b.srvc_prov_catgy_cd
    , b.hce_service_code as service_code
    , b.srvc_prov_id
    , b.prov_prtcp_sts_cd
    , b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    -- , b.state_fnl
    , b.region_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.full_nm as group_name
    , b.allw_amt_fnl
from tmp_1m.kn_transplant_cpt as a
join fichsrv.cosmos_op as b
    on  a.cpt = b.proc_cd
        and b.fst_srvc_year = 2024
        and b.product_level_3_fnl != 'INSTITUTIONAL'
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA' 
;

/*==============================================================================
 * Extract PR transplant claims
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_cosmos_claims_pr;
create table tmp_1m.kn_transplant_cosmos_claims_pr as
select
    a.cpt as proc_cd
    , b.component
    , b.gal_mbi_hicn_fnl as mbi
    , b.site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.adj_srvc_unit_cnt
    -- , b.service_catg ?
    , b.srvc_prov_catgy_cd
    , b.service_code
    , b.srvc_prov_id
    , b.prov_prtcp_sts_cd
    , b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    -- , b.state_fnl
    , b.region_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.full_nm as group_name
    , b.allw_amt_fnl
from tmp_1m.kn_transplant_cpt as a
join fichsrv.cosmos_pr as b
    on  a.cpt = b.proc_cd
        and b.fst_srvc_year = 2024
        and b.product_level_3_fnl != 'INSTITUTIONAL'
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA' 
;

/*==============================================================================
 * Union OP and PR claims
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_cosmos_op_pr;
create table tmp_1m.kn_transplant_cosmos_op_pr as
with union_claims as ( 
select 
    * 
from tmp_1m.kn_transplant_cosmos_claims_op
union all
select 
    * 
from tmp_1m.kn_transplant_cosmos_claims_pr
)
select
    proc_cd
    , component
    , mbi
    , site_clm_aud_nbr
    , clm_aud_nbr
    , clm_dnl_f
    , adj_srvc_unit_cnt
    , srvc_prov_catgy_cd
    , service_code
    , srvc_prov_id
    , prov_prtcp_sts_cd
    , prov_tin
    , brand_fnl
    , product_level_3_fnl
    , plan_level_2_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , region_fnl
    , groupnumber
    , group_ind_fnl
    , group_name
    , sum(allw_amt_fnl) as total_allowed
    , case
        when sum(allw_amt_fnl) < 0.01 then 'Denied'
        else 'Paid'
    end as clm_status
from union_claims
group by
    proc_cd
    , component
    , mbi
    , site_clm_aud_nbr
    , clm_aud_nbr
    , clm_dnl_f
    , adj_srvc_unit_cnt
    , srvc_prov_catgy_cd
    , service_code
    , srvc_prov_id
    , prov_prtcp_sts_cd
    , prov_tin
    , brand_fnl
    , product_level_3_fnl
    , plan_level_2_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , region_fnl
    , groupnumber
    , group_ind_fnl
    , group_name
;

/*==============================================================================
 * Extract transplant IP claims
 * Join on Auth mbi
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_cosmos_ip;
create table tmp_1m.kn_transplant_cosmos_ip as
select
    a.medicare_id as mbi
    , 'IP' as component
    , b.site_clm_aud_nbr
    --, b.clm_aud_nbr
    --, b.clm_dnl_f
    --, b.adj_srvc_unit_cnt
    -- , b.service_catg ?
    --, b.srvc_prov_catgy_cd
    , b.tadm_admit_type as service_code
    , b.srvc_prov_id
    , b.prov_prtcp_sts_cd
    , b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    --, b.plan_level_2_fnl
    , b.plan_level_1_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    --, b.state_fnl
    --, b.region_fnl
    --, b.groupnumber
    , substr(b.cust_seg_nbr, -5) as groupnumber
    , b.group_ind_fnl
	, b.full_nm as group_name
    , b.clm_sts
    , case
        when b.admit_drg_cd in ('00652', '00650', '00651') then 'Kidney'
        when b.admit_drg_cd in ('00005', '00006') then 'Liver'
        when b.admit_drg_cd in ('00007') then 'Lung'
        when b.admit_drg_cd in ('00001', '00002') then 'Heart'
        when b.admit_drg_cd in ('00010') then 'Pancreas'
        when b.admit_drg_cd in ('00008', '00019') then 'Pancreas & Kidney'
        when b.admit_drg_cd in ('00014', '00016', '00017') then 'Bone Marrow'
        when b.admit_drg_cd in ('00018') then 'CAR-T'
        else '0'
    end as transplant_flag
    , b.admit_num_of_claims
    , b.admit_num_of_paid_claims
    , b.clm_admit_type
    , b.admitid
    , b.admit_fnl_sts_pd
    , b.admit_ip_status_code
    , b.masteradmitid
    , b.indexadmitind
    , b.readmit_ind
	, b.clm_allw_amt_fnl_org
	, b.clm_allw_amt_fnl_adj
	, b.admit_allw_amt_fnl_org_sum
	, b.admit_allw_amt_fnl_adj_sum
from tmp_1m.kn_transplant_auth as a
join fichsrv.cosmos_ip_w_dnls_clm as b
    on  a.medicare_id = b.hicn
        and year(b.admit_start_dt) = 2024
        and b.product_level_3_fnl != 'INSTITUTIONAL'
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA'
;