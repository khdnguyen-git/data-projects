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
  	, b.business_segment
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
    and b.business_segment in ('MnR', 'CnS')
    and 
    (
	    (b.global_cap = 'NA' and b.fin_source_name = 'COSMOS')
	    or 
	    (b.fin_source_name = 'NICE' and b.nce_tadm_dec_risk_type = 'FFS') 
    )
    and avtar_mtch_ind = 1 -- added for consistency and not double-counting
;

-- MnR COSMOS FFS
select proc_cd, count(*) as n from tmp_1m.kn_transplant_auth as b
where b.global_cap = 'NA' 
	and b.fin_source_name = 'COSMOS'
	and migration_source != 'OAH'
	and tfm_include_flag = 1
	and b.fin_product_level_3 != 'INSTITUTIONAL'
group by proc_cd
order by n desc
limit 5;

--proc_cd	n
--99205	7,593
--50360	5,458
--47135	682
--38240	637
--0540T	485

select proc_cd, count(*) as n from tmp_1m.kn_transplant_auth as b
where fin_brand = 'C&S'
group by proc_cd
order by n desc
limit 5;

--proc_cd	n
--99205	4,643
--50360	4,161
--47135	284
--33945	250
--32853	115

select proc_cd, count(*) as n from tmp_1m.kn_transplant_auth as b
where migration_source = 'OAH'
group by proc_cd
order by n desc
limit 5;

--proc_cd	n
--50360	3,385
--99205	3,381
--33945	196
--47135	172
--32853	96




/*==============================================================================
 * Extract OP transplant claims
 * COSMOS
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_cosmos_claims_op;
create table tmp_1m.kn_transplant_cosmos_claims_op as
select
	'COSMOS' as data_source
    , a.cpt as proc_cd
    , b.component
    , b.gal_mbi_hicn_fnl as mbi
    , b.site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.adj_srvc_unit_cnt
    --, b.service_catg
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
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA' 
;

drop table if exists tmp_1m.kn_transplant_cosmos_claims_op_sum;
create table tmp_1m.kn_transplant_cosmos_claims_op_sum as
select
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
    , sum(b.allw_amt_fnl) as allowed
    , sum(b.adj_srvc_unit_cnt) as n_units
    , count(distinct concat(mbi, b.site_clm_aud_nbr)) as n_claims
from tmp_1m.kn_transplant_cosmos_claims_op as b
group by 
	data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
;

/*==============================================================================
 * Extract OP transplant claims
 * SMART
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_smart_claims_op;
create table tmp_1m.kn_transplant_smart_claims_op as
select
	'SMART' as data_source
    , a.cpt as proc_cd
    , b.component
    , b.gal_mbi_hicn_fnl as mbi
    , concat(b.site_cd, b.clm_aud_nbr) as site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.adj_srvc_unit_cnt
    --, b.service_catg
    , '' as srvc_prov_catgy_cd
    , b.hce_service_code as service_code
    , b.srvc_prov_id
    , b.prov_prtcp_sts_cd
    , '' as prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , 'NA' as migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    -- , b.state_fnl
    , b.region_fnl
    , substr(b.cust_seg_nbr, -5) as groupnumber
    , b.group_ind_fnl
	, b.full_nm as group_name
    , b.allw_amt_fnl
from tmp_1m.kn_transplant_cpt as a
join fichsrv.smart_op as b
    on  a.cpt = b.proc_cd
        and b.fst_srvc_year = 2024
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA' 
;

drop table if exists tmp_1m.kn_transplant_smart_claims_op_sum;
create table tmp_1m.kn_transplant_smart_claims_op_sum as
select
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
    , sum(b.allw_amt_fnl) as allowed
    , sum(b.adj_srvc_unit_cnt) as n_units
    , count(distinct concat(mbi, b.site_clm_aud_nbr)) as n_claims
from tmp_1m.kn_transplant_smart_claims_op as b
group by 
	data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
;


/*==============================================================================
 * Extract OP transplant claims
 * NICE
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_nice_claims_op;
create table tmp_1m.kn_transplant_nice_claims_op as
select
	'NICE' as data_source
    , a.cpt as proc_cd
    , b.component
    , b.mbi_hicn_fnl as mbi
    , concat(b.site_cd, b.clm_aud_nbr) as site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.srvc_unit_cnt as adj_srvc_unit_cnt
    --, b.service_catg
    , '' as srvc_prov_catgy_cd
    , b.hce_service_code as service_code
    , b.srvc_prov_id
    , b.prov_prtcp_sts_cd
    , b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.dec_risk_type_fnl as global_cap
    , 'NA' as migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    -- , b.state_fnl
    , b.region_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.full_nm as group_name
    , b.allw_amt_fnl
from tmp_1m.kn_transplant_cpt as a
join fichsrv.nice_op as b
    on  a.cpt = b.proc_cd
        and b.fst_srvc_year = 2024
        and b.brand_fnl in ('M&R', 'C&S')
        and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
;


drop table if exists tmp_1m.kn_transplant_nice_claims_op_sum;
create table tmp_1m.kn_transplant_nice_claims_op_sum as
select
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
    , sum(b.allw_amt_fnl) as allowed
    , sum(b.adj_srvc_unit_cnt) as n_units
    , count(distinct concat(mbi, b.site_clm_aud_nbr)) as n_claims
from tmp_1m.kn_transplant_nice_claims_op as b
group by 
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
;

drop table if exists tmp_1m.kn_transplant_op_claims;
create table tmp_1m.kn_transplant_op_claims as
select * from tmp_1m.kn_transplant_cosmos_claims_op_sum
union all
select * from tmp_1m.kn_transplant_smart_claims_op_sum
union all
select * from tmp_1m.kn_transplant_nice_claims_op_sum
;


/*==============================================================================
 * Extract PR transplant claims
 * COSMOS
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_cosmos_claims_pr;
create table tmp_1m.kn_transplant_cosmos_claims_pr as
select
	'COSMOS' as data_source
    , a.cpt as proc_cd
    , b.component
    , b.gal_mbi_hicn_fnl as mbi
    , b.site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.adj_srvc_unit_cnt
    --, b.service_catg
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
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA' 
;

drop table if exists tmp_1m.kn_transplant_cosmos_claims_pr_sum;
create table tmp_1m.kn_transplant_cosmos_claims_pr_sum as
select
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
    , sum(b.allw_amt_fnl) as allowed
    , sum(b.adj_srvc_unit_cnt) as n_units
    , count(distinct concat(mbi, b.site_clm_aud_nbr)) as n_claims
from tmp_1m.kn_transplant_cosmos_claims_pr as b
group by 
	data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
;

/*==============================================================================
 * Extract PR transplant claims
 * SMART
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_smart_claims_pr;
create table tmp_1m.kn_transplant_smart_claims_pr as
select
	'SMART' as data_source
    , a.cpt as proc_cd
    , b.component
    , b.gal_mbi_hicn_fnl as mbi
    , concat(b.site_cd, b.clm_aud_nbr) as site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.adj_srvc_unit_cnt
    --, b.service_catg
    , '' as srvc_prov_catgy_cd
    , b.service_code
    , b.srvc_prov_id
    , b.prov_prtcp_sts_cd
    , '' as prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , 'NA' as migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    -- , b.state_fnl
    , b.region_fnl
    , substr(b.cust_seg_nbr, -5) as groupnumber
    , b.group_ind_fnl
	, b.full_nm as group_name
    , b.allw_amt_fnl
from tmp_1m.kn_transplant_cpt as a
join fichsrv.smart_pr as b
    on  a.cpt = b.proc_cd
        and b.fst_srvc_year = 2024
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA' 
;

drop table if exists tmp_1m.kn_transplant_smart_claims_pr_sum;
create table tmp_1m.kn_transplant_smart_claims_pr_sum as
select
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
    , sum(b.allw_amt_fnl) as allowed
    , sum(b.adj_srvc_unit_cnt) as n_units
    , count(distinct concat(mbi, b.site_clm_aud_nbr)) as n_claims
from tmp_1m.kn_transplant_smart_claims_pr as b
group by 
	data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
;


/*==============================================================================
 * Extract PR transplant claims
 * NICE
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_nice_claims_pr;
create table tmp_1m.kn_transplant_nice_claims_pr as
select
	'NICE' as data_source
    , a.cpt as proc_cd
    , b.component
    , b.mbi_hicn_fnl as mbi
    , concat(b.site_cd, b.clm_aud_nbr) as site_clm_aud_nbr
    , b.clm_aud_nbr
    , b.clm_dnl_f
    , b.srvc_unit_cnt as adj_srvc_unit_cnt
    --, b.service_catg
    , '' as srvc_prov_catgy_cd
    , b.service_code
    , b.srvc_prov_id
    , b.prov_prtcp_sts_cd
    , b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.dec_risk_type_fnl as global_cap
    , 'NA' as migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    -- , b.state_fnl
    , b.region_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.full_nm as group_name
    , b.allw_amt_fnl
from tmp_1m.kn_transplant_cpt as a
join fichsrv.nice_pr as b
    on  a.cpt = b.proc_cd
        and b.fst_srvc_year = 2024
        and b.brand_fnl in ('M&R', 'C&S') 
		and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
;

drop table if exists tmp_1m.kn_transplant_nice_claims_pr_sum;
create table tmp_1m.kn_transplant_nice_claims_pr_sum as
select
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
    , b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
    , sum(b.allw_amt_fnl) as allowed
    , sum(b.adj_srvc_unit_cnt) as n_units
    , count(distinct concat(mbi, b.site_clm_aud_nbr)) as n_claims
from tmp_1m.kn_transplant_nice_claims_pr as b
group by 
	b.data_source
    , b.component
    , b.clm_dnl_f
    , b.product_level_3_fnl
    , b.plan_level_2_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    , b.region_fnl
  	, b.brand_fnl
    , b.groupnumber
    , b.group_ind_fnl
	, b.group_name
;

drop table if exists tmp_1m.kn_transplant_pr_claims;
create table tmp_1m.kn_transplant_pr_claims as
select * from tmp_1m.kn_transplant_cosmos_claims_pr_sum
union all
select * from tmp_1m.kn_transplant_smart_claims_pr_sum
union all
select * from tmp_1m.kn_transplant_nice_claims_pr_sum
;


/*==============================================================================
 * Union OP and PR claims
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_op_pr_claims;
create table tmp_1m.kn_transplant_op_pr_claims as
with union_claims as ( 
select distinct
	data_source
    , component
    , clm_dnl_f
    , product_level_3_fnl
    , plan_level_2_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , region_fnl
  	, brand_fnl
    , groupnumber
    , group_ind_fnl
	, group_name
	, allowed
	, n_units
	, n_claims
from tmp_1m.kn_transplant_op_claims
union all
select distinct
	data_source
    , component
    , clm_dnl_f
    , product_level_3_fnl
    , plan_level_2_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , region_fnl
  	, brand_fnl
    , groupnumber
    , group_ind_fnl
	, group_name
	, allowed
	, n_units
	, n_claims
from tmp_1m.kn_transplant_pr_claims
)
select
	data_source
    , component
    , clm_dnl_f
    , product_level_3_fnl
    , plan_level_2_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , region_fnl
  	, brand_fnl
    , groupnumber
    , group_ind_fnl
	, group_name
	, sum(allowed) as allowed
	, sum(n_units) as n_units
	, sum(n_claims) as n_claims
    , case
        when sum(allowed) < 0.01 then 'Denied'
        else 'Paid'
    end as clm_status
from union_claims
group by
	data_source
    , component
    , clm_dnl_f
    , product_level_3_fnl
    , plan_level_2_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , region_fnl
  	, brand_fnl
    , groupnumber
    , group_ind_fnl
	, group_name
;


select count(*) from tmp_1m.kn_transplant_op_pr_claims; -- 558606
select count(*) from tmp_1m.kn_transplant_op_claims; -- 16971
select count(*) from tmp_1m.kn_transplant_pr_claims; -- 541635


/*==============================================================================
 * Extract transplant IP claims
 * Join on Auth mbi
 *==============================================================================*/
drop table if exists tmp_1m.kn_transplant_cosmos_ip;
create table tmp_1m.kn_transplant_cosmos_ip as
select distinct
	'COSMOS' as data_source
    , a.medicare_id as mbi
    , a.case_status
    --, a.prim_diag_ahrq_genl_catgy_desc
    , a.transplant_flag as transplant_flag_auth
    , a.trans_cat_count 
    , a.transplantdate 
    , a.transplant_type 
    , a.medsurg_overlap_ind 
    , 'IP' as component
    --, b.site_clm_aud_nbr
    --, b.clm_aud_nbr
    --, b.clm_dnl_f
    --, b.adj_srvc_unit_cnt
    -- , b.service_catg ?
    --, b.srvc_prov_catgy_cd
    --, b.tadm_admit_type as service_code
    --, b.srvc_prov_id
    --, b.prov_prtcp_sts_cd
    --, b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , '' as product_level_2_fnl
    , b.plan_level_1_fnl
    , b.tfm_include_flag
    , b.global_cap
    , b.migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    --, b.state_fnl
    , '' as region_fnl
    , substr(b.cust_seg_nbr, -5) as groupnumber
    , b.group_ind_fnl
	-- b.full_nm as group_name
    --, b.clm_sts
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
    end as transplant_flag_clm
    , b.admit_num_of_claims
    , b.admit_num_of_paid_claims
    --, b.clm_admit_type
    , b.admitid
    --, b.admit_fnl_sts_pd
    , b.admit_ip_status_code as clm_dnl_f
    , b.masteradmitid
    , b.indexadmitind
    , b.readmit_ind
    , b.tadm_admits_fnl
    , coalesce(b.admit_allw_amt_fnl_org_sum, 0) +  coalesce(b.admit_allw_amt_fnl_adj_sum, 0) as admit_allw_amt_fnl
    --, sum(add)
from tmp_1m.kn_transplant_auth as a
join fichsrv.cosmos_ip_w_dnls_clm as b
    on  a.medicare_id = b.hicn
        and year(b.admit_start_dt) = 2024
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA'
;


drop table if exists tmp_1m.kn_transplant_cosmos_ip_sum;
create table tmp_1m.kn_transplant_cosmos_ip_sum as
select
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
    , count(distinct concat(mbi, admitid)) as n_claims
    , sum(admit_allw_amt_fnl) as allowed
from tmp_1m.kn_transplant_cosmos_ip
group by
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
;

select count(*) from tmp_1m.kn_transplant_cosmos_ip_sum; -- 25,630
select count(*) from tmp_1m.kn_transplant_smart_ip_sum; -- 799
select count(*) from tmp_1m.kn_transplant_nice_ip_sum; -- 120


-- SMART IP
drop table if exists tmp_1m.kn_transplant_smart_ip;
create table tmp_1m.kn_transplant_smart_ip as
select distinct
	'SMART' as data_source
    , a.medicare_id as mbi
    , a.case_status
    --, a.prim_diag_ahrq_genl_catgy_desc
    , a.transplant_flag as transplant_flag_auth
    , a.trans_cat_count 
    , a.transplantdate 
    , a.transplant_type 
    , a.medsurg_overlap_ind 
    , 'IP' as component
    --, b.site_clm_aud_nbr
    --, b.clm_aud_nbr
    --, b.clm_dnl_f
    --, b.adj_srvc_unit_cnt
    -- , b.service_catg ?
    --, b.srvc_prov_catgy_cd
    --, b.tadm_admit_type as service_code
    --, b.srvc_prov_id
    --, b.prov_prtcp_sts_cd
    --, b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , '' as product_level_2_fnl
    , b.plan_level_1_fnl
    , b.tfm_include_flag
    , b.global_cap
    , 'NA' as migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    --, b.state_fnl
    , '' as region_fnl
    , substr(b.cust_seg_nbr, -5) as groupnumber
    , b.group_ind_fnl
	-- b.full_nm as group_name
    --, b.clm_sts
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
    end as transplant_flag_clm
    , 0 as admit_num_of_claims
    , 0 as admit_num_of_paid_claims
    --, b.clm_admit_type
    , b.admitid
    --, b.admit_fnl_sts_pd
    , b.clm_dnl_f
    , '' as masteradmitid
    , 0 as indexadmitind
    , 0 as readmit_ind
    , b.allw_amt_fnl
    , tadm_admits as tadm_admits_fnl
    --, sum(add)
from tmp_1m.kn_transplant_auth as a
join fichsrv.smart_ip as b
    on  a.medicare_id = b.gal_mbi_hicn_fnl
        and year(b.admit_start_dt) = 2024
        and b.brand_fnl in ('M&R', 'C&S')
        and b.global_cap = 'NA'
;


drop table if exists tmp_1m.kn_transplant_smart_ip_sum;
create table tmp_1m.kn_transplant_smart_ip_sum as
select
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
    , count(distinct concat(mbi, admitid)) as n_claims
    , sum(allw_amt_fnl) as allowed
from tmp_1m.kn_transplant_smart_ip
group by
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
	, tadm_admits_fnl
;

-- NICE IP
drop table if exists tmp_1m.kn_transplant_nice_ip;
create table tmp_1m.kn_transplant_nice_ip as
select distinct
	'NICE' as data_source
    , a.medicare_id as mbi
    , a.case_status
    --, a.prim_diag_ahrq_genl_catgy_desc
    , a.transplant_flag as transplant_flag_auth
    , a.trans_cat_count 
    , a.transplantdate 
    , a.transplant_type 
    , a.medsurg_overlap_ind 
    , 'IP' as component
    --, b.site_clm_aud_nbr
    --, b.clm_aud_nbr
    --, b.clm_dnl_f
    --, b.adj_srvc_unit_cnt
    -- , b.service_catg ?
    --, b.srvc_prov_catgy_cd
    --, b.tadm_admit_type as service_code
    --, b.srvc_prov_id
    --, b.prov_prtcp_sts_cd
    --, b.prov_tin
    , b.brand_fnl
    , b.product_level_3_fnl
    , '' as product_level_2_fnl
    , b.plan_level_1_fnl
    , b.tfm_include_flag
    , b.dec_risk_type_fnl as global_cap
    , 'NA' as migration_source
    , b.contractpbp_fnl
    , b.market_fnl
    --, b.state_fnl
    , '' as region_fnl
    , cast(groupnumber as varchar(20)) as groupnumber
    , b.group_ind_fnl
	-- b.full_nm as group_name
    --, b.clm_sts
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
    end as transplant_flag_clm
    , 0 as admit_num_of_claims
    , 0 as admit_num_of_paid_claims
    --, b.clm_admit_type
    , b.admitid
    --, b.admit_fnl_sts_pd
    , b.denial_f as clm_dnl_f
    , '' as masteradmitid
    , 0 as indexadmitind
    , 0 as readmit_ind
    , tadm_admits as tadm_admits_fnl
    , b.allw_amt_fnl
    --, sum(add)
from tmp_1m.kn_transplant_auth as a
join fichsrv.nice_ip as b
    on  a.medicare_id = b.mbi_hicn_fnl
        and year(b.admit_start_dt) = 2024
        and b.brand_fnl in ('M&R', 'C&S')
        and b.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
;

drop table if exists tmp_1m.kn_transplant_nice_ip_sum;
create table tmp_1m.kn_transplant_nice_ip_sum as
select
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
    , count(distinct concat(mbi, admitid)) as n_claims
    , sum(allw_amt_fnl) as allowed
from tmp_1m.kn_transplant_nice_ip
group by
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
;


drop table if exists tmp_1m.kn_transplant_ip_claims;
create table tmp_1m.kn_transplant_ip_claims as
with union_claims as ( 
select distinct
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , cast(masteradmitid as varchar(20)) as masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
    , n_claims
    , allowed
from tmp_1m.kn_transplant_cosmos_ip_sum
union all
select distinct
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
    , n_claims
    , allowed
from tmp_1m.kn_transplant_smart_ip_sum
union all
select distinct
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
    , n_claims
    , allowed
from tmp_1m.kn_transplant_nice_ip_sum
)
select 
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
    , sum(n_claims) as n_claims
    , sum(allowed) as allowed
from union_claims
group by
	data_source
    , case_status
    , transplant_flag_auth
    , trans_cat_count 
    , transplantdate 
    , transplant_type 
    , medsurg_overlap_ind 
    , component
    , brand_fnl
    , product_level_3_fnl
    , plan_level_1_fnl
    , tfm_include_flag
    , global_cap
    , migration_source
    , contractpbp_fnl
    , market_fnl
    , groupnumber
    , group_ind_fnl
    , transplant_flag_clm
    , admit_num_of_claims
    , admit_num_of_paid_claims
    , clm_dnl_f
    , masteradmitid
    , indexadmitind
    , readmit_ind
    , tadm_admits_fnl
;

select count(*) from tmp_1m.kn_transplant_ip_claims; -- 26557
