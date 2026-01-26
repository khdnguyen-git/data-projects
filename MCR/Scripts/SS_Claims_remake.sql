--select * from tmp_1y.cl_ss_codelist_20250827;



--select * from fichsrv.cosmos_op
limit 100;
-- site_cd = substr(1,3)

--select * from fichsrv.cosmos_pr
limit 100;

--select * from tadm_tre_cpy.dcsp_op_f_202510
limit 100;

--select * from tadm_tre_cpy.dcsp_pr_f_202510
limit 100;


--select * from fichsrv.nice_op
limit 100;
-- site_cd = substr(1,3)

--select * from fichsrv.nice_pr
limit 100;



/*================================== BEGIN OF CLAIMS QUERY ============================================*/
----select * from tadm_tre_cpy.glxy_op_f_202505
--COSMOS PR AND OP
--drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes;
create or replace table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes as 
select  
	'COSMOS' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component 
	,a.hce_service_code 
	,a.ahrq_diag_dtl_catgy_desc 
	--,case when brand_fnl = 'C&S' then a.st_abbr_cd else a.market_fnl end as market 
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd
	, a.site_cd
	,a.site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_new_fnl
	,a.migration_source 
	,a.global_cap 
	,b.covered_unproven
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.gal_mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.hce_month as mth 
	,a.fst_srvc_year as years 
	,a.clm_pd_dt, a.clm_dnl_f
	,a.primary_diag_cd
	--,fnl_rsn_cd_sys_id
from fichsrv.cosmos_op   a  --fichsrv.cosmos_op   tadm_tre_cpy.glxy_op_f_202502  
join tmp_1y.cl_ss_codelist_20250827 b  --tmp_1y.cl_ss_codelist_20241028   cl_ss_codelist_20240901
on	trim(a.proc_cd) = trim(b.hcpcs_code)
where    
	a.brand_fnl in ('M&R', 'C&S')
	and a.hce_month >= '202201'	
	--and a.clm_dnl_f = 'N'
	and a.GLOBAL_CAP = 'NA'
union all 
select 
	'COSMOS' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component
	,a.service_code
	,a.ahrq_diag_dtl_catgy_desc 
	--,case when brand_fnl = 'C&S' then a.st_abbr_cd else a.market_fnl end as market 
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd 
	, a.site_cd
	,a.site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_new_fnl
	,a.migration_source
	,a.global_cap 
	,b.covered_unproven
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.gal_mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.fst_srvc_month as mth 
	,a.fst_srvc_year as years 
	,a.clm_pd_dt, a.clm_dnl_f
	,a.primary_diag_cd
	--,fnl_rsn_cd_sys_id
from fichsrv.cosmos_pr a  --fichsrv.cosmos_pr    tadm_tre_cpy.glxy_pr_f_202502
join tmp_1y.cl_ss_codelist_20250827 b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl in ('M&R', 'C&S')
	and a.fst_srvc_month >= '202201'
	--and a.clm_dnl_f = 'N'
	and a.GLOBAL_CAP = 'NA'
--SMART OP AND PR	
union all
select 
	'CSP' as Entity_Source
	,a.brand_fnl	
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component 
	,a.hce_service_code 
	,a.ahrq_diag_dtl_catgy_desc 
	--,case when brand_fnl = 'C&S' then a.st_abbr_cd else a.market_fnl end as market  
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.tin 
	,a.full_nm
	,a.st_abbr_cd 
	, a.site_cd
	,a.clm_aud_nbr as site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_fnl
	,a.migration_source 
	,a.global_cap
	,b.covered_unproven
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.gal_mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.fst_srvc_month as mth 
	,a.fst_srvc_year as years 
	,a.clm_pd_dt, a.clm_dnl_f
	,a.primary_diag_cd
from tadm_tre_cpy.dcsp_op_f_202510 a
join tmp_1y.cl_ss_codelist_20250827 b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 	
	a.brand_fnl ='C&S'
	and a.hce_month >= '202201'
	--and	a.CLM_DNL_F = 'N'     -- select distinct CLM_DNL_F FROM tadm_tre_cpy.dcsp_op_f_202501 --this field has 3 values: Y, D, N
	and a.GLOBAL_CAP = 'NA'   -- select distinct GLOBAL_CAP FROM tadm_tre_cpy.dcsp_op_f_202501 --this field has 2 values: NA, WM
union all
select 
	'CSP' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component
	,a.service_code
	,a.ahrq_diag_dtl_catgy_desc 
	--,case when brand_fnl = 'C&S' then a.st_abbr_cd else a.market_fnl end as market  
	,a.group_ind_fnl 
	,a._nbr 
	,a.tin 
	,a.full_nm
	,a.st_abbr_cd 
	, a.site_cd
	,a.clm_aud_nbr as site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_fnl
	,a.migration_source
	,a.global_cap 
	,b.covered_unproven
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.gal_mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.fst_srvc_month as mth 
	,a.fst_srvc_year as years 
	,a.clm_pd_dt, a.clm_dnl_f
	,a.primary_diag_cd
from tadm_tre_cpy.dcsp_pr_f_202510 a      ----select * FROM tadm_tre_cpy.dcsp_pr_f_202502 LIMIT 2;
join tmp_1y.cl_ss_codelist_20250827 b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl ='C&S'
    and a.fst_srvc_month >= '202201'
	--and a.clm_dnl_f = 'N' 
	and a.GLOBAL_CAP = 'NA'
--NICE CLAIMS
union all
select  
	'NICE' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.mbi_hicn_fnl as gal_mbi_hicn_fnl  --different
	,a.component 
	,a.hce_service_code 
	,a.ahrq_diag_dtl_catgy_desc 
	--,case when brand_fnl = 'C&S' then a.st_abbr_cd else a.market_fnl end as market 
	,a.group_ind_fnl 
	,a.mbi_hicn_fnl as sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd 
	, a.site_cd
	,a.clm_aud_nbr --different
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_fnl
	,'NA' as migration_source 
	,a.clm_cap_flag as global_cap --different 
	--,'M&R FFS' as entity
	,b.covered_unproven
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.srvc_unit_cnt as adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.hce_month as mth 
	,a.fst_srvc_year as years 
	,a.clm_pd_dt, a.clm_dnl_f
	,a.primary_diag_cd
	--,0 as CnS_Dual_Flag
	--,0 as OAH_FLAG
from fichsrv.nice_op   a    --tadm_tre_cpy.nce_op_dtl_f_202502 
join tmp_1y.cl_ss_codelist_20250827 b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl = 'M&R'
	and a.hce_month >= '202201'	
	--and a.clm_dnl_f = 'N'
	and a.clm_cap_flag = 'FFS'  --this field has 2 values: 'FFS' or 'ENC'
union all 
select 
	'NICE' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.mbi_hicn_fnl as gal_mbi_hicn_fnl
	,a.component
	,a.service_code as hce_service_code
	,a.ahrq_diag_dtl_catgy_desc 
	--,case when brand_fnl = 'C&S' then a.st_abbr_cd else a.market_fnl end as market 
	,a.group_ind_fnl 
	,a.mbi_hicn_fnl as sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd 
	, a.site_cd
	,a.clm_aud_nbr --different
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_fnl
	,'NA' as migration_source
	,a.clm_cap_flag as global_cap 
	--,'M&R FFS' as entity
	,b.covered_unproven
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.srvc_unit_cnt as adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.fst_srvc_month as mth 
	,a.fst_srvc_year as years
	,a.clm_pd_dt, a.clm_dnl_f
	,a.primary_diag_cd
	--,0 as CnS_Dual_Flag
	--,0 as OAH_FLAG
from fichsrv.nice_pr      a  -- fichsrv.nice_pr   tadm_tre_cpy.nce_pr_dtl_f_202502 
join tmp_1y.cl_ss_codelist_20250827 b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl = 'M&R'
	and a.fst_srvc_month >= '202201'
	--and a.clm_dnl_f = 'P'
	and a.clm_cap_flag = 'FFS'   --this field has 2 values: 'FFS' or 'ENC'
;





--select distinct clm_dnl_f from fichsrv.nice_pr
--SELECT Entity_Source, count(*) FROM tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes GROUP BY entity_source
/*================================== END OF CLAIMS QUERY (6 mins)============================================*/
--120102  117663 125716    --select count(*) from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes --



--adding in upper v lower extremity
/*
create or replace table tmp_1y.cl_skin_subs_mapping as
select distinct 
	icd_code, 
	location_type
from tmp_1y.ec_skin_subs_mapping

select distinct location_type from tmp_1y.cl_skin_subs_mapping
*/ 

--drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2;
create or replace table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2 as 
select 
	a.*
	,b.location_type
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes as a
left join tmp_1y.cl_skin_subs_mapping as b
on a.primary_diag_cd=b.icd_code
;
/*
select icd_code, count(*) as cnt 
from tmp_1y.cl_skin_subs_mapping
group by icd_code 
having count(*) >1
--0
*/
--select distinct location_type from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_2

----drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a_t;
--drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a;
create or replace table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as 
select 
	a.*
	,case when a.location_type is null then 'unknown'
	      when a.location_type in ('Upper', 'upper') then 'upper'
	      else a.location_type end as locationtype
	,b.market
	,b.fin_market
	,b.fin_state
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2 a
left join tmp_7d.cl_ss_mbi_market b
	on 	a.gal_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
	and a.mth = b.fin_inc_month
;

--select * from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a
limit 100;




use secondary role all;



, a.work_item_id as clm_aud_nbr
, a.u_div as site_cd
, a.mcr_disposition_code as mcr_decision
, a.urgency
, a.uhg_received_date
, a.routing_date
, a.route_reason
, a.uhc_dept_received_date
, a.u_resolved_date_reporting
, a.u_due_date_reporting
, a.resolved_by
, a.resolved_at
, a.resolution_code
, a.resolution_comments
, a.sys_class_name
, a.state
, a.source_system
, a.skills
, a.business_segment
, a.business_area
, a.*


--select * from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
limit 100;

--drop table if exists tmp_1m.kn_ss_mcr_join;
create or replace table tmp_1m.kn_ss_mcr_join as
with mcr_2025 as (
select 
    *
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where year(resolved_at) >= 2025
)
select
	a.work_item_id as clm_aud_nbr
	, a.u_div as site_cd
	, a.mcr_disposition_code as mcr_decision
	, a.urgency
	, a.uhg_received_date
	, a.routing_date
	, a.route_reason
	, a.uhc_dept_received_date
	, a.u_resolved_date_reporting
	, a.u_due_date_reporting
	, a.resolved_by
	, a.resolved_at
	, a.resolution_code
	, a.resolution_comments
	, a.sys_class_name
	, a.state
	, a.source_system
	, a.skills
	, a.business_segment
	, a.business_area
	, a.*
    , b.brand_fnl
    , b.site_clm_aud_nbr
from mcr_2025 as a
join ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
	on (a.member_id = b.sbscr_nbr 
		or a.member_id = substring(b.sbscr_nbr, 3)
		)
    and 
    (a.work_item_id = b.site_clm_aud_nbr -- 120
         or regexp_substr(a.work_item_id, '^[0-9]+') = regexp_substr(b.site_clm_aud_nbr, '^.{4}(\\d+.*)', 1, 1, 'e', 1)
         or regexp_substr(a.work_item_id, '^[0-9]+') = regexp_substr(b.site_clm_aud_nbr, '^.{3}(\\d+.*)', 1, 1, 'e', 1)
         or regexp_substr(a.work_item_id, '[0-9]{10}$') = regexp_substr(b.site_clm_aud_nbr, '^.{3}(\\d+.*)', 1, 1, 'e', 1)
         or regexp_substr(a.work_item_id, '[0-9]{11}$') = regexp_substr(b.site_clm_aud_nbr, '^.{4}(\\d+.*)', 1, 1, 'e', 1)
    )
    and (a.u_div = b.site_cd
    	or substring(a.u_div, 1,2) = b.site_cd)
    -- a.u_div = b.st_abbr_cd -- 675535719
;
    
-- on a.member_id = b.sbscr_nbr -- 57723
-- a.member_id = substring(b.sbscr_nbr, 3) -- 42881
-- on a.u_div = b.st_abbr_cd -- 0
-- on a.u_div = b.site_cd -- 675535719
-- on substring(a.u_div, 1,2) = b.site_cd)
-- on  a.work_item_id = b.site_clm_aud_nbr -- 120
--         or regexp_substr(a.work_item_id, '^[0-9]+') = regexp_substr(b.site_clm_aud_nbr, '^.{4}(\\d+.*)', 1, 1, 'e', 1) -- 668
--         or regexp_substr(a.work_item_id, '^[0-9]+') = regexp_substr(b.site_clm_aud_nbr, '^.{3}(\\d+.*)', 1, 1, 'e', 1) -- 64
--         or regexp_substr(a.work_item_id, '[0-9]{10}$') = regexp_substr(b.site_clm_aud_nbr, '^.{3}(\\d+.*)', 1, 1, 'e', 1) -- 62
    
   
select member_id, u_div, work_item_id from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
limit 200;

select sbscr_nbr, site_cd, site_clm_aud_nbr from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a
limit 200;

select distinct u_div from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work

select distinct work_item_id from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where business_area = 'MCR MR'
order by work_item_id desc

select distinct business_area  from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
    


-- 100604
--select count(*) from (
with mcr_2025 as (
select 
    *
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where year(resolved_at) >= 2025
	and member_id is not null
	and work_item_id is not null
	and u_div is not null
)
select
	a.work_item_id as clm_aud_nbr
	, a.u_div as site_cd
	, a.mcr_disposition_code as mcr_decision
	, a.business_area
    , b.brand_fnl
    , b.site_clm_aud_nbr
from mcr_2025 as a
join ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
	on (a.member_id = b.sbscr_nbr 
		or a.member_id = substring(b.sbscr_nbr, 3)
		)
) as j
;

/*==============================================================================
 * Only includes 2025 MCR data, and non-null ID for joining
 *==============================================================================*/
--drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_2025;
create or replace table ving_prd_trend_db.tmp_1m.kn_mcr_2025 as
select 
	*
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where year(resolved_at) >= 2025
	and member_id is not null
	and work_item_id is not null
;

-- Join 1: member_id and u_div
-- 192731
--drop table if exists tmp_1m.kn_mcr_ss_join_test_1 ;
create or replace table tmp_1m.kn_mcr_ss_join_test_1 as
select
	a.work_item_id as clm_aud_nbr
	, a.u_div as site_cd
	, a.mcr_disposition_code as mcr_decision
	, a.urgency
	, a.uhg_received_date
	, a.routing_date
	, a.route_reason
	, a.uhc_dept_received_date
	, a.u_resolved_date_reporting
	, a.u_due_date_reporting
	, a.resolved_by
	, a.resolved_at
	, a.resolution_code
	, a.resolution_comments
	, a.sys_class_name
	, a.state
	, a.source_system
	, a.skills
	, a.member_id
	, a.business_segment
	, a.business_area
    , b.brand_fnl
    , b.site_clm_aud_nbr
from ving_prd_trend_db.tmp_1m.kn_mcr_2025 as a
join ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
	on (a.member_id = b.sbscr_nbr -- 57839
		or a.member_id = substring(b.sbscr_nbr, 3) -- 290850
		)
	and (a.u_div = b.site_cd
		or substring(a.work_item_id, 1, 2) = b.site_cd
		or substring(a.work_item_id, 1, 3) = b.site_cd
		)
--	and (
--    	)
;

-- Join 2: member_id and clm_aud_nbr
-- 99
--drop table if exists tmp_1m.kn_mcr_ss_join_test_2;
create or replace table tmp_1m.kn_mcr_ss_join_test_2 as
select
	a.work_item_id as clm_aud_nbr
	, a.u_div as site_cd
	, a.mcr_disposition_code as mcr_decision
	, a.urgency
	, a.uhg_received_date
	, a.routing_date
	, a.route_reason
	, a.uhc_dept_received_date
	, a.u_resolved_date_reporting
	, a.u_due_date_reporting
	, a.resolved_by
	, a.resolved_at
	, a.resolution_code
	, a.resolution_comments
	, a.sys_class_name
	, a.state
	, a.source_system
	, a.skills
	, a.member_id
	, a.business_segment
	, a.business_area
    , b.brand_fnl
    , b.site_clm_aud_nbr
from ving_prd_trend_db.tmp_1m.kn_mcr_2025 as a
join ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
	on (a.member_id = b.sbscr_nbr -- 57839
		or a.member_id = substring(b.sbscr_nbr, 3) -- 290850
		)
	and (a.work_item_id = b.site_clm_aud_nbr -- 99
		or regexp_substr(a.work_item_id, '^[^-]+') = b.site_clm_aud_nbr)
	)
         or regexp_substr(a.work_item_id, '^[0-9]+') = substring(b.site_clm_aud_nbr, 4) -- first sequence of digits
         or regexp_substr(a.work_item_id, '^[0-9]+') = substring(b.site_clm_aud_nbr, 3)
         or regexp_substr(a.work_item_id, '[0-9]{10}$') = substring(b.site_clm_aud_nbr, 4) -- last 10 digits
         or regexp_substr(a.work_item_id, '[0-9]{11}$') = substring(b.site_clm_aud_nbr, 4)
		 or regexp_substr(a.work_item_id, '[0-9]{10}$') = substring(b.site_clm_aud_nbr, 3)
         or regexp_substr(a.work_item_id, '[0-9]{11}$') = substring(b.site_clm_aud_nbr, 3)
		)
;

-- a.work_item_id = b.site_clm_aud_nbr works for NICE
-- regexp_substr(a.work_item_id, '^[^-]+') = substring(b.site_clm_aud_nbr, 6) works for COSMOS


select site_cd from fichsrv.nice_op;

-- Join  1: 8 digits
-- 7434 
--drop table if exists tmp_1m.kn_mcr_ss_join_3;
create or replace table tmp_1m.kn_mcr_ss_join_3 as
with mcr_1 as (
select
	a.work_item_id
	, regexp_substr(a.work_item_id, '^[^-]+') as work_item_id_2 -- anything before -
	, regexp_substr(a.work_item_id, '[0-9]+') as work_item_id_3 -- first number sequence
	, a.member_id
	, a.u_div
	, substring(work_item_id, 1, 2) as u_div_2
	, substring(work_item_id, 1, 3) as u_div_3
from ving_prd_trend_db.tmp_1m.kn_mcr_2025 as a
),
ss_1 as (
select 
	b.site_clm_aud_nbr
	, substring(b.site_clm_aud_nbr, 6) as clm_aud_nbr_2
	, regexp_substr(b.site_clm_aud_nbr, '[0-9]+') as clm_aud_nbr_3
	, regexp_substr(b.site_clm_aud_nbr, '^.{4}(\\d+.*)', 1, 1, 'e', 1) as clm_aud_nbr_4
	, b.sbscr_nbr
	, substring(b.sbscr_nbr, 3) as sbscr_nbr_3
	, b.site_cd
	, substring(b.site_clm_aud_nbr, 1, 2) as site_cd_2
	, substring(b.site_clm_aud_nbr, 1, 3) as site_cd_3
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
)
select
	a.*
	, b.*
	, '1' as join_type
from mcr_1 as a
join ss_1 as b
on (a.member_id = b.sbscr_nbr
	 or a.member_id = sbscr_nbr_3
	)
and (a.work_item_id = b.site_clm_aud_nbr)
union all
select
	a.*
	, b.*
	, '2' as join_type
from mcr_1 as a
join ss_1 as b
on (a.member_id = b.sbscr_nbr
	 or a.member_id = sbscr_nbr_3
	)
and (a.work_item_id_2 = b.clm_aud_nbr_2)
order by member_id;

--select * from tmp_1m.kn_mcr_ss_join_3;

--select count(*) from tmp_1m.kn_mcr_ss_join_3;
-- 99

-- COSMOS
-- SMART 
-- NICE
-- clm_aud_nbr
-- 

--drop table if exists tmp_1m.kn_mcr_ss_join_4;
create or replace table tmp_1m.kn_mcr_ss_join_4 as
with mcr_1 as (
select
	a.work_item_id
	, regexp_substr(a.work_item_id, '^[^-]+') as work_item_id_2 -- anything before -
	, regexp_substr(a.work_item_id, '[0-9]+') as work_item_id_3 -- first number sequence
	, a.member_id
	, a.u_div
	, substring(work_item_id, 1, 2) as u_div_2
	, substring(work_item_id, 1, 3) as u_div_3
from ving_prd_trend_db.tmp_1m.kn_mcr_2025 as a
),
ss_1 as (
select 
	b.site_clm_aud_nbr
	, substring(b.site_clm_aud_nbr, 6) as clm_aud_nbr_2
	, regexp_substr(b.site_clm_aud_nbr, '[0-9]+') as clm_aud_nbr_3
	, regexp_substr(b.site_clm_aud_nbr, '^.{4}(\\d+.*)', 1, 1, 'e', 1) as clm_aud_nbr_4
	, b.sbscr_nbr
	, substring(b.sbscr_nbr, 3) as sbscr_nbr_3
	, b.site_cd
	, substring(b.site_clm_aud_nbr, 1, 2) as site_cd_2
	, substring(b.site_clm_aud_nbr, 1, 3) as site_cd_3
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
)
select
	a.*
	, b.*
from mcr_1 as a
join ss_1 as b
on (a.member_id = b.sbscr_nbr
	 or a.member_id = sbscr_nbr_3
	)
and position(b.clm_aud_nbr_2 in a.work_item_id) > 0
;


-- 348689
--drop table if exists tmp_1m.kn_mcr_ss_join_mmid;
create or replace table tmp_1m.kn_mcr_ss_join_mmid as
with mcr_1 as (
select
	a.work_item_id
	, regexp_substr(a.work_item_id, '^[^-]+') as work_item_id_2 -- anything before -
	, regexp_substr(a.work_item_id, '[0-9]+') as work_item_id_3 -- first number sequence
	, a.member_id
	, a.u_div
	, substring(work_item_id, 1, 2) as u_div_2
	, substring(work_item_id, 1, 3) as u_div_3
from ving_prd_trend_db.tmp_1m.kn_mcr_2025 as a
),
ss_1 as (
select 
	b.site_clm_aud_nbr
	, substring(b.site_clm_aud_nbr, 6) as clm_aud_nbr_2
	, regexp_substr(b.site_clm_aud_nbr, '[0-9]+') as clm_aud_nbr_3
	, regexp_substr(b.site_clm_aud_nbr, '^.{4}(\\d+.*)', 1, 1, 'e', 1) as clm_aud_nbr_4
	, b.sbscr_nbr
	, substring(b.sbscr_nbr, 3) as sbscr_nbr_3
	, b.site_cd
	, substring(b.site_clm_aud_nbr, 1, 2) as site_cd_2
	, substring(b.site_clm_aud_nbr, 1, 3) as site_cd_3
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
)
select
	a.*
	, b.*
from mcr_1 as a
join ss_1 as b
on (a.member_id = b.sbscr_nbr
	 or a.member_id = sbscr_nbr_3
	)
;

--select count(*) from tmp_1m.kn_mcr_ss_join_mmid;


use secondary role all;

--select * from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a;
--select * from ving_prd_trend_db.tmp_1m.kn_mcr_2025;

select
    b.entity_source
  , b.brand_fnl
  , b.proc_cd
  , b.gal_mbi_hicn_fnl
  , b.component
  , b.hce_service_code
  , b.ahrq_diag_dtl_catgy_
  , b.group_ind_fnl
  , b.sbscr_nbr
  , b.prov_tin
  , b.full_nm
  , b.st_abbr_cd
  , b.site_cd
  , b.site_clm_aud_nbr
  , b.prov_prtcp_sts_cd
  , b.tfm_include_flag
  , b.product_level_3_fnl
  , b.tfm_product_new_fnl
  , b.migration_source
  , b.global_cap
  , b.covered_unproven
  , b.sbmt_chrg_amt
  , b.allw_amt_fnl
  , b.net_pd_amt_fnl
  , b.adj_srvc_unit_cnt
  , b.tadm_units
  , b.ekp
  , b.mth
  , b.years
  , b.clm_pd_dt
  , b.primary_diag_cd
  , b.location_type
  , b.locationtype
  , b.market
  , b.fin_market
  , b.fin_state
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b;



--select count(*) from tmp_1m.kn_mcr_ss_join; -- 6376

--drop table if exists tmp_1m.kn_mcr_ss_join;
create or replace table tmp_1m.kn_mcr_ss_join as
with mcr_1 as (
select
	a.work_item_id
	, regexp_substr(a.work_item_id, '^[^-]+') as work_item_id_2 -- anything before -
	, a.member_id
	, a.u_div
	, substring(work_item_id, 1, 2) as u_div_2
	, a.mcr_disposition_code as mcr_decision
	, a.account
    , a.active
    , a.active_account_escalation
    , a.active_escalation
    , a.activity_due
    , a.additional_assignee_list
    , a.age_date
    , a.age_date_central
    , a.approval
    , a.approval_history
    , a.asset
    , a.assigned_on
    , a.assigned_to
    , a.assigned_to_user_id
    , a.assignment_batch
    , a.assignment_group
    , a.auto_close
    , a.business_area
    , a.business_duration
    , a.business_segment
    , a.business_service
    , a.calendar_duration
    , a.cancel_comments
    , a.case_report
    , a.cases
    , a.category
    , a.cause
    , a.caused_by
    , a.changes
    , a.close_notes
    , a.closed_at
    , a.closed_by
    , a.cmdb_ci
    , a.comments
    , a.comments_and_work_notes
    , a.company
    , a.company_days_aged
    , a.consumer
    , a.contact
    , a.contact_local_time
    , a.contact_time_zone
    , a.contact_type
    , a.contract
    , a.correlation_display
    , a.correlation_id
    , a.created_central
    , a.current_age
    , a.delivery_plan
    , a.delivery_task
    , a.dept_days_aged
    , a.description
    , a.due_date
    , a.entitlement
    , a.escalate
    , a.escalation
    , a.ewd_time_worked
    , a.expected_start
    , a.first_response_time
    , a.firstpass_rework_indicator
    , a.follow_the_sun
    , a.follow_up
    , a.geostate
    , a.group_list
    , a.impact
    , a.import_source
    , a.knowledge
    , a.location
    , a.made_sla
    , a.non_workable_to_workable
    , a.notes_to_comments
    , a.notify
    , a.number
    , a.opened_at
    , a.opened_by
    , a.orders
    , a.parent
    , a.partner
    , a.partner_contact
    , a.planning_queue
    , a.primary_skill
    , a.priority
    , a.priority_classification
    , a.proactive
    , a.problem
    , a.product
    , a.project
    , a.reassignment_count
    , a.requestor_comments
    , a.resolution_code
    , a.resolution_comments
    , a.resolved_at
    , a.resolved_by
    , a.route_reason
    , a.routing_date
    , a.service_offering
    , a.short_description
    , a.skill_id
    , a.skills
    , a.sla_due
    , a.sn_app_cs_social_social_profile
    , a.source_system
    , a.special_processing
    , a.special_processing_rule
    , a.state
    , a.subcategory
    , a.support_manager
    , a.sync_driver
    , a.sys_class_name
    , a.sys_created_by
    , a.sys_created_on
    , a.sys_domain
    , a.sys_domain_path
    , a.sys_mod_count
    , a.sys_tags
    , a.sys_updated_by
    , a.sys_updated_on
    , a.task_effective_number
    , a.time_worked
    , a.tw_min_dec
    , a.tw_sec_dec
    , a.u_current_inhouse_age
    , a.u_due_date_reporting
    , a.u_member
    , a.u_percent_complete
    , a.u_provider
    , a.u_region
    , a.u_resolved_date_reporting
    , a.u_updated_date_reporting
    , a.u_warehoused_on
    , a.u_workitem_level
    , a.uhc_dept_received_date
    , a.uhg_received_date
    , a.universal_request
    , a.upon_approval
    , a.upon_reject
    , a.urgency
    , a.user_input
    , a.warehouse_issued
    , a.watch_list
    , a.work_end
    , a.work_item_type
    , a.work_notes
    , a.work_notes_list
    , a.work_start
    , a.work_warehouse_issued
    , a.work_warehoused
    , a.x_uhgen_ewr_work_rule_log
    , a.tpsm_description
    , a.custom_identifier
    , a.claim_id
    , a.policy_number
    , a.enterprisenow_task
    , a.enterprisenow_issue_number
    , a.resolution_status
    , a.additional_comments
    , a.seq_no
    , a.primary
from ving_prd_trend_db.tmp_1m.kn_mcr_2025 as a
),
ss_1 as (
select 
	b.site_clm_aud_nbr
    , substring(b.site_clm_aud_nbr, 6) as site_clm_aud_nbr_2
    , b.sbscr_nbr
    , substring(b.sbscr_nbr, 3) as sbscr_nbr_2
    , b.site_cd
    , substring(b.site_clm_aud_nbr, 1, 2) as site_cd_2
   	, substring(b.site_clm_aud_nbr, 1, 3) as site_cd_3
    , b.entity_source
    , b.brand_fnl
    , b.proc_cd
    , b.gal_mbi_hicn_fnl
    , b.component
    , b.hce_service_code
    , b.ahrq_diag_dtl_catgy_desc
    , b.group_ind_fnl
    , b.prov_tin
    , b.full_nm
    , b.st_abbr_cd
    , b.prov_prtcp_sts_cd
    , b.tfm_include_flag
    , b.product_level_3_fnl
    , b.tfm_product_new_fnl
    , b.migration_source
    , b.global_cap
    , b.covered_unproven
    , b.sbmt_chrg_amt
    , b.allw_amt_fnl
    , b.net_pd_amt_fnl
    , b.adj_srvc_unit_cnt
    , b.tadm_units
    , b.ekp
    , b.mth
    , b.years
    , b.clm_pd_dt
    , b.primary_diag_cd
    , b.location_type
    , b.locationtype
    , b.market
    , b.fin_market
    , b.fin_state
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
),
-- 99 matches
join_1 as (
select
	a.*
	, b.*
from mcr_1 as a
join ss_1 as b
	on (a.member_id = b.sbscr_nbr
		 or a.member_id = sbscr_nbr_2
		)
	and (a.work_item_id = b.site_clm_aud_nbr)
),
-- 6277 matches
join_2 as (
select
	a.*
	, b.*
from mcr_1 as a
join ss_1 as b
	on (a.member_id = b.sbscr_nbr
		 or a.member_id = sbscr_nbr_2
		)
	and (a.work_item_id_2 = b.site_clm_aud_nbr_2)
)
select distinct
	*
from join_1
union all
select distinct
	*
from join_2
;

--select count(*) from tmp_1m.kn_mcr_ss_join;
-- 6376

--select count(*) from (
select member_id, work_item_id, site_clm_aud_nbr, work_item_id_2, site_clm_aud_nbr_2, count(*)
from tmp_1m.kn_mcr_ss_join
group by member_id, work_item_id, site_clm_aud_nbr, work_item_id_2, site_clm_aud_nbr_2
order by member_id
) as se

