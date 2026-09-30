--desc table fichsrv.cosmos_ip
--Skin Substitute, live on 9/1/2024 
--Post service, pre payment >>> MCR (medical claims review)
--10/28/2024 updated: 202 proc codes (previously 179 Proc Codes); of which only 5 are proven effective
--11/12/2024 updated "covered" coded from 5 to 4 codes 
--When there is a code set update, send a copy/notify Eva Yau on Dan Anderson's team.
--MCR denial reason codes: 524, 561
--3/18/2025: redefine Market: for C&S use fin_state; otherwise use fin_market

--Run on: 3/20/2025
--COSMOS paid through: 202502
--SMART  paid through: 202502


--describe formatted fichsrv.tre_membership
--DESC TABLE FICHSRV.DCSP_PR_F



/*================================== BEGIN OF MEMBERSHIP QUERY ==========================================*/
drop table if exists tmp_1y.cl_ss_membership_202codes; 
CREATE TABLE tmp_1y.cl_ss_membership_202codes as
--create table VING_PRD_TREND_DB.TMP_1M.CAB_ENROLL_FIRST as 

select 
	fin_brand
	,'COSMOS' AS Entity_Source
	,fin_inc_month as mth 
	,fin_market as market  --3/18 
	,fin_market
	,fin_state
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'OAH'
	      when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	      --when fin_product_level_3 = 'DUAL' then 'M&R DSNP'
	      else 'M&R FFS' end as entity
	
	      ,case when fin_market in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' end as lcd_status
	
	      ,sum(fin_member_cnt) as mm 
	
	,CASE WHEN fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_MARKET = 'MD' then 2
	      WHEN  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	,0 as cns_dual_flag
from fichsrv.tre_membership 
WHERE		
	SGR_SOURCE_NAME = 'COSMOS'
	and FIN_BRAND = 'M&R'
	and global_cap = 'NA'
	and fin_inc_month >= '202201'
group by 
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_state
	,fin_g_i
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'OAH'
	      when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	      --when fin_product_level_3 = 'DUAL' then 'M&R DSNP'
	      else 'M&R FFS' end
	,case when fin_market in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' end
		
	,CASE WHEN fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_MARKET = 'MD' then 2
	      WHEN  migration_source='OAH'  then 1 else 0 end 
union all
select
	fin_brand
	,'COSMOS' AS Entity_Source
	,fin_inc_month as mth 
	,fin_state as market 
	,fin_market
	,fin_state
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end as entity
	,case when fin_state in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' end as lcd_status
	,sum(fin_member_cnt) as mm 
	,CASE WHEN fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD' then 0
		  WHEN  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	,CASE WHEN (( migration_source <> 'OAH' and fin_product_level_3='DUAL' 
    --AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')
    ) OR (fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_Flag	
from fichsrv.tre_membership 
WHERE		
	FIN_BRAND = 'C&S'	
	AND SGR_SOURCE_NAME = 'COSMOS'
	and global_cap = 'NA'
	and fin_inc_month >= '202201'
group by 	
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_state
	,fin_g_i
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end
	,case when fin_state in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' end
	,CASE WHEN fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD' then 0
		  WHEN  migration_source='OAH'  then 1 else 0 end 
,CASE WHEN (( migration_source <> 'OAH' and fin_product_level_3='DUAL' 
    --AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')
    ) OR (fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end 
union all
select
	fin_brand
	,'CSP' AS Entity_Source
	,fin_inc_month as mth 
	,fin_state as market 
	,fin_market
	,fin_state
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end as entity
	,case when fin_state in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' end as lcd_status
	,sum(fin_member_cnt) as mm 
	,CASE WHEN fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD' then 0
          WHEN  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	,CASE WHEN (( migration_source <> 'OAH' and fin_product_level_3='DUAL' 
    --AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')
    ) OR 
(fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_Flag
from fichsrv.tre_membership 
WHERE		
	FIN_BRAND = 'C&S'	
	AND SGR_SOURCE_NAME = 'CSP'
	and global_cap = 'NA'
	and fin_inc_month >= '202201'
group by 	
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_state	
	,fin_g_i
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end
	,case when fin_state in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' END
	,CASE WHEN fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD' then 0
          WHEN  migration_source='OAH'  then 1 else 0 end
	,CASE WHEN (( migration_source <> 'OAH' and fin_product_level_3='DUAL' 
    --AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')
    ) OR 
(fin_inc_year ='2024' AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end 
union all
select
	fin_brand
	,'NICE' AS Entity_Source
	,fin_inc_month as mth 
	,fin_market as market 
	,fin_market
	,fin_state
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,'M&R FFS' as entity
	,case when fin_market in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' end as lcd_status
	,sum(fin_member_cnt) as mm 
	,CASE WHEN  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	,0 as cns_dual_flag
from fichsrv.tre_membership 
WHERE		
	FIN_BRAND = 'M&R'	
	AND SGR_SOURCE_NAME = 'NICE'
	and NCE_TADM_DEC_RISK_TYPE = 'FFS'
	and fin_inc_month >= '202201'
    --and clm_cap_flag = 'FFS' 
    --and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
group by 	
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_state
	,fin_g_i
	,fin_product_level_3 
	,fin_tfm_product_new
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
	,case when fin_market in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES') then 'LCD' else 'Non-LCD' end
	,CASE WHEN  migration_source='OAH'  then 1 else 0 end
	;
--20644  18516  12569 13176  12804   select count(*) from tmp_1y.cl_ss_membership_202codes 

/*================================== END OF MEMBERSHIP QUERY ==========================================*/


--getting member/state by mbi and month
drop table if exists tmp_7d.cl_ss_mbi_market;
create table tmp_7d.cl_ss_mbi_market as
select distinct 
	fin_brand
	,fin_mbi_hicn_fnl
	,fin_inc_month
	,case when fin_brand = 'C&S' then fin_state else fin_market end as market
	,fin_market
	,fin_state
from fichsrv.tre_membership
where fin_inc_month >= '202201'
;
--380038425  339770300		select count(*) from tmp_7d.cl_ss_mbi_market

/*
--check for uniqueness
select 
	fin_mbi_hicn_fnl
	,fin_inc_month
	,count(*) as cnt
from tmp_7d.cl_ss_mbi_market
group by
	fin_mbi_hicn_fnl
	,fin_inc_month
having count(*) > 1
--0
*/
--Before running claims data, check reference table: code set
--select count(*) from tmp_1y.cl_ss_codelist_20240901  --179
--select count(*) from tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED  --202


/*================================== BEGIN OF CLAIMS QUERY ============================================*/
--select * from tadm_tre_cpy.glxy_op_f_202505
--COSMOS PR AND OP
drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes;
create table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes as 
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
	,a.clm_pd_dt
	,a.primary_diag_cd
	--,fnl_rsn_cd_sys_id
	,a.adjd_month
                from fichsrv.GLXY_OP_F   a  --fichsrv.cosmos_op   tadm_tre_cpy.glxy_op_f_202502  
join tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED b  --tmp_1y.cl_ss_codelist_20241028   cl_ss_codelist_20240901
on	trim(a.proc_cd) = trim(b.hcpcs_code)
where    
	a.brand_fnl in ('M&R', 'C&S')
	and a.hce_month >= '202201'	
	and a.CLM_DNL_F = 'N'
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
	,a.clm_pd_dt
	,a.primary_diag_cd
    ,a.adjd_month
	--,fnl_rsn_cd_sys_id

from fichsrv.GLXY_PR_F a  --fichsrv.cosmos_pr    tadm_tre_cpy.glxy_pr_f_202502
join tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl in ('M&R', 'C&S')
	and a.fst_srvc_month >= '202201'
	and a.CLM_DNL_F = 'N'
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
	,a.clm_aud_nbr as site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_fnl
	,migration_source 
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
	,a.clm_pd_dt
	,a.primary_diag_cd
    ,a.adjd_month
    
from FICHSRV.DCSP_OP_F a
--from smart_op a
join tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 	
	a.brand_fnl ='C&S'
	and a.hce_month >= '202201'
	and	a.CLM_DNL_F = 'N'     -- select distinct CLM_DNL_F FROM tadm_tre_cpy.dcsp_op_f_202501 --this field has 3 values: Y, D, N
	and a.GLOBAL_CAP = 'NA'   -- select distinct GLOBAL_CAP FROM tadm_tre_cpy.dcsp_op_f_202501 --this field has 2 values: NA, WM
union all
select 
	'CSP' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component
	,service_code
	,a.ahrq_diag_dtl_catgy_desc 
	--,case when brand_fnl = 'C&S' then a.st_abbr_cd else a.market_fnl end as market  
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.tin 
	,a.full_nm
	,a.st_abbr_cd 
	,a.clm_aud_nbr as site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tfm_product_fnl
	,migration_source
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
	,a.clm_pd_dt
	,a.primary_diag_cd
    ,a.adjd_month

--from smart_pr a      --SELECT * FROM tadm_tre_cpy.dcsp_pr_f_202502 LIMIT 2;
from FICHSRV.DCSP_PR_F a
join tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl ='C&S'
    and a.fst_srvc_month >= '202201'
	and a.CLM_DNL_F = 'N' 
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
	--,a.prov_tin 
    ,tin 
	,a.full_nm
	,a.st_abbr_cd 
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
	,allw_amt as allw_amt_fnl
	,a.net_pd_amt as net_pd_amt_fnl
	,a.srvc_unit_cnt as adj_srvc_unit_cnt
	,a.procedure_unit as tadm_units
	,concat(a.mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.hce_month as mth 
	,a.fst_srvc_year as years 
	,a.clm_pd_dt
	,a.primary_diag_cd
    ,a.adjd_month
	--,0 as CnS_Dual_Flag
	--,0 as OAH_FLAG
from fichsrv.NCE_OP_F   a    --tadm_tre_cpy.nce_op_dtl_f_202502 
join tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl = 'M&R'
	and a.hce_month >= '202201'	
	--and a.CLM_DNL_F = 'N'
    and a.CLM_LN_LVL_DNL_F = 'N'
	and a.clm_cap_flag = 'FFS'  --this field has 2 values: 'FFS' or 'ENC'
    and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
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
	,a.tin as prov_tin
	,a.full_nm
	,a.st_abbr_cd 
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
	,a.allw_amt as allw_amt_fnl
	,a.net_pd_amt as net_pd_amt_fnl
	,a.srvc_unit_cnt as adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.fst_srvc_month as mth 
	,a.fst_srvc_year as years
	,a.clm_pd_dt
	,a.primary_diag_cd
    ,a.adjd_month
	--,0 as CnS_Dual_Flag
	--,0 as OAH_FLAG
from fichsrv.NCE_PR_F      a  -- fichsrv.nice_pr   tadm_tre_cpy.nce_pr_dtl_f_202502 
join tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED b
on	trim(a.proc_cd) = trim(b.hcpcs_code)
WHERE 
	a.brand_fnl = 'M&R'
	and a.fst_srvc_month >= '202201'
	and a.CLM_DNL_F = 'P'
	and a.clm_cap_flag = 'FFS'   --this field has 2 values: 'FFS' or 'ENC'
    and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
;

--select distinct clm_dnl_f from fichsrv.nice_pr
--SELECT Entity_Source, count(*) FROM tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes GROUP BY entity_source
/*================================== END OF CLAIMS QUERY (6 mins)============================================*/
--120102  117663 125716    select count(*) from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes --



--adding in upper v lower extremity
/*
create table tmp_1y.cl_skin_subs_mapping as
select distinct 
	icd_code, 
	location_type
from tmp_1y.ec_skin_subs_mapping

select distinct location_type from tmp_1y.cl_skin_subs_mapping
*/ 

drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2;
create table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2 as 
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
--select  distinct entity_source from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
--select * from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b where OAH_FLAG =1
--and mth = '202405'

--select distinct location_type from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_2

--drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a_t;
drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a;
create table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as 
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
--146439   117663   select count(*) from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a

----------------------------------------------------------------------------------------------------
drop table if exists tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b;
create table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b as 
SELECT *
	,CASE WHEN a.brand_fnl = 'C&S' AND a.years ='2024' AND a.MIGRATION_SOURCE = 'OAH' AND a.fin_state = 'MD' then 0 
	      WHEN a.brand_fnl <> 'C&S' AND a.years ='2024' AND a.MIGRATION_SOURCE = 'OAH' AND a.fin_market = 'MD' then 0 
		WHEN  a.migration_source='OAH'  then 1 else 0 end as OAH_FLAG
    
	,CASE WHEN (( brand_fnl = 'C&S' AND a.migration_source <> 'OAH' and a.product_level_3_fnl='DUAL' 
    --AND a.fin_state NOT IN ('OK','NC','NM','NV','OH','TX')
    ) OR 
              (a.brand_fnl = 'C&S' AND a.years ='2024' AND a.MIGRATION_SOURCE = 'OAH' AND a.fin_state = 'MD') OR
              (a.brand_fnl <> 'C&S' AND a.years ='2024' AND a.MIGRATION_SOURCE = 'OAH' AND a.fin_market = 'MD')
              ) then 1 else 0 end as CnS_Dual_Flag	
	
              /*,case when a.migration_source = 'OAH' then 'OAH'
	      when (a.brand_fnl = 'M&R' and a.product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
    	  --when (a.brand_fnl = 'M&R' and a.product_level_3_fnl = 'DUAL') then 'M&R DSNP'
	      when a.brand_fnl = 'M&R' then 'M&R FFS'
	      when a.brand_fnl = 'C&S' then 'C&S DSNP'
	      else a.brand_fnl end as entity
	*/
	,case when (a.brand_fnl  = 'C&S' and a.fin_state in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES')) then 'LCD' 
		  when (a.brand_fnl <> 'C&S' and a.fin_market in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES')) then 'LCD' 
		  else 'Non-LCD' end as lcd_status
    ,CASE WHEN PROC_CD IN ('A6021','A6022','A6023','A6024') THEN 1 ELSE 0 END as COLLAGEN_FLAG



FROM tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a a

;
--desc table  tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
----------------------------------------------------------------------------------------------------
--roll up desc formatted tmp_1y.cl_ss_final_202codes --84,721 
--DESC formatted tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b             
drop table if exists tmp_1y.cl_ss_final_202codes;
create table tmp_1y.cl_ss_final_202codes as 
select 
	'Claims' as DataType	
	,brand_fnl
	,entity_source
	,tfm_product_new_fnl
	,mth	
	--,entity
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
    	  WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	   --   WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN 
       --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END as entity
	,component
	,lcd_status
	,proc_cd
	,market
	,fin_market
	,fin_state
	,group_ind_fnl 
	,product_level_3_fnl 
	,migration_source
	,covered_unproven
	,locationtype as location_type
	,prov_prtcp_sts_cd 
	,count(distinct site_clm_aud_nbr) as claim_count 
	,sum(sbmt_chrg_amt) as billed
	,sum(allw_amt_fnl) as allowed
	,sum(net_pd_amt_fnl) as netpaid
	,sum(tadm_units) as tadm_units 
	,sum(adj_srvc_unit_cnt) as srvc_units
	,count(distinct ekp) as proc_count 
	,0 as mm
    ,COLLAGEN_FLAG
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
group by 	
	brand_fnl	
	,entity_source
	,tfm_product_new_fnl
	,mth	
	--,entity
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
	      WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	    --  WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN 
        --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END 
	,component
	,lcd_status
	,proc_cd
	,market
	,fin_market
	,fin_state
	,group_ind_fnl 
	,product_level_3_fnl 
	,migration_source
	,covered_unproven
	,locationtype
	,prov_prtcp_sts_cd
    ,COLLAGEN_FLAG
UNION 
select 
	'Mbrs' as DataType
	,fin_brand
	,entity_source
	,fin_tfm_product_new
	,mth
	--,entity
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
	      WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN fin_product_level_3 = 'INSTITUTIONAL' AND fin_brand = 'M&R' THEN 'M&R ISNP'
	     -- WHEN fin_brand = 'C&S' AND migration_source <> 'OAH' and fin_product_level_3='DUAL' AND FIN_STATE IN 
         --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END as entity
	,'' as component
	,lcd_status
	,'' as proc_cd
	,market
	,fin_market
	,fin_state
	,group_ind_fnl
	,fin_product_level_3
	,migration_source
	,'' as covered_unproven
	,'' as location_type
	,'' as prov_prtcp_sts_cd
	,0 as claim_count
	,0 as billed
	,0 as allowed
	,0 as netpaid
	,0 as tadm_units
	,0 as srvc_units
	,0 as proc_count
	,sum(mm) as mms
    ,null as COLLAGEN_FLAG
from tmp_1y.cl_ss_membership_202codes
group by
	fin_brand
	,entity_source
	,fin_tfm_product_new
	,mth
	,entity
	,lcd_status
	,market
	,fin_market
	,fin_state
	,group_ind_fnl
	,fin_product_level_3
	,migration_source
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
	      WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN fin_product_level_3 = 'INSTITUTIONAL' AND fin_brand = 'M&R' THEN 'M&R ISNP'
		  --WHEN fin_brand = 'C&S' AND migration_source <> 'OAH' and fin_product_level_3='DUAL' AND FIN_STATE IN 
          --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END 
;




----------------------------------------------------------------------------------------------------
--desc table tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
--select * from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b

/*
select 
PROV_TIN,FULL_NM,
ENTITY_SOURCE,COMPONENT
,SUM(ALLW_AMT_FNL) as allw 
,SUM(ADJ_SRVC_UNIT_CNT) units
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
where years = '2025'
GROUP BY 
PROV_TIN,FULL_NM
,ENTITY_SOURCE,COMPONENT
*/
--data for excel Data-1 tab
select count(*) from tmp_1y.cl_ss_final_202codes where COLLAGEN_FLAG = 0  OR COLLAGEN_FLAG IS NULL
--202604: 85867
--202605: 87246
--202606: 88793
select * from tmp_1y.cl_ss_final_202codes where (COLLAGEN_FLAG = 0 OR COLLAGEN_FLAG IS NULL)  --AND PROV_TIN = '264341152'

--DESC formatted tmp_1y.cl_ss_final_202codes

--############   Data for JD   ###############
--DESC formatted tmp_1y.cl_ss_final_JD

drop table if exists tmp_1y.cl_ss_final_JD; 
create table tmp_1y.cl_ss_final_JD as 
select 
	'Claims' as DataType	
	,mth	
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
    	  WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	      --WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN 
          --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END as entity
	--,case when entity = 'C&S' then 'C&S DSNP'
	--      when entity = 'M&R' then 'M&R FFS' 
	--      else entity end as entity1
	,component
	,lcd_status
	,prov_prtcp_sts_cd 
	,tfm_product_new_fnl as product_type
	,count(distinct site_clm_aud_nbr) as claim_count 
	,count(distinct gal_mbi_hicn_fnl) as ss_mbi_count
	,sum(sbmt_chrg_amt) as billed
	,sum(allw_amt_fnl) as allowed
	,sum(net_pd_amt_fnl) as netpaid
	,sum(tadm_units) as tadm_units 
	,sum(adj_srvc_unit_cnt) as srvc_units
	,count(distinct ekp) as proc_count 
	,0 as mm
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
where COLLAGEN_FLAG = 0
group by 	
	mth	
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
    	  WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	     -- WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN 
         --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END
	,component
	,lcd_status
	,prov_prtcp_sts_cd
	,tfm_product_new_fnl
UNION 
select 
	'Mbrs' as DataType
	,mth
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
	      WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN fin_product_level_3 = 'INSTITUTIONAL' AND fin_brand = 'M&R' THEN 'M&R ISNP'
	     -- WHEN fin_brand = 'C&S' AND migration_source <> 'OAH' and fin_product_level_3='DUAL' AND FIN_STATE IN 
         --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END as entity
	--,case when entity = 'C&S' then 'C&S DSNP'
	--      when entity = 'M&R' then 'M&R FFS' 
	--      else entity end as entity1
	,'' as component
	,lcd_status
	,'' as prov_prtcp_sts_cd
	,fin_tfm_product_new as product_type
	,0 as claim_count
	,0 as ss_mbi_count
	,0 as billed
	,0 as allowed
	,0 as netpaid
	,0 as tadm_units
	,0 as srvc_units
	,0 as proc_count
	,sum(mm) as mms
from tmp_1y.cl_ss_membership_202codes
group by
	mth
	,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
	      WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN fin_product_level_3 = 'INSTITUTIONAL' AND fin_brand = 'M&R' THEN 'M&R ISNP'
	     -- WHEN fin_brand = 'C&S' AND migration_source <> 'OAH' and fin_product_level_3='DUAL' AND FIN_STATE IN 
         --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END
	,lcd_status
	,fin_tfm_product_new
;

--data for excel Data-2 tab
select count(*) from tmp_1y.cl_ss_final_JD
--202604: 3835
--202605: 3882
select * from tmp_1y.cl_ss_final_JD 

--##########  Everything above ran on 6/30/2025  ############--
/*
 * --QA FIND MISSING PR NICE
select mth,entity1,component,allowed,claim_count from tmp_1y.cl_ss_final_JD

select mth, component,entity_source,allowed,claim_count from  tmp_1y.cl_ss_final_202codes
where entity_source = 'NICE'
*/
--==================== IBNR ===================


--IBNR NEW

-----------------------------------------------------------------------
select 
	'Claims' as DataType	
,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
    	  WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	      --WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN
          --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END as entity     
	,mth as incurred_month	
	,year(clm_pd_dt)|| SUBSTRING(clm_pd_dt, 6,2) as paid_month
	,sum(allw_amt_fnl) as allowed
	,sum(net_pd_amt_fnl) as netpaid
    
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
where COLLAGEN_FLAG = 0 OR COLLAGEN_FLAG IS NULL
group by 	
CASE WHEN OAH_FLAG = 1 THEN 'OAH'
    	  WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	      --WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN 
          --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END 
	,mth	
	,year(clm_pd_dt)|| SUBSTRING(clm_pd_dt, 6,2) 

;







--IBNR 2026
--THIS IS FOR THE TRIANGLE VIEWS
-----------------------------------------------------------------------
select 
	'Claims' as DataType	
,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
    	  WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	      --WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN
          --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END as entity     
	,mth as incurred_month	
	,year(clm_pd_dt)|| SUBSTRING(clm_pd_dt, 6,2) as paid_month
	,sum(allw_amt_fnl) as allowed
	,sum(net_pd_amt_fnl) as netpaid
    ,sum(adj_srvc_unit_cnt) as srvc_units
    
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
WHERE COLLAGEN_FLAG = 1
--COLLAGEN_FLAG = 0 OR COLLAGEN_FLAG IS NULL
group by 	
CASE WHEN OAH_FLAG = 1 THEN 'OAH'
    	  WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	      WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	      --WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN 
          --('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	      ELSE 'M&R FFS' END 
	,mth	
	,year(clm_pd_dt)|| SUBSTRING(clm_pd_dt, 6,2) 

;
/*

select proc_cd,count(adj_srvc_unit_cnt) 
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b
WHERE COLLAGEN_FLAG = 1
group by proc_cd


select a.gal_mbi_hicn_fnl,a.allw_amt_fnl,a.mth,a.clm_pd_dt,year(clm_pd_dt)|| SUBSTRING(clm_pd_dt, 6,2) as paid_month 
select collagen_flag,count(gal_mbi_hicn_fnl)
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b a
WHERE mth = '202501' group by collagen_flag
--WHERE  a.CnS_Dual_Flag = 1 AND a.mth = '202408' AND allw_amt_fnl > 0 AND a.clm_pd_dt = '9999-09-09'




--------------------------------------------------------------------

desc formatted tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a
tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a


--84721 83047  67131  66177  59832  60282  58966      select count(*) from tmp_1y.cl_ss_final_202codes

tmp_1y.cl_ss_final_202codes
--CLAIMS
select entity,count(*) from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a group by entity

entity	_c1
C&S DSNP	21,544
M&R FFS	100,980
M&R ISNP	4,745
OAH	19,170
		


select 
	CASE WHEN OAH_FLAG = 1 THEN 'OAH'
		 WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	     WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	     WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN ('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	     ELSE 'M&R FFS' END as entity2
	,count(*) 
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a 
group by  
	CASE WHEN OAH_FLAG = 1 THEN 'OAH'
		 WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	     WHEN product_level_3_fnl = 'INSTITUTIONAL' AND brand_fnl = 'M&R' THEN 'M&R ISNP'
	     WHEN brand_fnl = 'C&S' AND migration_source <> 'OAH' and product_level_3_fnl='DUAL' AND st_abbr_cd IN ('OK','NC','NM','NV','OH','TX') THEN 'N/A C&S'
	     ELSE 'M&R FFS' END 

	     
entity2	_c1
C&S DSNP	17,749
M&R FFS	100,980
M&R ISNP	4,745
N/A C&S	4,123
OAH	18,842
*/
--NEW HIGH COST CASES
DROP TABLE IF EXISTS tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b_high_cost;
CREATE TABLE tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b_high_cost AS
SELECT *
  ,ALLW_AMT_FNL / ADJ_SRVC_UNIT_CNT AS ALLW_PER_UNIT
  ,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
        WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
        WHEN PRODUCT_LEVEL_3_FNL = 'INSTITUTIONAL' AND BRAND_FNL = 'M&R' THEN 'M&R ISNP'
        ELSE 'M&R FFS' END AS entity
FROM VING_PRD_TREND_DB.TMP_1Y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2B
WHERE ADJ_SRVC_UNIT_CNT > 0
  AND (ALLW_AMT_FNL / ADJ_SRVC_UNIT_CNT) > 400
  and YEARS > '2025';

  select * from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b_high_cost
	     
entity2	_c1
C&S DSNP	16,965
M&R FFS	98,502
M&R ISNP	4,646
N/A C&S	4,060
OAH	18,128
--------------PR GREATER THAN 2025
DROP TABLE IF EXISTS tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b_high_cost;
CREATE TABLE tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b_high_cost AS
SELECT *
  ,ALLW_AMT_FNL / ADJ_SRVC_UNIT_CNT AS ALLW_PER_UNIT
  ,CASE WHEN OAH_FLAG = 1 THEN 'OAH'
        WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
        WHEN PRODUCT_LEVEL_3_FNL = 'INSTITUTIONAL' AND BRAND_FNL = 'M&R' THEN 'M&R ISNP'
        ELSE 'M&R FFS' END AS entity
FROM VING_PRD_TREND_DB.TMP_1Y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2B
WHERE ADJ_SRVC_UNIT_CNT > 0
  AND (ALLW_AMT_FNL / ADJ_SRVC_UNIT_CNT) > 400;




--MBRS
select entity,count(*) from tmp_1y.cl_ss_membership_202codes group by entity
select CASE WHEN OAH_FLAG = 1 THEN 'OAH'
	WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	WHEN fin_product_level_3 = 'INSTITUTIONAL' AND fin_brand = 'M&R' THEN 'M&R ISNP'
	ELSE 'M&R FFS' END as entity2
	,count(*) 
	from tmp_1y.cl_ss_membership_202codes
	group by  CASE WHEN OAH_FLAG = 1 THEN 'OAH'
	WHEN CnS_Dual_Flag = 1 THEN 'C&S DSNP'
	WHEN fin_product_level_3 = 'INSTITUTIONAL' AND fin_brand = 'M&R' THEN 'M&R ISNP'
	ELSE 'M&R FFS' END 

    CREATE STAGE VING_PRD_TREND_DB.FICHSRV.STAGE_ORDERS_JMSVING_PRD_TREND_DB.FICHSRV.STAGE_ORDERS_JMS
    */

    select * from tmp_1y.SS_CODELIST_2026_COLLAGEN_ADDED