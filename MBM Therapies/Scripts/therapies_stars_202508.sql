/*
Therapies spent for 2023, adapting code from Skin Sub to pull in all COSMOS, FACET, AND NICE
 */

--Skin Substitute, live on 9/1/2024 
--Post service, pre payment >>> MCR (medical claims review)
--10/28/2024 updated: 202 proc codes (previously 179 Proc Codes); of which only 5 are proven effective
--11/12/2024 updated "covered" coded from 5 to 4 codes 
--When there is a code set update, send a copy/notify Eva Yau on Dan Anderson's team.
--MCR denial reason codes: 524, 561

describe formatted fichsrv.tre_membership
;

--Membership
/*================================== BEGIN OF MEMBERSHIP QUERY ==========================================*/
drop table tmp_7d.kn_therapies_membership; 
create table tmp_7d.kn_therapies_membership stored as orc as 
select 
	fin_brand
	,'COSMOS' AS Entity_Source
	,fin_inc_month as mth 
	,fin_market as market_fnl 
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'M&R OAH'
	      when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	      else 'M&R FFS' end as entity
	,sum(fin_member_cnt) as mm 
from fichsrv.tre_membership 
WHERE		
	SGR_SOURCE_NAME = 'COSMOS'
	and FIN_BRAND = 'M&R'
	and global_cap = 'NA'
	and fin_inc_month >= '202401'
group by 
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_g_i
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'M&R OAH'
	      when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	      else 'M&R FFS' end
union all
select
	fin_brand
	,'COSMOS' AS Entity_Source
	,fin_inc_month as mth 
	,fin_market as market_fnl 
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'C&S OAH'
	      else 'C&S DSNP' end as entity
	,sum(fin_member_cnt) as mm 
from fichsrv.tre_membership 
WHERE		
	FIN_BRAND = 'C&S'	
	AND SGR_SOURCE_NAME = 'COSMOS'
	and global_cap = 'NA'
	and fin_inc_month >= '202401'
group by 	
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_g_i
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'C&S OAH'
	      else 'C&S DSNP' end
union all
select
	fin_brand
	,'CSP' AS Entity_Source
	,fin_inc_month as mth 
	,fin_market as market_fnl 
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'C&S OAH'
	      else 'C&S DSNP' end as entity
	,sum(fin_member_cnt) as mm 
from fichsrv.tre_membership 
WHERE		
	FIN_BRAND = 'C&S'	
	AND SGR_SOURCE_NAME = 'CSP'
	and global_cap = 'NA'
	and fin_inc_month >= '202401'
group by 	
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_g_i
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
	,case when migration_source = 'OAH' then 'C&S OAH'
	      else 'C&S DSNP' end
union all
select
	fin_brand
	,'NICE' AS Entity_Source
	,fin_inc_month as mth 
	,fin_market as market_fnl 
	,fin_g_i  as group_ind_fnl 	
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap 
	,nce_tadm_dec_risk_type
	,'M&R FFS' as entity
	,sum(fin_member_cnt) as mm 
from fichsrv.tre_membership 
WHERE		
	FIN_BRAND = 'M&R'	
	AND SGR_SOURCE_NAME = 'NICE'
	and NCE_TADM_DEC_RISK_TYPE = 'FFS'
	and fin_inc_month >= '202401'
group by 	
	fin_brand
	,fin_inc_month
	,fin_market
	,fin_g_i
	,fin_product_level_3 
	,fin_tadmprodrollup
	,migration_source
	,global_cap
	,nce_tadm_dec_risk_type
;
--10403  10104    select count(*) from tmp_1y.kn_ss_membership 

/*================================== END OF MEMBERSHIP QUERY ==========================================*/





/*================================== BEGIN OF CLAIMS QUERY ============================================*/
--COSMOS PR AND OP
drop table tmp_7d.kn_therapies_claims_op_pr_cosmos_smart_nice;
create table tmp_7d.kn_therapies_claims_op_pr_cosmos_smart_nice stored as orc as 
select  
	'COSMOS' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component 
	,a.hce_service_code 
	,a.ahrq_diag_dtl_catgy_desc 
	,a.market_fnl 
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd 
	,a.site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tadmprodrollup_fnl
	,a.migration_source 
	,a.global_cap 
	,case when (a.brand_fnl = 'M&R' and a.migration_source = 'OAH') then 'M&R OAH'
	      when (a.brand_fnl = 'C&S' and a.migration_source = 'OAH') then 'C&S OAH'
	      when (a.brand_fnl = 'M&R' and a.product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
	      else a.brand_fnl end as entity1
	,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
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
from fichsrv.cosmos_op a  --fichsrv.cosmos_op   tadm_tre_cpy.glxy_op_f_202410
where    a.brand_fnl in ('M&R', 'C&S')
		AND a.hce_month BETWEEN '202301' AND '202312'	
		AND a.CLM_DNL_F = 'N'
		AND a.GLOBAL_CAP = 'NA'
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3		-- REMOVE THIS TO INCLUDE HH CLAIMS	
		AND a.ama_pl_of_srvc_cd <> 12 			
		AND (a.proc_cd in 			
			  ('98940','98941','98942'		
		      ,'97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'			
		      ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		      ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		      ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		      ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		      ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'			
		      ,'97161','97162','97163','97164','97165','97166','97167','97168'			
		      ,'70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'			
		      ,'92627','92630','92633','96105','S9128'			
		      ) OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
union all 
select 
	'COSMOS' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component
	,a.service_code
	,a.ahrq_diag_dtl_catgy_desc 
	,a.market_fnl 
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd 
	,a.site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tadmprodrollup_fnl
	,a.migration_source
	,a.global_cap 
	,case when (a.brand_fnl = 'M&R' and a.migration_source = 'OAH') then 'M&R OAH'
	      when (a.brand_fnl = 'C&S' and a.migration_source = 'OAH') then 'C&S OAH'
	      when (a.brand_fnl = 'M&R' and a.product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
	      else a.brand_fnl end as entity1
	,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
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
from fichsrv.cosmos_pr a  
WHERE 	a.brand_fnl in ('M&R', 'C&S')
		AND a.fst_srvc_month BETWEEN '202301' AND '202312'	
		and a.CLM_DNL_F = 'N'
		and a.GLOBAL_CAP = 'NA'
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3		-- REMOVE THIS TO INCLUDE HH CLAIMS	
		AND a.ama_pl_of_srvc_cd <> 12 			
		AND (a.proc_cd in 			
			  ('98940','98941','98942'		
		      ,'97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'			
		      ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		      ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		      ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		      ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		      ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'			
		      ,'97161','97162','97163','97164','97165','97166','97167','97168'			
		      ,'70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'			
		      ,'92627','92630','92633','96105','S9128'			
		      ) OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
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
	,a.market_fnl 
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.tin 
	,a.full_nm
	,a.st_abbr_cd 
	,a.clm_aud_nbr as site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tadmprodrollup_fnl
	,a.migration_source 
	,a.global_cap 
	,case when a.migration_source = 'OAH' then 'C&S OAH'
	      else 'C&S DSNP' end as entity1
	,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
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
from tadm_tre_cpy.dcsp_op_f_202410 a
WHERE 	a.brand_fnl ='C&S'
		AND a.fst_srvc_month BETWEEN '202301' AND '202312'	
		and	a.CLM_DNL_F = 'N' 
		and a.GLOBAL_CAP = 'NA'
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3		-- REMOVE THIS TO INCLUDE HH CLAIMS	
		AND a.ama_pl_of_srvc_cd <> 12 			
		AND (a.proc_cd in 			
			  ('98940','98941','98942'		
		      ,'97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'			
		      ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		      ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		      ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		      ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		      ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'			
		      ,'97161','97162','97163','97164','97165','97166','97167','97168'			
		      ,'70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'			
		      ,'92627','92630','92633','96105','S9128'			
		      ) OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
union all
select 
	'CSP' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.gal_mbi_hicn_fnl 
	,a.component
	,a.service_code
	,a.ahrq_diag_dtl_catgy_desc 
	,a.market_fnl 
	,a.group_ind_fnl 
	,a.sbscr_nbr 
	,a.tin 
	,a.full_nm
	,a.st_abbr_cd 
	,a.clm_aud_nbr as site_clm_aud_nbr 
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tadmprodrollup_fnl
	,a.migration_source
	,a.global_cap 
	,case when a.migration_source = 'OAH' then 'C&S OAH'
	      else 'C&S DSNP' end as entity1
	,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
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
from tadm_tre_cpy.dcsp_pr_f_202410 a   
WHERE 	a.brand_fnl ='C&S'
	    AND a.fst_srvc_month BETWEEN '202301' AND '202312'
		and a.CLM_DNL_F = 'N' 
		and a.GLOBAL_CAP = 'NA'
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3		-- REMOVE THIS TO INCLUDE HH CLAIMS	
		AND a.ama_pl_of_srvc_cd <> 12 			
		AND (a.proc_cd in 			
			  ('98940','98941','98942'		
		      ,'97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'			
		      ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		      ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		      ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		      ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		      ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'			
		      ,'97161','97162','97163','97164','97165','97166','97167','97168'			
		      ,'70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'			
		      ,'92627','92630','92633','96105','S9128'			
		      ) OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
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
	,a.market_fnl 
	,a.group_ind_fnl 
	,a.mbi_hicn_fnl as sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd 
	,a.clm_aud_nbr --different
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tadmprodrollup_fnl
	,'NA' as migration_source 
	,a.clm_cap_flag as global_cap --different 
	,a.brand_fnl as entity1
	,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.claim_place_of_svc_cd in ('11', '49'), 'Office',if(a.claim_place_of_svc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.srvc_unit_cnt as adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.hce_month as mth 
	,a.fst_srvc_year as years 
	,a.clm_pd_dt
	,a.primary_diag_cd
from fichsrv.nice_op a
WHERE 	a.brand_fnl = 'M&R'
		AND a.hce_month BETWEEN '202301' AND '202312'	
		and a.CLM_DNL_F = 'N'
		and a.clm_cap_flag = 'FFS'  --this field has 2 values: 'FFS' or 'ENC'
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		--AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3		-- REMOVE THIS TO INCLUDE HH CLAIMS	
		AND a.claim_place_of_svc_cd <> 12 	--in cosmos this is ama_cos_pl_of_srvc_cd		 
		AND (a.proc_cd in 			
			  ('98940','98941','98942'		
		      ,'97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'			
		      ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		      ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		      ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		      ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		      ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'			
		      ,'97161','97162','97163','97164','97165','97166','97167','97168'			
		      ,'70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'			
		      ,'92627','92630','92633','96105','S9128'			
		      ) OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
union all 
select 
	'NICE' as Entity_Source
	,a.brand_fnl
	,a.proc_cd
	,a.mbi_hicn_fnl as gal_mbi_hicn_fnl
	,a.component
	,a.service_code as hce_service_code
	,a.ahrq_diag_dtl_catgy_desc 
	,a.market_fnl 
	,a.group_ind_fnl 
	,a.mbi_hicn_fnl as sbscr_nbr 
	,a.prov_tin 
	,a.full_nm
	,a.st_abbr_cd 
	,a.clm_aud_nbr --different
	,a.prov_prtcp_sts_cd 
	,a.tfm_include_flag 
	,a.product_level_3_fnl 
	,a.tadmprodrollup_fnl
	,'NA' as migration_source
	,a.clm_cap_flag as global_cap 
	,a.brand_fnl as entity1
	,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.claim_place_of_svc_cd in ('11', '49'), 'Office',if(a.claim_place_of_svc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
	,a.sbmt_chrg_amt
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.srvc_unit_cnt as adj_srvc_unit_cnt
	,a.tadm_units
	,concat(a.mbi_hicn_fnl,a.srvc_prov_id,a.fst_srvc_dt,a.proc_cd) as ekp
	,a.fst_srvc_month as mth 
	,a.fst_srvc_year as years
	,a.clm_pd_dt
	,a.primary_diag_cd
from fichsrv.nice_pr a
WHERE 	a.brand_fnl = 'M&R'
		AND a.fst_srvc_month BETWEEN '202301' AND '202312'
		and a.CLM_DNL_F = 'N'
		and a.clm_cap_flag = 'FFS'   --this field has 2 values: 'FFS' or 'ENC'
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		--AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND SUBSTRING(coalesce(a.nc_bill_typ,'0'),0,1) <> 3		-- REMOVE THIS TO INCLUDE HH CLAIMS	
		AND a.claim_place_of_svc_cd <> 12 			
		AND (a.proc_cd in 			
			  ('98940','98941','98942'		
		      ,'97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'			
		      ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		      ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		      ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		      ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		      ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'			
		      ,'97161','97162','97163','97164','97165','97166','97167','97168'			
		      ,'70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'			
		      ,'92627','92630','92633','96105','S9128'			
		      )) --OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
		
;
/*================================== END OF CLAIMS QUERY (6 mins)============================================*/
--22392925    select count(*) from tmp_7d.kn_therapies_claims_op_pr_cosmos_smart_nice

drop table tmp_7d.kn_therapies_claims_sum;
create table tmp_7d.kn_therapies_claims_sum as
select 
	case when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
	     when entity1 = 'C&S' then 'C&S DSNP'
	     when entity1 = 'M&R' and product_level_3_fnl = 'DUAL' THEN 'M&R DSNP'
	     when entity1 = 'M&R' then 'M&R FFS'
	     else entity1 end as entity
	,migration_source 
	,component 
	,market_fnl 
	,group_ind_fnl 
	,prov_prtcp_sts_cd 
	,product_level_3_fnl 
	,global_cap 
	,mth as dos_month
	,category 
	,sum(allw_amt_fnl) as allowed_amt
	,sum(net_pd_amt_fnl) as paid_amt
from tmp_7d.kn_therapies_claims_op_pr_cosmos_smart_nice 
group by 
	case when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
	     when entity1 = 'C&S' then 'C&S DSNP'
	     when entity1 = 'M&R' and product_level_3_fnl = 'DUAL' THEN 'M&R DSNP'
	     when entity1 = 'M&R' then 'M&R FFS'
	     else entity1 end
	,migration_source 
	,component 
	,market_fnl 
	,group_ind_fnl 
	,prov_prtcp_sts_cd 
	,product_level_3_fnl 
	,global_cap 
	,mth 
	,category
;
select count(*) from tmp_7d.kn_therapies_claims_sum;
select * from tmp_7d.kn_therapies_claims_sum;


select * from tmp_7d.kn_therapies_membership;

