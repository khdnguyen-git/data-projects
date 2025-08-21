-- Membership in the excluded HPBP/GroupIDs;
drop table if exists tmp_1m.kn_therapy_exclusion;
create table tmp_1m.kn_therapy_exclusion stored as orc as
select
	a.fin_market
	, a.fin_mbi_hicn_fnl -- include mbi for joining
	, a.fin_contractpbp
	, a.fin_tfm_product_new
	, a.global_cap
	, a.fin_source_name
	, a.fin_g_i
	, a.migration_source
	, a.fin_product_level_3
	, a.gal_cust_seg_nbr -- groupnumber in cosmos, cust_seg_nbr / cust_seg_nbr_fnl in nice and cosmos
	, a.nce_purchaser_id -- purchaser_id_fnl in nice
	, a.nce_src_sys_mdcl_pln_id -- nce_src_sys_mdcl_pln_id
	, case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	   else substr(a.gal_cust_seg_nbr,5,5)
     end as group_id
	, a.fin_inc_year
	, a.fin_inc_month
	, count(distinct a.fin_mbi_hicn_fnl) as mbrs
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202505 a
where a.fin_inc_year in ('2024','2025')  -- and a.fin_market in ('AZ', 'CO')
group by 
	a.fin_market
	, a.fin_mbi_hicn_fnl
	, a.fin_contractpbp
	, a.fin_tfm_product_new
	, a.global_cap
	, a.fin_source_name
	, a.fin_g_i
	, a.migration_source
	, a.fin_product_level_3
	, a.gal_cust_seg_nbr
	, a.nce_purchaser_id
	, a.nce_src_sys_mdcl_pln_id
	, case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	  else substr(a.gal_cust_seg_nbr,5,5) end 
	, a.fin_inc_year
	, a.fin_inc_month
;

-- COSMOS Claims;
drop table if exists tmp_1m.kn_therapy_cosmos_op;
create table tmp_1m.kn_therapy_cosmos_op as
-- COSMOS OP
select distinct
	'COSMOS' as Entity_Source
	, a.brand_fnl
	, a.proc_cd
	, a.gal_mbi_hicn_fnl
	, a.contractpbp_fnl
	, a.component 
	, a.hce_service_code 
	, a.market_fnl 
	, a.group_ind_fnl
	, a.tfm_include_flag 
	, a.product_level_3_fnl
	, a.tfm_product_fnl
	, a.migration_source 
	, a.global_cap 
	, case when (a.brand_fnl = 'M&R' and a.migration_source = 'OAH') then 'M&R OAH'
	       when (a.brand_fnl = 'C&S' and a.migration_source = 'OAH') then 'C&S OAH'
	       when (a.brand_fnl = 'M&R' and a.product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
	       else a.brand_fnl end as entity1
	, if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
	, a.adj_srvc_unit_cnt
	, a.hce_month as clm_month 
	, a.fst_srvc_year as clm_year
	, substr(cust_seg_nbr,5,5) as group_id
	, '' as purchaser_id_fnl
	, '' as nce_src_sys_mdcl_pln_id
from  tadm_tre_cpy.glxy_op_f_202505 as a
right join tmp_1m.kn_therapy_exclusion as b
on a.gal_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where     a.brand_fnl in ('M&R', 'C&S')
		AND a.hce_month >= '202401'
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
;

-- COSMOS PR
drop table if exists tmp_1m.kn_therapy_cosmos_pr;
create table tmp_1m.kn_therapy_cosmos_pr as
select distinct
	'COSMOS' as Entity_Source
	, a.brand_fnl
	, a.proc_cd
	, a.gal_mbi_hicn_fnl
	, a.contractpbp_fnl
	, a.component 
	, a.service_code as hce_service_code
	, a.market_fnl 
	, a.group_ind_fnl
	, a.tfm_include_flag 
	, a.product_level_3_fnl
	, a.tfm_product_fnl
	, a.migration_source 
	, a.global_cap 
	, case when (a.brand_fnl = 'M&R' and a.migration_source = 'OAH') then 'M&R OAH'
	       when (a.brand_fnl = 'C&S' and a.migration_source = 'OAH') then 'C&S OAH'
	       when (a.brand_fnl = 'M&R' and a.product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
	       else a.brand_fnl end as entity1
	, if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
	, a.adj_srvc_unit_cnt
	, a.fst_srvc_month as clm_month 
	, a.fst_srvc_year as clm_year
	, substr(cust_seg_nbr,5,5) as group_id
	, '' as purchaser_id_fnl
	, '' as nce_src_sys_mdcl_pln_id
from  tadm_tre_cpy.glxy_pr_f_202505 as a 
right join tmp_1m.kn_therapy_exclusion as b
on a.gal_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where     a.brand_fnl in ('M&R', 'C&S')
		AND a.fst_srvc_month >= '202401'
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
;

drop table if exists tmp_1m.kn_therapy_cosmos_op_pr;
create table tmp_1m.kn_therapy_cosmos_op_pr as
select * from tmp_1m.kn_therapy_cosmos_op
union all
select * from tmp_1m.kn_therapy_cosmos_pr
;

drop table if exists tmp_1m.kn_therapy_nice_op;
create table tmp_1m.kn_therapy_nice_op as
select  
	'NICE' as Entity_Source
	, a.brand_fnl
	, a.proc_cd
	, a.mbi_hicn_fnl as gal_mbi_hicn_fnl  --different
	, a.contractpbp_fnl
	, a.component 
	, a.hce_service_code 
	, a.market_fnl 
	, a.group_ind_fnl 
	, a.tfm_include_flag 
	, a.product_level_3_fnl
	, a.tfm_product_fnl
	, 'NA' as migration_source 
	, a.clm_cap_flag as global_cap --different 
	, a.brand_fnl as entity1
	, if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.claim_place_of_svc_cd in ('11', '49'), 'Office',if(a.claim_place_of_svc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  
	as category
	, a.allw_amt as allw_amt_fnl
	, a.net_pd_amt as net_pd_amt_fnl
	, a.srvc_unit_cnt as adj_srvc_unit_cnt
	, a.hce_month as clm_month 
	, a.fst_srvc_year as clm_year
	, a.purchaser_id_fnl
	, a.nce_src_sys_mdcl_pln_id
	, concat(a.purchaser_id_fnl, "-", substr(a.nce_src_sys_mdcl_pln_id,3,3)) as group_id
from tadm_tre_cpy.nce_op_dtl_f_202505 as a
right join tmp_1m.kn_therapy_exclusion as b
on a.mbi_hicn_fnl = b.fin_mbi_hicn_fnl
WHERE 	a.brand_fnl = 'M&R'
		AND a.hce_month >= '202401'
		and a.CLM_LN_LVL_DNL_F = 'N'
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
;

drop table if exists tmp_1m.kn_therapy_nice_pr;
create table tmp_1m.kn_therapy_nice_pr as
select distinct
	'NICE' as Entity_Source
	, a.brand_fnl
	, a.proc_cd
	, a.mbi_hicn_fnl as gal_mbi_hicn_fnl  --different
	, a.contractpbp_fnl
	, a.component 
	, a.service_code as hce_service_code 
	, a.market_fnl 
	, a.group_ind_fnl 
	, a.tfm_include_flag 
	, a.product_level_3_fnl
	, a.tfm_product_fnl
	, 'NA' as migration_source 
	, a.clm_cap_flag as global_cap --different 
	, a.brand_fnl as entity1
	, if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.claim_place_of_svc_cd in ('11', '49'), 'Office',if(a.claim_place_of_svc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  
	as category
	, a.allw_amt as allw_amt_fnl
	, a.net_pd_amt as net_pd_amt_fnl
	, a.srvc_unit_cnt as adj_srvc_unit_cnt
	, a.fst_srvc_month as clm_month 
	, a.fst_srvc_year as clm_year 
	, a.purchaser_id_fnl
	, a.nce_src_sys_mdcl_pln_id
	, concat(a.purchaser_id_fnl, "-", substr(a.nce_src_sys_mdcl_pln_id,3,3)) as group_id
from tadm_tre_cpy.nce_pr_dtl_f_202505 a
right join tmp_1m.kn_therapy_exclusion as b
on a.mbi_hicn_fnl = b.fin_mbi_hicn_fnl
WHERE 	a.brand_fnl = 'M&R'
		AND a.fst_srvc_month >= '202401'
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

drop table if exists tmp_1m.kn_therapy_nice_op_pr;
create table tmp_1m.kn_therapy_nice_op_pr as
select * from tmp_1m.kn_therapy_nice_op
union all
select * from tmp_1m.kn_therapy_nice_pr
;

drop table if exists tmp_1m.kn_therapy_cosmos_nice_op_pr;
create table tmp_1m.kn_therapy_cosmos_nice_op_pr as
select
	entity_source
	, brand_fnl
	, proc_cd
	, gal_mbi_hicn_fnl
	, contractpbp_fnl
	, component
	, hce_service_code
	, market_fnl
	, group_ind_fnl
	, tfm_include_flag
	, product_level_3_fnl
	, tfm_product_fnl
	, migration_source
	, global_cap
	, entity1
	, category
	, allw_amt_fnl
	, net_pd_amt_fnl
	, adj_srvc_unit_cnt
	, clm_month
	, clm_year
	, purchaser_id_fnl
    , nce_src_sys_mdcl_pln_id
    , group_id
from tmp_1m.kn_therapy_nice_op_pr
union all
select
	entity_source
	, brand_fnl
	, proc_cd
	, gal_mbi_hicn_fnl
	, contractpbp_fnl
	, component
	, hce_service_code
	, market_fnl
	, group_ind_fnl
	, tfm_include_flag
	, product_level_3_fnl
	, tfm_product_fnl
	, migration_source
	, global_cap
	, entity1
	, category
	, allw_amt_fnl
	, net_pd_amt_fnl
	, adj_srvc_unit_cnt
	, clm_month
	, clm_year
	, cast('' as string) as purchaser_id_fnl
    , cast('' as string) as nce_src_sys_mdcl_pln_id
    , group_id
from tmp_1m.kn_therapy_cosmos_op_pr
group by 
	entity_source
	, brand_fnl
	, proc_cd
	, gal_mbi_hicn_fnl
	, contractpbp_fnl
	, component
	, hce_service_code
	, market_fnl
	, group_ind_fnl
	, tfm_include_flag
	, product_level_3_fnl
	, tfm_product_fnl
	, migration_source
	, global_cap
	, entity1
	, category
	, allw_amt_fnl
	, net_pd_amt_fnl
	, adj_srvc_unit_cnt
	, clm_month
	, clm_year
	, cast('' as string)
    , cast('' as string)
    , group_id
;
drop table if exists tmp_1m.kn_therapy_exclusion_with_claims 
;
create table tmp_1m.kn_therapy_exclusion_with_claims as
select
    a.fin_market 
    , a.fin_contractpbp 
    , a.fin_tfm_product_new 
    , a.global_cap 
    , a.fin_source_name 
    , a.fin_g_i 
    , a.migration_source 
    , a.fin_product_level_3 
    , a.gal_cust_seg_nbr 
    , a.nce_purchaser_id 
    , a.nce_src_sys_mdcl_pln_id
    , a.group_id
    , concat(a.fin_contractpbp, "-", a.group_id) as excl_id
    , a.fin_inc_month
    , a.fin_inc_year
    , count(distinct a.fin_mbi_hicn_fnl) as mbrs
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1m.kn_therapy_exclusion as a
left join tmp_1m.kn_therapy_cosmos_nice_op_pr as b
    on  a.fin_mbi_hicn_fnl = b.gal_mbi_hicn_fnl
    and (a.fin_inc_month = b.clm_month)
group by
    a.fin_market 
    , a.fin_contractpbp 
    , a.fin_tfm_product_new 
    , a.global_cap 
    , a.fin_source_name 
    , a.fin_g_i 
    , a.migration_source 
    , a.fin_product_level_3 
    , a.gal_cust_seg_nbr 
    , a.nce_purchaser_id 
    , a.nce_src_sys_mdcl_pln_id 
    , a.group_id
    , concat(a.fin_contractpbp, "-", a.group_id)
    , a.fin_inc_month
    , a.fin_inc_year
;

select count(*) from tmp_1m.kn_therapy_exclusion_with_claims
