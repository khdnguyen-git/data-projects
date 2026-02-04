/*==============================================================================
 * MBM Claims
 *==============================================================================*/
-- COSMOS
drop table if exists tmp_1m.kn_mbm_cosmos_claims;
create table tmp_1m.kn_mbm_cosmos_claims as
select
	'COSMOS' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd
    , primary_diag_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.glxy_pr_f
where brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
	and st_abbr_cd = market_fnl
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     
	and (proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and fst_srvc_year >= '2023'
union all
select
	'COSMOS' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd
    , primary_diag_cd
    , hce_month as fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.glxy_op_f
where brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
	and st_abbr_cd = market_fnl
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     
	and (proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and fst_srvc_year >= '2023'
;
--
--select count(*) from tmp_1m.kn_mbm_cosmos_claims;
---- 66425402 (without st_abbr_cd = market_fnl)
---- 60114648 (with st_abbr_cd = market_fnl)
--
--select sum(allw_amt_fnl) from tmp_1m.kn_mbm_cosmos_claims
--where fst_srvc_month = '202406'
---- 97534178.31 (without st_abbr_cd = market_fnl)
---- 86404147.25 (with st_abbr_cd = market_fnl)

-- CSP
drop table if exists tmp_1m.kn_mbm_csp_claims;
create table tmp_1m.kn_mbm_csp_claims as
select
	'CSP' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd
    , primary_diag_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
	, allw_amt_fnl
	, net_pd_amt_fnl
from tadm_tre_cpy.dcsp_pr_f_202512
where brand_fnl = 'C&S'
	and global_cap = 'NA'		
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
	and st_abbr_cd = market_fnl
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     
	and (proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and fst_srvc_year >= '2023'
union all
select
	'CSP' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd
    , primary_diag_cd
    , hce_month as fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
	, allw_amt_fnl
	, net_pd_amt_fnl
from tadm_tre_cpy.dcsp_op_f_202512
where brand_fnl = 'C&S'
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
	and st_abbr_cd = market_fnl
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     
	and (proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and fst_srvc_year >= '2023'
;


-- NICE 
drop table if exists tmp_1m.kn_mbm_nice_claims;
create table tmp_1m.kn_mbm_nice_claims as
select
	'NICE' as entity
	, clm_aud_nbr as clm_id
	, mbi_hicn_fnl as mbi
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd
    , primary_diag_cd
    , fst_srvc_month
    , fst_srvc_year
    , iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
	, calc_allw as allw_amt_fnl
	, calc_net_pd as net_pd_amt_fnl
from fichsrv.nce_pr_f
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and dnl_f = 'N'
	and st_abbr_cd = market_fnl
	and (clm_cap_flag = 'FFS' and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	-- and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (claim_place_of_svc_cd != 12 or claim_place_of_svc_cd != '12')     
	and proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942')
	and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and fst_srvc_year >= '2023'
union all
select
	'NICE' as entity
	, clm_aud_nbr as clm_id
	, mbi_hicn_fnl as mbi
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd
    , primary_diag_cd
    , hce_month as fst_srvc_month
    , fst_srvc_year
    , iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
	, allw_amt as allw_amt_fnl
	, net_pd_amt as net_pd_amt_fnl
from fichsrv.nce_op_f
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and dnl_f = 'N'
	and st_abbr_cd = market_fnl
	and (clm_cap_flag = 'FFS' and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	--and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3') -- Home Health
	and (claim_place_of_svc_cd != 12 or claim_place_of_svc_cd != '12')     
	and proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
	and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and fst_srvc_year >= '2023'
;


drop table if exists tmp_1m.kn_mbm_cosmos_csp_nice_claims;
create table tmp_1m.kn_mbm_cosmos_csp_nice_claims as
with cte_union as (
select
	entity
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
    , proc_cd
    , primary_diag_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'COSMOS' AND product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'NICE') 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.kn_mbm_cosmos_claims
union all
select
	entity
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
    , proc_cd
    , primary_diag_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'COSMOS' AND product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'NICE') 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.kn_mbm_csp_claims
union all
select
	entity
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
    , proc_cd
    , primary_diag_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'COSMOS' AND product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'NICE') 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.kn_mbm_nice_claims
)
select
	*
	, case when OAH_flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when MnR_Dual_flag = 1 then 'M&R DSNP'
		   when MnR_ISNP_flag = 1 then 'M&R ISNP' 
		   when MnR_FFS_flag  = 1 then 'M&R FFS'
		   
		   else 'N/A'
	end as population
from cte_union;

drop table if exists tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary;
create table tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary as
select 
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , count(distinct clm_id) as n_claims
    , sum(allw_amt_fnl) as sum_allowed
    , sum(net_pd_amt_fnl) as sum_paid
from tmp_1m.kn_mbm_cosmos_csp_nice_claims
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
;
	
select count(*) from tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary;
-- 202509: 246402
-- 202601: 

select population, sum(sum_allowed)
from tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary
where fst_srvc_month = '202406'
group by population
order by sum(sum_allowed) desc
;
-- 202509
--POPULATION	SUM(SUM_ALLOWED)
--M&R FFS		73,919,589.898201
--OAH			14,392,699.77
--C&S DSNP		12,613,322.57
--M&R ISNP		5,282,439.06
--M&R DSNP		2,311,986.38
--N/A C&S		1,055,759.11

-- 202601
-- POPULATION	SUM(SUM_ALLOWED)
-- M&R FFS	73,966,242.718201
-- OAH	14,400,600.16
-- C&S DSNP	13,741,959.4
-- M&R ISNP	5,283,744.21
-- M&R DSNP	2,311,005.89




/*==============================================================================
 * Membership
 *==============================================================================*/
-- COSMOS
drop table if exists tmp_1m.kn_mbm_cosmos_mm; 
create table tmp_1m.kn_mbm_cosmos_mm as 
select 
	'COSMOS' as entity
	, '' as component
	, '' as service_code
	, fin_inc_month as fst_srvc_month
	, fin_inc_year as fst_srvc_year
	, global_cap
	, fin_market as market_fnl
	, fin_state as st_abbr_cd
	, fin_brand as brand_fnl
	, fin_g_i as group_ind_fnl
	, tfm_include_flag
	, migration_source
	, fin_tfm_product_new as tfm_product_new_fnl 
	, fin_product_level_3 as product_level_3_fnl
	, nce_tadm_dec_risk_type
	, fin_member_cnt
from fichsrv.tre_membership
where		
	sgr_source_name = 'COSMOS'
	and fin_brand in ('M&R', 'C&S')
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
;

select count(*) from tmp_1m.kn_mbm_cosmos_mm;
-- 197887476 (without fin_state = fin_market)
-- 

select sum(fin_member_cnt) from tmp_1m.kn_mbm_cosmos_mm
where fst_srvc_month = '202406'
-- 5986575 (without fin_state = fin_market)
-- 5552675 (with fin_state = fin_market)


-- CSP
drop table if exists tmp_1m.kn_mbm_csp_mm;
create table tmp_1m.kn_mbm_csp_mm as 
select 
	'CSP' as entity
	, '' as component
	, '' as service_code
	, fin_inc_month as fst_srvc_month
	, fin_inc_year as fst_srvc_year
	, global_cap
	, fin_market as market_fnl
	, fin_state as st_abbr_cd
	, fin_brand as brand_fnl
	, fin_g_i as group_ind_fnl
	, tfm_include_flag
	, migration_source
	, fin_tfm_product_new as tfm_product_new_fnl 
	, fin_product_level_3 as product_level_3_fnl
	, nce_tadm_dec_risk_type
	, fin_member_cnt
from fichsrv.tre_membership
where		
	sgr_source_name = 'CSP'
	and fin_brand in ('M&R', 'C&S')
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
;

-- NICE
drop table if exists tmp_1m.kn_mbm_nice_mm;
create table tmp_1m.kn_mbm_nice_mm as 
select 
	'NICE' as entity
	, '' as component
	, '' as service_code
	, fin_inc_month as fst_srvc_month
	, fin_inc_year as fst_srvc_year
	, global_cap
	, fin_market as market_fnl
	, fin_state as st_abbr_cd
	, fin_brand as brand_fnl
	, fin_g_i as group_ind_fnl
	, tfm_include_flag
	, migration_source
	, fin_tfm_product_new as tfm_product_new_fnl 
	, fin_product_level_3 as product_level_3_fnl
	, nce_tadm_dec_risk_type
	, fin_member_cnt
from fichsrv.tre_membership
where		
	sgr_source_name = 'NICE'
	and fin_brand in ('M&R', 'C&S')
	and nce_tadm_dec_risk_type = 'FFS'
	and fin_inc_month >= '202301'
;


drop table if exists tmp_1m.kn_mbm_cosmos_csp_nice_mm; 
create table tmp_1m.kn_mbm_cosmos_csp_nice_mm as 
with cte_union as (
select
	entity
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , fin_member_cnt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
		when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
		when migration_source = 'OAH' then 1
		else 0
	end as OAH_flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'COSMOS' AND product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'NICE') 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.kn_mbm_cosmos_mm
union all
select
	entity
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , fin_member_cnt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
		when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
		when migration_source = 'OAH' then 1
		else 0
	end as OAH_flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'COSMOS' AND product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'NICE') 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.kn_mbm_csp_mm
union all
select
	entity
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , fin_member_cnt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
		when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
		when migration_source = 'OAH' then 1
		else 0
	end as OAH_flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD')
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'COSMOS' AND product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and entity = 'NICE') 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.kn_mbm_nice_mm
)
select
	*
	, case when OAH_flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when MnR_Dual_flag = 1 then 'M&R DSNP'
		   when MnR_ISNP_flag = 1 then 'M&R ISNP' 
		   when MnR_FFS_flag  = 1 then 'M&R FFS'
		   else 'N/A'
	end as population
from cte_union;


drop table if exists tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary; 
create table tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary as 
select 
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , sum(fin_member_cnt) as sum_mm
from tmp_1m.kn_mbm_cosmos_csp_nice_mm
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
;


select count(*) from tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary
-- 202509: 16730
-- 202601: 18859


select population, sum(sum_mm)
from tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary
where fst_srvc_month = '202406'
group by population
order by sum(sum_mm) desc
;
-- 202509
--POPULATION	SUM(SUM_MM)
--M&R FFS		5,405,476
--OAH			957,950
--C&S DSNP		693,219
--M&R DSNP		165,350
--N/A C&S		72,074
--M&R ISNP		69,252

-- POPULATION	SUM(SUM_MM)
-- M&R FFS	5,404,687
-- OAH	957,874
-- C&S DSNP	765,134
-- M&R DSNP	165,334
-- M&R ISNP	69,239




/*==============================================================================
 * Union Claims and Membership
 *==============================================================================*/
drop table if exists tmp_1m.kn_mbm_cosmos_csp_nice_claims_mm_summary;
create table tmp_1m.kn_mbm_cosmos_csp_nice_claims_mm_summary as
select
	'Claims' as data_type
	, entity
	, population
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , sum(n_claims) as n_claims
    , sum(sum_allowed) as sum_allowed
    , sum(sum_paid) as sum_paid
    , 0 as sum_mm
from tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
union all
select
	'Membership' as data_type
	, entity
	, population
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , 0 as n_claims
    , 0 as sum_allowed
    , 0 as sum_paid
    , sum(sum_mm) as sum_mm
from tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
;

select count(*) from tmp_1m.kn_mbm_cosmos_csp_nice_claims_mm_summary;
-- 263132

select population, sum(sum_allowed) as allowed, sum(sum_mm) as mm, sum(sum_allowed) / sum(sum_mm) as pmpm
from tmp_1m.kn_mbm_cosmos_csp_nice_claims_mm_summary
where fst_srvc_month = '202406'
group by population
order by sum(sum_allowed) / sum(sum_mm) desc
;
-- Without st_abbr_cd = market_fnl
--POPULATION	ALLOWED				MM			PMPM
--M&R ISNP		5,486,517.3			69,252		79.225398544
--C&S DSNP		13,225,400.83		693,219		19.078243427
--OAH			15,173,140.73		957,950		15.839178172
--M&R FFS		84,536,326.288201	5,405,476	15.639016118
--N/A C&S		1,084,628.02		72,074		15.048811222
--M&R DSNP		2,374,736.43		165,350		14.361877412

-- With st_abbr_cd = market_fnl
--POPULATION	ALLOWED				MM			PMPM
--M&R ISNP		5,282,439.06		69,252		76.278505458
--C&S DSNP		12,613,322.57		693,219		18.195292642
--OAH			14,392,699.77		957,950		15.024479117
--N/A C&S		1,055,759.11		72,074		14.64826581
--M&R DSNP		2,311,986.38		165,350		13.982379075
--M&R FFS		73,919,589.898201	5,405,476	13.674945536

-- POPULATION	ALLOWED	MM	PMPM
-- M&R ISNP	5,283,744.21	69,239	76.31167709
-- C&S DSNP	13,741,959.4	765,134	17.960199651
-- OAH	14,400,600.16	957,874	15.033919033
-- M&R DSNP	2,311,005.89	165,334	13.977801844
-- M&R FFS	73,966,242.718201	5,404,687	13.685573784

select 
	population
	, product_level_3_fnl 
	, sum(sum_allowed)
from tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary
where entity = 'COSMOS'
	and fst_srvc_month = '202406'
group by population
	, product_level_3_fnl 

	

select * from tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary
where st_abbr_cd != market_fnl
