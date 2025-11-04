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
from fichsrv.cosmos_pr
where brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
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
from fichsrv.cosmos_op
where brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
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
from tadm_tre_cpy.dcsp_pr_f_202509
where brand_fnl = 'C&S'
	and global_cap = 'NA'		
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
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
from tadm_tre_cpy.dcsp_op_f_202509
where brand_fnl = 'C&S'
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'N'
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
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.nice_pr
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'P'
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
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.nice_op
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and clm_dnl_f = 'P'
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
		end as OAH_Flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and st_abbr_cd not in ('OK','NC','NM','NV','OH','TX'))
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
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
		end as OAH_Flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and st_abbr_cd not in ('OK','NC','NM','NV','OH','TX'))
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
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
		end as OAH_Flag
	, case when (
			   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and st_abbr_cd not in ('OK','NC','NM','NV','OH','TX'))
			or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD') 
			or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
			) then 1
		else 0
	end as CnS_Dual_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.kn_mbm_nice_claims
)
select
	*
	, case when OAH_Flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 'M&R DSNP'
		   when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 'M&R ISNP' 
		   when brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and st_abbr_cd in ('OK','NC','NM','NV','OH','TX') then 'N/A C&S'
		   else 'M&R FFS'
	end as entity_pop
from cte_union;

drop table if exists tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary;
create table tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary as
select 
	entity
	, entity_pop
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
	, entity_pop
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
-- 246402

/*==============================================================================
 * Membership
 *==============================================================================*/
drop table tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary; 
create table tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary as 
with cte_union as (
select 
	fin_brand as brand_fnl
	, 'COSMOS' as entity
	, fin_inc_month as fst_srvc_month 
	, fin_market as market_fnl 
	, fin_state as st_abbr_cd
	, fin_g_i as group_ind_fnl 	
	, fin_product_level_3 as product_level_3_fnl
	, fin_tfm_product_new as tfm_product_new_fnl 
	, migration_source
	, global_cap
	, '' as component
	, '' as service_code
	, '' as proc_cd
	, '' as primary_diag_cd
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	      when fin_product_level_3 = 'DUAL' then 'M&R DSNP'
	      else 'M&R FFS' end as entity_pop1
	, sum(fin_member_cnt) as mm 
	, case when fin_inc_year ='2024' and migration_source = 'OAH' and FIN_MARKET = 'MD' then 2
	      when  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	, 0 as CnS_Dual_flag
from fichsrv.tre_membership 
where		
	sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
group by 
	fin_brand
	, fin_inc_month
	, fin_market 
	, fin_state
	, fin_g_i 
	, fin_product_level_3 
	, fin_tfm_product_new 
	, migration_source
	, global_cap
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	      when fin_product_level_3 = 'DUAL' then 'M&R DSNP'
	      else 'M&R FFS' end	
	, case when fin_inc_year ='2024' and migration_source = 'OAH' and FIN_MARKET = 'MD' then 2
	      when  migration_source='OAH'  then 1 else 0 end 
union all
select
	fin_brand as brand_fnl
	, 'COSMOS' as entity
	, fin_inc_month as fst_srvc_month 
	, fin_market as market_fnl  
	, fin_state as st_abbr_cd
	, fin_g_i as group_ind_fnl 	
	, fin_product_level_3 as product_level_3_fnl
	, fin_tfm_product_new as tfm_product_new_fnl 
	, migration_source
	, global_cap
	, '' as component
	, '' as service_code
	, '' as proc_cd
	, '' as primary_diag_cd
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end as entity_pop1
	, sum(fin_member_cnt) as mm 
	, case when fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD' then 0
		  when  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	, case when (( migration_source <> 'OAH' and fin_product_level_3='DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) or 
	            (fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag	
from fichsrv.tre_membership 
where		
	fin_brand = 'C&S'	
	and sgr_source_name = 'COSMOS'
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
group by 	
	fin_brand
	, fin_inc_month
	, fin_market
	, fin_state
	, fin_g_i
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end
	, case when fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD' then 0
		  when  migration_source='OAH'  then 1 else 0 end 
	, case when (( migration_source <> 'OAH' and fin_product_level_3='DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) or 
	            (fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end
union all
select
	fin_brand as brand_fnl
	, 'CSP' as entity
	, fin_inc_month as fst_srvc_month 
	, fin_market as market_fnl  
	, fin_state as st_abbr_cd
	, fin_g_i as group_ind_fnl 	
	, fin_product_level_3 as product_level_3_fnl
	, fin_tfm_product_new as tfm_product_new_fnl 
	, migration_source
	, global_cap
	, '' as component
	, '' as service_code
	, '' as proc_cd
	, '' as primary_diag_cd
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end as entity_pop1
	, sum(fin_member_cnt) as mm 
	, case when fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD' then 0
          when  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	, case when (( migration_source <> 'OAH' and fin_product_level_3='DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) or 
	            (fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag
from fichsrv.tre_membership 
where		
	fin_brand = 'C&S'	
	and sgr_source_name = 'CSP'
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
group by 	
	fin_brand
	, fin_inc_month 
	, fin_market
	, fin_state 
	, fin_g_i 	
	, fin_product_level_3 
	, fin_tfm_product_new 
	, migration_source
	, global_cap
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' end
	, case when fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD' then 0
          when  migration_source='OAH'  then 1 else 0 end
	,case when (( migration_source <> 'OAH' and fin_product_level_3='DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) or 
	            (fin_inc_year ='2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end
union all
select
	fin_brand as brand_fnl
	, 'NICE' as entity
	, fin_inc_month as fst_srvc_month 
	, fin_market as market_fnl  
	, fin_state as st_abbr_cd
	, fin_g_i as group_ind_fnl 	
	, fin_product_level_3 as product_level_3_fnl
	, fin_tfm_product_new as tfm_product_new_fnl 
	, migration_source
	, global_cap
	, '' as component
	, '' as service_code
	, '' as proc_cd
	, '' as primary_diag_cd
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	,'M&R FFS' as entity_pop1
	, sum(fin_member_cnt) as mm 
	, case when  migration_source='OAH'  then 1 else 0 end as OAH_FLAG
	, 0 as CnS_Dual_flag
from fichsrv.tre_membership 
where		
	fin_brand = 'M&R'	
	and sgr_source_name = 'NICE'
	and nce_tadm_dec_risk_type = 'FFS'
	and fin_inc_month >= '202301'
group by 	
	fin_brand
	, fin_inc_month 
	, fin_market
	, fin_state 
	, fin_g_i 	
	, fin_product_level_3 
	, fin_tfm_product_new 
	, migration_source
	, global_cap
	,  tfm_include_flag
	, nce_tadm_dec_risk_type
	, case when  migration_source='OAH'  then 1 else 0 end
)
select 
	*
	, case when OAH_Flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 'M&R DSNP'
		   when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 'M&R ISNP' 
		   when brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and st_abbr_cd in ('OK','NC','NM','NV','OH','TX') then 'N/A C&S'
		   else 'M&R FFS'
	end as entity_pop
from cte_union
;

select count(*) from tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary
-- 16718

/*==============================================================================
 * Union Claims and Membership
 *==============================================================================*/
drop table if exists tmp_1m.kn_mbm_cosmos_csp_nice_claims_mm;
create table tmp_1m.kn_mbm_cosmos_csp_nice_claims_mm as
select
	'Claims' as data_type
	, entity
	, entity_pop
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
    , 0 as mm
from tmp_1m.kn_mbm_cosmos_csp_nice_claims_summary
group by
	entity
	, entity_pop
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
	, entity_pop
    , fst_srvc_month
    , substring(fst_srvc_month, 0, 4) as fst_srvc_year
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
    , sum(mm) as mm
from tmp_1m.kn_mbm_cosmos_csp_nice_mm_summary
group by
	entity
	, entity_pop
    , fst_srvc_month
    , substring(fst_srvc_month, 0, 4)
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

select count(*) from tmp_1m.kn_mbm_cosmos_csp_nice_claims_mm;
-- 263120