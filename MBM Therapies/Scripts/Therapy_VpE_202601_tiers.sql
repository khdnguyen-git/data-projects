/*============================================================================================================
 * OP Therapies VpE Calculation
 * 02/09: added rvnu_cd to NICE
 * 02/09: fixed substring(coalesce(bil_typ_cd,'0'), 0, 1) != '3' to substring(coalesce(bil_typ_cd,'0'), 1, 1) != '3'
 * 02/09: removed service_code from visit aggregation
 * 
 * 02/05: removing claim_status case when (resulted in, same claim, same ID, 2 claim_status) in extraction,
 *    but adding it back after aggregation (not using clm_dnl_f field)
 * 02/05: changed M&R FFS to M&R FFS excl. DSNP (product_level_3 not in ('DUAL', 'INSTITUTIONAL')
 * 02/05: updated script to use lag() window instead of 2 int. tables
 * Visits definition (from _stable script): count(concat(eventkey, fst_srvc_dt))
 *  Eventkey: field in claims data, equiv. to mbi | fst_srvc_dt | srvc_prov_id 
 * Episode definition
 *	Partition by mbi-category (Office/Chiro/OP_Rehab), mbm_deploy_dt (National/Pilot)
 *  Order by fst_srvc_dt
 *  If the (current fst_srvc_dt - previous fst_srvc_dt) for this partition > 30 -> New Episode
 *  Or if (current fst_srvc_dt - previous fst_srvc_dt) is NULL -> New Episode
 *===========================================================================================================*/


-- COSMOS claims
drop table if exists tmp_1m.knd_mbm_cosmos_claims;
create table tmp_1m.knd_mbm_cosmos_claims as
select
	'COSMOS' as entity
	, component
	, eventkey as visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, adjd_dt)) as hctapaidmonth
    , fst_srvc_year
	, gal_mbi_hicn_fnl as mbi
    , proc_cd
    , case
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , prov_tin
    , primary_diag_cd
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , case when market_fnl in ('AR','GA','NJ','SC') and group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as mbm_deploy_dt
--    , case when clm_dnl_f = 'N' then 'Paid'
--    	else 'Denied'
--    end as claim_status
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.glxy_pr_f
where brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and st_abbr_cd = market_fnl
	and substring(coalesce(bil_typ_cd,'0'),1,1) != '3'
	and ama_pl_of_srvc_cd != '12' 
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
	, component
	, eventkey as visit_id
	, hce_service_code as service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, adjd_dt)) as hctapaidmonth
    , fst_srvc_year
	, gal_mbi_hicn_fnl as mbi
    , proc_cd
    , case
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , prov_tin
 	, primary_diag_cd
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , case when market_fnl in ('AR','GA','NJ','SC') and group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as mbm_deploy_dt
--    , case when clm_dnl_f = 'N' then 'Paid'
--    	else 'Denied'
--    end as claim_status
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.glxy_op_f
where brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'
	and st_abbr_cd = market_fnl
	and substring(coalesce(bil_typ_cd,'0'),1,1) != '3'
	and ama_pl_of_srvc_cd != '12'   
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

--select sum(allw_amt_fnl) from tmp_1m.knd_mbm_cosmos_claims
--where fst_srvc_month = '202406'
--;
-- 86404147.25 (202509)
-- 86475439.04 (202601)

-- CSP claims
drop table if exists tmp_1m.knd_mbm_csp_claims;
create table tmp_1m.knd_mbm_csp_claims as
select
	'CSP' as entity
	, component
	, eventkey as visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, adjd_dt)) as hctapaidmonth
    , fst_srvc_year
	, gal_mbi_hicn_fnl as mbi
    , proc_cd
    , case
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , tin as prov_tin
    , primary_diag_cd
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , case when market_fnl in ('AR','GA','NJ','SC') and group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as mbm_deploy_dt
--    , case when clm_dnl_f = 'N' then 'Paid'
--    	else 'Denied'
--    end as claim_status
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.dcsp_pr_f
where brand_fnl = 'C&S'	
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and st_abbr_cd = market_fnl
	and substring(coalesce(bil_typ_cd,'0'),1,1) != '3'
	and ama_pl_of_srvc_cd != '12'  
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
	, component
	, eventkey as visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, adjd_dt)) as hctapaidmonth
    , fst_srvc_year
	, gal_mbi_hicn_fnl as mbi
    , proc_cd
    , case
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , tin as prov_tin
    , primary_diag_cd
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , case when market_fnl in ('AR','GA','NJ','SC') and group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as mbm_deploy_dt
--    , case when clm_dnl_f = 'N' then 'Paid'
--    	else 'Denied'
--    end as claim_status
	, allw_amt_fnl
	, net_pd_amt_fnl
from fichsrv.dcsp_op_f
where brand_fnl = 'C&S'
	and global_cap = 'NA'
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and st_abbr_cd = market_fnl
	and substring(coalesce(bil_typ_cd,'0'),1,1) != '3'
	and ama_pl_of_srvc_cd != '12'
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

--select sum(allw_amt_fnl) from tmp_1m.knd_mbm_csp_claims
--where fst_srvc_month = '202406'
--;
-- 24460463.76 

--
--select * from fichsrv.nce_pr_f
--
--select
--	table_name
--	, column_name
--	, ordinal_position
--	, is_nullable
--	, data_type
--from information_schema.columns
--where table_schema = 'FICHSRV'
--	and table_name = 'NCE_PR_F'
--	and column_name ilike '%_cd%'
--order by column_name
--;

-- NICE claims
-- special_network doesn't exist in NCE; ericksonflag doesn't work
drop table if exists tmp_1m.knd_mbm_nice_claims;
create table tmp_1m.knd_mbm_nice_claims as
select
	'NICE' as entity
	, component
	, eventkey as visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, adjd_dt)) as hctapaidmonth
    , fst_srvc_year
	, mbi_hicn_fnl as mbi
    , proc_cd
    , case
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , tin as prov_tin
    , primary_diag_cd
	, iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , case when market_fnl in ('AR','GA','NJ','SC') and group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as mbm_deploy_dt
--    , case when dnl_f = 'N' then 'Paid'
--    	else 'Denied'
--    end as claim_status
	, calc_allw as allw_amt_fnl
	, calc_net_pd as net_pd_amt_fnl
from fichsrv.nce_pr_f
where brand_fnl = 'M&R'
	and plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and st_abbr_cd = market_fnl
	and (clm_cap_flag = 'FFS' and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(bil_typ_cd,'0'),1,1) != '3' -- Home Health
	and claim_place_of_svc_cd != '12'   
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
	'NICE' as entity
	, component
	, eventkey as visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, adjd_dt)) as hctapaidmonth
    , fst_srvc_year
	, mbi_hicn_fnl as mbi
    , proc_cd
    , case
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , tin as prov_tin
    , primary_diag_cd
	, iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
	, market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , case when market_fnl in ('AR','GA','NJ','SC') and group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as mbm_deploy_dt
--    , case when dnl_f = 'N' then 'Paid'
--    	else 'Denied'
--    end as claim_status
	, allw_amt as allw_amt_fnl
	, net_pd_amt as net_pd_amt_fnl
from fichsrv.nce_op_f
where brand_fnl = 'M&R'
	and plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'		
	and st_abbr_cd = market_fnl
	and (clm_cap_flag = 'FFS' and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(bil_typ_cd,'0'),1,1) != '3' -- Home Health
	and claim_place_of_svc_cd != '12'
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

-- Stack COSMOS + CSP + NICE claims
-- Make flags for population
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims;
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims as
with cte_union as (
select
	entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
	, hctapaidmonth
    , fst_srvc_year
	, mbi
	, proc_cd
	, category
	, prov_tin
    , primary_diag_cd
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, mbm_deploy_dt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
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
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
-- 	, claim_status
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_claims
union all
select
	entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , hctapaidmonth
    , fst_srvc_year
	, mbi
	, proc_cd
	, category
	, prov_tin
    , primary_diag_cd
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, mbm_deploy_dt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
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
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
-- 	, claim_status
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_csp_claims
union all
select
	entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , hctapaidmonth
    , fst_srvc_year
	, mbi
	, proc_cd
	, category
	, prov_tin
    , primary_diag_cd
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, mbm_deploy_dt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
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
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
-- 	, claim_status
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_nice_claims
)
select
	*
	, case when OAH_flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when MnR_Dual_flag = 1 then 'M&R DSNP'
		   when MnR_ISNP_flag = 1 then 'M&R ISNP'
		   when MnR_FFS_flag  = 1 then 'M&R FFS (excl. DSNP)'
		   else 'N/A'
	end as population
from cte_union;

-- Aggregate to sum(allowed) and sum(paid) before VpE analysis
-- Adding claim_status
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated;
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated as
with aggregated as (
select
	population
	, entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
	, fst_srvc_month
	, fst_srvc_qtr
	, hctapaidmonth
	, fst_srvc_year
	, mbi
	, proc_cd
	, category
	, prov_tin
	, primary_diag_cd
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, tfm_product_new_fnl
	, product_level_3_fnl
	, mbm_deploy_dt
    , sum(allw_amt_fnl) as allw_amt_fnl
    , sum(net_pd_amt_fnl) as net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_csp_nice_claims
group by
	population
	, entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
	, fst_srvc_month
	, fst_srvc_qtr
	, hctapaidmonth
	, fst_srvc_year
	, mbi
	, proc_cd
	, category
	, prov_tin
	, primary_diag_cd
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, tfm_product_new_fnl
	, product_level_3_fnl
	, mbm_deploy_dt
)
select
	*
	, iff(sum(allw_amt_fnl) over (partition by visit_id, fst_srvc_dt, category)  > 0.01, 'Paid', 'Denied') as claim_status
from aggregated
;

--9PK2N41YE29_20250303_KLC00710019979
--6H22MK7TG16_20240701_NTL00500002437
select 
	*
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated
where visit_id = '6H22MK7TG16_20240701_NTL00500002437'


-- Defining visits ranking structure
-- Grouping proc_cd into mbmserv_dtl (PT/OT/ST, )
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1;
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1 as
select 
    entity
    , concat(mbi, '-', category) as mbi_key
	, component
	, visit_id
    , case when proc_cd in ('98940','98941','98942') 
    		then 'Chiro'
           when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') 
			then 'PT-OT'
           when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') 
           	then 'ST'
           else 'Other' 
	end as mbmserv_dtl
	, fst_srvc_dt
    , fst_srvc_month
    , min(hctapaidmonth) as min_hctapaidmonth
    , fst_srvc_year
	, category
    , market_fnl
    , mbm_deploy_dt
    , population
    , claim_status
--    , count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated
group by
    entity
    , concat(mbi, '-', category)
	, component
	, visit_id
    , case when proc_cd in ('98940','98941','98942') 
    		then 'Chiro'
           when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') 
			then 'PT-OT'
           when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') 
           	then 'ST'
           else 'Other' 
	end
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_year
	, category
    , market_fnl
    , mbm_deploy_dt
    , population
    , claim_status
;

select * from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1
where visit_id = '9PK2N41YE29_20250303_KLC00710019979' and fst_srvc_dt = '2025-03-03'
-- 2 visits

select * from tmp_1q.kn_mbm_episode_3_202512
where id = '9PK2N41YE29_20250303_KLC00710019979' and start_dt = '2025-03-03'
-- 2 visits

select sum(allowed) from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1
where visit_id = '9PK2N41YE29_20250303_KLC00710019979'
-- 1327.57
select sum(allowed) from tmp_1q.kn_mbm_episode_3_202512
where id = '9PK2N41YE29_20250303_KLC00710019979'
-- 1364.94


select population, sum(allowed)
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1
where fst_srvc_month = '202406'
group by population
order by sum(allowed) desc
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
--POPULATION			SUM(ALLOWED)
--M&R FFS (excl. DSNP)	73,635,413.0198578
--OAH					14,202,190.01
--C&S DSNP				13,500,190.53
--M&R ISNP				5,135,002.06
--M&R DSNP				2,282,443.86

-- Mark new episode
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2;
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2 as
select
	mbi_key
	, entity
	, component
	, visit_id
	, mbmserv_dtl
	, fst_srvc_dt
    , fst_srvc_month
    , min_hctapaidmonth
    , fst_srvc_year
	, category
    , market_fnl
	, mbm_deploy_dt
    , population
    , claim_status
    , allowed
    , paid
 	, lag(fst_srvc_dt) over (partition by mbi_key, mbm_deploy_dt order by fst_srvc_dt) 
 	as prev_srvc_dt
    , datediff('day'
    		, lag(fst_srvc_dt) over (partition by mbi_key, mbm_deploy_dt order by fst_srvc_dt)
    		, fst_srvc_dt) 
    as visit_day_diff
    , iff(datediff('day'
    		, lag(fst_srvc_dt) over (partition by mbi_key, mbm_deploy_dt order by fst_srvc_dt)
    		, fst_srvc_dt) > 30, 1 , 0) 
    as ep_flag
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1
;

select * from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1
where visit_id = '9PK2N41YE29_20250303_KLC00710019979'

select * from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2
where visit_id = '9PK2N41YE29_20250303_KLC00710019979'
-- 1 continuous episode
-- 19 visits

select * from tmp_1q.kn_mbm_episode_lag_202512
where id = '9PK2N41YE29_20250303_KLC00710019979'
-- 1 continuous episode
-- 19 visits


-- Episodes
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3 as
with ep_numbering as 
(
select
	*
  	, sum(iff(prev_srvc_dt is null, 1, ep_flag)) over (partition by mbi_key, mbm_deploy_dt order by fst_srvc_dt rows between unbounded preceding and current row) 
  	as cmltv_episodes
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2
)
select 
	mbi_key
	, fst_srvc_dt
	, prev_srvc_dt
	, visit_day_diff
	, iff(prev_srvc_dt is null, 1, ep_flag) as ep_flag 
	, cmltv_episodes
	, min(fst_srvc_dt) over (partition by mbi_key, mbm_deploy_dt, cmltv_episodes) as ep_start_dt
	, min(min_hctapaidmonth) over (partition by mbi_key, mbm_deploy_dt, cmltv_episodes) as ep_hctapaidmonth
	, entity
	, mbmserv_dtl
	, visit_id
    , fst_srvc_month
    , min_hctapaidmonth
    , fst_srvc_year
	, category
    , market_fnl
	, mbm_deploy_dt
    , population
    , claim_status
    , allowed
    , paid    
from ep_numbering
;

select * from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2
where mbi_key = '9PK2N41YE29-OP_REHAB'


select * from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
where visit_id = '9PK2N41YE29_20250303_KLC00710019979'
      
-- Episodes summary
create or replace table tmp_1m.knd_mbm_episodes_summary as
select 
	'EPISODES' as data_type
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') as ep_start_year
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
	, cast(null as varchar) as visit_month
	, cast(null as varchar) as visit_year
	, cast(null as varchar) as visit_paid_month
	, ep_hctapaidmonth as ep_paid_month
	, entity
	, mbmserv_dtl
	, category
	, market_fnl
	, mbm_deploy_dt
	, population
	, claim_status
	, 0 as visit_ep_runout_month
	, 0 as visit_runout_month
	, sum(ep_flag) as n_episodes
	, 0 as n_visits
	, 0 as sum_allowed
	, 0 as sum_paid
	, 0 as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
where ep_flag = 1  -- Filter for only episode-starting visits
group by 
	to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy')
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
	, ep_hctapaidmonth
	, entity
	, mbmserv_dtl
	, category
	, market_fnl
	, mbm_deploy_dt
	, population
	, claim_status
;

--Visits summary
create or replace table tmp_1m.knd_mbm_visits_summary as
select
    'VISITS' as data_type
    , to_char(ep_start_dt, 'yyyyMM') as ep_start_month
    , to_char(ep_start_dt, 'yyyy') as ep_start_year
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
    , fst_srvc_month as visit_month
    , fst_srvc_year as visit_year
    , min_hctapaidmonth as visit_paid_month
    , cast(null as varchar) as ep_paid_month
    , entity
    , mbmserv_dtl
    , category
    , market_fnl
    , mbm_deploy_dt
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5) as visit_ep_runout_month
    , floor((datediff('day', fst_srvc_dt, min_hctapaidmonth) + 20) / 30.5) as visit_runout_month
    , 0 as n_episodes
    , count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
    , sum(allowed) as sum_allowed
    , sum(paid) as sum_paid
    , count(distinct mbi_key) as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
group by
    to_char(ep_start_dt, 'yyyyMM')
    , to_char(ep_start_dt, 'yyyy')
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
    , fst_srvc_month
    , fst_srvc_year
    , min_hctapaidmonth
    , entity
    , mbmserv_dtl
    , category
    , market_fnl
    , mbm_deploy_dt
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5)
    , floor((datediff('day', fst_srvc_dt, min_hctapaidmonth) + 20) / 30.5);
;

-- Stack VISITS and EPISODES
create or replace table tmp_1m.knd_mbm_visits_episodes_stacked as
select * from tmp_1m.knd_mbm_visits_summary
union all
select * from tmp_1m.knd_mbm_episodes_summary
;

-- Summary
create or replace table tmp_1m.knd_mbm_vpe_summary as
select
    data_type
    , ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , mbmserv_dtl
    , category
    , market_fnl
    , mbm_deploy_dt
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
    , sum(n_episodes) as total_episodes
    , sum(n_visits) as total_visits
    , sum(sum_allowed) as allowed
    , sum(sum_paid) as paid
    , sum(mbr_count) as mbr_count
from tmp_1m.knd_mbm_visits_episodes_stacked
group by
    data_type
    , ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , mbmserv_dtl
    , category
    , market_fnl
    , mbm_deploy_dt
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
;
-- Remove 'NA' population before loading into Excel
create or replace table tmp_1m.knd_mbm_visits_episodes_extract as
select
	*
from tmp_1m.knd_mbm_vpe_summary
where population != 'NA'
;



-- VpE in episodes
create or replace table tmp_1m.knd_mbm_vpe_aggregated as
with vpe as (
select
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, ep_hctapaidmonth
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt) as ep_start_qtr
	, mbm_deploy_dt
	, category
	, population
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_vpe
	, sum(allowed) as allowed
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
group by
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, ep_hctapaidmonth
	, to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt)
	, mbm_deploy_dt
	, category
	, population
)
select * from vpe;

select * from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
where mbi_key = '9PK2N41YE29-OP_REHAB'

select * from tmp_1m.knd_mbm_vpe_aggregated
where mbi_key = '9PK2N41YE29-OP_REHAB'

select * from tmp_1m.knd_mbm_vpe_aggregated_category_mnr
where mbi_key = '9PK2N41YE29-OP_REHAB'


-- VpE in episodes
create or replace table tmp_1m.knd_mbm_vpe_aggregated_category_mnr as
with pct_mnr as (
    select
        *
        , percentile_cont(0.25) within group (order by n_vpe)
            over (partition by mbi_key, mbm_deploy_dt, category) as p25
        , percentile_cont(0.50) within group (order by n_vpe)
            over (partition by mbi_key, mbm_deploy_dt, category) as p50
        , percentile_cont(0.75) within group (order by n_vpe)
            over (partition by mbi_key, mbm_deploy_dt, category) as p75
    from tmp_1m.knd_mbm_vpe_aggregated
    where population = 'M&R FFS (excl. DSNP)'
)
select
	*
	, case when n_vpe between 0 and 6 then '0 - 6'
		   when n_vpe between 7 and 12 then '7 - 12'
		   when n_vpe between 13 and 24 then '13 - 24'
		   when n_vpe between 25 and 35 then '25 - 35'
		   when n_vpe between 36 and 45 then '36 - 45'
		   when n_vpe >= 46 then '46+'
		   else ''
	end as vpe_cat1
    , case when n_vpe between 1 and 10 then '1 - 10'
           when n_vpe between 11 and 20 then '11 - 20'
           when n_vpe between 21 and 30 then '21 - 30'
           when n_vpe >= 31 then '31+'
           else ''
    end as vpe_cat2
	, case when n_vpe > (p75 + 1.5 * (p75 - p25)) then 'Outlier'
		   when n_vpe >= p50 then 'High'
		   else 'Low - Normal'
	end as vpe_cat3
	, 1 as n_episodes
from pct_mnr
where ep_start_month >= '202401'
;

select * from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
where mbi_key = '9PK2N41YE29-OP_REHAB'
;
select * from tmp_1m.knd_mbm_vpe_aggregated_category_mnr
where mbi_key = '9PK2N41YE29-OP_REHAB'
;







select * from tmp_1m.knd_mbm_vpe_aggregated_category_mnr
where mbi_key = '3WA5F82HJ95-OP_REHAB'

select * from tmp_1m.knd_mbm_vpe_aggregated
where mbi_key = '3WA5F82HJ95-OP_REHAB'


-- VpE in episodes
create or replace table tmp_1m.knd_mbm_vpe_aggregated_test as
with vpe as (
select
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt) as ep_start_qtr
	, mbm_deploy_dt
	, market_fnl
	, category
	, population
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
	, sum(allowed) as allowed
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
where mbi_key = '9PK2N41YE29-OP_REHAB'
group by
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt)
	, mbm_deploy_dt
	, market_fnl
	, category
	, population
)
select
	ep_start_month
	, ep_start_qtr
	, mbm_deploy_dt
	, category
	, market_fnl
	, population
	, case when n_visits between 0 and 6 then '0 - 6'
		   when n_visits between 7 and 12 then '7 - 12'
		   when n_visits between 13 and 24 then '13 - 24'
		   when n_visits between 25 and 35 then '25 - 35'
		   when n_visits between 36 and 45 then '36 - 45'
		   when n_visits >= 46 then '46+'
		   else ''
	end as vpe_cat
	, 1 as n_episodes
	, n_visits
	, allowed
from vpe
where ep_start_month >= '202401'
;

select ep_start_qtr
, vpe_cat
, sum(n_visits)
, sum(n_episodes)
from tmp_1m.knd_mbm_vpe_aggregated_test
group by 1, 2

select count(*) from tmp_1m.knd_mbm_vpe_aggregated_category


select * from tmp_1m.knd_mbm_vpe_aggregated
where mbi_key = '9PK2N41YE29-OP_REHAB'

select 
	mbi
	, category
	, fst_srvc_dt
	, count(distinct concat(visit_id, fst_srvc_dt))
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated
where mbi = '9PK2N41YE29' and category = 'OP_REHAB'
group by 1, 2,3
order by fst_srvc_dt


select count(*) from tmp_1m.knd_mbm_vpe_aggregated_category





select 
	mbi
	, category
	, visit_id
	, proc_cd
    , case when proc_cd in ('98940','98941','98942') 
    		then 'Chiro'
           when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') 
			then 'PT-OT'
           when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') 
           	then 'ST'
           else 'Other' 
	end as mbmserv_dtl
	, fst_srvc_dt
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated
where mbi = '9PK2N41YE29' and category = 'OP_REHAB'
order by fst_srvc_dt

select
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt) as ep_start_qtr
	, mbm_deploy_dt
	, category
	, population
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
	, sum(allowed) as allowed
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3
where mbi_key = '9PK2N41YE29-OP_REHAB'
group by
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt)
	, mbm_deploy_dt
	, category
	, population




































































select
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt) as ep_start_qtr
	, mbm_deploy_dt
	, category
	, population
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
	, sum(allowed) as allowed
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3 
 group by
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt)
	, mbm_deploy_dt
	, category
	, population

select * fr










select
	vpe_cat
	, sum(n_visits)
	, sum(n_episodes)
from tmp_1m.knd_mbm_vpe_cat
where ep_start_month >= '202401'  
	and population = 'M&R FFS (excl. DSNP)'
group by 1
order by 1
;
	





-- QA 
-- Results
select
	ep_start_month
	, sum(allowed) as total_allowed
	, sum(total_episodes) as total_episodes
	, sum(total_visits) as total_visits
	, sum(total_visits) / sum(total_episodes) as VpE
from tmp_1m.knd_mbm_vpe_summary
where ep_start_year = '2024'  
	and population = 'M&R FFS (excl. DSNP)'
	and mbm_deploy_dt = 'National'
	and category = 'OP_REHAB'
group by 1
order by 1
;
-- Post-fixed
--EP_START_MONTH	TOTAL_ALLOWED	TOTAL_EPISODES	TOTAL_VISITS
--202401	95,657,095.5251416	161,953	1,447,681
--202402	76,240,951.991272	135,814	1,139,411
--202403	73,545,042.8449925	129,499	1,093,063
--202404	77,920,660.3424232	144,207	1,167,808
--202405	74,731,816.9506473	141,701	1,124,527
--202406	69,959,438.6068112	136,320	1,055,334
--202407	72,987,961.7960574	147,120	1,131,405
--202408	65,352,330.2667911	143,450	1,059,541
--202409	59,568,380.9378911	134,551	981,670
--202410	68,326,695.5308343	148,636	1,090,604
--202411	57,084,270.87	124,565	890,164
--202412	53,163,106.5609544	121,512	827,522
--EP_START_MONTH	TOTAL_ALLOWED	TOTAL_EPISODES	TOTAL_VISITS	VPE
--202401	95,657,095.5251416	161,953	1,447,681	8.938896
--202402	76,240,951.991272	135,814	1,139,411	8.389496
--202403	73,545,042.8449926	129,499	1,093,063	8.440706
--202404	77,920,660.3424233	144,207	1,167,808	8.098137
--202405	74,731,816.9506473	141,701	1,124,527	7.935914
--202406	69,959,438.6068112	136,320	1,055,334	7.741593
--202407	72,987,961.7960574	147,120	1,131,405	7.690355
--202408	65,352,330.2667911	143,450	1,059,541	7.386135
--202409	59,568,380.9378911	134,551	981,670	7.295895
--202410	68,326,695.5308343	148,636	1,090,604	7.337415
--202411	57,084,270.87	124,565	890,164	7.146181
--202412	53,163,106.5609544	121,512	827,522	6.810208

-- Pre-fixed
--EP_START_MONTH	TOTAL_ALLOWED	TOTAL_EPISODES	TOTAL_VISITS
--202401	95,324,065.4467985	161,953	1,544,757
--202402	75,937,868.231272	135,814	1,217,347
--202403	73,214,336.9749926	129,499	1,164,517
--202404	77,584,576.0324233	144,207	1,246,655
--202405	74,408,843.3706473	141,701	1,199,449
--202406	69,612,628.6668112	136,319	1,125,923
--202407	72,637,804.7460574	147,120	1,216,173
--202408	65,030,054.2067911	143,450	1,152,842
--202409	59,276,649.5178911	134,551	1,069,686
--202410	67,999,637.6008343	148,635	1,178,134
--202411	56,800,371.85	124,565	958,213
--202412	52,947,148.6709544	121,512	888,685

-- Current data
select 
	ep_start_mo
	, sum(allowed_amt)
	, sum(ep_cnt)
	, sum(visit_cnt)
	, sum(visit_cnt) / sum(ep_cnt)
from tmp_1q.kn_mbm_202601
where ep_year = '2024'
group by 1 
order by 1
;

--EP_START_MO	SUM(ALLOWED_AMT)	SUM(EP_CNT)	SUM(VISIT_CNT)
--202401	95,377,389.85	161,789	1,452,238
--202402	75,953,021.77	135,480	1,140,383
--202403	73,274,206.24	129,164	1,093,919
--202404	77,533,457.14	143,588	1,168,416
--202405	74,364,946	141,262	1,126,764
--202406	69,677,322.34	135,889	1,058,468
--202407	72,821,701.68	146,621	1,137,630
--202408	65,359,015.6	142,968	1,070,611
--202409	59,560,225.59	134,106	993,336
--202410	68,109,796.18	148,145	1,100,549
--202411	56,861,416.47	124,170	895,674
--202412	52,844,970.31	121,067	829,827


--- VpE Code ends here ---


/*==============================================================================
 * Membership
 *==============================================================================*/
-- COSMOS
drop table if exists tmp_1m.knd_mbm_cosmos_mm; 
create table tmp_1m.knd_mbm_cosmos_mm as 
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

select count(*) from tmp_1m.knd_mbm_cosmos_mm;
-- 197887476 (without fin_state = fin_market)
-- 

select sum(fin_member_cnt) from tmp_1m.knd_mbm_cosmos_mm
where fst_srvc_month = '202406'
-- 5986575 (without fin_state = fin_market)
-- 5552675 (with fin_state = fin_market)


-- CSP
drop table if exists tmp_1m.knd_mbm_csp_mm;
create table tmp_1m.knd_mbm_csp_mm as 
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
drop table if exists tmp_1m.knd_mbm_nice_mm;
create table tmp_1m.knd_mbm_nice_mm as 
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


drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_mm; 
create table tmp_1m.knd_mbm_cosmos_csp_nice_mm as 
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
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
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
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.knd_mbm_cosmos_mm
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
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
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
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.knd_mbm_csp_mm
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
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
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
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.knd_mbm_nice_mm
)
select
	*
	, case when OAH_flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when MnR_Dual_flag = 1 then 'M&R DSNP'
		   when MnR_ISNP_flag = 1 then 'M&R ISNP'
		   when MnR_FFS_flag  = 1 then 'M&R FFS (excl. DSNP)'
		   else 'N/A'
	end as population
from cte_union;


drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary; 
create table tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary as 
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
from tmp_1m.knd_mbm_cosmos_csp_nice_mm
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


select distinct product_level_3_fnl from tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary
where population = 'M&R FFS (excl. DSNP)'


select count(*) from tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary
-- 202509: 16730
-- 202601: 18859


select population, sum(sum_mm)
from tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary
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
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_mm_summary;
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims_mm_summary as
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
    , sum(allw_amt_fnl) as sum_allowed
    , sum(net_pd_amt_fnl) as sum_paid
    , 0 as sum_mm
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated
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
    , 0 as sum_allowed
    , 0 as sum_paid
    , sum(sum_mm) as sum_mm
from tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary
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

select count(*) from tmp_1m.knd_mbm_cosmos_csp_nice_claims_mm_summary;
-- 129,038
