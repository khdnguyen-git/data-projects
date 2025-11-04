/*==============================================================================
 * MEMBERSHIP DETAIL PROCESSING
 * Creates base membership table with pilot/national deployment flags
 *==============================================================================*/

drop table if exists tmp_7d.kn_mbm_dtl_202508;					
create temporary table tmp_7d.kn_mbm_dtl_202508 stored as orc as 					
select 			
	fin_mbi_hicn_fnl			
	, fin_inc_month			
	, fin_inc_qtr 		
	, fin_market as market_fnl		
	, case when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot' else 'National' end as mbm_deploy_dt
	, fin_g_i as group_ind_fnl		
	, case when b.migration_source = 'CIP' then 'CIP'				
		  when b.migration_source in ('PC','MEDICA') then 'SouthFlorida'					
		  when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 1 then 'M&R DUALS'					
		  when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 0 then 'C&S DUALS'					
		  when b.migration_source = 'NA' and b.fin_g_i = 'I' then 'Legacy Individual'					
	      when b.fin_g_i = 'G' then 'Group'					
	      else 'OTHERS' end as population					
	, if(global_cap = 'NA', 1, 0) as global_cap		
	, if(tfm_include_flag = '1', 1, 0) as tfm_include	
	, if(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0) as inst		
	, if(fin_product_level_2 in ('PFFS'), 1, 0) as pffs		
	, if(special_network in ('ERICKSON'), 1, 0) as erk			
	, sgr_source_name  		
	, 1 as mm		
from fichsrv.tre_membership as b 			  
where year(fin_incurred_dt) > 2018			
 	  and b.fin_brand = 'M&R'
 	  and b.fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
group by 			
	fin_mbi_hicn_fnl			
	, fin_inc_month			
	, fin_inc_qtr 		
	, fin_market  		
	, case when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot' else 'National' end
	, fin_g_i  		
    , case when b.migration_source = 'CIP' then 'CIP'				
          when b.migration_source in ('PC','MEDICA') then 'SouthFlorida'					
		  when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 1 then 'M&R DUALS'					
		  when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 0 then 'C&S DUALS'					
		  when b.migration_source = 'NA' and b.fin_g_i = 'I' then 'Legacy Individual'					
		  when b.fin_g_i = 'G' then 'Group'					
		  else 'OTHERS' end  					
	, if(global_cap = 'NA', 1, 0)  		 
	, if(tfm_include_flag = '1', 1, 0)  	
	, if(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0)  		
	, if(fin_product_level_2 in ('PFFS'), 1, 0) 		
	, if(special_network in ('ERICKSON'), 1, 0) 			
	, sgr_source_name
;

select sgr_source_name from fichsrv.tre_membership
group by sgr_source_name


		
select count(*) from tmp_7d.kn_mbm_dtl_202508;  -- 430370488 415060257  407463331 399836379   392256742 (removed 2019) 442855070  435558291  428264965  450161568
								
/*==============================================================================
 * MEMBERSHIP SUMMARY CREATION
 * Aggregates membership data and creates summary tables for analysis
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_mshp_202508; 							
create temporary table tmp_1m.kn_mbm_mshp_202508 stored as orc as 							
select 							
	fin_inc_month as ep_start_mo							
	, substring(market_fnl,0,2) as market_fnl							
	, mbm_deploy_dt							
	, group_ind_fnl							
	, population							
	, global_cap							
	, tfm_include							
	, inst							
	, pffs							
	, erk							
	, sgr_source_name							
	, sum(mm) as mm 							
	, substring(fin_inc_month,0,4) as ep_yr 							
	, substring(fin_inc_month,5,2) as ep_mnth							
from tmp_7d.kn_mbm_dtl_202508 as a
where global_cap = 1
group by 							
	fin_inc_month							
	, substring(market_fnl,0,2)  							
	, mbm_deploy_dt							
	, group_ind_fnl							
	, population							
	, global_cap							
	, tfm_include							
	, inst							
	, pffs							
	, erk							
	, sgr_source_name
;	


select sgr_source_name 

select count(*) from tmp_1m.kn_mbm_mshp_202508; --9743  9606

drop table if exists tmp_1m.kn_mbm_mshp_sum1_202508;
create table tmp_1m.kn_mbm_mshp_sum1_202508 stored as orc as
select 
	'MM' as data_type
	, ep_start_mo
	, '' as visit_mo
	, mbm_deploy_dt as pilot_nat
	, '' as category
	, '' as claim_status
	, 0 as visit_ep_lag
	, 0 as visit_runout_mo
	, 0 as ep_cnt
	, 0 as visit_cnt
	, 0 as allowed_amt
	, sum(mm) as mms
from tmp_1m.kn_mbm_mshp_202508
where population not in ('M&R DUALS', 'C&S DUALS')
group by 
	ep_start_mo
	, mbm_deploy_dt
;

select count(*) from tmp_1m.kn_mbm_mshp_sum1_202508; -- 134 132 128 126 124  144 142

select * from tmp_1m.kn_mbm_mshp_sum1_202508;
--_____________[ END OF MEMBERSHIP ]_____________________________________


/*==============================================================================
 * LOPA DATA INTEGRATION
 * On-track to be removed
 *==============================================================================*/

describe formatted tmp_1y.pa_trckng_op_evnt_lopa_dtl;
describe formatted tmp_1y.pa_trckng_pr_evnt_lopa_dtl;

drop table if exists tmp_1m.kn_lopa_op_1_202508;
create table tmp_1m.kn_lopa_op_1_202508 as
select  
	case when include_non_sug_event=1 then mbi_dos end as total_mbi_dos
	, case when final_lopa_ind=1 and mbr_dos_latest_submission=1 and include_non_sug_event=1 then mbi_dos end as still_lopa_mbi_dos
	, case when include_non_sug_event=1 and (final_lopa_ind<>1 or mbr_dos_latest_submission<>1) then mbi_dos end as overturn_lopa_mbi_dos
	, *
from tmp_1y.pa_trckng_op_evnt_lopa_dtl
;	

select count(*) from tmp_1m.kn_lopa_op_1_202508; -- 2484707 2417275  2306199  2164101  1729931 1526791   1338831

drop table if exists tmp_1m.kn_lopa_op_202508;
create table tmp_1m.kn_lopa_op_202508 as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1m.kn_lopa_op_1_202508
; 

select count(*) from tmp_1m.kn_lopa_op_202508; -- 2417275  2306199 2164101

drop table if exists tmp_1m.kn_lopa_pr_1_202508;
create table tmp_1m.kn_lopa_pr_1_202508 as
select  
	mbi_dos as total_mbi_dos
	, case when final_lopa_ind=1 and mbr_dos_latest_submission=1 then mbi_dos end as still_lopa_mbi_dos
	, case when final_lopa_ind<>1 or mbr_dos_latest_submission<>1 then mbi_dos end as overturn_lopa_mbi_dos
	, *
from tmp_1y.pa_trckng_pr_evnt_lopa_dtl
;	

select count(*) from tmp_1m.kn_lopa_pr_1_202508; -- 3714719  3501938 3263702

drop table if exists tmp_1m.kn_lopa_pr_202508;
create table tmp_1m.kn_lopa_pr_202508 as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1m.kn_lopa_pr_1_202508
;

select count(*) from tmp_1m.kn_lopa_pr_202508; -- 3714719  3501938 3263702
		
/*==============================================================================
 * PROFESSIONAL CLAIMS PROCESSING
 * Pull in PR claims, combine with LOPA flags
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_episode_pr_202508; 
create table tmp_1m.kn_mbm_episode_pr_202508 stored as orc as
select			
	a.gal_mbi_hicn_fnl as mbi		
	, a.component		
	, a.eventkey as id		
	, a.service_code  		
	, a.fst_srvc_dt as start_dt		
	, a.fst_srvc_month as serv_month		
	, a.fst_srvc_qtr as hce_qtr		
	, to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as hctapaidmonth		
	, a.market_fnl		
	, a.group_ind_fnl		
	, a.proc_cd		
	, a.rvnu_cd		
	, a.primary_diag_cd		
	, a.ahrq_diag_genl_catgy_desc		
	, a.ahrq_diag_dtl_catgy_desc		
	, a.prov_prtcp_sts_cd		
	, a.prov_tin		
	, a.full_nm as prov_full_nm
	, case when b.ever_lopa is not null then 1 else 0 end as lopa_flg
	, case when b.still_lopa_mbi_dos is not null then 1 else 0 end as still_lopa
	, case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end as overturn_lopa
	, 0 as apc_pbl_flg
	, if(a.proc_cd in ('98940','98941','98942') and a.component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd in ('22','62', '19', '24') and a.component = 'OP','OP_REHAB','Other') )) as category		
	, sum(a.allw_amt_fnl) as allowed		
	, sum(a.net_pd_amt_fnl) as paid		
	, sum(0) as tadm_util		
	, count(distinct a.eventkey) as visits                     		
	, sum(a.adj_srvc_unit_cnt) as adj_srvc_units		
from fichsrv.cosmos_pr as a
left join tmp_1m.kn_lopa_pr_202508 as b		   
	on concat(a.gal_mbi_hicn_fnl, "_", a.fst_srvc_dt) = b.total_mbi_dos 
    and a.proc_cd = b.proc_cd
    and a.prov_tin = b.prov_tin
where a.tfm_include_flag = 1 			
	and a.global_cap in ('NA')			
	and a.product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL' )			
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.st_abbr_cd = a.market_fnl			
	and a.prov_prtcp_sts_cd = 'P'			
	and (substring(coalesce(a.bil_typ_cd,'0'),0,1) <> 3 or substring(coalesce(a.bil_typ_cd,'0'),0,1) <> '3')
	and (a.ama_pl_of_srvc_cd <> 12 or a.ama_pl_of_srvc_cd <> '12')     
	and (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
group by			
	a.gal_mbi_hicn_fnl		
	, a.component		
	, a.eventkey		
	, a.service_code		
	, a.fst_srvc_dt  		
	, a.fst_srvc_month  		
	, a.fst_srvc_qtr  		
	, to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
	, a.market_fnl		
	, a.group_ind_fnl		
	, a.proc_cd		
	, a.rvnu_cd		
	, a.primary_diag_cd		
	, a.ahrq_diag_genl_catgy_desc		
	, a.ahrq_diag_dtl_catgy_desc		
	, a.prov_prtcp_sts_cd		
	, a.prov_tin		
	, a.full_nm 
	, case when b.ever_lopa is not null then 1 else 0 end
	, case when b.still_lopa_mbi_dos is not null then 1 else 0 end
	, case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end
	, if(a.proc_cd in ('98940','98941','98942') and a.component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd in ('22','62', '19', '24') and a.component = 'OP','OP_REHAB','Other') )) 		
order by a.gal_mbi_hicn_fnl asc;

select count(*) from tmp_1m.kn_mbm_episode_pr_202508; -- 64354173  62711063  60990327



select sum(allowed) from tmp_1m.kn_mbm_episode_pr_202508
where serv_month = '202406'

	
/*==============================================================================
 * OUTPATIENT CLAIMS APC PROCESSING
 * Pull in OP claims with APC flags and LOPA flags
 *==============================================================================*/
drop table if exists tmp_1m.kn_mbm_op_claims;
create table tmp_1m.kn_mbm_op_claims stored as orc as 
select *
	, max(if(instr(clm_rev_rsn_1_10,'00473-') > 0 , 1,0)) over (partition by site_cd,clm_aud_nbr,sbscr_nbr) as clm_apc_flg
	, sum(allw_amt_fnl) over (partition by site_cd,clm_aud_nbr,sbscr_nbr) as clm_allw_amnt
from 
	(select *	
	    	, concat(a.clm_rev_rsn_1_cd ,'-',a.clm_rev_rsn_2_cd,'-',a.clm_rev_rsn_3_cd,'-',a.clm_rev_rsn_4_cd,'-',a.clm_rev_rsn_5_cd,'-',a.clm_rev_rsn_6_cd,'-',a.clm_rev_rsn_7_cd,'-',a.clm_rev_rsn_8_cd,'-',a.clm_rev_rsn_9_cd,'-',a.clm_rev_rsn_10_cd, '-') as clm_rev_rsn_1_10																																													
     from fichsrv.cosmos_op as a
	 where (a.proc_cd in 			
			  ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	   '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
	   		   '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
	 	       '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 	   '98940', '98941', '98942')
		      or rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		    and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	) as e ;

select count(*) from tmp_1m.kn_mbm_op_claims; -- 77536654  75713104  73889849     72274650

drop table if exists tmp_1m.kn_mbm_episode_op_202508; 
create table tmp_1m.kn_mbm_episode_op_202508 as 
select			
	a.gal_mbi_hicn_fnl as mbi		
	, a.component		
	, a.eventkey as id		
	, a.hce_service_code as service_code  		
	, a.fst_srvc_dt as start_dt		
	, a.fst_srvc_month as serv_month		
	, a.fst_srvc_qtr as hce_qtr		
	, to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as hctapaidmonth		
	, a.market_fnl		
	, a.group_ind_fnl		
	, a.proc_cd		
	, a.rvnu_cd		
	, a.primary_diag_cd		
	, a.ahrq_diag_genl_catgy_desc		
	, a.ahrq_diag_dtl_catgy_desc		
	, a.prov_prtcp_sts_cd		
	, a.prov_tin		
	, a.full_nm as prov_full_nm
	, case when b.ever_lopa is not null then 1 else 0 end as lopa_flg
	, case when b.still_lopa_mbi_dos is not null then 1 else 0 end as still_lopa
	, case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end as overturn_lopa
	, case when a.clm_apc_flg = 1 and c.rsn_cd in ('208','176','943') then 1 else 0 end as apc_pbl_flg  	
	, if(a.proc_cd in ('98940','98941','98942') and a.component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd in ('22','62', '19', '24') and a.component = 'OP','OP_REHAB','Other') )) as category		
	, sum(a.allw_amt_fnl) as allowed		
	, sum(a.net_pd_amt_fnl) as paid		
	, sum(0) as tadm_util		
	, count(distinct a.eventkey) as visits                     		
	, sum(a.adj_srvc_unit_cnt) as adj_srvc_units		
from tmp_1m.kn_mbm_op_claims as a
left join tmp_1m.kn_lopa_op_202508 as b
	on concat(a.gal_mbi_hicn_fnl, "_", a.fst_srvc_dt) = b.total_mbi_dos 
    and a.proc_cd = b.proc_cd
    and a.prov_tin = b.prov_tin
left join fichsrv.tadm_glxy_reason_code as c
    on a.fnl_rsn_cd_sys_id = c.rsn_cd_sys_id
where a.tfm_include_flag = 1 			
	and a.global_cap in ('NA')			
	and a.product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')			
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.st_abbr_cd = a.market_fnl			
	and a.prov_prtcp_sts_cd = 'P'			
	and substring(coalesce(a.bil_typ_cd,'0'),0,1) <> 3			
	and a.ama_pl_of_srvc_cd <> 12 			
	and (a.proc_cd in 			
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
		or a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')			
group by			
	a.gal_mbi_hicn_fnl		
	, a.component		
	, a.eventkey		
	, a.hce_service_code		
	, a.fst_srvc_dt  		
	, a.fst_srvc_month  		
	, a.fst_srvc_qtr  		
	, to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
	, a.market_fnl		
	, a.group_ind_fnl		
	, a.proc_cd		
	, a.rvnu_cd		
	, a.primary_diag_cd		
	, a.ahrq_diag_genl_catgy_desc		
	, a.ahrq_diag_dtl_catgy_desc		
	, a.prov_prtcp_sts_cd		
	, a.prov_tin		
	, a.full_nm 
	, case when b.ever_lopa is not null then 1 else 0 end
	, case when b.still_lopa_mbi_dos is not null then 1 else 0 end
	, case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end
	, case when a.clm_apc_flg = 1 and c.rsn_cd in ('208','176','943') then 1 else 0 end
	, if(a.proc_cd in ('98940','98941','98942') and a.component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd in ('22','62', '19', '24') and a.component = 'OP','OP_REHAB','Other') ))   		
order by a.gal_mbi_hicn_fnl asc
; 

select count(*) from tmp_1m.kn_mbm_episode_op_202508; -- 38657890  37706450  36737912  35885437

/*==============================================================================
 * CLAIMS UNION AND CATEGORIZATION
 * Combines current and 2018-2020 PR and OP claims
 * Add claim_status flag, add MBM-related flags
 *==============================================================================*/

-- Refresh 2018-2020 tables
drop table if exists tmp_1y.kn_mbm_episode_pr_2018_2020;
create table tmp_1y.kn_mbm_episode_pr_2018_2020 as
select
	*
from tmp_1y.cl_mbm_episode_1_2018_2020
;

drop table if exists tmp_1y.kn_mbm_episode_op_2018_2020;
create table tmp_1y.kn_mbm_episode_op_2018_2020 as
select
	*
from tmp_1y.cl_mbm_episode_1b_2018_2020
;

drop table if exists tmp_1m.kn_mbm_episode_1c;
create table tmp_1m.kn_mbm_episode_1c as
select * from tmp_1m.kn_mbm_episode_pr_202508
union all
select * from tmp_1y.kn_mbm_episode_pr_2018_2020
union all
select * from tmp_1m.kn_mbm_episode_op_202508
union all
select * from tmp_1y.kn_mbm_episode_op_2018_2020
;


select count(*) from tmp_1m.kn_mbm_episode_1c; -- 136759944 134268620 141253070  138658520  135969246

select * from tmp_1m.kn_mbm_episode_1c limit 2;
select serv_month, sum(allowed) as allowedamt 
from tmp_1m.kn_mbm_episode_1c
where serv_month = '202407'
group by serv_month;


drop table if exists tmp_1m.kn_mbm_episode_2_202508; 
create table tmp_1m.kn_mbm_episode_2_202508 as 			
select			
	*			
	, if(dnl_allowed > 0.01, 'Paid', if(still_lopa = 1, 'LOPA', if(apc_pbl_flg = 1, 'APC-Paid', 'Other Denied' ))) as claim_status
	, if(tin_num is null,0,1) as optum_flg 			
	, case when proc_cd in ('98940','98941','98942')  			
		 then 'Chiro'			
			when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'	
		  ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		  ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		  ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		  ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		  ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131')			
			     then 'PT-OT'	
			when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'	
		    ,'92627','92630','92633','96105','S9128') 			
			     then 'ST'	
		  else 'Other' end 			
			as mbmserv_dtl	
	, case when (market_fnl in ('AR', 'GA', 'NJ', 'SC') and group_ind_fnl = 'I') then
			case when category = 'OP_REHAB' then 'Phase-II'
			else
			case when tin_num is null then'Phase-II' 
			else 'Phase-I' end 
			end 
			else 'National'
	end as mbm_deploy_dt	
from (select a.*, tin_num  			
      from (select *
		        , sum(allowed) over (partition by id,start_dt,category) as dnl_allowed
                , max(lopa_flg) over (partition by id,start_dt,category ) as max_lopa_flg
             from tmp_1m.kn_mbm_episode_1c) as a 			
             left join tmp_1y.p8001_optum_tin_2 as b 			
             on prov_tin = tin_num 			
             and i = 1 ) as b 	
;

select count(*) from tmp_1m.kn_mbm_episode_2_202508; -- 134268620 141253070  138658520  135969246

describe formatted tmp_1m.kn_mbm_episode_2_202508;

select serv_month, sum(allowed) as allowedamt 
from tmp_1m.kn_mbm_episode_2_202508
where serv_month = '202406'  
group by serv_month; -- 73,520,205.78 73,491,717.81 73432103.42  73635236.05  73,514,079.66 (shorter proc_cd list)  76,561,115.88 (original proc_cd list)


/*==============================================================================
 * EPISODE ANALYSIS
 * Creates episode aggregation and visit ranking structure
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_episode_3_202508;  
create table tmp_1m.kn_mbm_episode_3_202508 as 
select			
	concat(mbi,'-',category) as mbi		
	, component		
	, id		
	, start_dt		
	, serv_month		
	, hce_qtr		
	, min(hctapaidmonth) as hctapaidmonth		
	, mbm_deploy_dt		
	, market_fnl	
	, claim_status
	, cast(mbmserv_dtl as varchar (10)) as mbmserv 		
	, category	
	, sum(allowed) as allowed		
	, sum(paid) as paid		
	, sum(tadm_util) as tadm_util
	, count(distinct concat(id,start_dt)) as visits 
	, count(visits) as vsts                     		
	, sum(adj_srvc_units) as adj_srvc_units		
from tmp_1m.kn_mbm_episode_2_202508
where prov_prtcp_sts_cd = 'P'	
group by			
    concat(mbi,'-',category)
	, component		
	, id		
	, start_dt		
	, serv_month		
	, hce_qtr		
	, mbm_deploy_dt		
	, market_fnl		
	, claim_status		
	, mbmserv_dtl		
	, optum_flg		
	, category	
order by mbi,mbmserv,start_dt,id
;

select count(*) from tmp_1m.kn_mbm_episode_3_202508; -- 64545494 63423287 66929218  65797555     64650521

drop table if exists tmp_1m.kn_mbm_episode_4_202508;  
create table tmp_1m.kn_mbm_episode_4_202508 as  			
select 
	mbi
	, component
	, id
	, start_dt
	, row_number() over (partition by mbi,mbm_deploy_dt order by start_dt ) as i 
	, serv_month
	, hce_qtr
	, hctapaidmonth
	, mbm_deploy_dt
	, market_fnl
	, claim_status
	, mbmserv
	, category
	, allowed
	, paid
	, tadm_util
	, visits
	, vsts
	, adj_srvc_units
from tmp_1m.kn_mbm_episode_3_202508 as a
;

select count(*) from tmp_1m.kn_mbm_episode_4_202508; -- 64545494 66929218  65797555   64650521

drop table if exists tmp_1m.kn_mbm_episode_lag_202508 ;
create table tmp_1m.kn_mbm_episode_lag_202508 as 
select 
	*
from tmp_1y.knt_mbm_episode_lag 
;


/*==============================================================================
 * VISIT EPISODE LAG CALCULATION
 * Calculates time gaps between visits and episode flags
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_episode_lag_202508;  
create table tmp_1m.kn_mbm_episode_lag_202508 as  
select a.mbi
	, a.component
	, a.id
	, a.start_dt
	, b.start_dt as prev_start_dt
	, datediff(a.start_dt,b.start_dt) as visit_dy_lag
	, if(datediff(a.start_dt,b.start_dt) > 30, 1, 0) as ep_flag
	, a.i 
	, b.i as prev_i
	, a.serv_month
	, a.hce_qtr
	, a.hctapaidmonth
	, a.mbm_deploy_dt
	, a.market_fnl
	, a.claim_status
	, a.mbmserv
	, a.category
	, a.allowed
	, a.paid
	, a.tadm_util
	, a.visits
	, a.vsts
	, a.adj_srvc_units
from tmp_1m.kn_mbm_episode_4_202508 as a
left join tmp_1m.kn_mbm_episode_4_202508 as b 
	on a.mbi = b.mbi 
	and a.mbm_deploy_dt = b.mbm_deploy_dt
	and a.i = b.i+1 
;

select count(*) from tmp_1m.kn_mbm_episode_lag_202508; -- 64545494 66929218  65797555  64650521
 
/*==============================================================================
 * EPISODE START DATE DETERMINATION
 * Identifies episode boundaries and calculates cumulative episodes
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_episode_vst_ep_2_202508;  
create table tmp_1m.kn_mbm_episode_vst_ep_2_202508 as  
select a.mbi
	, a.component
	, a.id
	, a.start_dt
	, a.prev_start_dt
	, visit_dy_lag
	, ep_flag
	, min(start_dt) over (partition by mbi,cmltv_episodes ) as ep_start_dt
	, cmltv_episodes
	, a.i 
	, a.prev_i
	, a.serv_month
	, a.hce_qtr
	, a.hctapaidmonth
	, min(hctapaidmonth) over (partition by mbi,cmltv_episodes ) as ep_hctapaidmonth
	, a.mbm_deploy_dt
	, a.market_fnl
	, a.claim_status
	, a.mbmserv
	, a.category
	, a.allowed
	, a.paid
	, a.tadm_util
	, a.visits
	, a.vsts
	, a.adj_srvc_units
from (select *  
 		, sum(if(prev_start_dt is null,1,ep_flag)) over (partition by mbi order by start_dt rows between unbounded preceding and current row) as cmltv_episodes 
	  from tmp_1m.kn_mbm_episode_lag_202508 ) as a
;

select count(*) from tmp_1m.kn_mbm_episode_vst_ep_2_202508; -- 66929218  65797555  64650521

drop table if exists tmp_1m.kn_mbm_episode_smry_202508;  
create table tmp_1m.kn_mbm_episode_smry_202508 as  
select  
	a.serv_month as visit_month
	, date_format(ep_start_dt,'yyyyMM') as ep_start_mo 
	, a.hctapaidmonth
	, a.mbm_deploy_dt
	, a.market_fnl
	, a.claim_status
	, a.mbmserv
	, a.category
	, count(distinct mbi) as mbr_count
	, sum(a.allowed) as allw 
	, sum(a.paid) as pd 
	, sum(a.visits) as visits
	, sum(ep_flag) as episodes
from tmp_1m.kn_mbm_episode_vst_ep_2_202508 as a
group by 
	a.serv_month  
	, date_format(ep_start_dt,'yyyyMM') 
	, a.hctapaidmonth
	, a.mbm_deploy_dt
	, a.market_fnl	
	, a.claim_status
	, a.mbmserv
	, a.category
;

select count(*) from tmp_1m.kn_mbm_episode_smry_202508; -- 1555388  1526174  1496541
  
/*==============================================================================
 * EPISODE SUMMARY AND RUNOUT ANALYSIS
 * Calculates runout periods and creates aggregated episodes
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_episode_ro_lag_202508;  
create table tmp_1m.kn_mbm_episode_ro_lag_202508 as  
select a.mbi
	, a.component
	, a.id
	, a.start_dt
	, floor((datediff(hctapaidmonth,start_dt)+20)/30.5) as visit_runout_mo  
	, round((datediff(hctapaidmonth,start_dt)+20)/1,0) as visit_runout 
	, floor(datediff(start_dt,ep_start_dt)/30.5) as visit_ep_lag 
	, visit_dy_lag
	, if(prev_start_dt is null,1,ep_flag) as ep_flag
	, ep_start_dt
	, cmltv_episodes
	, a.i 
	, a.prev_i
	, a.serv_month
	, a.hce_qtr
	, a.hctapaidmonth
	, ep_hctapaidmonth
	, a.mbm_deploy_dt
	, a.market_fnl
	, a.claim_status
	, a.mbmserv
	, a.category
	, a.allowed
	, a.paid
	, a.tadm_util
	, a.visits
	, a.vsts
	, a.adj_srvc_units
from tmp_1m.kn_mbm_episode_vst_ep_2_202508 as a
;

select count(*) from tmp_1m.kn_mbm_episode_ro_lag_202508; -- 66929218  65797555  64650521

drop table if exists tmp_1m.kn_mbm_episode_ro_lag2_202508;  
create table tmp_1m.kn_mbm_episode_ro_lag2_202508 as  
select 
	a.mbi
	, a.id
	, ep_start_dt
	, cmltv_episodes
	, start_dt
	, date_format(ep_start_dt,'yyyyMM') as ep_start_mo 
	, date_format(ep_start_dt,'yyyy') as ep_start_year
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
	, hctapaidmonth
	, mbmserv as visit_mbmserv
	, visit_runout_mo
	, 0 as ep_runout_mo
	, date_format(start_dt,'yyyyMM') as visit_mo
	, visit_ep_lag 
	, ep_flag as episodes
	, visits
	, allowed
	, 0 as mm 
from tmp_1m.kn_mbm_episode_ro_lag_202508 as a
;


select ep_start_mo, count(episodes) from tmp_1m.kn_mbm_episode_ro_lag2_202508
where episodes = 1
group by ep_start_mo

select count(*) from tmp_1m.kn_mbm_episode_ro_lag2_202508; -- 66929218  65797555  64650521

drop table if exists tmp_1m.kn_mbm_episode_agg6_ep_202508;  
create table tmp_1m.kn_mbm_episode_agg6_ep_202508 as  
select 
	'EPISODES' as data_type
	, ep_start_mo 
	, concat(ep_start_year,'Q9') as ep_start_qtr
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
	, '' as visit_mbmserv
	, 0 as visit_runout_mo
	, 0 as ep_runout_mo
	, 0 as visit_mo
	, 0 as visit_ep_lag 
	, sum(episodes) as episodes 
	, 0 as visits 
	, 0 as allowed
	, 0 as mm 
from (select * from tmp_1m.kn_mbm_episode_ro_lag2_202508 where episodes = 1 ) as a
group by 
	ep_start_mo
	, concat(ep_start_year,'Q9')
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
;

select count(*) from tmp_1m.kn_mbm_episode_agg6_ep_202508; -- 45630  44961 44193  43381

/*==============================================================================
 * COMBINE VISITS AND EPISODES
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_episode_agg6_202508;  
create table tmp_1m.kn_mbm_episode_agg6_202508 as  
select 
	'VISITS' as data_type
	, ep_start_mo 
	, concat(ep_start_year,'Q9') as ep_start_qtr
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
	, visit_mbmserv
	, visit_runout_mo
	, ep_runout_mo
	, visit_mo
	, visit_ep_lag 
	, sum(0) as episodes 
	, sum(visits) as visits 
	, sum(allowed) as allowed
	, 0 as mm 
from tmp_1m.kn_mbm_episode_ro_lag2_202508
group by 
	ep_start_mo 
	, concat(ep_start_year,'Q9') 
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
	, visit_mbmserv
	, visit_runout_mo
	, ep_runout_mo
	, visit_mo
	, visit_ep_lag
;

select count(*) from tmp_1m.kn_mbm_episode_agg6_202508; -- 2322113  2278610 2234635

insert into tmp_1m.kn_mbm_episode_agg6_202508   
select * from tmp_1m.kn_mbm_episode_agg6_ep_202508 as a;

-- Minor format adjustments
-- Considering removing because of formatting issues
alter table tmp_1m.kn_mbm_episode_agg6_202508 change data_type data_type varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6_202508 change ep_start_mo ep_start_mo varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6_202508 change ep_start_qtr ep_start_qtr varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6_202508 change mbm_deploy_dt mbm_deploy_dt varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6_202508 change claim_status claim_status varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6_202508 change visit_mo visit_mo varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6_202508 change category category varchar(20);

select count(*) from tmp_1m.kn_mbm_episode_agg6_202508; --2089616 2367743  2323571  2278828  2235149





select sum(allowed) 
from tmp_1m.kn_mbm_episode_visits_after2021
where visit_mo = '202406'; --73246897.76 73520205.78 73432103.42  73635236.05  73514079.66


select sum(allowed) 
from tmp_1m.kn_mbm_episode_agg6_202508
where visit_mo = '202406'; --73246897.76 73520205.78 73432103.42  73635236.05  73514079.66


/*___________________[ SUMARIZING DATA FOR EXCEL ]_________________________________________________*/

/*==============================================================================
 * FINAL DATA AGGREGATION FOR REPORTING
 * Creates summary tables for Excel reporting and analysis
 * Stich up 2023- and 2023+ tables
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_episode_agg6_sum1_after2023_202508;
create table tmp_1m.kn_mbm_episode_agg6_sum1_after2023_202508 stored as orc as 
select 
	data_type,
	ep_start_mo,
	substring(ep_start_mo, 0, 4) as ep_year,
	substring(ep_start_mo, 5,2) as ep_month,
	visit_mo,
	case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as pilot_nat,
	category,
	visit_ep_lag ,
	visit_runout_mo,
	sum(episodes) as ep_cnt,
	sum(visits) as visit_cnt,
	sum(allowed) as allowed_amt,
	sum(mm) as mms
from tmp_1m.kn_mbm_episode_agg6_202508
where ep_start_mo >= '202301' -- was '201812' 
group by
	data_type,
	ep_start_mo,
	substring(ep_start_mo, 0, 4),
	substring(ep_start_mo, 5,2),
	visit_mo,
	case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end,
	category,
	claim_status,
	visit_ep_lag,
	visit_runout_mo
union 
select 
	data_type,
	ep_start_mo,
	substring(ep_start_mo, 0, 4) as ep_year,
	substring(ep_start_mo, 5,2) as ep_month,
	visit_mo,
	pilot_nat,
	category,
	visit_ep_lag,
	visit_runout_mo,
	ep_cnt,
	visit_cnt,
	allowed_amt,
	mms
from tmp_1m.kn_mbm_mshp_sum1_202508
;
select count(*) from tmp_1m.kn_mbm_episode_agg6_sum1_after2023_202508; -- 72856


drop table if exists tmp_1y.kn_mbm_episode_agg6_sum1_before2023;
create table tmp_1y.kn_mbm_episode_agg6_sum1_before2023 as
select
	*
from tmp_1y.cl_mbm_episode_agg6_sum1
where ep_start_mo < '202301'
;

select count(*) from tmp_1y.kn_mbm_episode_agg6_sum1_before2023
-- 176560;


drop table if exists tmp_1m.kn_mbm_202508;
create table tmp_1m.kn_mbm_202508 as
select
	*
from tmp_1m.kn_mbm_episode_agg6_sum1_after2023_202508
union all
select 
	*
from tmp_1y.kn_mbm_episode_agg6_sum1_before2023;

select count(*) from tmp_1m.kn_mbm_202508; -- 253665
;




