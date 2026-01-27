/*==============================================================================
 * MEMBERSHIP DETAIL PROCESSING
 * Creates base membership table with pilot/national deployment flags
 *==============================================================================*/
drop table if exists tmp_1q.kn_mbm_dtl_202512;					
create table tmp_1q.kn_mbm_dtl_202512 as 					
select 			
	fin_mbi_hicn_fnl			
	, fin_inc_month			
	, fin_inc_qtr 		
	, fin_market as market_fnl		
	, case when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot' else 'National' end as mbm_deploy_dt
	, fin_g_i as group_ind_fnl		
	, case when migration_source = 'CIP' then 'CIP'				
		  when migration_source in ('PC','MEDICA') then 'SouthFlorida'					
		  when fin_product_level_3 = 'DUAL' and tfm_include_flag = 1 then 'M&R DUALS'					
		  when fin_product_level_3 = 'DUAL' and tfm_include_flag = 0 then 'C&S DUALS'					
		  when migration_source = 'NA' and fin_g_i = 'I' then 'Legacy Individual'					
	      when fin_g_i = 'G' then 'Group'					
	      else 'OTHERS' end as population					
	, iff(global_cap = 'NA', 1, 0) as global_cap		
	, iff(tfm_include_flag = '1', 1, 0) as tfm_include	
	, iff(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0) as inst		
	, iff(fin_product_level_2 in ('PFFS'), 1, 0) as pffs		
	, iff(special_network in ('ERICKSON'), 1, 0) as erk			
	, sgr_source_name  		
	, 1 as mm		
from fichsrv.tre_membership
where fin_inc_year > 2018			
 	  and fin_brand = 'M&R'
 	  and fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
group by 			
	fin_mbi_hicn_fnl			
	, fin_inc_month			
	, fin_inc_qtr 		
	, fin_market  		
	, case when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot' else 'National' end
	, fin_g_i  		
    , case when migration_source = 'CIP' then 'CIP'				
          when migration_source in ('PC','MEDICA') then 'SouthFlorida'					
		  when fin_product_level_3 = 'DUAL' and tfm_include_flag = 1 then 'M&R DUALS'					
		  when fin_product_level_3 = 'DUAL' and tfm_include_flag = 0 then 'C&S DUALS'					
		  when migration_source = 'NA' and fin_g_i = 'I' then 'Legacy Individual'					
		  when fin_g_i = 'G' then 'Group'					
		  else 'OTHERS' end  					
	, iff(global_cap = 'NA', 1, 0)  		 
	, iff(tfm_include_flag = '1', 1, 0)  	
	, iff(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0)  		
	, iff(fin_product_level_2 in ('PFFS'), 1, 0) 		
	, iff(special_network in ('ERICKSON'), 1, 0) 			
	, sgr_source_name
;
-- select count(*) from tmp_1q.kn_mbm_dtl_202512;  
-- 476794132 469046914 430370488 415060257  407463331 399836379   392256742 (removed 2019) 442855070  435558291  428264965  450161568
								

/*==============================================================================
 * MEMBERSHIP SUMMARY CREATION
 * Aggregates membership data and creates summary tables for analysis
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_mshp_202512;
create temporary table tmp_1q.kn_mbm_mshp_202512 as
select
    fin_inc_month as ep_start_mo
    , substring(market_fnl, 1, 2) as market_fnl
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
    , substring(fin_inc_month, 1, 4) as ep_yr
    , substring(fin_inc_month, 5, 2) as ep_mnth
from tmp_1q.kn_mbm_dtl_202512 as a
where global_cap = 1
group by
    fin_inc_month
    , substring(market_fnl, 1, 2)
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

-- select count(*) from tmp_1q.kn_mbm_mshp_202512; -- 10428 10291 9743  9606

drop table if exists tmp_1q.kn_mbm_mshp_sum1_202512;
create table tmp_1q.kn_mbm_mshp_sum1_202512 as
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
from tmp_1q.kn_mbm_mshp_202512
where population not in ('M&R DUALS', 'C&S DUALS')
group by 
	ep_start_mo
	, mbm_deploy_dt
;

--select count(*) from tmp_1q.kn_mbm_mshp_sum1_202512; -- 144 142 134 132 128 126 124  144 142

--select * from tmp_1q.kn_mbm_mshp_sum1_202512;
--_____________[ END OF MEMBERSHIP ]_____________________________________


/*==============================================================================
 * LOPA DATA INTEGRATION
 * On-track to be removed
 *==============================================================================*/

--describe formatted tmp_1y.pa_trckng_op_evnt_lopa_dtl;
drop table if exists tmp_1q.kn_lopa_op_1_202512;
create table tmp_1q.kn_lopa_op_1_202512 as
select  
	case when include_non_sug_event = 1 then mbi_dos end as total_mbi_dos
	, case when final_lopa_ind = 1 and mbr_dos_latest_submission = 1 and include_non_sug_event = 1 
	then mbi_dos 
	end as still_lopa_mbi_dos
	, case when include_non_sug_event = 1 and (final_lopa_ind != 1 or mbr_dos_latest_submission != 1) 
	then mbi_dos end as overturn_lopa_mbi_dos
	, *
from hce_ops_stage.pa_trckng_op_evnt_lopa_dtl
;	

--select count(*) from tmp_1q.kn_lopa_op_1_202512; -- 2981365 2909228 2484707 2417275  2306199  2164101  1729931 1526791   1338831
;

drop table if exists tmp_1q.kn_lopa_op_202512;
create table tmp_1q.kn_lopa_op_202512 as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1q.kn_lopa_op_1_202512
; 

--select count(*) from tmp_1q.kn_lopa_op_202512; -- 2981365 2909228 2417275  2306199 2164101

drop table if exists tmp_1q.kn_lopa_pr_1_202512;
create table tmp_1q.kn_lopa_pr_1_202512 as
select  
	mbi_dos as total_mbi_dos
	, case when final_lopa_ind = 1 and mbr_dos_latest_submission = 1 then mbi_dos end as still_lopa_mbi_dos
	, case when final_lopa_ind != 1 or mbr_dos_latest_submission != 1 then mbi_dos end as overturn_lopa_mbi_dos
	, *
from hce_ops_stage.pa_trckng_pr_evnt_lopa_dtl
;	

--select count(*) from tmp_1q.kn_lopa_pr_1_202512; -- 4790609 4659061 3714719  3501938 3263702

drop table if exists tmp_1q.kn_lopa_pr_202512;
create table tmp_1q.kn_lopa_pr_202512 as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1q.kn_lopa_pr_1_202512
;

--select count(*) from tmp_1q.kn_lopa_pr_202512; -- 4790609 4659061 3714719  3501938 3263702
		


/*==============================================================================
 * PROFESSIONAL CLAIMS PROCESSING
 * Pull in PR claims, combine with LOPA flags
 *==============================================================================*/
drop table if exists tmp_1q.kn_mbm_episode_pr_202512;
create table tmp_1q.kn_mbm_episode_pr_202512 as
select
    a.gal_mbi_hicn_fnl as mbi
    , a.component
    , a.eventkey as id
    , a.service_code
    , a.fst_srvc_dt as start_dt
    , a.fst_srvc_month as serv_month
    , a.fst_srvc_qtr as hce_qtr
	, date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hctapaidmonth
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
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
	end as category
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
    , sum(0) as tadm_util
    , count(distinct a.eventkey) as visits
    , sum(a.adj_srvc_unit_cnt) as adj_srvc_units
from fichsrv.cosmos_pr as a
left join tmp_1q.kn_lopa_pr_202512 as b
    on concat(a.gal_mbi_hicn_fnl, '_', a.fst_srvc_dt) = b.total_mbi_dos
    and a.proc_cd = b.proc_cd
    and a.prov_tin = b.prov_tin
where a.tfm_include_flag = 1
    and a.global_cap = 'NA'
    and a.product_level_3_fnl not in ('INSTITUTIONAL','DUAL')
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.st_abbr_cd = a.market_fnl
    and a.prov_prtcp_sts_cd = 'P'
    and (substring(coalesce(a.bil_typ_cd,'0'), 1, 1) <> '3')
    and (a.ama_pl_of_srvc_cd <> '12')
    and (
        a.proc_cd in 
        ('92507','92508','92526','97012','97016','97018','97022','97024','97026','97028',
		 '97032','97033','97034','97035','97036','97039','97110','97112','97113','97116',
         '97124','97139','97140','97150','97164','97168','97530','97533','97535','97537',
         '97542','97545','97546','97750','97755','97760','97761','97799','G0283',
         '98940','98941','98942')
        or a.rvnu_cd in 
        ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429',
         '0440','0441','0442','0443','0444','0449')
    )
    and a.proc_cd not in 
    ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151',
     'G0152','G9041','G9043','G9044','S9128','S9129','S9131')
group by
    a.gal_mbi_hicn_fnl
    , a.component
    , a.eventkey
    , a.service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
	, date_trunc('month', dateadd(day, 10, a.adjd_dt))
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
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
	end
;



--select count(*) from tmp_1q.kn_mbm_episode_pr_202512; -- 69597958 68110261 66530074 64354173  62711063  60990327

select sum(allowed) from tmp_1q.kn_mbm_episode_pr_202512
where serv_month = '202406'
-- 40768591.31
-- 40796038.61
	
/*==============================================================================
 * OUTPATIENT CLAIMS APC PROCESSING
 * Pull in OP claims with APC flags and LOPA flags
 *==============================================================================*/
drop table if exists tmp_1q.kn_mbm_op_claims;
create table tmp_1q.kn_mbm_op_claims as
select
    e.*
    , max(iff(position('00473-' in clm_rev_rsn_1_10) > 0, 1, 0)) over (partition by site_cd, clm_aud_nbr, sbscr_nbr) as clm_apc_flg
    , sum(allw_amt_fnl) over (partition by site_cd, clm_aud_nbr, sbscr_nbr) as clm_allw_amnt
from (
    select
        a.*
        , concat_ws('-', a.clm_rev_rsn_1_cd, a.clm_rev_rsn_2_cd, a.clm_rev_rsn_3_cd, a.clm_rev_rsn_4_cd,
              a.clm_rev_rsn_5_cd, a.clm_rev_rsn_6_cd, a.clm_rev_rsn_7_cd, a.clm_rev_rsn_8_cd,
              a.clm_rev_rsn_9_cd, a.clm_rev_rsn_10_cd) as clm_rev_rsn_1_10
    from fichsrv.cosmos_op as a
    where (a.proc_cd in 			
			  ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	   '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
	   		   '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
	 	       '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 	   '98940', '98941', '98942')
		      or rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') 
		  )
		    and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	) as e;


--select count(*) from tmp_1q.kn_mbm_op_claims; -- 82571364 77536654  75713104  73889849     72274650
drop table if exists tmp_1q.kn_mbm_episode_op_202512;
create table tmp_1q.kn_mbm_episode_op_202512 as
select
    a.gal_mbi_hicn_fnl as mbi
    , a.component
    , a.eventkey as id
    , a.hce_service_code as service_code
    , a.fst_srvc_dt as start_dt
    , a.fst_srvc_month as serv_month
    , a.fst_srvc_qtr as hce_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hctapaidmonth
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
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
    , sum(0) as tadm_util
    , count(distinct a.eventkey) as visits
    , sum(a.adj_srvc_unit_cnt) as adj_srvc_units
from tmp_1q.kn_mbm_op_claims as a
left join tmp_1q.kn_lopa_op_202512 as b
    on concat_ws('_', a.gal_mbi_hicn_fnl, a.fst_srvc_dt) = b.total_mbi_dos
    and a.proc_cd = b.proc_cd
    and a.prov_tin = b.prov_tin
left join fichsrv.tadm_glxy_reason_code as c
    on a.fnl_rsn_cd_sys_id = c.rsn_cd_sys_id
where a.tfm_include_flag = 1
    and a.global_cap = 'NA'
    and a.product_level_3_fnl not in ('INSTITUTIONAL','DUAL')
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.st_abbr_cd = a.market_fnl
    and a.prov_prtcp_sts_cd = 'P'
    and substring(coalesce(a.bil_typ_cd,'0'), 1, 1) != '3'
    and a.ama_pl_of_srvc_cd <> '12'
    and (
        a.proc_cd in
        ('92507','92508','92526','97012','97016','97018','97022','97024','97026','97028',
         '97032','97033','97034','97035','97036','97039','97110','97112','97113','97116',
         '97124','97139','97140','97150','97164','97168','97530','97533','97535','97537',
         '97542','97545','97546','97750','97755','97760','97761','97799','G0283',
         '98940','98941','98942')
        or a.rvnu_cd in
        ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429',
         '0440','0441','0442','0443','0444','0449')
    )
    and a.proc_cd not in
    ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151',
     'G0152','G9041','G9043','G9044','S9128','S9129','S9131')
group by
    a.gal_mbi_hicn_fnl
    , a.component
    , a.eventkey
    , a.hce_service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt))
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
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
	end
;


--select count(*) from tmp_1q.kn_mbm_episode_op_202512; -- 41551071 40620968 39607841 40613266 38657890  37706450  36737912  35885437

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
from tmp_1y.kn_mbm_episode_1_2018_2020
;

-- select count(*) from tmp_1y.kn_mbm_episode_1_2018_2020; 
-- 21694518

-- select count(*) from tmp_1y.kn_mbm_episode_pr_2018_2020; 
-- 21694518


drop table if exists tmp_1y.kn_mbm_episode_op_2018_2020;
create table tmp_1y.kn_mbm_episode_op_2018_2020 as
select
	*
from tmp_1y.kn_mbm_episode_1b_2018_2020
;

-- select count(*) from tmp_1y.kn_mbm_episode_1b_2018_2020; 
-- 16546489

-- select count(*) from tmp_1y.kn_mbm_episode_op_2018_2020; 
-- 16546489

/*==============================================================================
 * REFACTORED THERAPY SAVINGS EPISODE ANALYSIS
 * Consolidated flow from claims union through final aggregation
 *==============================================================================*/

/*------------------------------------------------------------------------------
 * STEP 1: UNION ALL CLAIMS (PR + OP, current + historical)
 *------------------------------------------------------------------------------*/
drop table if exists tmp_1q.knd_mbm_episode_claims_202512;
create table tmp_1q.knd_mbm_episode_claims_202512 as
select * from tmp_1q.kn_mbm_episode_pr_202512
union all
select * from tmp_1y.kn_mbm_episode_pr_2018_2020
union all
select * from tmp_1q.kn_mbm_episode_op_202512
union all
select * from tmp_1y.kn_mbm_episode_op_2018_2020
;

/*------------------------------------------------------------------------------
 * STEP 2: ADD CLAIM STATUS, OPTUM FLAG, MBM SERVICE DETAIL, DEPLOYMENT FLAG
 *------------------------------------------------------------------------------*/
drop table if exists tmp_1q.knd_mbm_episode_enriched_202512;
create table tmp_1q.knd_mbm_episode_enriched_202512 as
with claim_base as (
    select 
        a.*
        , sum(allowed) over (partition by id, start_dt, category) as dnl_allowed
        , max(lopa_flg) over (partition by id, start_dt, category) as max_lopa_flg
    from tmp_1q.knd_mbm_episode_claims_202512 as a
)
select 
    b.*
    , case 
        when b.dnl_allowed > 0.01 then 'Paid'
        when b.still_lopa = 1 then 'LOPA'
        when b.apc_pbl_flg = 1 then 'APC-Paid'
        else 'Other Denied' 
    end as claim_status
    , case when t.tin_num is null then 0 else 1 end as optum_flg
    , case 
        when b.proc_cd in ('98940','98941','98942') then 'Chiro'
        when b.proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
        when b.proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
        else 'Other' 
    end as mbmserv_dtl
    , case 
        when b.market_fnl in ('AR','GA','NJ','SC') and b.group_ind_fnl = 'I'
        then case 
            when b.category = 'OP_REHAB' then 'Phase-II'
            else case when t.tin_num is null then 'Phase-II' else 'Phase-I' end 
        end
        else 'National' 
    end as mbm_deploy_dt
from claim_base as b
left join tmp_1y.p8001_optum_tin_2 as t
    on b.prov_tin = t.tin_num and t.i = 1
;

/*------------------------------------------------------------------------------
 * STEP 3: EPISODE IDENTIFICATION AND METRICS
 * - Aggregates to visit level
 * - Identifies episodes (>30 day gap = new episode)
 * - Calculates visit lag, runout, visits per episode
 *------------------------------------------------------------------------------*/
drop table if exists tmp_1q.knd_mbm_episode_analysis_202512;
create table tmp_1q.knd_mbm_episode_analysis_202512 as
with visit_aggregation as (
    -- Aggregate claims to visit level
    select
        concat(mbi, '-', category) as mbi_key
        , component
        , id
        , start_dt
        , serv_month
        , hce_qtr
        , min(hctapaidmonth) as hctapaidmonth
        , market_fnl
        , mbm_deploy_dt
        , claim_status
        , cast(mbmserv_dtl as varchar(10)) as mbmserv
        , category
        , sum(allowed) as allowed
        , sum(paid) as paid
        , sum(tadm_util) as tadm_util
        , count(distinct concat(id, start_dt)) as visits
        , sum(adj_srvc_units) as adj_srvc_units
    from tmp_1q.knd_mbm_episode_enriched_202512
    where prov_prtcp_sts_cd = 'P'
    group by 1,2,3,4,5,6,8,9,10,11,12
),

visit_with_lag as (
    -- Add previous visit date and gap calculation
    select
        *
        , lag(start_dt) over (
            partition by mbi_key, mbm_deploy_dt 
            order by start_dt
        ) as prev_start_dt
        , datediff('day', 
            lag(start_dt) over (
                partition by mbi_key, mbm_deploy_dt 
                order by start_dt
            ), 
            start_dt
        ) as visit_dy_lag
    from visit_aggregation
),

episode_identification as (
    -- Flag and number episodes
    select
        *
        , case 
            when prev_start_dt is null then 1
            when visit_dy_lag > 30 then 1
            else 0
        end as ep_flag
        , sum(case 
            when prev_start_dt is null then 1
            when visit_dy_lag > 30 then 1
            else 0
        end) over (
            partition by mbi_key, mbm_deploy_dt
            order by start_dt
            rows between unbounded preceding and current row
        ) as episode_num
    from visit_with_lag
),

episode_metrics as (
    -- Calculate episode-level attributes
    select
        *
        , min(start_dt) over (
            partition by mbi_key, mbm_deploy_dt, episode_num
        ) as ep_start_dt
        , min(hctapaidmonth) over (
            partition by mbi_key, mbm_deploy_dt, episode_num
        ) as ep_hctapaidmonth
    from episode_identification
)

select
    mbi_key as mbi
    , component
    , id
    , start_dt
    , prev_start_dt
    , visit_dy_lag
    , ep_flag
    , ep_start_dt
    , episode_num as cmltv_episodes
    , serv_month
    , hce_qtr
    , hctapaidmonth
    , ep_hctapaidmonth
    , mbm_deploy_dt
    , market_fnl
    , claim_status
    , mbmserv
    , category
    , allowed
    , paid
    , tadm_util
    , visits
    , adj_srvc_units
    -- Derived fields for downstream
    , to_char(ep_start_dt, 'yyyyMM') as ep_start_mo
    , to_char(ep_start_dt, 'yyyy') as ep_start_year
    , to_char(start_dt, 'yyyyMM') as visit_mo
    , floor(datediff('day', ep_start_dt, start_dt) / 30.5) as visit_ep_lag
    , floor((datediff('day', start_dt, hctapaidmonth) + 20) / 30.5) as visit_runout_mo
from episode_metrics
;

/*------------------------------------------------------------------------------
 * STEP 4: FINAL AGGREGATION FOR REPORTING
 * Creates visits and episodes summary tables
 *------------------------------------------------------------------------------*/

-- Visits aggregation
drop table if exists tmp_1q.knd_mbm_agg_visits_202512;
create table tmp_1q.knd_mbm_agg_visits_202512 as
select 
    'VISITS' as data_type
    , ep_start_mo
    , concat(ep_start_year, 'Q9') as ep_start_qtr
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , mbmserv as visit_mbmserv
    , visit_runout_mo
    , 0 as ep_runout_mo
    , visit_mo
    , visit_ep_lag
    , 0 as episodes
    , sum(visits) as visits
    , sum(allowed) as allowed
    , 0 as mm
from tmp_1q.knd_mbm_episode_analysis_202512
group by 
    ep_start_mo
    , concat(ep_start_year, 'Q9')
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , mbmserv
    , visit_runout_mo
    , visit_mo
    , visit_ep_lag
;

-- Episodes aggregation
drop table if exists tmp_1q.knd_mbm_agg_episodes_202512;
create table tmp_1q.knd_mbm_agg_episodes_202512 as
select 
    'EPISODES' as data_type
    , ep_start_mo
    , concat(ep_start_year, 'Q9') as ep_start_qtr
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , '' as visit_mbmserv
    , 0 as visit_runout_mo
    , 0 as ep_runout_mo
    , '0' as visit_mo
    , 0 as visit_ep_lag
    , sum(ep_flag) as episodes
    , 0 as visits
    , 0 as allowed
    , 0 as mm
from tmp_1q.knd_mbm_episode_analysis_202512
where ep_flag = 1
group by 
    ep_start_mo
    , concat(ep_start_year, 'Q9')
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
;

-- Combined aggregation
drop table if exists tmp_1q.knd_mbm_agg_combined_202512;
create table tmp_1q.knd_mbm_agg_combined_202512 as
select * from tmp_1q.knd_mbm_agg_visits_202512
union all
select * from tmp_1q.knd_mbm_agg_episodes_202512
;

/*------------------------------------------------------------------------------
 * STEP 5: FINAL SUMMARY FOR EXCEL REPORTING
 * Splits by time period and unions with membership
 *------------------------------------------------------------------------------*/
drop table if exists tmp_1q.knd_mbm_summary_post2023_202512;
create table tmp_1q.knd_mbm_summary_post2023_202512 as
select 
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 1, 4) as ep_year
    , substring(ep_start_mo, 5, 2) as ep_month
    , visit_mo
    , case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as pilot_nat
    , category
    , visit_ep_lag
    , visit_runout_mo
    , sum(episodes) as ep_cnt
    , sum(visits) as visit_cnt
    , sum(allowed) as allowed_amt
    , sum(mm) as mms
from tmp_1q.knd_mbm_agg_combined_202512
where ep_start_mo >= '202301'
group by
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 1, 4)
    , substring(ep_start_mo, 5, 2)
    , visit_mo
    , case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end
    , category
    , visit_ep_lag
    , visit_runout_mo
union all
select 
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 1, 4) as ep_year
    , substring(ep_start_mo, 5, 2) as ep_month
    , visit_mo
    , pilot_nat
    , category
    , visit_ep_lag
    , visit_runout_mo
    , ep_cnt
    , visit_cnt
    , allowed_amt
    , mms
from tmp_1q.kn_mbm_mshp_sum1_202512
;

-- Final output table
drop table if exists tmp_1q.knd_mbm_final_202512;
create table tmp_1q.knd_mbm_final_202512 as
select * from tmp_1q.knd_mbm_summary_post2023_202512
union all
select * from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202512
;

drop table if exists tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202512;
create table tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202512 as
select
	*
from tmp_1y.kn_mbm_episode_agg6_sum1_before2023
;

select count(*) from tmp_1y.kn_mbm_episode_agg6_sum1_before2023
-- 176,560

select count(*) from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202512
-- 176,560


drop table if exists tmp_1q.kn_mbm_202512;
create table tmp_1q.kn_mbm_202512 as
select
	*
from tmp_1q.kn_mbm_episode_agg6_sum1_after2023_202512
union all
select 
	*
from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202512;

select count(*) from tmp_1q.kn_mbm_202512; -- 271316 266615 266213 257905 253665
;
