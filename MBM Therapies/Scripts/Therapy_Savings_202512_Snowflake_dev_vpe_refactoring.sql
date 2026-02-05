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
 * UNION ALL CLAIMS (PR + OP, current + 2018-2020)
 *==============================================================================*/
drop table if exists tmp_1q.knd_mbm_episode_all_202512;
create table tmp_1q.knd_mbm_episode_all_202512 as
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
drop table if exists tmp_1q.knd_mbm_episode_extras_202512;
create table tmp_1q.knd_mbm_episode_extras_202512 as
with 
claim_with_lopa as (
	select 
	    a.*
	    , sum(allowed) over (partition by id, start_dt, category) as dnl_allowed
	    , max(lopa_flg) over (partition by id, start_dt, category) as max_lopa_flg
	from tmp_1q.knd_mbm_episode_all_202512 as a
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
from claim_with_lopa as b
left join tmp_1y.p8001_optum_tin_2 as t
    on b.prov_tin = t.tin_num and t.i = 1
;


select count(*) from tmp_1q.knd_mbm_episode_extras_202512;
-- 149390036

select claim_status, mbm_deploy_dt, count(*) from tmp_1q.knd_mbm_episode_extras_202512
group by 1, 2
;

select optum_flg, mbmserv_dtl, count(*) from tmp_1q.knd_mbm_episode_extras_202512
group by 1, 2
;

select serv_month, sum(allowed) as allowedamt 
from tmp_1q.knd_mbm_episode_extras_202512
where serv_month = '202406'  
group by serv_month;
-- 73580481.79

select serv_month, sum(visits)
from tmp_1q.knd_mbm_episode_extras_202512
where serv_month = '202406'  
group by serv_month;



/*------------------------------------------------------------------------------
 * STEP 3: EPISODE IDENTIFICATION AND METRICS
 * - Aggregates to visit level
 * - Identifies episodes (>30 day gap = new episode)
 * - Calculates visit lag, runout, visits per episode
 *------------------------------------------------------------------------------*/
drop table if exists tmp_1q.knd_mbm_episode_analysis_202512;
create table tmp_1q.knd_mbm_episode_analysis_202512 as
with 
visit_aggregation as (
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
    from tmp_1q.knd_mbm_episode_extras_202512
    where prov_prtcp_sts_cd = 'P'
    group by 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12
),

visit_with_lag as (
    -- Add previous visit date and gap calculation
    select
        *
        , lag(start_dt) over (partition by mbi_key, mbm_deploy_dt order by start_dt) 
        as prev_start_dt
        , datediff('day', lag(start_dt) over (partition by mbi_key, mbm_deploy_dt order by start_dt), start_dt) 
        as visit_day_diff
    from visit_aggregation
),

episode_id as (
    -- Flag and number episodes
    select
        *
        , case 
            when prev_start_dt is null then 1
            when visit_day_diff > 30 then 1
            else 0
        end as ep_flag
        , sum(case 
            	when prev_start_dt is null then 1
            	when visit_day_diff > 30 then 1
            	else 0
        	end) over (partition by mbi_key order by start_dt rows between unbounded preceding and current row) 
    	as episode_num
    from visit_with_lag
),

episode_calc as (
    -- Calculate episode-level attributes
    select
        *
        , min(start_dt) over (partition by mbi_key, episode_num) 
        as ep_start_dt
        , min(hctapaidmonth) over (partition by mbi_key, episode_num) 
        as ep_hctapaidmonth
    from episode_id
)
select
    mbi_key as mbi
    , component
    , id
    , start_dt
    , prev_start_dt
    , visit_day_diff
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
    , to_char(ep_start_dt, 'yyyyMM') as ep_start_mo
    , to_char(ep_start_dt, 'yyyy') as ep_start_year
    , to_char(start_dt, 'yyyyMM') as visit_mo
    , floor((datediff('day', start_dt, hctapaidmonth) + 20) / 30.5) as visit_runout_mo
    , floor(datediff('day', ep_start_dt, start_dt) / 30.5) as visit_ep_lag
from episode_calc
;


select ep_start_mo, sum(allowed), sum(visits), sum(ep_flag)
from tmp_1q.knd_mbm_episode_analysis_202512
where ep_start_mo >= '202401'
group by 1
order by 1


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



select ep_start_mo, sum(allowed), sum(visits), sum(episodes)
from tmp_1q.knd_mbm_agg_combined_202512
where ep_start_mo >= '202401'
group by 1 
order by 1


select visit_mo, sum(allowed), sum(visits), sum(episodes)
from tmp_1q.knd_mbm_agg_combined_202512
where visit_mo >= '202401'
group by 1 
order by 1

/*------------------------------------------------------------------------------
 * STEP 5: FINAL SUMMARY FOR EXCEL REPORTING
 * Splits by time period and unions with membership
 *------------------------------------------------------------------------------*/
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


drop table if exists tmp_1q.knd_mbm_summary_after2023_202512;
create table tmp_1q.knd_mbm_summary_after2023_202512 as
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
drop table if exists tmp_1q.knd_mbm_202512;
create table tmp_1q.knd_mbm_202512 as
select * from tmp_1q.knd_mbm_summary_after2023_202512
union all
select * from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202512
;


select count(*) from tmp_1q.knd_mbm_202512
-- 224,089

select ep_start_mo, sum(allowed_amt), sum(visit_cnt), sum(ep_cnt)
from tmp_1q.knd_mbm_202512
where ep_start_mo >= '202401'
group by 1 
order by 1
;

select visit_mo, sum(allowed_amt), sum(visit_cnt), sum(ep_cnt)
from tmp_1q.knd_mbm_202512
where visit_mo >= '202401'
group by 1 
order by 1
;

-- Alternate logic
/*==============================================================================
 * ALTERNATIVE APPROACH: EPISODE-FIRST LOGIC
 * Instead of: visits → flag episodes → calculate metrics
 * Do: identify episode boundaries → create episodes → join visits
 *==============================================================================*/

-- STEP 1: Get all visits with basic info
drop table if exists tmp_1q.alt_all_visits;
create table tmp_1q.alt_all_visits as
select 
    mbi,
    category,
    concat(mbi, '-', category) as member_key,
    id as visit_id,
    start_dt as visit_date,
    serv_month,
    hctapaidmonth as paid_date,
    market_fnl,
    mbm_deploy_dt,
    claim_status,
    mbmserv_dtl,
    allowed,
    paid,
    visits,
    row_number() over (partition by mbi, category order by start_dt) as visit_sequence
from tmp_1q.knd_mbm_episode_extras_202512
where prov_prtcp_sts_cd = 'P';


-- STEP 2: Find episode start dates (first visits and visits after >30 day gaps)
drop table if exists tmp_1q.alt_episode_starts;
create table tmp_1q.alt_episode_starts as
select 
    member_key,
    visit_date as episode_start_date,
    visit_id as first_visit_id,
    row_number() over (partition by member_key order by visit_date) as episode_num
from tmp_1q.alt_all_visits a
where 
    -- First visit ever
    visit_sequence = 1
    or
    -- OR visit more than 30 days after previous visit
    datediff('day', 
        lag(visit_date) over (partition by member_key order by visit_date),
        visit_date
    ) > 30;


-- STEP 3: Assign each visit to an episode
-- Logic: Find the most recent episode start that happened before or on this visit date
drop table if exists tmp_1q.alt_visits_with_episodes;
create table tmp_1q.alt_visits_with_episodes as
select 
    v.*,
    e.episode_num,
    e.episode_start_date,
    datediff('day', e.episode_start_date, v.visit_date) as days_from_episode_start
from tmp_1q.alt_all_visits v
left join tmp_1q.alt_episode_starts e
    on v.member_key = e.member_key
    and e.episode_start_date <= v.visit_date
    and e.episode_start_date = (
        -- Get the most recent episode start before this visit
        select max(e2.episode_start_date)
        from tmp_1q.alt_episode_starts e2
        where e2.member_key = v.member_key
          and e2.episode_start_date <= v.visit_date
    );


-- STEP 4: Calculate all derived metrics
drop table if exists tmp_1q.alt_final_data;
create table tmp_1q.alt_final_data as
select 
    member_key,
    mbi,
    category,
    visit_id,
    visit_date,
    episode_num,
    episode_start_date,
    
    -- Time metrics
    to_char(episode_start_date, 'YYYYMM') as episode_start_month,
    to_char(episode_start_date, 'YYYY') as episode_start_year,
    to_char(visit_date, 'YYYYMM') as visit_month,
    
    -- Episode progression
    floor(days_from_episode_start / 30.5) as months_into_episode,
    
    -- Payment lag
    floor((datediff('day', visit_date, paid_date) + 20) / 30.5) as payment_lag_months,
    
    -- Is this the first visit of an episode?
    case when visit_date = episode_start_date then 1 else 0 end as is_episode_start,
    
    -- Original fields
    serv_month,
    paid_date,
    market_fnl,
    mbm_deploy_dt,
    claim_status,
    mbmserv_dtl,
    allowed,
    paid,
    visits
from tmp_1q.alt_visits_with_episodes;


/*==============================================================================
 * VERIFICATION QUERIES
 *==============================================================================*/

-- Check: Do episodes match the old logic?
select 
    episode_start_month,
    count(distinct case when is_episode_start = 1 then concat(member_key, episode_num) end) as episode_count,
    count(*) as visit_count,
    sum(allowed) as total_allowed
from tmp_1q.alt_final_data
group by episode_start_month
order by episode_start_month;

-- Check: See one member's episodes
select 
    member_key,
    episode_num,
    episode_start_date,
    visit_date,
    days_from_episode_start,
    months_into_episode,
    is_episode_start,
    allowed
from tmp_1q.alt_final_data
where mbi = 'SOME_MBI'
order by visit_date;


/*==============================================================================
 * AGGREGATE FOR REPORTING (same output as before)
 *==============================================================================*/

drop table if exists tmp_1q.alt_agg_final;
create table tmp_1q.alt_agg_final as
-- Visits
select 
    'VISITS' as data_type,
    episode_start_month,
    concat(episode_start_year, 'Q9') as episode_start_qtr,
    visit_month as visit_mo,
    market_fnl,
    mbm_deploy_dt,
    category,
    claim_status,
    mbmserv_dtl as visit_mbmserv,
    months_into_episode as visit_ep_lag,
    payment_lag_months as visit_runout_mo,
    0 as ep_runout_mo,
    sum(visits) as visit_cnt,
    0 as ep_cnt,
    sum(allowed) as allowed_amt,
    0 as mms
from tmp_1q.alt_final_data
group by 1,2,3,4,5,6,7,8,9,10,11

union all

-- Episodes
select 
    'EPISODES' as data_type,
    episode_start_month,
    concat(episode_start_year, 'Q9') as episode_start_qtr,
    '0' as visit_mo,
    market_fnl,
    mbm_deploy_dt,
    category,
    claim_status,
    '' as visit_mbmserv,
    0 as visit_ep_lag,
    0 as visit_runout_mo,
    0 as ep_runout_mo,
    0 as visit_cnt,
    sum(is_episode_start) as ep_cnt,
    0 as allowed_amt,
    0 as mms
from tmp_1q.alt_final_data
group by 1,2,3,4,5,6,7,8,9,10,11,12;




-- Step 1: Create EPISODES aggregation (only rows where episodes start)
create or replace table tmp_1m.knd_mbm_episode_agg_episodes as
select 
	'EPISODES' as data_type
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') as ep_start_year
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
	, '' as visit_month
	, '' as visit_year
	, min_hctapaidmonth as paid_month
	, entity
	, category
	, market_fnl
	, mbm_deploy_dt
	, population
	, claim_status
	, 0 as visit_ep_lag
	, 0 as visit_runout_mo
	, sum(ep_flag) as n_episodes
	, 0 as n_visits
	, 0 as sum_allowed
	, 0 as sum_paid
	, 0 as mbr_count
from tmp_1m.knd_mbm_visit_analysis_2
where ep_flag = 1  -- Only episode-starting visits
group by 
	to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy')
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
	, min_hctapaidmonth
	, entity
	, category
	, market_fnl
	, mbm_deploy_dt
	, population
	, claim_status
;

-- Step 2: Create VISITS aggregation (all visits with additional dimensions)
create or replace table tmp_1m.knd_mbm_episode_agg_visits as
select 
	'VISITS' as data_type
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') as ep_start_year
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
	, fst_srvc_month as visit_month
	, fst_srvc_year as visit_year
	, min_hctapaidmonth as paid_month
	, entity
	, category
	, market_fnl
	, mbm_deploy_dt
	, population
	, claim_status
	, datediff('month', ep_start_dt, fst_srvc_dt) as visit_ep_lag
	, floor((datediff('day', fst_srvc_dt, min_hctapaidmonth) + 20) / 30.5) as visit_runout_mo
	, 0 as n_episodes
	, sum(n_visits) as n_visits
	, sum(allowed) as sum_allowed
	, sum(paid) as sum_paid
	, count(distinct mbi_key) as mbr_count
from tmp_1m.knd_mbm_visit_analysis_2
group by 
	to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy')
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
	, fst_srvc_month
	, fst_srvc_year
	, min_hctapaidmonth
	, entity
	, category
	, market_fnl
	, mbm_deploy_dt
	, population
	, claim_status
	, datediff('month', ep_start_dt, fst_srvc_dt)
	, floor((datediff('day', fst_srvc_dt, min_hctapaidmonth) + 20) / 30.5)
;

-- Step 3: Stack VISITS and EPISODES together
create or replace table tmp_1m.knd_mbm_episode_agg_combined as
select * from tmp_1m.knd_mbm_episode_agg_visits
union all
select * from tmp_1m.knd_mbm_episode_agg_episodes
;

-- Step 4: Summary query to get totals by population and episode start month
select 
	population
	, ep_start_month
	, sum(n_episodes) as total_episodes
	, sum(n_visits) as total_visits
	, sum(sum_allowed) as total_allowed
	, sum(sum_paid) as total_paid
	, case 
		when sum(n_episodes) > 0 
		then sum(n_visits) / sum(n_episodes) 
		else 0 
	end as visits_per_episode
	, case 
		when sum(n_episodes) > 0 
		then sum(sum_allowed) / sum(n_episodes) 
		else 0 
	end as allowed_per_episode
from tmp_1m.knd_mbm_episode_agg_combined
where population != 'N/A'
group by 
	population
	, ep_start_month
order by 
	population
	, ep_start_month
;

-- Step 5: Validation - Compare with stable file logic
select 
	ep_start_month
	, sum(n_episodes) as total_episodes
	, sum(n_visits) as total_visits
	, sum(sum_allowed) as total_allowed
from tmp_1m.knd_mbm_episode_agg_combined
where ep_start_year = '2024'
group by ep_start_month
order by ep_start_month
;
