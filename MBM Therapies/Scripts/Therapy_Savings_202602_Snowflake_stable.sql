/*==============================================================================
 * VARIABLE SETUP
 * Set current and previous month tags for reuse throughout script
 *==============================================================================*/
SET current_month  = '202602';
SET previous_month = '202601';


/*==============================================================================
 * MEMBERSHIP DETAIL PROCESSING
 * Creates base membership table with pilot/national deployment flags
 *==============================================================================*/
drop table if exists tmp_1q.kn_mbm_dtl_$current_month;					
create table tmp_1q.kn_mbm_dtl_$current_month as 					
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

-- QA: kn_mbm_dtl | expected ~450M+ rows (prev run: 476794132)
select 'kn_mbm_dtl' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_dtl_$current_month;


/*==============================================================================
 * MEMBERSHIP SUMMARY CREATION
 * Aggregates membership data and creates summary tables for analysis
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_mshp_$current_month;
create temporary table tmp_1q.kn_mbm_mshp_$current_month as
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
from tmp_1q.kn_mbm_dtl_$current_month as a
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

-- QA: kn_mbm_mshp | expected ~10K rows (prev run: 10428)
select 'kn_mbm_mshp' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_mshp_$current_month;

drop table if exists tmp_1q.kn_mbm_mshp_sum1_$current_month;
create table tmp_1q.kn_mbm_mshp_sum1_$current_month as
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
from tmp_1q.kn_mbm_mshp_$current_month
where population not in ('M&R DUALS', 'C&S DUALS')
group by 
	ep_start_mo
	, mbm_deploy_dt
;

-- QA: kn_mbm_mshp_sum1 | expected ~140+ rows (prev run: 144)
select 'kn_mbm_mshp_sum1' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_mshp_sum1_$current_month;

--_____________[ END OF MEMBERSHIP ]_____________________________________


/*==============================================================================
 * LOPA DATA INTEGRATION
 * On-track to be removed
 *==============================================================================*/

drop table if exists tmp_1q.kn_lopa_op_1_$current_month;
create table tmp_1q.kn_lopa_op_1_$current_month as
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

-- QA: kn_lopa_op_1 | expected ~2.9M+ rows (prev run: 2981365)
select 'kn_lopa_op_1' as tbl, count(*) as row_cnt from tmp_1q.kn_lopa_op_1_$current_month;

drop table if exists tmp_1q.kn_lopa_op_$current_month;
create table tmp_1q.kn_lopa_op_$current_month as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1q.kn_lopa_op_1_$current_month
; 

-- QA: kn_lopa_op | expected ~2.9M+ rows (prev run: 2981365)
select 'kn_lopa_op' as tbl, count(*) as row_cnt from tmp_1q.kn_lopa_op_$current_month;

drop table if exists tmp_1q.kn_lopa_pr_1_$current_month;
create table tmp_1q.kn_lopa_pr_1_$current_month as
select  
	mbi_dos as total_mbi_dos
	, case when final_lopa_ind = 1 and mbr_dos_latest_submission = 1 then mbi_dos end as still_lopa_mbi_dos
	, case when final_lopa_ind != 1 or mbr_dos_latest_submission != 1 then mbi_dos end as overturn_lopa_mbi_dos
	, *
from hce_ops_stage.pa_trckng_pr_evnt_lopa_dtl
;	

-- QA: kn_lopa_pr_1 | expected ~4.7M+ rows (prev run: 4790609)
select 'kn_lopa_pr_1' as tbl, count(*) as row_cnt from tmp_1q.kn_lopa_pr_1_$current_month;

drop table if exists tmp_1q.kn_lopa_pr_$current_month;
create table tmp_1q.kn_lopa_pr_$current_month as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1q.kn_lopa_pr_1_$current_month
;

-- QA: kn_lopa_pr | expected ~4.7M+ rows (prev run: 4790609)
select 'kn_lopa_pr' as tbl, count(*) as row_cnt from tmp_1q.kn_lopa_pr_$current_month;


/*==============================================================================
 * PROFESSIONAL CLAIMS PROCESSING
 * Pull in PR claims, combine with LOPA flags
 *==============================================================================*/
drop table if exists tmp_1q.kn_mbm_episode_pr_$current_month;
create table tmp_1q.kn_mbm_episode_pr_$current_month as
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
from fichsrv.glxy_pr_f as a
left join tmp_1q.kn_lopa_pr_$current_month as b
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

-- QA: kn_mbm_episode_pr | expected ~69M+ rows, serv_month='202407' sum(allowed) ~40.8M (prev run: 69597958 / 40768591.31)
select 'kn_mbm_episode_pr' as tbl, count(*) as row_cnt, sum(allowed) as total_allowed from tmp_1q.kn_mbm_episode_pr_$current_month;
select 'kn_mbm_episode_pr latest_mo' as tbl, serv_month, sum(allowed) as allowed_amt
from tmp_1q.kn_mbm_episode_pr_$current_month
where serv_month = (select max(serv_month) from tmp_1q.kn_mbm_episode_pr_$current_month)
group by serv_month;
	
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
    from fichsrv.glxy_op_f as a
    where (a.proc_cd in 			
			  ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	   '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
	   		   '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
	 	       '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 	   '98940', '98941', '98942')
		      or rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') 
		  )
		    and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	) as e
;

-- QA: kn_mbm_op_claims | expected ~82M+ rows (prev run: 82571364)
select 'kn_mbm_op_claims' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_op_claims;

drop table if exists tmp_1q.kn_mbm_episode_op_$current_month;
create table tmp_1q.kn_mbm_episode_op_$current_month as
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
left join tmp_1q.kn_lopa_op_$current_month as b
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

-- QA: kn_mbm_episode_op | expected ~42M+ rows (prev run: 42475737)
select 'kn_mbm_episode_op' as tbl, count(*) as row_cnt, sum(allowed) as total_allowed from tmp_1q.kn_mbm_episode_op_$current_month;


/*==============================================================================
 * CLAIMS UNION AND CATEGORIZATION
 * Combines current and prior-month PR and OP claims
 * Add claim_status flag, add MBM-related flags
 *==============================================================================*/

-- Refresh 2018-2020 PR table
drop table if exists tmp_1y.kn_mbm_episode_pr_2018_2020;
create table tmp_1y.kn_mbm_episode_pr_2018_2020 as
select * from tmp_1y.kn_mbm_episode_1_2018_2020;

-- QA: kn_mbm_episode_pr_2018_2020 | expected 21694518
select 'kn_mbm_episode_pr_2018_2020' as tbl, count(*) as row_cnt from tmp_1y.kn_mbm_episode_pr_2018_2020;

-- Refresh 2018-2020 OP table
drop table if exists tmp_1y.kn_mbm_episode_op_2018_2020;
create table tmp_1y.kn_mbm_episode_op_2018_2020 as
select * from tmp_1y.kn_mbm_episode_1b_2018_2020;

-- QA: kn_mbm_episode_op_2018_2020 | expected 16546489
select 'kn_mbm_episode_op_2018_2020' as tbl, count(*) as row_cnt from tmp_1y.kn_mbm_episode_op_2018_2020;


-- Stack OP + PR Episodes from 2018 to current
drop table if exists tmp_1q.kn_mbm_episode_1c_$current_month;
create table tmp_1q.kn_mbm_episode_1c_$current_month as
select * from tmp_1q.kn_mbm_episode_pr_$current_month
union all
select * from tmp_1y.kn_mbm_episode_pr_2018_2020
union all
select * from tmp_1q.kn_mbm_episode_op_$current_month
union all
select * from tmp_1y.kn_mbm_episode_op_2018_2020
;

-- QA: kn_mbm_episode_1c | expected ~149M+ rows (prev run: 149390036)
--     serv_month latest sum(allowed) spot check vs previous month run
select 'kn_mbm_episode_1c' as tbl, count(*) as row_cnt, sum(allowed) as total_allowed from tmp_1q.kn_mbm_episode_1c_$current_month;
select 'kn_mbm_episode_1c latest_mo' as tbl, serv_month, sum(allowed) as allowed_amt
from tmp_1q.kn_mbm_episode_1c_$current_month
where serv_month = (select max(serv_month) from tmp_1q.kn_mbm_episode_1c_$current_month)
group by serv_month;


drop table if exists tmp_1q.kn_mbm_episode_2_$current_month;
create table tmp_1q.kn_mbm_episode_2_$current_month as
with episode_base as (
    select *
        , sum(allowed) over (partition by id, start_dt, category) as dnl_allowed
        , max(lopa_flg) over (partition by id, start_dt, category) as max_lopa_flg
    from tmp_1q.kn_mbm_episode_1c_$current_month
),
joined as (
    select a.*, b.tin_num
    from episode_base as a
    left join tmp_1y.p8001_optum_tin_2 as b
        on a.prov_tin = b.tin_num and b.i = 1
)
select *
    , case when dnl_allowed > 0.01 then 'Paid'
           when still_lopa = 1 then 'LOPA'
           when apc_pbl_flg = 1 then 'APC-Paid'
           else 'Other Denied' end as claim_status
    , case when tin_num is null then 0 else 1 end as optum_flg
    , case when proc_cd in ('98940','98941','98942') then 'Chiro'
           when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
           when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
           else 'Other' end as mbmserv_dtl
    , case when market_fnl in ('AR','GA','NJ','SC') and group_ind_fnl = 'I'
           then case when category = 'OP_REHAB' then 'Phase-II'
                     else case when tin_num is null then 'Phase-II' else 'Phase-I' end end
           else 'National' end as mbm_deploy_dt
from joined
;

-- QA: kn_mbm_episode_2 | expected ~151M+ rows (prev run: 151638417)
select 'kn_mbm_episode_2' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_2_$current_month;
select 'kn_mbm_episode_2 serv_month 202406' as tbl, sum(allowed) as allowed_amt
from tmp_1q.kn_mbm_episode_2_$current_month
where serv_month = '202406';
-- prev: 73580481.79

-- QA: distribution checks
select claim_status, mbm_deploy_dt, count(*) as row_cnt
from tmp_1q.kn_mbm_episode_2_$current_month
group by 1, 2;

select optum_flg, mbmserv_dtl, count(*) as row_cnt
from tmp_1q.kn_mbm_episode_2_$current_month
group by 1, 2;


/*==============================================================================
 * EPISODE ANALYSIS
 * Creates episode aggregation and visit ranking structure
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_3_$current_month;  
create table tmp_1q.kn_mbm_episode_3_$current_month as 
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
from tmp_1q.kn_mbm_episode_2_$current_month
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
order by mbi, mbmserv, start_dt, id
;

-- QA: kn_mbm_episode_3 | expected ~70M+ rows (prev run: 70228347)
select 'kn_mbm_episode_3' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_3_$current_month;

drop table if exists tmp_1q.kn_mbm_episode_4_$current_month;  
create table tmp_1q.kn_mbm_episode_4_$current_month as  			
select 
	mbi
	, component
	, id
	, start_dt
	, row_number() over (partition by mbi, mbm_deploy_dt order by start_dt) as i 
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
from tmp_1q.kn_mbm_episode_3_$current_month as a
;

-- QA: kn_mbm_episode_4 | expected ~70M+ rows (prev run: 70228347)
select 'kn_mbm_episode_4' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_4_$current_month;


/*==============================================================================
 * VISIT EPISODE LAG CALCULATION
 * Calculates time gaps between visits and episode flags
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_lag_$current_month;  
create table tmp_1q.kn_mbm_episode_lag_$current_month as  
select a.mbi
	, a.component
	, a.id
	, a.start_dt
	, b.start_dt as prev_start_dt
	, datediff('day', b.start_dt, a.start_dt) as visit_dy_lag
    , iff(datediff('day', b.start_dt, a.start_dt) > 30, 1, 0) as ep_flag
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
from tmp_1q.kn_mbm_episode_4_$current_month as a
left join tmp_1q.kn_mbm_episode_4_$current_month as b 
	on a.mbi = b.mbi 
	and a.mbm_deploy_dt = b.mbm_deploy_dt
	and a.i = b.i+1 
;

-- QA: kn_mbm_episode_lag | expected ~70M+ rows (prev run: 70228347)
select 'kn_mbm_episode_lag' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_lag_$current_month;


/*==============================================================================
 * EPISODE START DATE DETERMINATION
 * Identifies episode boundaries and calculates cumulative episodes
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_vst_ep_2_$current_month;  
create or replace table tmp_1q.kn_mbm_episode_vst_ep_2_$current_month as
select
    a.mbi
  , a.component
  , a.id
  , a.start_dt
  , a.prev_start_dt
  , a.visit_dy_lag
  , a.ep_flag
  , min(a.start_dt) over (partition by a.mbi, a.cmltv_episodes) as ep_start_dt
  , a.cmltv_episodes
  , a.i
  , a.prev_i
  , a.serv_month
  , a.hce_qtr
  , a.hctapaidmonth
  , min(a.hctapaidmonth) over (partition by a.mbi, a.cmltv_episodes) as ep_hctapaidmonth
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
from (
    select
        *
      , sum(iff(prev_start_dt is null, 1, ep_flag)) over (
            partition by mbi
            order by start_dt
            rows between unbounded preceding and current row
        ) as cmltv_episodes
    from tmp_1q.kn_mbm_episode_lag_$current_month
) as a
;

-- QA: kn_mbm_episode_vst_ep_2 | expected ~70M+ rows (prev run: 70228347)
select 'kn_mbm_episode_vst_ep_2' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_vst_ep_2_$current_month;

drop table if exists tmp_1q.kn_mbm_episode_smry_$current_month;  
create or replace table tmp_1q.kn_mbm_episode_smry_$current_month as  
select  
	a.serv_month as visit_month
	, to_char(ep_start_dt,'yyyyMM') as ep_start_mo 
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
from tmp_1q.kn_mbm_episode_vst_ep_2_$current_month as a
group by 
	a.serv_month  
	, to_char(ep_start_dt,'yyyyMM') 
	, a.hctapaidmonth
	, a.mbm_deploy_dt
	, a.market_fnl	
	, a.claim_status
	, a.mbmserv
	, a.category
;

-- QA: kn_mbm_episode_smry | expected ~1.5M+ rows (prev run: 1509656)
select 'kn_mbm_episode_smry' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_smry_$current_month;

-- QA: spot check recent ep_start_mo totals vs previous run
select ep_start_mo, sum(allw) as allw, sum(visits) as visits, sum(episodes) as episodes
from tmp_1q.kn_mbm_episode_smry_$current_month
where ep_start_mo >= '202401'
group by 1
order by 1;


/*==============================================================================
 * EPISODE SUMMARY AND RUNOUT ANALYSIS
 * Calculates runout periods and creates aggregated episodes
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_episode_ro_lag_$current_month as
select
    a.mbi
  , a.component
  , a.id
  , a.start_dt
  , floor((datediff('day', a.start_dt, a.hctapaidmonth) + 20) / 30.5) as visit_runout_mo
  , round((datediff('day', a.start_dt, a.hctapaidmonth) + 20) / 1, 0) as visit_runout
  , floor(datediff('day', a.ep_start_dt, a.start_dt) / 30.5) as visit_ep_lag
  , a.visit_dy_lag
  , iff(a.prev_start_dt is null, 1, a.ep_flag) as ep_flag
  , a.ep_start_dt
  , a.cmltv_episodes
  , a.i
  , a.prev_i
  , a.serv_month
  , a.hce_qtr
  , a.hctapaidmonth
  , a.ep_hctapaidmonth
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
from tmp_1q.kn_mbm_episode_vst_ep_2_$current_month as a
;

-- QA: kn_mbm_episode_ro_lag | expected ~70M+ rows (prev run: 70228347)
select 'kn_mbm_episode_ro_lag' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_ro_lag_$current_month;

drop table if exists tmp_1q.kn_mbm_episode_ro_lag2_$current_month;  
create table tmp_1q.kn_mbm_episode_ro_lag2_$current_month as  
select 
	a.mbi
	, a.id
	, ep_start_dt
	, cmltv_episodes
	, start_dt
	, to_char(ep_start_dt,'yyyyMM') as ep_start_mo 
	, to_char(ep_start_dt,'yyyy') as ep_start_year
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
	, hctapaidmonth
	, mbmserv as visit_mbmserv
	, visit_runout_mo
	, 0 as ep_runout_mo
	, to_char(start_dt,'yyyyMM') as visit_mo
	, visit_ep_lag 
	, ep_flag as episodes
	, visits
	, allowed
	, 0 as mm 
from tmp_1q.kn_mbm_episode_ro_lag_$current_month as a
;

-- QA: kn_mbm_episode_ro_lag2 | expected ~70M+ rows (prev run: 70228347)
select 'kn_mbm_episode_ro_lag2' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_ro_lag2_$current_month;

drop table if exists tmp_1q.kn_mbm_episode_agg6_ep_$current_month;  
create or replace table tmp_1q.kn_mbm_episode_agg6_ep_$current_month as  
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
from (select * from tmp_1q.kn_mbm_episode_ro_lag2_$current_month where episodes = 1 ) as a
group by 
	ep_start_mo
	, concat(ep_start_year,'Q9')
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
;

-- QA: kn_mbm_episode_agg6_ep | expected ~47K+ rows (prev run: 47476)
select 'kn_mbm_episode_agg6_ep' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_agg6_ep_$current_month;


/*==============================================================================
 * COMBINE VISITS AND EPISODES
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_agg6_$current_month;  
create or replace table tmp_1q.kn_mbm_episode_agg6_$current_month as  
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
from tmp_1q.kn_mbm_episode_ro_lag2_$current_month
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

-- QA: kn_mbm_episode_agg6 visits-only | expected ~2.2M+ rows (prev run: 2242957)
select 'kn_mbm_episode_agg6 (pre-insert)' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_agg6_$current_month;

alter table tmp_1q.kn_mbm_episode_agg6_$current_month
  alter column data_type set data type varchar(50);

insert into tmp_1q.kn_mbm_episode_agg6_$current_month   
select * from tmp_1q.kn_mbm_episode_agg6_ep_$current_month as a;

alter table tmp_1q.kn_mbm_episode_agg6_$current_month
  alter column data_type    set data type varchar(50);

alter table tmp_1q.kn_mbm_episode_agg6_$current_month
  alter column claim_status set data type varchar(50);

alter table tmp_1q.kn_mbm_episode_agg6_$current_month
  alter column category     set data type varchar(50);

-- QA: kn_mbm_episode_agg6 (final, visits+episodes) | expected ~2.3M+ rows (prev run: 2324504)
select 'kn_mbm_episode_agg6 (final)' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_agg6_$current_month;

-- QA: spot check visit_mo allowed vs previous run (prev run visit_mo=202406: 73607414.24)
select 'agg6 visit_mo 202406' as tbl, sum(allowed) as allowed_amt
from tmp_1q.kn_mbm_episode_agg6_$current_month
where visit_mo = '202406';

-- QA: recent ep_start_mo breakdown
select ep_start_mo, sum(allowed) as allowed, sum(visits) as visits, sum(episodes) as episodes
from tmp_1q.kn_mbm_episode_agg6_$current_month
where ep_start_mo >= '202401'
group by 1 
order by 1;

-- QA: recent visit_mo breakdown
select visit_mo, sum(allowed) as allowed, sum(visits) as visits, sum(episodes) as episodes
from tmp_1q.kn_mbm_episode_agg6_$current_month
where visit_mo >= '202401'
group by 1 
order by 1;


/*___________________[ SUMARIZING DATA FOR EXCEL ]_________________________________________________*/

/*==============================================================================
 * FINAL DATA AGGREGATION FOR REPORTING
 * Creates summary tables for Excel reporting and analysis
 * Stitch up pre-2023 and 2023+ tables
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_agg6_sum1_after2023_$current_month;
create table tmp_1q.kn_mbm_episode_agg6_sum1_after2023_$current_month as 
select 
	data_type,
	ep_start_mo,
	substring(ep_start_mo, 0, 4) as ep_year,
	substring(ep_start_mo, 5,2) as ep_month,
	visit_mo,
	case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as pilot_nat,
	category,
	visit_ep_lag,
	visit_runout_mo,
	sum(episodes) as ep_cnt,
	sum(visits) as visit_cnt,
	sum(allowed) as allowed_amt,
	sum(mm) as mms
from tmp_1q.kn_mbm_episode_agg6_$current_month
where ep_start_mo >= '202301'
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
from tmp_1q.kn_mbm_mshp_sum1_$current_month
;

-- QA: after2023 summary | expected ~94K+ rows (prev run: 94756)
select 'agg6_sum1_after2023' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_episode_agg6_sum1_after2023_$current_month;


drop table if exists tmp_1y.kn_mbm_episode_agg6_sum1_before2023_$current_month;
create table tmp_1y.kn_mbm_episode_agg6_sum1_before2023_$current_month as
select * from tmp_1y.kn_mbm_episode_agg6_sum1_before2023;

-- QA: before2023 | expected 176560
select 'agg6_sum1_before2023' as tbl, count(*) as row_cnt from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_$current_month;


drop table if exists tmp_1q.kn_mbm_$current_month;
create table tmp_1q.kn_mbm_$current_month as
select * from tmp_1q.kn_mbm_episode_agg6_sum1_after2023_$current_month
union all
select * from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_$current_month;

-- QA: kn_mbm FINAL | expected ~275K+ rows (prev run: 275176)
select 'kn_mbm FINAL' as tbl, count(*) as row_cnt from tmp_1q.kn_mbm_$current_month;

-- QA: final ep_start_mo spot check
select ep_start_mo, sum(allowed_amt) as allowed, sum(visit_cnt) as visits, sum(ep_cnt) as episodes
from tmp_1q.kn_mbm_$current_month
where ep_start_mo >= '202401'
group by 1 
order by 1;

-- QA: final visit_mo spot check
select visit_mo, sum(allowed_amt) as allowed, sum(visit_cnt) as visits, sum(ep_cnt) as episodes
from tmp_1q.kn_mbm_$current_month
where visit_mo >= '202401'
group by 1 
order by 1;

-- QA: compare row count to previous month's final table
select $previous_month as prev_month, count(*) as prev_row_cnt from tmp_1q.kn_mbm_$previous_month
union all
select $current_month as curr_month, count(*) as curr_row_cnt from tmp_1q.kn_mbm_$current_month;

-- QA: AVTAR max admit date check
select max(admit_dt_act) 
from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_f 
where 	
       svc_setting ='Inpatient'
       and plc_of_svc_cd ='21 - Acute Hospital'
       and admit_cat_cd  in ('17 - Medical','30 - Surgical')			
       and fin_brand in ('M&R','C&S')
       and TO_VARCHAR(admit_dt_act, 'MM/dd/yyyy') is not null 
       and TO_VARCHAR(admit_dt_act  ,'yyyy') in ('2026')
;
