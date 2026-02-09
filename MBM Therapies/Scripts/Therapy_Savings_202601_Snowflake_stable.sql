/*==============================================================================
 * MEMBERSHIP DETAIL PROCESSING
 * Creates base membership table with pilot/national deployment flags
 *==============================================================================*/
drop table if exists tmp_1q.kn_mbm_dtl_202601;					
create table tmp_1q.kn_mbm_dtl_202601 as 					
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
-- select count(*) from tmp_1q.kn_mbm_dtl_202601;  
-- 476794132 469046914 430370488 415060257  407463331 399836379   392256742 (removed 2019) 442855070  435558291  428264965  450161568
								

/*==============================================================================
 * MEMBERSHIP SUMMARY CREATION
 * Aggregates membership data and creates summary tables for analysis
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_mshp_202601;
create temporary table tmp_1q.kn_mbm_mshp_202601 as
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
from tmp_1q.kn_mbm_dtl_202601 as a
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

-- select count(*) from tmp_1q.kn_mbm_mshp_202601; -- 10428 10291 9743  9606

drop table if exists tmp_1q.kn_mbm_mshp_sum1_202601;
create table tmp_1q.kn_mbm_mshp_sum1_202601 as
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
from tmp_1q.kn_mbm_mshp_202601
where population not in ('M&R DUALS', 'C&S DUALS')
group by 
	ep_start_mo
	, mbm_deploy_dt
;

--select count(*) from tmp_1q.kn_mbm_mshp_sum1_202601; -- 144 142 134 132 128 126 124  144 142

--select * from tmp_1q.kn_mbm_mshp_sum1_202601;
--_____________[ END OF MEMBERSHIP ]_____________________________________


/*==============================================================================
 * LOPA DATA INTEGRATION
 * On-track to be removed
 *==============================================================================*/

--describe formatted tmp_1y.pa_trckng_op_evnt_lopa_dtl;
drop table if exists tmp_1q.kn_lopa_op_1_202601;
create table tmp_1q.kn_lopa_op_1_202601 as
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

--select count(*) from tmp_1q.kn_lopa_op_1_202601; -- 2981365 2909228 2484707 2417275  2306199  2164101  1729931 1526791   1338831
;

drop table if exists tmp_1q.kn_lopa_op_202601;
create table tmp_1q.kn_lopa_op_202601 as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1q.kn_lopa_op_1_202601
; 

--select count(*) from tmp_1q.kn_lopa_op_202601; -- 2981365 2909228 2417275  2306199 2164101

drop table if exists tmp_1q.kn_lopa_pr_1_202601;
create table tmp_1q.kn_lopa_pr_1_202601 as
select  
	mbi_dos as total_mbi_dos
	, case when final_lopa_ind = 1 and mbr_dos_latest_submission = 1 then mbi_dos end as still_lopa_mbi_dos
	, case when final_lopa_ind != 1 or mbr_dos_latest_submission != 1 then mbi_dos end as overturn_lopa_mbi_dos
	, *
from hce_ops_stage.pa_trckng_pr_evnt_lopa_dtl
;	

--select count(*) from tmp_1q.kn_lopa_pr_1_202601; -- 4790609 4659061 3714719  3501938 3263702

drop table if exists tmp_1q.kn_lopa_pr_202601;
create table tmp_1q.kn_lopa_pr_202601 as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_lopa
	, *
from tmp_1q.kn_lopa_pr_1_202601
;

--select count(*) from tmp_1q.kn_lopa_pr_202601; -- 4790609 4659061 3714719  3501938 3263702
		


/*==============================================================================
 * PROFESSIONAL CLAIMS PROCESSING
 * Pull in PR claims, combine with LOPA flags
 *==============================================================================*/
drop table if exists tmp_1q.kn_mbm_episode_pr_202601;
create table tmp_1q.kn_mbm_episode_pr_202601 as
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
left join tmp_1q.kn_lopa_pr_202601 as b
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



--select count(*) from tmp_1q.kn_mbm_episode_pr_202601; -- 69597958 68110261 66530074 64354173  62711063  60990327

select sum(allowed) from tmp_1q.kn_mbm_episode_pr_202601
where serv_month = '202406'
;
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


-- select count(*) from tmp_1q.kn_mbm_op_claims; -- 82571364 77536654  75713104  73889849     72274650
drop table if exists tmp_1q.kn_mbm_episode_op_202601;
create table tmp_1q.kn_mbm_episode_op_202601 as
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
left join tmp_1q.kn_lopa_op_202601 as b
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


--
select count(*) from tmp_1q.kn_mbm_episode_op_202601; -- 42475737 41551071 40620968 39607841 40613266 38657890  37706450  36737912  35885437

/*==============================================================================
 * CLAIMS UNION AND CATEGORIZATION
 * Combines current and 2018-2020 PR and OP claims
 * Add claim_status flag, add MBM-related flags
 *==============================================================================*/

-- Refresh 2018-2020 PR table
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

-- Refresh 2018-2020 OP table
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


-- Stack OP + PR Episodes from 2018 to current
drop table if exists tmp_1q.kn_mbm_episode_1c_202601;
create table tmp_1q.kn_mbm_episode_1c_202601 as
select * from tmp_1q.kn_mbm_episode_pr_202601
union all
select * from tmp_1y.kn_mbm_episode_pr_2018_2020
union all
select * from tmp_1q.kn_mbm_episode_op_202601
union all
select * from tmp_1y.kn_mbm_episode_op_2018_2020
;


--select count(*) from tmp_1q.kn_mbm_episode_1c_202601; 
-- 149390036 146972236 146949829 144378922 146949829 136759944 134268620 141253070  138658520  135969246

select serv_month, sum(allowed) as allowedamt 
from tmp_1q.kn_mbm_episode_1c_202601
where serv_month = '202407'
group by serv_month;

-- 81177919.11
-- 81205032.56
-- 81217382.23


--select serv_month, sum(visits) 
--from tmp_1q.kn_mbm_episode_2_202601
--where serv_month = '202406'  
--group by serv_month;


drop table if exists tmp_1q.kn_mbm_episode_2_202601;
create table tmp_1q.kn_mbm_episode_2_202601 as
with episode_base as (
    select *
        , sum(allowed) over (partition by id, start_dt, category) as dnl_allowed
        , max(lopa_flg) over (partition by id, start_dt, category) as max_lopa_flg
    from tmp_1q.kn_mbm_episode_1c_202601
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

--select count(*) from tmp_1q.kn_mbm_episode_2_202601; -- 151638417 149390036 146972236 146949829 144378922 134268620 141253070  138658520  135969246


select claim_status, mbm_deploy_dt, count(*) from tmp_1q.kn_mbm_episode_2_202601
group by 1,2

select optum_flg, mbmserv_dtl, count(*) from tmp_1q.kn_mbm_episode_2_202601
group by 1, 2


select serv_month, sum(allowed) as allowedamt 
from tmp_1q.kn_mbm_episode_2_202601
where serv_month = '202406'  
group by serv_month; -- 73580481.79 73560084.49 73,520,205.78 73,491,717.81 73432103.42  73635236.05  73,514,079.66 (shorter proc_cd list)  76,561,115.88 (original proc_cd list)


/*==============================================================================
 * EPISODE ANALYSIS
 * Creates episode aggregation and visit ranking structure
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_3_202601;  
create table tmp_1q.kn_mbm_episode_3_202601 as 
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
from tmp_1q.kn_mbm_episode_2_202601
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

--select count(*) from tmp_1q.kn_mbm_episode_3_202601; -- 70228347 69143262 69136216 64545494 63423287 66929218  65797555     64650521

drop table if exists tmp_1q.kn_mbm_episode_4_202601;  
create table tmp_1q.kn_mbm_episode_4_202601 as  			
select 
	mbi
	, component
	, id
	, start_dt
	, row_number() over (partition by mbi,mbm_deploy_dt order by start_dt) as i 
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
from tmp_1q.kn_mbm_episode_3_202601 as a
;

--select count(*) from tmp_1q.kn_mbm_episode_4_202601; -- 70228347 69143262 64545494 66929218  65797555   64650521



/*==============================================================================
 * VISIT EPISODE LAG CALCULATION
 * Calculates time gaps between visits and episode flags
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_lag_202601;  
create table tmp_1q.kn_mbm_episode_lag_202601 as  
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
from tmp_1q.kn_mbm_episode_4_202601 as a
left join tmp_1q.kn_mbm_episode_4_202601 as b 
	on a.mbi = b.mbi 
	and a.mbm_deploy_dt = b.mbm_deploy_dt
	and a.i = b.i+1 
;

--select count(*) from tmp_1q.kn_mbm_episode_lag_202601; -- 70228347 69143262 64545494 66929218  65797555  64650521
-- 
/*==============================================================================
 * EPISODE START DATE DETERMINATION
 * Identifies episode boundaries and calculates cumulative episodes
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_vst_ep_2_202601;  
create or replace table tmp_1q.kn_mbm_episode_vst_ep_2_202601 as
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
    from tmp_1q.kn_mbm_episode_lag_202601
) as a
;





--select count(*) from tmp_1q.kn_mbm_episode_vst_ep_2_202601; -- 70228347 69143262 66929218  65797555  64650521

drop table if exists tmp_1q.kn_mbm_episode_smry_202601;  
create or replace table tmp_1q.kn_mbm_episode_smry_202601 as  
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
from tmp_1q.kn_mbm_episode_vst_ep_2_202601 as a
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

select ep_start_mo, sum(allw), sum(visits), sum(episodes)
from tmp_1q.kn_mbm_episode_smry_202601
where ep_start_mo >= '202401'
group by 1
order by 1


--select count(*) from tmp_1q.kn_mbm_episode_smry_202601; -- 1509656 1482158 1479372 1555388  1526174  1496541

select sum(visits), sum(episodes), sum(allw), sum(mbr_count)
from tmp_1q.kn_mbm_episode_smry_202601
where ep_start_mo = '202406'


/*==============================================================================
 * EPISODE SUMMARY AND RUNOUT ANALYSIS
 * Calculates runout periods and creates aggregated episodes
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_episode_ro_lag_202601 as
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
from tmp_1q.kn_mbm_episode_vst_ep_2_202601 as a
;

--select count(*) from tmp_1q.kn_mbm_episode_ro_lag_202601; -- 70228347 69143262 66929218  65797555  64650521

drop table if exists tmp_1q.kn_mbm_episode_ro_lag2_202601;  
create table tmp_1q.kn_mbm_episode_ro_lag2_202601 as  
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
from tmp_1q.kn_mbm_episode_ro_lag_202601 as a
;

--select count(*) from tmp_1q.kn_mbm_episode_ro_lag2_202601; -- 70228347 69143262 69136216 66929218  65797555  64650521

drop table if exists tmp_1q.kn_mbm_episode_agg6_ep_202601;  
create or replace table tmp_1q.kn_mbm_episode_agg6_ep_202601 as  
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
from (select * from tmp_1q.kn_mbm_episode_ro_lag2_202601 where episodes = 1 ) as a
group by 
	ep_start_mo
	, concat(ep_start_year,'Q9')
	, market_fnl
	, mbm_deploy_dt
	, category
	, claim_status
;
--select count(*) from tmp_1q.kn_mbm_episode_agg6_ep_202601; -- 47476 46706 46496 45630  44961 44193  43381






/*==============================================================================
 * COMBINE VISITS AND EPISODES
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_agg6_202601;  
create or replace table tmp_1q.kn_mbm_episode_agg6_202601 as  
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
from tmp_1q.kn_mbm_episode_ro_lag2_202601
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




--select count(*) from tmp_1q.kn_mbm_episode_agg6_202601; -- 2242957 2202473 2198844 2322113  2278610 2234635


select count(*) from tmp_1q.kn_mbm_episode_agg6_202601 
select count(*) from tmp_1q.kn_mbm_episode_agg6_ep_202601 

alter table tmp_1q.kn_mbm_episode_agg6_202601
alter column data_type set data type varchar(50);

insert into tmp_1q.kn_mbm_episode_agg6_202601   
select * from tmp_1q.kn_mbm_episode_agg6_ep_202601 as a;




--
---- Minor format adjustments
---- Considering removing because of formatting issues




alter table tmp_1q.kn_mbm_episode_agg6_202601
  alter column data_type       set data type varchar(50);
--
--alter table tmp_1q.kn_mbm_episode_agg6_202601
--  alter column ep_start_mo     set data type varchar(50);
--
--alter table tmp_1q.kn_mbm_episode_agg6_202601
--  alter column ep_start_qtr    set data type varchar(50);
--
--alter table tmp_1q.kn_mbm_episode_agg6_202601
--  alter column mbm_deploy_dt   set data type varchar(50);

alter table tmp_1q.kn_mbm_episode_agg6_202601
  alter column claim_status    set data type varchar(50);

--alter table tmp_1q.kn_mbm_episode_agg6_202601
--  alter column visit_mo        set data type varchar(50);

alter table tmp_1q.kn_mbm_episode_agg6_202601
  alter column category        set data type varchar(50);

select count(*) from tmp_1q.kn_mbm_episode_agg6_202601; 

-- 2324504 2290433 2245340 2089616 2367743  2323571  2278828  2235149

select sum(allowed) 
from tmp_1q.kn_mbm_episode_agg6_202601
where visit_mo = '202406'; 

--73607414.24 73580481.79 73246897.76 73520205.78 73432103.42  73635236.05  73514079.66

select ep_start_mo, sum(allowed), sum(visits), sum(episodes)
from tmp_1q.kn_mbm_episode_agg6_202601
where ep_start_mo >= '202401'
group by 1 
order by 1

select visit_mo, sum(allowed), sum(visits), sum(episodes)
from tmp_1q.kn_mbm_episode_agg6_202601
where visit_mo >= '202401'
group by 1 
order by 1



/*___________________[ SUMARIZING DATA FOR EXCEL ]_________________________________________________*/

/*==============================================================================
 * FINAL DATA AGGREGATION FOR REPORTING
 * Creates summary tables for Excel reporting and analysis
 * Stich up 2023- and 2023+ tables
 *==============================================================================*/

drop table if exists tmp_1q.kn_mbm_episode_agg6_sum1_after2023_202601;
create table tmp_1q.kn_mbm_episode_agg6_sum1_after2023_202601 as 
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
from tmp_1q.kn_mbm_episode_agg6_202601
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
from tmp_1q.kn_mbm_mshp_sum1_202601
;
--select count(*) from tmp_1q.kn_mbm_episode_agg6_sum1_after2023_202601; -- 94756 90055 89653 89653 72856


drop table if exists tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202601;
create table tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202601 as
select
	*
from tmp_1y.kn_mbm_episode_agg6_sum1_before2023
;

select count(*) from tmp_1y.kn_mbm_episode_agg6_sum1_before2023
-- 176,560

select count(*) from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202601
-- 176,560


drop table if exists tmp_1q.kn_mbm_202601;
create table tmp_1q.kn_mbm_202601 as
select
	*
from tmp_1q.kn_mbm_episode_agg6_sum1_after2023_202601
union all
select 
	*
from tmp_1y.kn_mbm_episode_agg6_sum1_before2023_202601;

select count(*) from tmp_1q.kn_mbm_202601; -- 275176 271316 266615 266213 257905 253665
;

select ep_start_mo, sum(allowed_amt), sum(visit_cnt), sum(ep_cnt)
from tmp_1q.kn_mbm_202601
where ep_start_mo >= '202401'
group by 1 
order by 1
;


select visit_mo, sum(allowed_amt), sum(visit_cnt), sum(ep_cnt)
from tmp_1q.kn_mbm_202601
where visit_mo >= '202401'
group by 1 
order by 1
;

select max(admit_dt_act) from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_f 
where 	
       svc_setting ='Inpatient' --Inpatient Services
       and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
       and admit_cat_cd  in ('17 - Medical','30 - Surgical')			
       and fin_brand in ('M&R','C&S')
       and TO_VARCHAR(admit_dt_act, 'MM/dd/yyyy') is not null 
       and TO_VARCHAR(admit_dt_act  ,'yyyy') in ('2026')	;	