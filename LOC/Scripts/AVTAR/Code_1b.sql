--START SD -- Rough Work
--FMTNAME IN ('$MIDM_ADR_USER_HIST_ROLE','$MIDM_ADR_USER_CURRENT_ROLE')
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_NAME' and "START" in ('KJADAUN')
--
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_MS_ID' and "START" in ('KJADAUN')
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_HIST_ROLE' and "START" like ('%KJADAUN%')
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_HIST_ROLE' and "START" like ('%S1066%')
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_HIST_ROLE' and "START" like ('%UHC_CLIENT_PROD%')
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_HIST_ROLE' and "START" like ('%SYSTEM_HIPAA_278N%')
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_HIST_ROLE' and "START" like ('%SYSTEM_OCM%')
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_HIST_ROLE' and "START" like ('%KMOBRY%');
--select * from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE' and "START" like ('%KMOBRY%');

--select substring("START",1,instr("START",'_')-1) user_id,count(1) from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE' group by  substring("START",1,instr("START",'_')-1)  having count(1)>1; -- very few dups found and these can be ignored for now
--select substring("START",1,instr("START",'_')-1) as user_id,count(1) from HCE_OPS_STAGE.adr_xref_fmts_v   WHERE FMTNAME ='$MIDM_ADR_USER_HIST_ROLE' group by substring("START",1,instr("START",'_')-1)  having count(1)>1; -- have user history timeline view hence need to formated data

--END SD -- Rough Work
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
--Format User time line history
--ONE TIME LOAD
--drop table if exists HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted;
--create table HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted  as
--select 
--	fmtname 
--	,"START" as start_value
--	,substring("START",1,instr("START",'_')-1) as user_id
--	,to_date(from_unixtime(unix_timestamp(substring("START",instr("START",'_')+1),'yyyyMMdd'))) as role_from_dt
--	,to_date(from_unixtime(unix_timestamp(substring(`end` ,instr(`end`,'_')+1),'yyyyMMdd'))) as role_end_dt
--	,label as user_role
--from HCE_OPS_STAGE.adr_xref_fmts_v   
--where FMTNAME ='$MIDM_ADR_USER_HIST_ROLE'
--; --  63707	
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------	
--select * from   HCE_OPS_STAGE.adr_xref_fmts_v  where FMTNAME ='$MIDM_ADR_USER_HIST_ROLE'; --63707
--select * from HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted where user_role like '%ICM%'
--ICM MEDICAL DIRECTOR
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
--Join to USER HIST Roles
drop table if exists HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_HIST_userrole ;
create table HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_HIST_userrole  as
select 
	a.case_key  
	,a.asgn_from_userid 
	,b.user_role as from_user_role
	,b.role_from_dt as from_user_eff_dt
	,b.role_end_dt as from_user_exp_dt
	,a.asgn_to_userid 	
	,c.user_role as end_user_role
	,c.role_from_dt as end_user_eff_dt
	,c.role_end_dt as end_user_exp_dt
	,a.asgn_typ_cd 
	,a.asgn_init_creat_dttm 
	,a.asgn_from_dttm 
	,a.asgn_thru_dttm 
	,a.asgn_desc_typ_cd 
	,a.asgn_otcome_typ_cd 
	,a.asgn_rsn_typ_cd 
	,a.asgn_prr_typ_cd 
	,a.asgn_othr_desc_txt 
from HCE_OPS_STAGE.hsr_adr_asgn_v  a 
left outer join
HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted b
on a.asgn_from_userid =b.user_id
and a.asgn_from_dttm between b.role_from_dt and b.role_end_dt 
left outer join
HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted c
on a.asgn_to_userid =c.user_id
and a.asgn_thru_dttm between c.role_from_dt and c.role_end_dt 
WHERE cancelled_ind ='N';

--DESC TABLE HCE_OPS_STAGE.hsr_adr_asgn_v 
--SELECT * FROM HCE_OPS_STAGE.HCEOPS_HSR_ADR_ASGN_TAG_HIST_USERROLE WHERE case_key LIKE '%280855699%'
--desc formatted  HCE_OPS_STAGE.hsr_adr_asgn_v
--select * from HCE_OPS_STAGE.hsr_adr_asgn_v WHERE case_key='HSR-220248734' ;
--
--select from_user_role, end_user_role,count(distinct case_key) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles  group by from_user_role, end_user_role ;
--
--select * from HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted where user_id='KJADAUN';
--
--select * from  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles  where end_user_role like '%DIRECTOR%'

--select * from  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles  where case_key in ('HSR-237191678','HSR-235797315','HSR-237087030','HSR-236681010','HSR-236657313','HSR-236023150');

--select * from HCE_OPS_STAGE.adr_xref_fmts_v where FMTNAME like '%ASGN%PRR%'
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------

drop table if exists HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_Curr_userroles ;
create table HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_Curr_userroles  as
select 
	a.case_key  
	,a.asgn_from_userid 
	,d.label as curr_from_user_role
	,a.from_user_role
	,a.from_user_eff_dt
	,a.from_user_exp_dt
	,a.asgn_to_userid 	
	,e.label as curr_TO_user_role
	,a.end_user_role 
	,a.end_user_eff_dt
	,a.end_user_exp_dt
	,a.asgn_typ_cd 
	,a.asgn_init_creat_dttm 
	,a.asgn_from_dttm 
	,a.asgn_thru_dttm 
	,a.asgn_desc_typ_cd 
	,a.asgn_otcome_typ_cd 
	,a.asgn_rsn_typ_cd 
	,a.asgn_prr_typ_cd 
	,a.asgn_othr_desc_txt 
from HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_HIST_userrole a 
left outer join
 HCE_OPS_STAGE.adr_xref_fmts_v   d
on a.asgn_from_userid = d."START"
and d.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
left outer join
 HCE_OPS_STAGE.adr_xref_fmts_v   e
on a.asgn_to_userid = e."START"
and e.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
;


SELECT * FROM HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_Curr_userroles 
WHERE case_key  in ('HSR-237191678','HSR-235797315','HSR-237087030','HSR-236681010','HSR-236657313','HSR-236023150');
--create a new table using above with all fields and create 2 new drived fields***
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
drop table if exists HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_Merge ;
create table HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_Merge  as
SELECT
a.*
,COALESCE (from_user_role,curr_from_user_role) as from_user_role_fnl
,COALESCE (end_user_role,curr_to_user_role) as to_user_role_fnl

from HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_Curr_userroles a
;
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------	
--***add an indicator  MD Escalations =1 when to_user_role_fnl like '%DIRECTOR%'  - case when*** 

drop table if exists  HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD ;
create table HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD  as
SELECT
a.*
,CASE WHEN to_user_role_fnl like '%DIRECTOR%' THEN 1 ELSE 0 END as MD_Escalation_IND
,CASE WHEN to_user_role_fnl = 'ICM MEDICAL DIRECTOR' THEN 1 ELSE 0 END as ICM_MD_REVIEWED_IND
FROM HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_Merge a
;

--select count(distinct case_key) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD where MD_Escalation_IND = 1 --2,762,773
--select count(distinct case_key) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD where ICM_MD_REVIEWED_IND = 1 --1,487,632
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
--***Filter cases with MD Esclation=1.***
drop table if exists  HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD_2 ;
create table HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD_2  as
SELECT DISTINCT
--a.*
REPLACE(case_key,'HSR-','') as case_key
--,from_user_role 
--,from_user_eff_dt
--,from_user_exp_dt 
--,to_user_role_fnl as end_user_role_fnl
--,end_user_eff_dt
--,end_user_exp_dt
,MD_Escalation_IND
,ICM_MD_REVIEWED_IND
FROM HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD
WHERE MD_Escalation_IND = 1
;

--select count(distinct case_key) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_2 where MD_Escalation_IND = 1 --2820462
--select count(distinct case_key) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_2 where ICM_MD_REVIEWED_IND = 1 --1519355
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------

----IGNORED FOR TESTING WITH WEEKLY
----***Roll data to 1 line per Case and count number of MD escaltions per CASE***--
--drop table if exists  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_3 ;
--create table HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_3  as
--SELECT 
--REPLACE(case_key,'HSR-','') as case_key
--,COUNT(case_key) as MD_Escalation_Instances
--
--FROM HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_2 a
--GROUP BY case_key
--
--;

----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
drop table if exists  HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD_NOROLLUP ;
create table HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD_NOROLLUP  as
select 
a.*
,ROW_NUMBER () OVER(partition by case_key ORDER BY (ICM_MD_REVIEWED_IND) desc) as ICM_RowNumber
from HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD_2 a
;


--DESC formatted  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_NOROLLUP 
--select count(1) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_NOROLLUP --2,926,114
--select count(1) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_NOROLLUP where ICM_RowNumber = 1 --2,820,462
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
--drop table if exists  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR ;
--create table HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR  as
--select 
--a.* 
----,b.from_user_role 
----,b.from_user_eff_dt
----,b.from_user_exp_dt 
----,b.end_user_eff_dt
----,b.end_user_exp_dt
----,b.end_user_role_fnl
--,b.MD_Escalation_IND
--,b.ICM_MD_REVIEWED_IND
----,b.MD_Escalation_Instances --readd for monthly
--,CASE WHEN b.case_key IS NOT NULL THEN 1 ELSE 0 END as MD_Join_Match_IND
--
--
--from HCE_OPS_STAGE.hce_adr_avtar_like_22_23_f_a a
--LEFT JOIN HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_NOROLLUP b on b.case_key = a.case_id AND ICM_RowNumber = 1
----LEFT JOIN HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_3 b on b.case_key = a.case_id --readd for monthly compare
--
--;
--desc formatted HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR --48,984,171 
--select count(1) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR where MD_Escalation_IND = 1  --5,813,997  
--select count(1) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR where MD_Join_Match_IND = 1  --5,813,997 
--select count(1) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR where ICM_MD_REVIEWED_IND = 1 --1,559,903
--select count(distinct case_id)  from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR where MD_Escalation_IND = 1 --2,820,450
--select count(distinct case_id)  from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR where ICM_MD_REVIEWED_IND = 1 --1,519,353
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------


--desc  HCE_OPS_STAGE.hce_adr_avtar_like_22_23_1 --48,984,171            
--desc HCE_OPS_STAGE.hce_adr_avtar_like_22_23_f --52,134,990   
--select count(distinct case_key) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_NOROLLUP --2,820,462
--select count(distinct case_key,MD_Escalation_IND) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_NOROLLUP --2,820,462
--select count(distinct case_key,ICM_MD_REVIEWED_IND) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_NOROLLUP --2,926,114
--select count(distinct case_key,ICM_MD_REVIEWED_IND) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_2 --2,926,114
--
--
--,ROW_NUMBER () OVER(partition by case_key ORDER BY (ICM_MD_REVIEWED_IND) desc) as order_test
--case_key	md_escalation_ind	icm_md_reviewed_ind	  ICM_RowNumber
--192997468	1	                   1	                1
--192997468	1	                   0	                2

----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
 --row_num_example:--,ROW_NUMBER () OVER(partition by medicare_id,episode_id ORDER BY (claim_start_date,claim_end_date,OP_CLAIM_SRVC_DT) asc ) as DATE_ORDER_2
--select * from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_MD_3 where case_key = 'HSR-192997277'
--HSR-192997277 = 2 instances

--***Join  with current AVATR Final table (keep AVTAR as left table meaning need al lrow from AVATR and marched ones from MD escalations) by --Case_ID or CASE_KEY and pull number of #MD escalations filed








--select * from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
--QA
--select 
--COALESCE(CONCAT(YEAR(dschg_dt_act),MONTH(dschg_dt_act)))
--,count(distinct case_id)
--from HCE_OPS_STAGE.hce_adr_avtar_like_22_23_f
--from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR2
--WHERE COALESCE(YEAR(dschg_dt_act)) IN ('2022','2023')
--and fin_brand = 'M&R'
--and plc_of_svc_cd ='21 - Acute Hospital' 
--AND admit_cat_cd  in ('17 - Medical','30 - Surgical')
--AND MD_Join_Match_IND = 1
--group by COALESCE(CONCAT(YEAR(dschg_dt_act),MONTH(dschg_dt_act)))
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------


-- --ONLY NEEDED FOR QA
--drop table if exists  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR2 ;
--create table HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR2  as
--
--select * 
--
--from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR
----where business_segment ='MnR'
--where 
--fin_brand = 'M&R'
--and plc_of_svc_cd ='21 - Acute Hospital' 
--AND admit_cat_cd  in ('17 - Medical','30 - Surgical')
----fin_source_name ='COSMOS'
----and 	fin_product_level_3 <>'INSTITUTIONAL' 
----and   global_cap ='NA'  --remove for now
----and   tfm_include_flag =1
--;
--
----select * from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR2

----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.priority_rev_weekly_md_updated;
--CREATE TABLE tmp_1m.priority_rev_weekly_md_updated  as
--SELECT DISTINCT
--caseid
--,sys_flag
--,srn
--,segment
--,admitdt
--,CAST(discharge_date as date) as discharge_date
--,enddt
--,ooa_oon
--,patient_state
--,case_status
--,treatmt_setting
--,casetype
--,tpl
--,ipmnr
--,hosp_state
--,days_total
--,hplan
--,exchange_ind
--,ocm_managed_case
--,company_code
--,plan
--,entity
--,fund_arrng
--,business
--,mbr_med_nec
--,post_disch_notif
--,adv_notif_flag
--,policy_issue_state
--,drg_flag
--,med_necessity_typ_id
--,ini_adv_det_rsn_grp
--,ini_full_adv_det_flag
--,ini_adv_det_cnt
--,ini_full_adv_det_cnt
--,ini_adv_det_cnt_clinc
--,ini_full_adv_det_cnt_clinc
--,icm_md_reviewed
--,loc_recon
--,icm_nurse_reviews
--,actual_loc_reviews
--,actual_los_reviews
--,actual_dcp_reviews
--,icm_ever_owned
--,service_description
--,minor_mkt_name
--,hosp_system
--,los_review_start_day
--,tmr_stoploss_flag
--,med_nec_elig_final
--,hosp_name_uid
--,bcrt_business
--,new_facl_mednec_status_dtl
--,mednec_restrict_loc
--,mednec_restrict_los
--,cah_flag
--,expected_loc_reviews
--,expected_los_reviews
--,pr_model_exception
--,exception_rule
--,expected_dcp_reviews
--,actual_crc_activities
--,expected_crc_activities
--,actual_crc_flag
--,expected_crc_flag
--,icm_access
--,icm_onsite_only
--,msa_name
--,icm_managed
--,fst_icm_asgn_dttm
--,fst_asmt_dttm
--,fst_icm_rvw_dttm
--,presvc_rvw
--,loc_full
--,loc_macro
--,los_full
--,los_macro
--,icm_md_reviews
--,discharge_planning_bfdisch
--,asgn_to_initrvw_wkdy_tat
--,dischgplanning_wkdy_tat
--,md_name
--,first_icm_rnrvw_dttm
--,first_loc_dttm
--,first_los_dttm
--,first_md_dttm
--,icm_n2p_case_cnt
--,n2p_case_cnt
--,icm_p2p_case_cnt
--,first_icm_msid
--,first_icm_name
--,first_icm_mgr_msid
--,first_icm_mgr_name
--,last_icm_msid
--,last_icm_name
--,last_icm_mgr_msid
--,last_icm_mgr_name
--,covid19_flag
--,late_notification
--,pod_outlier_facility
--,disch_mth
--,case_count
--,prior_auth_case
--,prior_auth_required
--,team_e
--,owner_name
--,owner_role
--,owner_manager
--,loc_required_flag
--,los_required_flag
--,dcp_required_flag
--,actual_loc_reviewed
--,actualreq_loc_reviews
--,actualreq_los_reviews
--,actualreq_dcp_reviews
--,required_actual_loc_reviewed
--,required_actual_los_reviewed
--,required_actual_loc_reviewed_cnt
--,required_actual_los_reviewed_cnt
--,mcg_loc_flag
--,primary_dx_all
--,nonmednec_or_drg_flag
--,region
--,length_of_stay_category
--,group_flag
--,weekend_hol_admit
--,weekend_hol_dis
--,weekend_hol_notification
--,weekday_afterhr_notif
--,md_escalation_count
--,week_admit_flag
--,week_flag
--,um_suppression
--,flu_flag
--,icm_rollup_role_combined
--,admit_notif_dt
--,drg_or_non_mednec
--,oon_and_mbr_mednec
--,mednec_and_per_diem
--,cat_day14
--,dsnp_tmr
--,icm_nurse_reviewed
--,cmo_prov_mkt
--,cmo_mbr_mkt
--,oah_flag
--
--FROM tmp_1m.priority_rev_weekly_md
--;
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
--*******WEEKLY************************
--select * from tmp_1m.priority_rev_weekly_md
--select * from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR2
--select count(1) from tmp_1m.priority_rev_weekly_md --126,219
--md_escalation_count
--md_escalation_instances

--drop table if exists  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY ;
--create table HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY  as
--select 
--a.* 
----,b.md_escalation_instances
--,b.to_user_role_fnl
--,b.MD_Escalation_IND
--,b.ICM_MD_REVIEWED_IND
--,DATE_FORMAT(admit_dt_act,'YYYY-MM-dd') as admit_dt_actual 
--,CASE WHEN b.case_id is not null then 1 else 0 end as Match_Weekly
--,CASE WHEN b.case_id is not null and coalesce(a.discharge_date,'') = coalesce(DATE_FORMAT(trim(b.dschg_dt_act),'YYYY-MM-dd'),'')
--THEN 1 ELSE 0 END as discharge_match
--,CASE WHEN b.case_id is not null and a.admitdt = DATE_FORMAT(b.admit_dt_act,'YYYY-MM-dd') THEN 1 ELSE 0 END as admit_match
--
--from tmp_1m.priority_rev_weekly_md_updated a
--LEFT JOIN HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR2 b on b.case_id = a.caseid
--;
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--
--drop table if exists  HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN ;
--create table HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN  as
--select 
--a.* 
----,b.*
----,b.md_escalation_instances
--,b.case_id
--,b.to_user_role_fnl
--,b.MD_Escalation_IND
--,b.ICM_MD_REVIEWED_IND
--,DATE_FORMAT(b.admit_dt_act,'YYYY-MM-dd') as admit_dt_actual
--,DATE_FORMAT(b.dschg_dt_act,'YYYY-MM-dd') as dschg_dt_actual
--,CASE WHEN b.case_id is not null AND a.caseid is not null then 1 else 0 end as Match_Weekly
--,CASE WHEN b.case_id is not null AND a.caseid is null then 1 else 0 end as Avtar_Orphan
--,CASE WHEN a.caseid is not null AND b.case_id is null then 1 else 0 end as priority_rev_Orphan
--
--,CASE WHEN b.case_id is not null and a.caseid is not null
--and coalesce(a.discharge_date,'') = coalesce(DATE_FORMAT(trim(b.dschg_dt_act),'YYYY-MM-dd'),'')
----and a.discharge_date = coalesce(DATE_FORMAT(b.dschg_dt_act,'YYYY-MM-dd'),'')
--THEN 1 ELSE 0 END as discharge_match
--
--,CASE WHEN b.case_id is not null and a.caseid is not null
--and a.admitdt = DATE_FORMAT(b.admit_dt_act,'YYYY-MM-dd') THEN 1 ELSE 0 END as admit_match
--
--from tmp_1m.priority_rev_weekly_md_updated a
--FULL JOIN HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR2 b on trim(b.case_id) = trim(a.caseid)
--;
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--select match_weekly, count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN group by match_weekly
--match_weekly	_c1
--0	1,790
--1	124,429
--select discharge_match, count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN group by discharge_match
--0	18589
--1	107630
--
--select discharge_match, count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY 
--where ICM_MD_REVIEWED_IND = 1 group by discharge_match
--0	6035
--1	34582
--discharge_match	_c1
--0	4,256
--1	36,361
--
--select admit_match, count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN group by admit_match
--0	2089
--1	124130
--
--select count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN
--where week_flag = 'Week_End_08/26/2023' AND Match_Weekly = 1
----25,906
--select count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN
--where week_flag = 'Week_End_08/26/2023' AND priority_rev_Orphan = 1
----110
--select count(distinct case_id) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN
--where Avtar_Orphan = 1 AND dschg_dt_actual BETWEEN '2023-08-20' AND '2023-08-26'
----315
--
--select count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN
--where priority_rev_Orphan = 1
----1790
--
--select match_weekly, count(distinct caseid) from HCE_OPS_STAGE.hsr_adr_asgn_tag_userroles_AVTAR_WEEKLY_FULLJOIN 
--where ICM_MD_REVIEWED_IND = 1 group by match_weekly
--match_weekly	_c1
--0	0
--1	40,617