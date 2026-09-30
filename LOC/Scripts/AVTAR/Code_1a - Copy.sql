--select case_key,* from HCE_OPS_STAGE.HCEOPS_HSR_ADR_TRANS_SERV_DECN_V  where decn_seq_nbr>1 and svc_seq_nbr>0 and cancelled_ind ='N'


--SERV Processing
drop table if exists  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_curr_1;
create table HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_curr_1 as 
select 
	a.*
from 	HCE_OPS_STAGE.HSR_ADR_TRANS_SERV_DECN_V  a
where cancelled_ind ='N' 
--and svc_seq_nbr=0 
;


--select * from HCE_OPS_STAGE.HCEOPS_HSR_ADR_TRANS_SERV_DECN_V  where case_key='HSR-253757465';


--Combine current(2022,2023)& 2021
drop table if exists  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_1 ;
create table HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_1 as
select * from  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_curr_1 where decn_otcm_cd in (1,2);
--union all 
--select * from HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_2021_1 where decn_otcm_cd in (1,2) ;


drop table if exists  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_2;
create table HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_2 as
select
	dense_rank() over (partition by a.case_key,svc_seq_nbr  order by a.decn_seq_nbr asc) initial_rnk
	,dense_rank() over (partition by  a.case_key,svc_seq_nbr  order by a.decn_seq_nbr desc) latest_rnk
	,a.*
from 
	HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_1 a;



drop table if exists  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_3;
create table HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_3 as 
select 
	case when initial_rnk=1 and decn_otcm_cd=2 then '2-Denied'  
		when  initial_rnk=1 and decn_otcm_cd=1 then '1-Approved' 
	end as inital_adr_flag
	,case when latest_rnk=1 and decn_otcm_cd=2 then '2-Denied'  
		when  latest_rnk=1 and decn_otcm_cd=1 then '1-Approved' 
	end as latest_adr_flag
	,a.*
from 
 HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_2 a ;



drop table if exists  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_4;
create table HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_4 as
select   
	a.case_key
	,svc_seq_nbr
	,max(a.inital_adr_flag) inital_adr_flag
	,min(case when inital_adr_flag='2-Denied'  then decn_userid end ) initial_dnl_decn_userid
	,max(case when latest_adr_flag='2-Denied'  then decn_userid end) latest_dnl_decn_userid
	,min(case when inital_adr_flag='2-Denied'  then decn_dttm  end ) initial_dnl_decn_dttm
	,max(case when latest_adr_flag='2-Denied'  then decn_dttm end) latest_dnl_decn_dttm
	,min(case when inital_adr_flag='2-Denied'  then 1  end ) initial_dnl_decn_ind
	,max(case when latest_adr_flag='2-Denied'  then 1 end) latest_dnl_decn_ind
	,max(latest_adr_flag)  latest_adr_flag
from 
	HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_3 a
group by case_key,svc_seq_nbr
;

--select case_key ,count(1) from HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_3 group by case_key having count(1)>1

--select * from HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_4 where case_key in ('HSR-192998406', 'HSR-193000841');

--adding roles
drop table if exists  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_5 ;
create table HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_5  as
select 
	a.*
	,COALESCE (b.user_role,c.label)	as Initial_dnl_decn_user_role
	,COALESCE (d.user_role,e.label)	as Latest_dnl_decn_user_role
from 
	HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_4 a
left outer join
	HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted b
on
	a.Initial_dnl_decn_userid =b.user_id and Initial_dnl_decn_userid is not null
	and a.Initial_dnl_decn_dttm between b.role_from_dt and b.role_end_dt 
left outer join
	HCE_OPS_STAGE.adr_xref_fmts_v   c
on 
	a.Initial_dnl_decn_userid = c."START" and Initial_dnl_decn_userid is not null
	and c.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
left outer join
	HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted d
on
	a.latest_dnl_decn_userid =d.user_id
	and a.latest_dnl_decn_dttm between d.role_from_dt and d.role_end_dt 
left outer join
	HCE_OPS_STAGE.adr_xref_fmts_v   e
on 
	a.latest_dnl_decn_userid = e."START"
	and e.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
;

--change
--you can replace below FOR hsit & curr USER lookup , pull tev.derrived_role,tev.derived_role_detail,derived_team,job_title
--SELECT * FROM tmp_7d.TIMELINE_EMP_V tev WHERE emp_msid = case_user_id and a.latest_dnl_decn_dttm between tev.START_dt AND etev.nd_dt
--
----QA :
--	CHECK duplicates
--	CHECK how many missing roles AFTER joinging WITH NEW TABLE-- making sure the jon IS c=working
--	pull MD codes FROM BOTH exiting AND NEW methods TO compare the counts. 
--
--SELECT * FROM tmp_7d.TIMELINE_EMP_V tev WHERE emp_msid IN ('JHARGRO6','LDAVI189','LNASON1') ORDER BY emp_msid,START_dt
--SELECT * FROM HCE_OPS_STAGE.adr_xref_fmts_v  WHERE "START" IN ('JHARGRO6','LDAVI189','LNASON1') AND FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE';
--SELECT * FROM HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted  WHERE user_id IN ('JHARGRO6','LDAVI189','LNASON1') ORDER BY role_from_dt;
----division LIKE '%MD%'
-----END Decision Processing
----END : adding initial & latest denial  user role
----desc formatted  HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_5
----select  SUBSTRING(case_key,5) ,* from HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_3  a where SUBSTRING(case_key,5) in (198416987,193208523,194724142)
----
----select  SUBSTRING(case_key,5) ,* from HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_3  a where SUBSTRING(case_key,5) in (195221424,194557148,198984640,197800985)
--
--
--
----select * from HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_4 where latest_adr_flag ='2-Denied' and latest_decn_userid is null
--
--
--
----select * from tmp_1y.MNRHCE_ACUTE_CASE_By_WEEK a where a.case_id =184001678
--
----Export the final results into excel 
----drop table if exists  tmp_7d.HCE_IP_ADM_Counts_F;
----create table tmp_7d.HCE_IP_ADM_Counts_F as
----select *,
----	date_format(svc_start_dt,'yyyyMM') mon
----	,count(distinct case_id) unq_cases
----	,count(case_id) cases
----	,sum(case when Case_Initial_full_ADR_Flag in ('InitialFullADR','2-Denied') then 1 else 0 end) InitialFullADR_cases
----	,sum(case when Case_Initial_full_ADR_Flag in ('InitialPartialADR','2-Denied') then 1 else 0 end) InitialPartialADR_cases
----	,sum(case when case_persistent_full_ADR_Flag in ('PersistentFullADR','2-Denied') then 1 else 0 end) PersistentFullADR_cases
----from 
----	tmp_7d.HCE_IP_ADM_Counts
----where date_format(svc_start_dt,'yyyyMM')>202012
----group by 
----date_format(svc_start_dt,'yyyyMM')
----;
--
--SELECT * from  tmp_7d.TIMELINE_EMP_V