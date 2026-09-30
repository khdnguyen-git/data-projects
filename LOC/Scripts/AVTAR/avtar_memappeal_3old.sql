------------------------------------------- JOIN WITH Appeals Processing START --------------------------------------------
--alter table HCE_OPS_STAGE.hce_adr_avtar_like_25_26_ApplJoin rename to tmp_1y.hce_adr_avtar_like_25_26_ApplJoin;

--,case when initialfulladr_cases=1 and persistentfulladr_cases<>1 and appel_outcome='Overturned' 
--and member_appeal_ind=y
--and MCR_Ovtrn_ind=0 and Appeal_ovrtn_Ind=0 then 1 else 0 end as P2P_full_ovtn																																																																																																																																																																		

drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin  as 
select 
	--CASE WHEN b.authcaseid IS NOT NULL AND trim(b.appliability)='Member' AND COALESCE(b.apprcvddt,'9999-12-31') BETWEEN COALESCE(ADMIT_DT_ACT,'9999-12-31') AND COALESCE(dateadd(DAY,3,DSCHG_DT_ACT),To_TIMESTAMP('9999-12-31')) THEN 1 ELSE 0 END AS Member_Appeal_ind
	CASE WHEN b.authcaseid IS NOT NULL AND COALESCE(b.apprcvddt,'9999-12-31') > COALESCE(ADMIT_DT_ACT,'9999-12-31') AND trim(b.appliability)='Member'
	AND apptype='Pre-Service' AND appoutcome IN ('Overturned','Upheld') THEN 1 ELSE 0 END AS Member_Appeal_ind
	,case when (b.authcaseid IS not NULL AND COALESCE(b.apprcvddt,'9999-12-31') <= COALESCE(ADMIT_DT_ACT,'9999-12-31') ) OR
		 (b.authcaseid IS not NULL AND COALESCE(b.apprcvddt,'9999-12-31') > COALESCE(ADMIT_DT_ACT,'9999-12-31')  AND coalesce(b.appliability,'N/A')<>'Member') OR 
		(b.authcaseid IS not NULL AND  COALESCE(b.apprcvddt,'9999-12-31') > COALESCE(ADMIT_DT_ACT,'9999-12-31') AND trim(b.appliability)='Member' AND apptype<>'Pre-Service') OR
		(b.authcaseid IS not NULL AND COALESCE(b.apprcvddt,'9999-12-31') > COALESCE(ADMIT_DT_ACT,'9999-12-31') AND  trim(b.appliability)='Member' AND apptype='Pre-Service' AND appoutcome NOT IN ('Partial OT','Overturned','Upheld')) then 1 else 0 end as Appeal_ind
	,case when b.authcaseid is not null and appoutcome<>'Overturned' and persistentfulladr_cases <>1 and a.initialfulladr_cases=1 then 1 else 0 end as oth_ovrtn_Ind
	,b.authcaseid
	,b.appliability
	,b.apprcvddt
	,b.appdecnmkr_user_id
	,b.appdecnmkr_user_nm
	,b.appdecnmkr_user_role
	,b.appdecndt
	,b.appoutcome
	,b.authlob
	,b.appmcrprevreviewfmd
	,b.appissuetype
	,b.apptype
	,a.* 
from 
	HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_a  a
left outer join
	HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag b
on a.case_id= b.authcaseid
;

--SELECT DISTINCT appoutcome FROM HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag
--SELECT DISTINCT appliability FROM HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag
--SELECT 
--	appliability
--	,authlob
--	,CASE WHEN APPRCVDDT IS NULL THEN 1 ELSE 0 END apprcvdt_isNull
--	,count( DISTINCT AUTHCASEID) cse_cnt_dsnt
--	,count( AUTHCASEID) cse_cnt
--FROM 
--	HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag b GROUP BY 
--appliability
--,authlob
--,CASE WHEN APPRCVDDT IS NULL THEN 1 ELSE 0 END 
--;
--

--select 
--       to_char(a.dschg_dt_act  ,'yyyyMM') dischrgdt
--       ,count(distinct case_id) unq_case_cnt
--       ,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
--       ,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--       ,count(DISTINCT( CASE WHEN MEMBER_APPEAL_IND=1 THEN case_id end )) member_org_appeals
--from 
----       pa_operations.HCE_ADR_AVTAR_Like_25_26_F  a
--	HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin a
--where
--       svc_setting ='Inpatient' --Inpatient Services
--       and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--       and admit_cat_cd  in ('17 - Medical','30 - Surgical')
--       and fin_product_level_3<>'INSTITUTIONAL'
--       and tfm_include_flag=1
--       and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--group by 
--       to_char(a.dschg_dt_act   ,'yyyyMM') 
--;

--SELECT 
--	appliability
--	,authlob
--	,Member_Appeal_ind1
--	,Member_Appeal_ind
--	,CASE WHEN APPRCVDDT IS NULL THEN 1 ELSE 0 END apprcvdt_isNull
--	,count( DISTINCT AUTHCASEID) cse_cnt_dsnt
--	,count( AUTHCASEID) cse_cnt
--FROM 
--	HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin b GROUP BY 
--appliability
--,authlob
--,CASE WHEN APPRCVDDT IS NULL THEN 1 ELSE 0 END 
--,Member_Appeal_ind1
--,Member_Appeal_ind
--;


--SELECT * FROM HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag 

--SELECT count(DISTINCT case_id) FROM  HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin WHERE appeal_ind=1--1093387
--SELECT count(DISTINCT case_id) FROM  HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin WHERE appeal_ind=1--900441
--SELECT   count(DISTINCT case_id) FROM  HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin WHERE member_appeal_ind=1--192946



------------------------------------------- JOIN WITH Appeals Processing END --------------------------------------------

-------------------------------------------- JOIN WITH REVERSALS Processing START --------------------------------------------
--alter table HCE_OPS_STAGE.avatar_appeal_rvsls_info_1 rename to tmp_1y.avatar_appeal_rvsls_info_1;
drop table if exists HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_1;
create table HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_1 as 
select * from HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin where oth_ovrtn_Ind=1;

--alter table HCE_OPS_STAGE.avatar_appeal_rvsls_info_2 rename to tmp_1y.avatar_appeal_rvsls_info_2;
drop table if exists  HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_2;
create table HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_2 as
select 
	b.*
	,a.case_id
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_1 as a
inner join 
	HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_BED_DECN_1 as b
on 
	a.case_id= substring(b.case_key,5)
;

--alter table HCE_OPS_STAGE.avatar_appeal_rvsls_dnd_only  rename to tmp_1y.avatar_appeal_rvsls_dnd_only ;
drop table if exists  HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_dnd_only ;
create table HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_dnd_only as
select 
	a.*
	,dense_rank() over (partition by a.case_key, a.bed_decn_from_dttm, a.bed_decn_end_dttm  order by a.bed_decn_dttm asc,bed_decn_seq_id asc) initial_rnk
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_2   a
where bed_decn_otcm_cd=2
;

--alter table HCE_OPS_STAGE.avatar_appeal_rvsls_app_only rename to tmp_1y.avatar_appeal_rvsls_app_only;
drop table if exists HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_app_only;
create table HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_app_only as
select 
	a.*
	,dense_rank() over (partition by a.case_key, a.bed_decn_from_dttm, a.bed_decn_end_dttm  order by a.bed_decn_dttm asc,bed_decn_seq_id asc) initial_rnk
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_2   a
where bed_decn_otcm_cd=1
;

--alter table HCE_OPS_STAGE.reverse_bed_all rename to tmp_1y.reverse_bed_all;
drop table if exists HCE_OPS_STAGE.HCEOPS_reverse_bed_all ;
create table HCE_OPS_STAGE.HCEOPS_reverse_bed_all as
select 
	distinct
	row_number() over ( partition by b.case_key order by b.bed_decn_from_dttm asc,b.BED_DECN_DTTM asc) as rnk
	,a.case_id as dnl_case_id
	,a.bed_decn_dttm as dnl_decn_dttm
	,b.case_key
	,b.cancelled_ind
	,b.asmt_rvw_id
	,b.bed_decn_crmk_cd
	,b.bed_decn_dttm
	,b.bed_decn_end_dttm rvsl_bed_decn_end_dttm
	,b.bed_decn_from_dttm rvsl_bed_decn_from_dttm
	,b.bed_decn_input_userid
	,b.bed_decn_otcm_cd
	,b.bed_decn_otcm_crmk_cd
	,b.bed_decn_rsn_cd
	,b.bed_decn_seq_id
	,b.bed_decn_typ_cd
	,b.bed_decn_user_comploc_cd
	,b.bed_decn_user_dept_cd
	,b.bed_decn_user_pstn_cd
	,b.bed_decn_userid rvsl_bed_decn_userid
	,b.bed_input_dttm
	,b.bed_typ_cd
	,b.svc_seq_id
	,b.row_eff_dt
	,b.accum_bed_day_cnt
--	,b.ovrd_rsn_txt
	,b.ovrd_rsn_typ_cd
	,b.rvnu_cd
	,b.case_id
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_dnd_only a
inner join 
 	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_app_only b
 on
 	a.case_id=b.case_id 
 	and a.bed_decn_from_dttm=b.bed_decn_from_dttm 
 	and a.bed_decn_dttm<=b.bed_decn_dttm
 ;


--alter table HCE_OPS_STAGE.reverse_bed_all_fnl rename to tmp_1y.reverse_bed_all_fnl;
drop table if exists HCE_OPS_STAGE.HCEOPS_reverse_bed_all_fnl;
create table  HCE_OPS_STAGE.HCEOPS_reverse_bed_all_fnl as
select * from  HCE_OPS_STAGE.HCEOPS_reverse_bed_all WHERE rnk=1;

-- SERV DECN processing
--alter table HCE_OPS_STAGE.avatar_appeal_rvsls_srvdecn_info_2 rename to tmp_1y.avatar_appeal_rvsls_srvdecn_info_2;
drop table if exists  HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_info_2;
create table HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_info_2 as
select 
	b.*
	,a.case_id
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_1 as a
inner join 
	HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_1 as b
on 
	a.case_id= substring(b.case_key,5)
;



--not found
--alter table HCE_OPS_STAGE.avatar_appeal_rvsls_srvdecn_dnd_only rename to tmp_1y.avatar_appeal_rvsls_srvdecn_dnd_only;
drop table if exists  HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_dnd_only;
create table HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_dnd_only as
select 
	a.*
	,dense_rank() over (partition by a.case_key order by decn_seq_nbr asc,a.decn_dttm asc) initial_rnk
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_info_2   a
where decn_otcm_cd=2
;

--alter table  HCE_OPS_STAGE.avatar_appeal_rvsls_srvdecn_app_only rename to  tmp_1y.avatar_appeal_rvsls_srvdecn_app_only ;
drop table if exists  HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_app_only ;
create table HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_app_only as
select 
	a.*
	,dense_rank() over (partition by a.case_key order by decn_seq_nbr asc,a.decn_dttm asc) initial_rnk
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_info_2   a
where decn_otcm_cd=1
;


--alter table  HCE_OPS_STAGE.reverse_srvdecn_all rename to  tmp_1y.reverse_srvdecn_all;
drop table if exists  HCE_OPS_STAGE.HCEOPS_reverse_srvdecn_all ;
create table HCE_OPS_STAGE.HCEOPS_reverse_srvdecn_all as
select 
	distinct
	row_number() over ( partition by b.case_key order by a.decn_dttm asc,b.DECN_DTTM asc) as rnk
	,a.case_id as dnl_case_id
	,a.decn_dttm as dnl_decn_dttm
	,b.case_key
	,b.decn_dttm rvsl_srv_decn_dttm
	,b.decn_userid rvsl_srv_decn_userid
--	,b.bed_decn_input_userid
	,b.case_id
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_dnd_only a
inner join 
 	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_srvdecn_app_only b
 on
 	a.case_id=b.case_id 
 	and a.decn_dttm<=b.decn_dttm
 ;


--alter table HCE_OPS_STAGE.reverse_srvdecn_all_fnl rename to tmp_1y.reverse_srvdecn_all_fnl;
drop table if exists HCE_OPS_STAGE.HCEOPS_reverse_srvdecn_all_fnl;
create table  HCE_OPS_STAGE.HCEOPS_reverse_srvdecn_all_fnl as
select * from  HCE_OPS_STAGE.HCEOPS_reverse_srvdecn_all WHERE rnk=1;


--alter table HCE_OPS_STAGE.reverse_bed_srv_decn_all_fnl rename to tmp_1y.reverse_bed_srv_decn_all_fnl;
--combine Bed & Serv. Take Bed decision fist ifnot pull from srv
drop table if exists HCE_OPS_STAGE.HCEOPS_reverse_bed_srv_decn_all_fnl;
create table HCE_OPS_STAGE.HCEOPS_reverse_bed_srv_decn_all_fnl as
select distinct
	a.case_id 
	,COALESCE (b.rvsl_bed_decn_userid,c.rvsl_srv_decn_userid)	as rvsl_decn_userid
	,COALESCE (b.bed_decn_dttm,c.rvsl_srv_decn_dttm) rvsl_decn_dttm
	,case when b.case_id  is null then 0 else 1 end bed_decn_mtch_ind
	,case when c.case_id  is null then 0 else 1 end srv_decn_mtch_ind
from 
	HCE_OPS_STAGE.HCEOPS_avatar_appeal_rvsls_info_1 a
left outer join
	 HCE_OPS_STAGE.HCEOPS_reverse_bed_all_fnl b
on 
	a.case_id = b.case_id 
left outer join
 	HCE_OPS_STAGE.HCEOPS_reverse_srvdecn_all_fnl c
on
	a.case_id = c.case_id 
;


--alter table HCE_OPS_STAGE.reverse_bed_srv_decn_role_fnl rename to tmp_1y.reverse_bed_srv_decn_role_fnl;
drop table if exists HCE_OPS_STAGE.HCEOPS_reverse_bed_srv_decn_role_fnl ;
create table HCE_OPS_STAGE.HCEOPS_reverse_bed_srv_decn_role_fnl as
select 
	a.*
	,COALESCE (b.user_role,c.label)	as rvsl_decn_user_role
from 
	HCE_OPS_STAGE.HCEOPS_reverse_bed_srv_decn_all_fnl a
left outer join 
	 HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted b
on	
	a.rvsl_decn_userid =b.user_id --and Initial_dnl_decn_userid is not null
	and a.rvsl_decn_dttm between b.role_from_dt and b.role_end_dt 
left outer join
	HCE_OPS_STAGE.adr_xref_fmts_v   c
on 
	a.rvsl_decn_userid = c."START" 
	and c.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
;

--Joining Reverals and appeals into one dataset
--alter table HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_rvsls rename to tmp_1y.hce_adr_avtar_like_25_26_Appl_rvsls;
drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_rvsls;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_rvsls as 
select 
	 CASE WHEN B.CASE_ID IS Null then 0 else 1 end as rvsl_ind
	,b.rvsl_decn_userid
	,b.rvsl_decn_dttm
	,b.rvsl_decn_user_role
	,case when coalesce(a.appdecnmkr_user_role,b.rvsl_decn_user_role) like '%MCR%' then 1 else 0 end as MCR_rvsls
	,b.bed_decn_mtch_ind rvsl_bed_decn_mtch_ind
	,b.srv_decn_mtch_ind rvsl_srv_decn_mtch_ind
	,a.*
from 
 	HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_ApplJoin a
left outer join
	HCE_OPS_STAGE.HCEOPS_reverse_bed_srv_decn_role_fnl b
on a.case_id =B.CASE_ID
;

-------------------------------------------- JOIN WITH REVERSALS Processing END --------------------------------------------

-------------------------------------------- JOIN WITH MCR Overturns Processing START --------------------------------------------
--alter table HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCRJoin rename to tmp_1y.hce_adr_avtar_like_25_26_Appl_MCRJoin;
drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCRJoin ;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCRJoin as
select 
	case when   ( COALESCE (b.bed_latest_mcr_ind,0) >0 OR  COALESCE (c.serv_latest_mcr_ind,0) > 0  OR  MCR_rvsls>0 ) then 1 else 0 end MCR_Reconsideration_ind
	,case when initialfulladr_cases=1 and COALESCE (b.bed_latest_mcr_ind,0)>0 OR COALESCE(c.serv_latest_mcr_ind,0)>0 OR cOALESCE (b.bed_initial_mcr_ind,0)>0 OR COALESCE (c.serv_initial_mcr_ind,0) >0 then 1 else 0 end as MCR_evertouched_DECN_Ind
	,case when  initialfulladr_cases=1 and COALESCE (persistentfulladr_cases,0)<1 and (COALESCE (b.bed_latest_mcr_ind,0) >0 OR  COALESCE (c.serv_latest_mcr_ind,0) >0 OR  MCR_rvsls>0)   then 1 else 0 end MCR_Ovtrn_ind
	,case when  initialfulladr_cases=1 and COALESCE (persistentfulladr_cases,0)<1 and (COALESCE (b.bed_latest_mcr_ind,0) >0 OR  COALESCE (c.serv_latest_mcr_ind,0) >0 OR  MCR_rvsls=0)   then 1 else 0 end MCR_Uphelp_ind
	,a.*
from 
	 HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_rvsls a
left outer join 
	HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_touch_2 b
on
	a.case_id = substring(b.case_key,5)
left outer join 
	HCE_OPS_STAGE.HCEOPS_sd_ip_notif_serv_mcr_touch_2 c 
on
	a.case_id = substring(c.case_key,5)	
;


drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCRJoin_1 ;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCRJoin_1 AS 
SELECT a.*
	,case when appoutcome='Overturned' and persistentfulladr_cases <>1 and a.initialfulladr_cases=1 AND appeal_ind=1 then 1 else 0 end as Appeal_ovrtn_Ind
	,case when initialfulladr_cases=1 and persistentfulladr_cases<>1 and appoutcome='Overturned' and member_appeal_ind=1 and MCR_Ovtrn_ind=0  then 1 else 0 end as member_appeal_ovtn_ind
FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCRJoin a ;


--SELECT DISTINCT appoutcome FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCRJoin 
--select initialfulladr_cases,persistentfulladr_cases,* from HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin a where case_id =235099378;
--select * from HCE_OPS_STAGE.sd_ip_notif_bedday_mcr_touch_2 b where b.case_key ='HSR-235099378'
--select * from HCE_OPS_STAGE.sd_ip_notif_serv_mcr_touch_2 b where b.case_key='HSR-235099378'
--
--select initialfulladr_cases,persistentfulladr_cases,* from HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin a where case_id =235476177; -- Not flagged as MCR in Ryans
--select * from HCE_OPS_STAGE.sd_ip_notif_bedday_mcr_touch_2 b where b.case_key='HSR-235476177';
--select * from HCE_OPS_STAGE.sd_ip_notif_serv_mcr_touch_2 b where b.case_key='HSR-235476177';
--
--
--select initialfulladr_cases,persistentfulladr_cases,* from HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin a where case_id =235585149; -- Why this is not marked as Appeal in Ryans
--
--desc formatted  HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCRJoin	;--57854586
-------------------------------------------- JOIN WITH MCR Overturns Processing END --------------------------------------------
      
-------------------------------------------- JOIN WITH P2P Overturns Processing START --------------------------------------------                        
--alter table HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin rename to tmp_1y.hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin;
drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin  as 
select 
	case when initialfulladr_cases=1 and p2p='Y' then 1 else 0 end as P2P_full_evertouched_cnt
	,case when initialfulladr_cases=1 and persistentfulladr_cases<>1 and p2p='Y' and MCR_Ovtrn_ind=0 and Appeal_ovrtn_Ind=0 AND member_appeal_ovtn_ind=0 then 1 else 0 end as P2P_full_ovtn
	,COALESCE(b.p2p,'N') AS P2P_Match_ind
	
	,a.*
from 
	HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCRJoin_1 a
left outer join 
	HCE_OPS_FNL.MONTHLY_P2P_FLAG_MULTIYR b
on a.case_id=b.caseid
;



--SELECT count(DISTINCT case_id) FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin WHERE member_appeal_ovtn_ind=1--82243
--SELECT count(DISTINCT case_id) FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin WHERE appeal_ovrtn_ind=1--150298
--SELECT count(DISTINCT case_id) FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin WHERE appeal_ovrtn_ind=1 AND member_appeal_ovtn_ind=1--0
--SELECT count(DISTINCT case_id) FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin WHERE  member_appeal_ovtn_ind=1 AND P2P_full_ovtn=1--91
--SELECT count(DISTINCT case_id) FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin WHERE  appeal_ovrtn_ind=1 AND P2P_full_ovtn=1--0

-------------------------------------------- JOIN WITH P2P Overturns Processing END --------------------------------------------

-------------------------------------------- QA START ----------------------------------------------------------

--check  no rows missed  and no duplicates added through the process

--desc formatted HCE_OPS_STAGE.hce_adr_avtar_like_25_26_f_a ;--34359765                                    
--desc formatted HCE_OPS_STAGE.hce_adr_avtar_like_25_26_ApplJoin;--34359765                            
--desc formatted HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCRJoin;--34359765                                    
--desc formatted HCE_OPS_STAGE.hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin;--34359765            


       