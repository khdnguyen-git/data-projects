drop table if exists HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_2;
create table HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_2 as
select 
	a.*
from 
	 HCE_OPS_STAGE.APPAUTHXWALK_V a
where 
	authlob <> 'CnS'
union all
select 
	b.*
from 
	HCE_OPS_STAGE.APPAUTHXWALK_V b
where 
	authlob = 'CnS' 
	and AuthPlatform = 'HSR' 	
	and (
	   (auth_decnrow_lastvalid_ind=1) 
		or 
	   AppIssueType in ('1st Level Appeal','2nd Level Appeal','Appeal','Appeal-Reopen','Dispute')
	   )
;
--
--DESC table HCE_OPS_STAGE.APPAUTHXWALK_V
--2026-03-17 00:00:00.000
-- select max(APPDECNDT) FROM HCE_OPS_STAGE.APPAUTHXWALK_V
-- 
--  select loc_appeal,apprcvddt,count(authcaseid)  FROM HCE_OPS_STAGE.ALL_APPEALS_XWALK_V
--  WHERE apprcvddt>='2026-03-01'
--  GROUP BY loc_appeal,apprcvddt
  
--
--SELECT  reviewlevel,count(authcaseid),loc_appeal,appoutcome FROM HCE_OPS_STAGE.ALL_APPEALS_XWALK_V
--GROUP BY reviewlevel,loc_appeal,APPOUTCOME 
--DESC TABLE  HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_2

--SELECT count(DISTINCT AUTHCASEID) FROM HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_2 where LOC_Appeal='Y'
--DESC TABLE HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_2
--SELECT DISTINCT APPOUTCOME  FROM   HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_2 WHERE LOC_APPEAL='Y'

--desc formatted HCE_OPS_STAGE.ALL_APPEALS_XWALK_V_1
drop table if exists HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_3;
create table HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_3 as
select 
	row_number() over (partition by authcaseid order by COALESCE (auth_decnrow_lastvalid_ind,0) desc, appdecndt desc) rnk
	,a.*
FROM 
	HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_2 a
;


drop table if exists HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_4;
create table HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_4 as
select 
	a.*
FROM 
	HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_3 a
where
	rnk=1
;
--QA No duplicates - make sure to check below query resutls zero rows
--select 
--	authcaseid 
--	,count(*)
--from 
--	HCE_OPS_STAGE.ALL_APPEALS_XWALK_V_4
--group by 
--	authcaseid 
--having count(1)>1
--;



drop table if exists HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag;
create table HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag  as
select 
	a.authplatform
	,a.appsyscaseid
	,a.authcaseid
	,a.authlob
	,a.matchauthlink
	,a.matchsource
	,a.appdecnmkruserid appdecnmkr_user_id
	,coalesce(c.label,a.appdecnmkruserid) appdecnmkr_user_nm
	,case when a.appdecnrole in ('UNASSIGNED OWNER','New','NEW','NA','N/A','','UNKNOWN')  then coalesce(b.user_role,d.label) else appdecnrole end appdecnmkr_user_role
	,a.appdecndt
	,a.auth_decnrow_lastvalid_ind
	,a.appplatform
	,case when a.appoutcome in ('Partial OT','Overturn') then 'Overturned'
		 when a.appoutcome='Upheld' then 'Upheld'
	 	else 'Other'
	 end as appoutcome
	,a.appstarsoverturnrsn
	,a.apprcvddt
	,a.appliability
	,a.appcloseddt
	,a.appoutcomersn
	,a.appreviewtype
	,a.appmcrprevreviewfmd
	,a.applob
	,a.appissuetype
	,a.apptype
	,a.appcaseid
	,a.loc_appeal
from 
	HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_V_4 a 
left outer join
	HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted b
	
on 
	a.appdecnmkruserid  =b.user_id
	and a.appdecndt between b.role_from_dt and b.role_end_dt 
left outer join
	HCE_OPS_STAGE.adr_xref_fmts_v   d
	
	
on 
	a.appdecnmkruserid = d."START"
	and d.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
left outer join
	HCE_OPS_STAGE.adr_xref_fmts_v   c
on 
	a.appdecnmkruserid = c."START"
	and c.FMTNAME ='$MIDM_ADR_USER_NAME'
;
--appdecnmkruserid
--appdecndt
--select * from  HCE_OPS_STAGE.ALL_APPEALS_XWALK_USRROLE_Tag  where authcaseid =233622141

--select count(1) from  HCE_OPS_STAGE.ALL_APPEALS_XWALK_V_1  where appoutcome ='Dismissal'; --103758 of 3,419,148
select distinct
	apptype
	, appoutcome
from HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag