-------------------------------------------- BEDDAY DATA Processing START --------------------------------------------
drop table if exists HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_HIST_USR_1 ;
create table HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_HIST_USR_1 as
select
distinct 
	case when b.user_role like '%MCR%' or c.user_role like '%MCR%'  then 1 else 0 end as MCR_HISTUSR_flg
	,b.user_role bed_decn_HIST_user_role
	,c.user_role  bed_input_HIST_user_role
	,a.initial_rnk
	,a.latest_rnk
	,a.case_key
	,a.cancelled_ind
	,a.asmt_rvw_id
	,a.bed_decn_crmk_cd
	,a.bed_decn_dttm
	,a.bed_decn_end_dttm
	,a.bed_decn_from_dttm
	,a.bed_decn_input_userid
	,a.bed_decn_otcm_cd
	,a.bed_decn_otcm_crmk_cd
	,a.bed_decn_rsn_cd
	,a.bed_decn_seq_id
	,a.bed_decn_typ_cd
	,a.bed_decn_user_comploc_cd
	,a.bed_decn_user_dept_cd
	,a.bed_decn_user_pstn_cd
	,a.bed_decn_userid
	,a.bed_input_dttm
	,a.bed_typ_cd
	,a.svc_seq_id
	,a.row_eff_dt  
from 
HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_BED_DECN_2 a
--where DATEDIFF(bed_decn_dttm,bed_input_dttm) >0
left outer join
	HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted b
on a.bed_decn_userid  =b.user_id
and a.bed_decn_dttm between b.role_from_dt and b.role_end_dt 
left outer join
	HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted c
on a.bed_decn_input_userid  =c.user_id
and a.bed_input_dttm  between c.role_from_dt and c.role_end_dt 
;

--select * from HCE_OPS_STAGE.adr_xref_fmts_v where fmtname like '%HSR%DECN%RSN%';


drop table if exists  HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_CURR_USR_2 ;
create table HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_CURR_USR_2 as
select
distinct 
	case when d.label like '%MCR%' or e.label like '%MCR%' then 1 else 0 end as MCR_CURRUSR_flg
	,d.label bed_decn_CURR_user_role
	,e.label   bed_input_CURR_user_role
	,a.MCR_HISTUSR_flg
	,a. bed_decn_HIST_user_role
	,a.bed_input_HIST_user_role
	,a.initial_rnk
	,a.latest_rnk
	,a.case_key
	,a.cancelled_ind
	,a.asmt_rvw_id
	,a.bed_decn_crmk_cd
	,a.bed_decn_dttm
	,a.bed_decn_end_dttm
	,a.bed_decn_from_dttm
	,a.bed_decn_input_userid
	,a.bed_decn_otcm_cd
	,a.bed_decn_otcm_crmk_cd
	,a.bed_decn_rsn_cd
	,a.bed_decn_seq_id
	,a.bed_decn_typ_cd
	,a.bed_decn_user_comploc_cd
	,a.bed_decn_user_dept_cd
	,a.bed_decn_user_pstn_cd
	,a.bed_decn_userid
	,a.bed_input_dttm
	,a.bed_typ_cd
	,a.svc_seq_id
	,a.row_eff_dt  
from 
  	HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_HIST_USR_1 a
--where DATEDIFF(bed_decn_dttm,bed_input_dttm) >0
left outer join
	HCE_OPS_STAGE.adr_xref_fmts_v   d
on a.bed_decn_userid = d."START"
and d.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
left outer join
 HCE_OPS_STAGE.adr_xref_fmts_v   e
on a.bed_decn_input_userid = e."START"
and e.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
;
--CLAGMAN1
--SELECT * FROM HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_HIST_USR_1
--SELECT * FROM HCE_OPS_STAGE.adr_xref_fmts_v WHERE "START"='CLAGMAN1'
drop table if exists HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_touch_2 ;
create table HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_touch_2  as 
select 
	case_key
	,max(case when initial_rnk=1  then  coalesce (MCR_HISTUSR_flg,0)+ COALESCE (MCR_CURRUSR_flg,0) end) bed_Initial_MCR_Ind
	,max(case when latest_rnk=1  then coalesce (MCR_HISTUSR_flg,0)+ COALESCE (MCR_CURRUSR_flg,0) end) bed_latest_MCR_Ind
from 
	HCE_OPS_STAGE.HCEOPS_sd_ip_notif_bedday_mcr_CURR_USR_2
--where 
--	case_key='HSR-193016274'
group by 
	case_key
;

-------------------------------------------- BEDDAY DATA Processing END --------------------------------------------

-------------------------------------------- SERVICE DATA Processing START --------------------------------------------
drop table if exists HCE_OPS_STAGE.HCEOPS_sd_ip_notif_SERV_mcr_HIST_USR_1 ;
create table HCE_OPS_STAGE.HCEOPS_sd_ip_notif_SERV_mcr_HIST_USR_1  as
select
distinct 
	case when b.user_role like '%MCR%' or c.user_role like '%MCR%' then 1 else 0 end as MCR_HISTUSR_flg
	,b.user_role serv_decn_HIST_user_role
	,c.user_role  serv_input_HIST_user_role
	,a.initial_rnk
	,a.latest_rnk
	,a.case_key
	,a.cancelled_ind
	,a.decn_input_userid
	,a.decn_userid
	,a.svc_seq_nbr
	,a.svc_seq_id
	,a.row_eff_dt
	,a.decn_typ_cd
from 
  	HCE_OPS_STAGE.HCEOPS_SD_ADR_TRANS_SERV_DECN_2  a
--where DATEDIFF(bed_decn_dttm,bed_input_dttm) >0
left outer join
	HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted b
on a.decn_userid  =b.user_id
and a.decn_dttm  between b.role_from_dt and b.role_end_dt 
left outer join
	HCE_OPS_STAGE.MIDM_ADR_USER_HIST_ROLE_Formatted c
on a.decn_input_userid  =c.user_id
and a.decn_input_dttm  between c.role_from_dt and c.role_end_dt 
;



drop table if exists HCE_OPS_STAGE.HCEOPS_sd_ip_notif_SERV_mcr_CURR_USR_2  ;
create table HCE_OPS_STAGE.HCEOPS_sd_ip_notif_SERV_mcr_CURR_USR_2   as
select
distinct 
	case when d.label like '%MCR%' or e.label like '%MCR%'  then 1 else 0 end as MCR_CURRUSR_flg
	,d.label serv_decn_CURR_user_role
	,e.label   serv_input_CURR_user_role
	,a.MCR_HISTUSR_flg
	,a.serv_decn_HIST_user_role
	,a.serv_input_HIST_user_role
	,a.initial_rnk
	,a.latest_rnk
	,a.case_key
	,a.cancelled_ind
	,a.decn_input_userid
	,a.decn_userid
	,a.svc_seq_nbr
	,a.svc_seq_id
	,a.row_eff_dt
	,a.decn_typ_cd
from 
  	HCE_OPS_STAGE.HCEOPS_sd_ip_notif_SERV_mcr_HIST_USR_1   a
--where DATEDIFF(bed_decn_dttm,bed_input_dttm) >0
left outer join
	HCE_OPS_STAGE.adr_xref_fmts_v   d
on a.decn_userid = d."START"
and d.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
left outer join
 HCE_OPS_STAGE.adr_xref_fmts_v   e
on a.decn_input_userid = e."START"
and e.FMTNAME ='$MIDM_ADR_USER_CURRENT_ROLE'
;

 
--sample 1
--select * from HCE_OPS_STAGE.sd_ip_notif_serv_mcr_touch_1 where case_key='HSR-193016274'
--select * from  HCE_OPS_STAGE.sd_ip_notif_SERV_mcr_CURR_USR_2  where  case_key='HSR-213467158';
--select * from  HCE_OPS_STAGE.sd_ip_notif_serv_mcr_touch_2  where  case_key='HSR-217960897';

drop table if exists HCE_OPS_STAGE.HCEOPS_sd_ip_notif_serv_mcr_touch_2 ;
create table HCE_OPS_STAGE.HCEOPS_sd_ip_notif_serv_mcr_touch_2  as 
select 
	case_key
	,max(case when initial_rnk=1  then   coalesce (MCR_HISTUSR_flg,0)+ COALESCE (MCR_CURRUSR_flg,0) end) serv_Initial_MCR_Ind
	,max(case when latest_rnk=1  then   coalesce (MCR_HISTUSR_flg,0)+ COALESCE (MCR_CURRUSR_flg,0)  end) serv_latest_MCR_Ind
from 
	HCE_OPS_STAGE.HCEOPS_sd_ip_notif_SERV_mcr_CURR_USR_2
--where 
--	case_key='HSR-193016274'
group by 
	case_key

;	
-------------------------------------------- SERVICE DATA Processing END --------------------------------------------


