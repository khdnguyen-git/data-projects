drop table IF EXISTS HCE_OPS_STAGE.ADR_XREF_FMTS_V_format;
create table HCE_OPS_STAGE.ADR_XREF_FMTS_V_format  as
select * from HCE_OPS_STAGE.ADR_XREF_FMTS_V
where fmtname in 
('$ADR_L_CASE_CATEGORY_CD',
'$ADR_L_SVC_FREQ_TYP_CD',
'$ADR_L_SVC_CRMK_CD',
'$ADR_L_SVC_CAT_CD',
'$HSR_L_SVC_CAT_DTL_CD',
'$HSR_L_PLC_OF_SVC_CD',
'$ADR_L_PLC_OF_SVC_DRV_CD',
'$ADR_L_CASE_STATUS_CD',
'$HSR_L_CASE_STATUS_RSN_CD',
'$HSR_L_CASE_CUR_SVC_CAT_DTL_CD',
'$ADR_L_CASE_SVC_DECN_STAT_CD',
'$ADR_L_CASE_DECN_STAT_CD',
'$ADR_L_CASE_PROV_PAR_STATUS_CD',
'$ADR_L_ADMIT_CAT_CD',
'$ADR_L_AUTH_TYP_CD',
'$HSR_L_CHANNEL_CD',
'$ADR_L_CASE_INIT_DECN_CD',
'$ADR_L_CASE_SVC_INIT_DECN_CD',
'$HSR_L_SVCDECN_CURR_GAP_OTCM_CD'
);


drop table if exists HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_1;
create table HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_1  as
SELECT
adrcase.case_id
,case when adrcase.svc_setting_cd='2' then 'Outpatient Physician'
when adrcase.svc_setting_cd ='3' then 'Outpatient Facility' 
when adrcase.svc_setting_cd ='1' then 'Inpatient'end as svc_setting 
,casedrv.medicare_id
,adrcase.notif_recd_dttm
,to_char(adrcase.notif_recd_dttm,'yyyyMM') AS  notif_yrmonth
,adrcase.case_status_cd
,adrcase.src_case_status_cd
,adrcase.case_status_rsn_cd
,adrcase.prim_diag_cd
,adrcase.icd_ver_cd
,adrcase.create_dt
,adrcase.admit_dt_act
,adrcase.admit_dt_exp
,adrcase.dschg_dt_act
,adrcase.dschg_dt_exp
,adrcase.ocm_migration
,case when adrcase.admit_dt_act is null and adrcase.admit_dt_exp is null then 1 else 0 end as BCRT_VOID_IND
,casedrv.ab_excl --should be 'N' to tie to AVTAR
,casedrv.case_category_cd --should be '01' to tie to AVTAR
,casedrv.case_svc_decn_stat_cd
,casedrv.case_decn_stat_cd 
,casedrv.business_segment
,casedrv.entity
,casedrv.lob_cd
,casedrv.member_dob
,casedrv.member_id
,casedrv.purchaser_id
,casedrv.subscriber_id
,casedrv.case_prov_par_status_cd
,casedrv.palist
,casedrv.prim_svc_palist
,casedrv.pa_program
,casedrv.admit_cat_cd
,casedrv.auth_typ_cd
,casedrv.notif_typ_cd
,casedrv.channel_cd
,casedrv.case_init_decn_cd
,casedrv.case_svc_init_decn_cd
,casedrv.case_bed_init_decn_cd
,casedrv.case_init_svc_cur_decn_dttm
,casedrv.case_init_bed_cur_decn_dttm
,casedrv.case_init_cur_decn_dttm
,casedrv.appeal
--,casedrv.appeal_tch
,casedrv.case_bed_decn_stat_cd
,casedrv.case_cur_svc_cat_dtl_cd
,casedrv.mh_ind
,casedrv.member_sex
,casedrv.ooa_oon
,casedrv.member_state
,serv.svc_seq_id
,serv.svc_seq_nbr
,serv.plc_of_svc_cd
,serv.proc_cd
,serv.svc_crmk_cd
,serv.svc_notif_recd_dttm
,serv.svc_start_dt
,serv.svc_end_dt
,serv.svc_cat_cd
,serv.svc_cat_dtl_cd
,serv.svc_freq
,serv.svc_freq_typ_cd
,serv.proc_unit_cnt
,serv.svc_units_auth
,serv.svc_units_req
,servdrv.prim_proc_ind --should be 'Y' to tie to AVTAR
,casedrv.prim_proc_last_decn
,servdrv.adv_det_rate_exclusion  --should be 'N' to tie to AVTAR
,servdrv.svcdecn_otcm_crmkcur_cd
,servdrv.plc_of_svc_drv_cd
,servdrv.max_ccr_svc_decn_cd
,adrcase.cancelled_ind as adrcase_cancelled_ind
,casedrv.cancelled_ind as casedrv_cancelled_ind 
,serv.cancelled_ind as serv_cancelled_ind 
,servdrv.cancelled_ind  as  servdrv_cancelled_ind  
,servdrv.prov_key as servdrv_prov_key
,servdrv.svcdecn_curr_gap_otcm_cd
FROM
HCE_OPS_STAGE.HSR_ADR_CASE_V  adrcase  
inner join
HCE_OPS_STAGE.HSR_ADR_CASE_DRV_V  CASEDRV
on ADRCase.CASE_id = substr(CASEDRV.CASE_KEY,5,length(CASEDRV.CASE_KEY)-4)
--and upper(CASEDRV.business_segment) in ('MNR', 'ENI', 'CNS')
and COALESCE (CaseDRV.obligor_cd,'0')<>'02'
--and date_format(ADRCASE.notif_recd_dttm,'YYYY') >= 2020
--and adrcase.svc_setting_cd in ('2','3')
and adrcase.current_ind='Y' and  casedrv.current_ind = 'Y' 
and adrcase.cancelled_ind='N' and casedrv.cancelled_ind='N'
--should be 1 to tie to AVTAR. this filters to EPAL codes
--and casedrv.ab_excl='N'  
inner join HCE_OPS_STAGE.HSR_ADR_SERV_V SERV
on adrcase.case_id = substr(serv.case_key,5,LENGTH (serv.case_key)-4)
and serv.current_ind = 'Y'
and serv.cancelled_ind='N'
--and serv.proc_cd='29827'
inner join HCE_OPS_STAGE.HSR_ADR_SERV_DRV_V  SERVDRV 
on SERV.SVC_SEQ_ID = SERVDRV.SVC_SEQ_ID
and servdrv.current_ind = 'Y' 
and servdrv.cancelled_ind ='N';
--should be 'N' to tie to AVTAR;




drop table if exists HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_2_fix_1;
create table HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_2_fix_1  as 
select business_segment,
entity,
medicare_id,
member_dob,
member_sex,
member_state,
member_id,
purchaser_id,
subscriber_id,
create_dt,
case when ab_excl='N' and case_category_cd='01' and prim_proc_ind='Y' and adv_det_rate_exclusion='N' then 1 else 0 end AVTAR_Mtch_Ind,
Case_id,
b.label as case_category_cd,
svc_setting,
notif_recd_dttm,
notif_yrmonth,
svc_seq_id,
svc_seq_nbr,
proc_cd,
prim_proc_ind,
prim_diag_cd,
icd_ver_cd,
prim_proc_last_decn,
svc_freq,
c.label as svc_freq_typ_cd,
proc_unit_cnt,
d.label as svc_crmk_cd,
svc_start_dt,
svc_end_dt,
e.label as svc_cat_cd,
f.label as svc_cat_dtl_cd,
g.label as plc_of_svc_cd,
h.label as plc_of_svc_drv_cd,
i.label as case_status_cd,
j.label as case_status_rsn_cd,
a.case_cur_svc_cat_dtl_cd,
a.case_init_decn_cd,
a.case_svc_init_decn_cd,
a.case_decn_stat_cd,
a.case_svc_decn_stat_cd,
appeal,
--appeal_tch,
a.case_prov_par_status_cd,
palist,
prim_svc_palist,
pa_program,
a.admit_cat_cd,
a.auth_typ_cd,
a.channel_cd,
case_init_cur_decn_dttm,
case_init_svc_cur_decn_dttm,
adrcase_cancelled_ind,
casedrv_cancelled_ind,
serv_cancelled_ind,
servdrv_cancelled_ind,
ab_excl,
adv_det_rate_exclusion,
servdrv_prov_key
,svcdecn_curr_gap_otcm_cd
,admit_dt_act
,admit_dt_exp
,dschg_dt_act
,dschg_dt_exp
,BCRT_VOID_IND
,a.ocm_migration
from 
HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_1  a
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format b
on a.case_category_cd = b."START" and b.fmtname ='$ADR_L_CASE_CATEGORY_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format c
on a.svc_freq_typ_cd = c."START" and c.fmtname ='$ADR_L_SVC_FREQ_TYP_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format d
on a.svc_crmk_cd = d."START" and d.fmtname ='$ADR_L_SVC_CRMK_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format e
on a.svc_cat_cd = e."START" and e.fmtname ='$ADR_L_SVC_CAT_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format f
on a.svc_cat_dtl_cd = f."START" and f.fmtname ='$HSR_L_SVC_CAT_DTL_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format g
on a.plc_of_svc_cd = g."START" and g.fmtname ='$HSR_L_PLC_OF_SVC_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format h
on a.plc_of_svc_drv_cd = h."START" and h.fmtname ='$ADR_L_PLC_OF_SVC_DRV_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format i
on a.case_status_cd = i."START" and i.fmtname ='$ADR_L_CASE_STATUS_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format j
on a.case_status_rsn_cd = j."START" and j.fmtname ='$HSR_L_CASE_STATUS_RSN_CD';




drop table if exists HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_2;
create table HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_2  as 
select business_segment,
entity,
medicare_id,
member_dob,
member_sex,
member_state,
member_id,
purchaser_id,
subscriber_id,
create_dt,
AVTAR_Mtch_Ind,
Case_id,
case_category_cd,
svc_setting,
notif_recd_dttm,
notif_yrmonth,
svc_seq_id,
svc_seq_nbr,
proc_cd,
prim_proc_ind,
prim_diag_cd,
icd_ver_cd,
prim_proc_last_decn,
svc_freq,
svc_freq_typ_cd,
proc_unit_cnt,
svc_crmk_cd,
svc_start_dt,
svc_end_dt,
svc_cat_cd,
svc_cat_dtl_cd,
plc_of_svc_cd,
plc_of_svc_drv_cd,
case_status_cd,
case_status_rsn_cd,
appeal,
--appeal_tch,
palist,
prim_svc_palist,
pa_program,
case_init_cur_decn_dttm,
case_init_svc_cur_decn_dttm,
adrcase_cancelled_ind,
casedrv_cancelled_ind,
serv_cancelled_ind,
servdrv_cancelled_ind,
ab_excl,
adv_det_rate_exclusion,
servdrv_prov_key,
svcdecn_curr_gap_otcm_cd,
k.label as case_cur_svc_cat_dtl_cd,
r.label as case_init_decn_cd,
s.label as case_svc_init_decn_cd,
m.label as case_decn_stat_cd,
l.label as case_svc_decn_stat_cd,
n.label case_prov_par_status_cd	,
o.label as admit_cat_cd,
p.label as auth_typ_cd,
q.label as channel_cd
,t.label as svcdecn_curr_gap_otcm_cd_desc
,admit_dt_act
,admit_dt_exp
,dschg_dt_act
,dschg_dt_exp
,BCRT_VOID_IND
,ocm_migration
from 
HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_2_fix_1  a
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format k
on a.case_cur_svc_cat_dtl_cd = k."START" and k.fmtname ='$HSR_L_CASE_CUR_SVC_CAT_DTL_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format l
on a.case_svc_decn_stat_cd = l."START" and l.fmtname ='$ADR_L_CASE_SVC_DECN_STAT_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format m
on a.case_decn_stat_cd = m."START" and m.fmtname ='$ADR_L_CASE_DECN_STAT_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format n
on a.case_prov_par_status_cd = n."START" and n.fmtname ='$ADR_L_CASE_PROV_PAR_STATUS_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format o
on a.admit_cat_cd = o."START" and o.fmtname ='$ADR_L_ADMIT_CAT_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format p
on a.auth_typ_cd = p."START" and p.fmtname ='$ADR_L_AUTH_TYP_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format q
on a.channel_cd = q."START" and q.fmtname ='$HSR_L_CHANNEL_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format r
on a.case_init_decn_cd = r."START" and r.fmtname ='$ADR_L_CASE_INIT_DECN_CD'
left outer join
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format s
on a.case_svc_init_decn_cd = s."START" and s.fmtname ='$ADR_L_CASE_SVC_INIT_DECN_CD'
LEFT OUTER JOIN 
HCE_OPS_STAGE.ADR_XREF_FMTS_V_format t
on a.svcdecn_curr_gap_otcm_cd = t."START" and t.fmtname ='$HSR_L_SVCDECN_CURR_GAP_OTCM_CD';


drop table if exists HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26;
create table  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26  as 
select 
substring(case_key,5,length(case_key)-4) case_id 
,a.*
from HCE_OPS_STAGE.hsr_adr_provtable_xref_v a; 
	

drop table if exists  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_1 ;
create table HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_1  as
select 
a.*,
row_number() over (partition by case_key, prov_key, PROV_TYP_CD order by PROV_UPDTE_DTTM desc) case_provkey_provtype_rnk
from HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26 as a;
	

drop table if exists  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_2; 
create table  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_2  as
SELECT
*
FROM
HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_1 where case_provkey_provtype_rnk=1 ;



drop table if exists  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_3;
create table   HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_3  as
select
CASE_ID
--,PROV_KEY
,max(case when PROV_TYP_CD='AD' then 1 else 0 end )                    AD_PROV_TYP_F
,max(case when PROV_TYP_CD='AD' then Prov_key end )                    AD_PROV_ID
,max(case when PROV_TYP_CD='AD' then PROV_PAR_STATUS_IND end )   AD_PROV_PAR_STATUS_IND
,max(case when PROV_TYP_CD='AD' then PROV_CLM_ID  end )           AD_PROV_CLM_ID
,max(case when PROV_TYP_CD='AT' then 1 else 0 end )                     AT_PROV_TYP_F
,max(case when PROV_TYP_CD='AT' then Prov_key  end )                    AT_PROV_ID
,max(case when PROV_TYP_CD='AT' then PROV_PAR_STATUS_IND end )   AT_PROV_PAR_STATUS_IND
,max(case when PROV_TYP_CD='AT' then PROV_CLM_ID  end )           AT_PROV_CLM_ID
,max(case when PROV_TYP_CD='FA' then 1 else 0 end )                     FA_PROV_TYP_F
,max(case when PROV_TYP_CD='FA' then Prov_key  end )               FA_PROV_ID			
,max(case when PROV_TYP_CD='FA' then PROV_PAR_STATUS_IND end )   FA_PROV_PAR_STATUS_IND
,max(case when PROV_TYP_CD='FA' then PROV_CLM_ID  end )           FA_PROV_CLM_ID
,max(case when PROV_TYP_CD='PC' then 1 else 0 end )                     PC_PROV_TYP_F
,max(case when PROV_TYP_CD='PC' then Prov_key end )             PC_PROV_ID
,max(case when PROV_TYP_CD='PC' then PROV_PAR_STATUS_IND  end )   PC_PROV_PAR_STATUS_IND
,max(case when PROV_TYP_CD='PC' then PROV_CLM_ID  end )           PC_PROV_CLM_ID
,max(case when PROV_TYP_CD='RF' then 1 else 0 end )                     RF_PROV_TYP_F
,max(case when PROV_TYP_CD='RF' then Prov_key  end )           RF_PROV_ID			
,max(case when PROV_TYP_CD='RF' then PROV_PAR_STATUS_IND end )   RF_PROV_PAR_STATUS_IND
,max(case when PROV_TYP_CD='RF' then PROV_CLM_ID end )           RF_PROV_CLM_ID
,max(case when PROV_TYP_CD='SJ' then 1 else 0 end )                     SJ_PROV_TYP_F
,max(case when PROV_TYP_CD='SJ' then Prov_key  end )            SJ_PROV_ID
,max(case when PROV_TYP_CD='SJ' then PROV_PAR_STATUS_IND  end )   SJ_PROV_PAR_STATUS_IND
,max(case when PROV_TYP_CD='SJ' then PROV_CLM_ID end )           SJ_PROV_CLM_ID
,max(case when PROV_TYP_CD='SO' then 1 else 0 end )                     SO_PROV_TYP_F
,max(case when PROV_TYP_CD='SO' then Prov_key end )         SO_PROV_ID			
,max(case when PROV_TYP_CD='SO' then PROV_PAR_STATUS_IND  end )   SO_PROV_PAR_STATUS_IND
,max(case when PROV_TYP_CD='SO' then PROV_CLM_ID  end )           SO_PROV_CLM_ID
from
HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_2
group by
CASE_ID;
--,PROV_KEY;

		
drop table if exists  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_4;
create table  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_4  as
select 
a.*,
row_number() over (partition by case_key,PROV_TYP_CD order by PROV_UPDTE_DTTM desc) case_RF_provtype_rnk
from
HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26 a
where prov_typ_cd='RF';


drop table if exists  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_5;
create table  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_5  as
select * from  HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_4 where case_RF_provtype_rnk=1;




drop table if exists HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_3;
create table HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_3  as
select
a.*
,case when NVL(c.PROV_PAR_STATUS_IND,'U')='N' and NVL(b.SJ_PROV_PAR_STATUS_IND,'U') in ('N','U') then 'Non-Par'
when  NVL(c.PROV_PAR_STATUS_IND,'U')='N' and NVL(b.SJ_PROV_PAR_STATUS_IND,'U') ='Y' then 'Par'
when NVL(c.PROV_PAR_STATUS_IND,'U')='N' and NVL(b.SJ_PROV_PAR_STATUS_IND,'U') is null then 'Non-Par'
when  NVL(c.PROV_PAR_STATUS_IND,'U')='U' and NVL(b.SJ_PROV_PAR_STATUS_IND,'U') ='N' then 'Non-Par'
when  NVL(c.PROV_PAR_STATUS_IND,'U')='U' and NVL(b.SJ_PROV_PAR_STATUS_IND,'U') ='U' then 'Par' ---Uncertain of status
when  NVL(c.PROV_PAR_STATUS_IND,'U')='U' and NVL(b.SJ_PROV_PAR_STATUS_IND,'U') ='Y' then 'Par'
when  NVL(c.PROV_PAR_STATUS_IND,'U')='Y'  then 'Par'
when NVL(b.SJ_PROV_PAR_STATUS_IND,'U') ='N' then 'Non-Par'
when NVL(b.SJ_PROV_PAR_STATUS_IND,'U') in ('Y','U') then 'Par'
end MnR_HCE_DRV_PAR_STATUS,
b.SO_PROV_ID,
b.SO_PROV_CLM_ID,
b.SO_PROV_PAR_STATUS_IND,
b.SO_PROV_TYP_F,
b.SJ_PROV_ID,
b.SJ_PROV_CLM_ID,
b.SJ_PROV_PAR_STATUS_IND,
b.SJ_PROV_TYP_F,
c.PROV_CLM_ID DRV_CSE_RF_PROV_CLM_ID,
c.PROV_KEY DRV_CSE_RF_PROV_KEY,
c.PROV_PAR_STATUS_IND DRV_CSE_RF_PAR_STATUS,
b.RF_PROV_ID,
b.RF_PROV_CLM_ID,
b.RF_PROV_PAR_STATUS_IND,
b.RF_PROV_TYP_F,
b.PC_PROV_ID,
b.PC_PROV_CLM_ID,
b.PC_PROV_PAR_STATUS_IND,
b.PC_PROV_TYP_F,
b.FA_PROV_ID,
b.FA_PROV_CLM_ID,
b.FA_PROV_PAR_STATUS_IND,
b.FA_PROV_TYP_F,
b.AT_PROV_ID,
b.AT_PROV_CLM_ID,
b.AT_PROV_PAR_STATUS_IND,
b.AT_PROV_TYP_F,
b.AD_PROV_ID,
b.AD_PROV_CLM_ID,
b.AD_PROV_PAR_STATUS_IND,
b.AD_PROV_TYP_F,
b.Case_id as B_Case_id,
c.case_id as C_Case_id
FROM 
HCE_OPS_STAGE.HCEOPS_ADR_AVTAR_Like_25_26_2 a
left outer join
HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_3 b
on a.case_id = b.case_id
left outer join
HCE_OPS_STAGE.HCEOPS_HSR_ADR_PROV_XREF_CURR_25_26_5 c
on a.case_id =c.case_id -- Using Case Id in here bcoz _5 is limited to Ref_provider so that Case_id is good enough to join
;



drop table if exists HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4;
create table HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4  as
select 
	b.category HCE_Category
	,c.prim_srvc_cat
	,c.prim_srvc_sub_cat
	,a.*
FROM 
	HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_3 a
left outer join
	HCE_OPS_LKUP.PA2020_CODE_LISTS_2019 b -- HCE category 
on a.proc_cd = b.proc_code
left outer join 
	HCE_OPS_LKUP.avtar_proc_catg_subcatg_desc c -- AVTAR Primary Category and sub category Desc
	on a.proc_cd = c.prim_proc_cd;

DROP TABLE IF EXISTS HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4a_backup;
ALTER TABLE HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4a rename TO HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4a_backup;


drop table if exists HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4a;
create table HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4a  as 
select   a.hce_category
		,a.prim_srvc_cat
		,a.prim_srvc_sub_cat
		,a.business_segment
		,a.entity
		,case when trim(a.medicare_id)=trim(b.hicn) then b.mbi else a.medicare_id end as medicare_id
		,a.member_dob
		,a.member_sex
		,a.member_state
		,member_id
		,a.purchaser_id
		,a.subscriber_id
		,a.create_dt
		,a.avtar_mtch_ind
		,a.case_id
		,a.case_category_cd
		,a.svc_setting
		,a.notif_recd_dttm
		,a.notif_yrmonth
		,a.svc_seq_id
		,a.svc_seq_nbr
		,a.proc_cd
		,a.prim_proc_ind
		,a.prim_diag_cd
		,a.icd_ver_cd
		,a.prim_proc_last_decn
		,a.svc_freq
		,a.svc_freq_typ_cd
		,a.proc_unit_cnt
		,a.svc_crmk_cd
		,a.svc_start_dt
		,a.svc_end_dt
		,a.svc_cat_cd
		,a.svc_cat_dtl_cd
		,a.plc_of_svc_cd
		,a.plc_of_svc_drv_cd
		,a.case_status_cd
		,a.case_status_rsn_cd
		,a.appeal
		,a.palist
		,a.prim_svc_palist
		,a.pa_program
		,a.case_init_cur_decn_dttm
		,a.case_init_svc_cur_decn_dttm
		,a.adrcase_cancelled_ind
		,a.casedrv_cancelled_ind
		,a.serv_cancelled_ind
		,a.servdrv_cancelled_ind
		,a.ab_excl
		,a.adv_det_rate_exclusion
		,a.svcdecn_curr_gap_otcm_cd
		,a.servdrv_prov_key
		,a.case_cur_svc_cat_dtl_cd
		,a.case_init_decn_cd
		,a.case_svc_init_decn_cd
		,a.case_decn_stat_cd
		,a.case_svc_decn_stat_cd
		,a.case_prov_par_status_cd
		,a.admit_cat_cd
		,a.auth_typ_cd
		,a.channel_cd
		,a.svcdecn_curr_gap_otcm_cd_desc
		,a.admit_dt_act
		,a.admit_dt_exp
		,a.dschg_dt_act
		,a.dschg_dt_exp
		,a.bcrt_void_ind
		,a.ocm_migration
		,a.mnr_hce_drv_par_status
		,a.so_prov_id
		,a.so_prov_clm_id
		,a.so_prov_par_status_ind
		,a.so_prov_typ_f
		,a.sj_prov_id
		,a.sj_prov_clm_id
		,a.sj_prov_par_status_ind
		,a.sj_prov_typ_f
		,a.drv_cse_rf_prov_clm_id
		,a.drv_cse_rf_prov_key
		,a.drv_cse_rf_par_status
		,a.rf_prov_id
		,a.rf_prov_clm_id
		,a.rf_prov_par_status_ind
		,a.rf_prov_typ_f
		,a.pc_prov_id
		,a.pc_prov_clm_id
		,a.pc_prov_par_status_ind
		,a.pc_prov_typ_f
		,a.fa_prov_id
		,a.fa_prov_clm_id
		,a.fa_prov_par_status_ind
		,a.fa_prov_typ_f
		,a.at_prov_id
		,a.at_prov_clm_id
		,a.at_prov_par_status_ind
		,a.at_prov_typ_f
		,a.ad_prov_id
		,a.ad_prov_clm_id
		,a.ad_prov_par_status_ind
		,a.ad_prov_typ_f
		,a.b_case_id
		,a.c_case_id
       from HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4 a
left join 
HCE_OPS_ARCHV.Macra_Crosswalk_202607 b
--tadm_tre_cpy.Macra_Crosswalk_202410 b
on trim(a.medicare_id)=trim(b.hicn);



DROP TABLE IF EXISTS HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_1_backup;
ALTER TABLE HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_1 rename TO HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_1_backup;

drop table if exists HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_1;
create table HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_1  as
select a.*
	   ,b.fin_source_name
	   ,b.Migration_source
	   ,b.fin_product_level_3
	   ,b.tfm_include_flag 
	   ,b.global_cap 
	   ,b.nce_tadm_dec_risk_type 
--SD- added below on March 18th to support DAN' IP ACCUTE Notifications data req
	   ,b.fin_contractpbp
	   ,b.fin_contract_nbr
	   ,b.fin_pbp
	   ,b.fin_submarket
	   ,b.fin_market
	   ,b.fin_region
	   ,b.fin_state
	   ,b.fin_plan_level_2
	   ,b.fin_g_i
	   ,b.fin_brand
           ,substring(b.gal_cust_seg_nbr,5) as group_number
           ,b.hierarchy as ACO
	   ,concat(b.acp_network_number,'_',b.acp_network_name) as ACO_Network
	   ,c.group_name
	   ,b.fin_segment_name
           ,b.fin_tfm_product
           ,b.fin_mbi_hicn_fnl
           ,case when trim(a.medicare_id)=trim(b.fin_mbi_hicn_fnl) then 1 else 0 end as mbi_match_flag
--TP-added below on 11/20/2024 for Emma and team
	   ,b.sgr_source_name
	   ,b.fin_tfm_product_new
		 ,b.FIN_PS9_BUSINESS_UNIT
	   ,b.FIN_PS9_LOCATION
           ,b.FIN_PS9_OPERATING_UNIT
           ,b.FIN_PS9_PRODUCT


FROM 
    HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_4a a
left outer join
    HCE_OPS_ARCHV.GL_RSTD_GPSGALNCE_F_202607 b -- HCE category 
 --HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix_1 b
on 
  a.medicare_id = b.fin_mbi_hicn_fnl 
and 
  a.notif_yrmonth =b.fin_inc_month
left join 
   fichsrv.group_crosswalk c
on substring(b.gal_cust_seg_nbr,5) = c.group_number  
and b.fin_inc_year = c."YEAR";


--
--SELECT * FROM HCE_OPS_ARCHV.GL_RSTD_GPSGALNCE_F_202603 WHERE fin_mbi_hicn_fnl IN ('1JV0WX0TR28','6UJ6C26DD48'
--)
--select * FROM  HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix_1 WHERE fin_mbi_hicn_fnl LIKE '%6UJ6C26DD48%'
--DESC TABLE VING_PRD_TREND_DB.HCE_OPS_FNL.TRE_MBR_SDOH_202603

--select TABLE_SCHEMA,TABLE_NAME, LAST_ALTERED from information_schema.tables
--WHERE TABLE_SCHEMA = 'TADM_TRE_CPY'
--ORDER BY LAST_ALTERED DESC
--SELECT * FROM HCE_OPS_STAGE.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_1
--WHERE notif_yrmonth='202506'

--SELECT * FROM  tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202508 WHERE fin_inc_month='202506'
--SELECT count(*) FROM tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202508--516836672
--SELECT count(*) FROM tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202508--564986236


--		select TABLE_SCHEMA,TABLE_NAME, LAST_ALTERED from information_schema.tables
--		WHERE TABLE_SCHEMA = 'HCE_OPS_ARCHV'
--ORDER BY LAST_ALTERED DESC 
--
--
--HCE_OPS_ARCHV.Macra_Crosswalk_202603 
--HCE_OPS_ARCHV.GL_RSTD_GPSGALNCE_F_202603