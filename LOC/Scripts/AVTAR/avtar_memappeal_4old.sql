--set hive.auto.convert.join=false;
--alter table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_0 rename to tmp_1y.hce_adr_avtar_like_25_26_f_0;
drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_0 ;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_0  as
select 
a.* 
--,b.from_user_role 
--,b.from_user_eff_dt
--,b.from_user_exp_dt 
--,b.end_user_eff_dt
--,b.end_user_exp_dt
--,b.end_user_role_fnl
,b.MD_Escalation_IND
,b.ICM_MD_REVIEWED_IND
--,b.MD_Escalation_Instances --readd for monthly
--,CASE WHEN b.case_key IS NOT NULL THEN 1 ELSE 0 END as MD_Join_Match_IND
from HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin a
LEFT JOIN HCE_OPS_STAGE.HCEOPS_hsr_adr_asgn_tag_userroles_MD_NOROLLUP b on b.case_key = a.case_id and ICM_RowNumber = 1;


--act_adm_start_dt ,actual_discharge, apprcvdt
--alter table HCE_OPS_STAGE.HCEOPS_diagnosis_code_avtar rename to tmp_1y.diagnosis_code_avtar;
drop table if exists HCE_OPS_STAGE.HCEOPS_diagnosis_code_avtar;
create table HCE_OPS_STAGE.HCEOPS_diagnosis_code_avtar  as 
select * 
	   ,row_number() over(partition by diag_cd order by glxy_load_dt desc,glxy_updt_dt desc) as latest_rec
from fichsrv.tadm_glxy_diagnosis_code;

--
--drop table if exists tmp_1m.hce_adr_avtar_like_25_26_f_mabackup;
--alter table tmp_1m.hce_adr_avtar_like_25_26_f_ma rename to tmp_1m.hce_adr_avtar_like_25_26_f_mabackup;


drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_fa1;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_fa1  as
select a.* 
	  ,b.ahrq_diag_genl_catgy_cd  as  prim_diag_ahrq_genl_catgy_cd 
	  ,b.ahrq_diag_genl_catgy_desc as prim_diag_ahrq_genl_catgy_desc
	  ,b.ahrq_diag_dtl_catgy_cd as prim_diag_ahrq_diag_dtl_catgy_cd
      ,b.ahrq_diag_dtl_catgy_desc as prim_diag_ahrq_diag_dtl_catgy_desc
      ,case when c.md_escalation_list is null then 0 else 1 end as Dx_240_MD_ESCLTN_IND
      ,case when d.ili_flag is null then 0 else 1 end as ILI_DX_IND
      ,case when e.covid_flag is null then 0 else 1 end as COVID_DX_IND
	  ,case when f.icd_with_period is null then 0 else 1 end as adj_24_dx_retain_IND
      ,to_char(admit_dt_exp,'yyyyMM') as admit_exp_month
	  ,to_char(admit_dt_act,'yyyyMM') as admit_act_month
	  ,to_char(dschg_dt_exp,'yyyyMM') as dschg_exp_month
	  ,to_char(dschg_dt_act,'yyyyMM') as dschg_act_month
	  ,concat(year(admit_dt_exp),'Q',quarter(admit_dt_exp)) as admit_exp_qtr
	  ,concat(year(admit_dt_act),'Q',quarter(admit_dt_act)) as admit_act_qtr
	  ,concat(year(dschg_dt_exp),'Q',quarter(dschg_dt_exp)) as dschg_exp_qtr
	  ,concat(year(dschg_dt_act),'Q',quarter(dschg_dt_act)) as dschg_act_qtr
	  ,case when (dschg_dt_act is not null and admit_dt_act is not null) then DATEDIFF('day',admit_dt_act,dschg_dt_act) 
		else DATEDIFF('day',admit_dt_exp,dschg_dt_exp) end as LOS
from 
HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_0 a
left join 
HCE_OPS_STAGE.HCEOPS_diagnosis_code_avtar b 
on regexp_replace(a.prim_diag_cd,'\.','')=b.diag_cd and latest_rec=1
left join HCE_OPS_LKUP.dx240_avtar_list c
on a.prim_diag_cd=c.md_escalation_list
left join HCE_OPS_LKUP.flu_flag_avtar d
on a.prim_diag_cd=d.ili_flag 
left join HCE_OPS_LKUP.covid_flag_avtar e 
on a.prim_diag_cd=e.covid_flag
left join HCE_OPS_LKUP.RETAIN_LIST_24_ADJ_DX f
on a.prim_diag_cd=f.ICD_with_period;

--Compare HCE With ADM Query
--
--select 
--	COALESCE (a.dschg_act_month,a.dschg_exp_month) dischrgdt
--	a.dschg_act_month
--	,count(distinct case_id) unq_case_cnt
--	,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
--	,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--	Appeals
--	,count(distinct (case when initialfulladr_cases=1 AND Appeal_ind=1 then case_id  end )) Appeal_case_cnt
--	,count(distinct (case when Appeal_ovrtn_Ind=1 then case_id  end )) Appeal_Ovrtn_case_cnt
--	MCR
--	,count(distinct (case when mcr_reconsideration_ind=1 then case_id  end )) MCR_Reconsideration_case_cnt
--	,count(distinct (case when MCR_Ovtrn_ind=1  then case_id  end )) MCR_Ovrtn_case_cnt
--	P2P
--	,count(distinct (case when P2P_full_evertouched_cnt=1 then case_id  end )) P2P_case_cnt
--	,count(distinct (case when P2P_full_ovtn=1  then case_id  end )) P2P_Ovrtn_case_cnt
--	OTHERS 
--	,count(distinct (case when P2P_full_ovtn=0 and Appeal_ovrtn_Ind=0 and MCR_Ovtrn_ind=0 and initialfulladr_cases=1 and  persistentfulladr_cases=0 then case_id end))  Other_ovtrns
--from 
--	HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_Appl_MCR_P2PJoin        a
-- 	HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f a
--where
--       svc_setting ='Inpatient' --Inpatient Services
--       and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--       and admit_cat_cd  in ('17 - Medical','30 - Surgical')
--       and a.business_segment ='MnR'
--       and fin_product_level_3<>'INSTITUTIONAL'
--       and tfm_include_flag=1
--       and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--group by 
--	COALESCE (a.dschg_act_month,a.dschg_exp_month)
--	a.dschg_act_month 
--union ALL 
--select 
--	COALESCE (a.dschg_act_month,a.dschg_exp_month) dischrgdt
--	a.dschg_act_month
--	,count(distinct case_id) unq_case_cnt
--	,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
--	,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--	Appeals
--	,count(distinct (case when initialfulladr_cases=1 AND Appeal_ind=1 then case_id  end )) Appeal_case_cnt
--	,count(distinct (case when Appeal_ovrtn_Ind=1 then case_id  end )) Appeal_Ovrtn_case_cnt
--	MCR
--	,count(distinct (case when mcr_reconsideration_ind=1 then case_id  end )) MCR_Reconsideration_case_cnt
--	,count(distinct (case when MCR_Ovtrn_ind=1  then case_id  end )) MCR_Ovrtn_case_cnt
--	P2P
--	,count(distinct (case when P2P_full_evertouched_cnt=1 then case_id  end )) P2P_case_cnt
--	,count(distinct (case when P2P_full_ovtn=1  then case_id  end )) P2P_Ovrtn_case_cnt
--	OTHERS 
--	,count(distinct (case when P2P_full_ovtn=0 and Appeal_ovrtn_Ind=0 and MCR_Ovtrn_ind=0 and initialfulladr_cases=1 and  persistentfulladr_cases=0 then case_id end))  Other_ovtrns
--from 
--	 HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_2022_f a
--where
--       svc_setting ='Inpatient' --Inpatient Services
--       and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--       and admit_cat_cd  in ('17 - Medical','30 - Surgical')
--              and a.business_segment ='MnR'
--       and fin_product_level_3<>'INSTITUTIONAL'
--       and tfm_include_flag=1
--       and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--group by 
--       COALESCE (a.dschg_act_month,a.dschg_exp_month) 
--	a.dschg_act_month
--;



drop table if exists HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_trsjoin_1;
create table HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_trsjoin_1  as
select a.*
	  ,case when b.fin_mbi_hicn_fnl is null then 'N' ELSE 'Y' END AS transplant_flag
	  ,count(case when a.admit_cat_cd='33 - Transplant' then 1 end )over(partition by a.medicare_id,a.admit_dt_act) as trans_cat_count
	  ,b.transplantdate
	  ,b.programlvl2 as transplant_type
	  ,b.admission_date
FROM  HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_fa1 a
LEFT JOIN HCE_OPS_FNL.HCEOPS_TRS_DATA_SET_FNL b on
a.medicare_id = b.fin_mbi_hicn_fnl
AND
(
b.transplantdate BETWEEN a.admit_dt_act AND COALESCE (a.dschg_dt_act,a.dschg_dt_exp)
OR DATEADD(DAY,-1,b.transplantdate)   BETWEEN a.admit_dt_act AND COALESCE (a.dschg_dt_act,a.dschg_dt_exp)
OR DATEADD(DAY,1,b.transplantdate)  BETWEEN a.admit_dt_act AND COALESCE (a.dschg_dt_act,a.dschg_dt_exp)
)
--AND a.prim_proc_ind = 'Y'
AND a.svc_setting = 'Inpatient';


DROP TABLE  HCE_OPS_FNL.hce_adr_avtar_like_25_26_f_backup;
ALTER TABLE HCE_OPS_FNL.hce_adr_avtar_like_25_26_f RENAME TO HCE_OPS_FNL.hce_adr_avtar_like_25_26_f_backup;
----DESC TABLE tmp_1m.HCEOPS_hce_adr_avtar_like_25_26_f_trsjoin_1
----SELECT * FROM HCE_OPS_FNL.hce_adr_avtar_like_25_26_f_12032025
--tmp_1m.hce_adr_avtar_like_25_26_f
--member_appeal_ind
--member_appeal_ovtn_ind
--SELECT dschg_dt_exp,dschg_dt_act,admit_dt_exp,admit_dt_act,los FROM HCE_OPS_FNL.hce_adr_avtar_like_25_26_f
--SELECT count(*) FROM HCE_OPS_FNL.hce_adr_avtar_like_25_26_f--52564258
--SELECT count(*) FROM HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_fa1 --50809567
drop table if exists HCE_OPS_FNL.hce_adr_avtar_like_25_26_f;
create table HCE_OPS_FNL.hce_adr_avtar_like_25_26_f  as 
select  p2p_full_evertouched_cnt
		,p2p_full_ovtn
		,p2p_match_ind
		,mcr_reconsideration_ind
		,mcr_evertouched_decn_ind
		,mcr_ovtrn_ind
		,mcr_uphelp_ind
		,rvsl_ind
		,rvsl_decn_userid
		,rvsl_decn_dttm
		,rvsl_decn_user_role
		,mcr_rvsls
		,rvsl_bed_decn_mtch_ind
		,rvsl_srv_decn_mtch_ind
		,appeal_ind
		,appeal_ovrtn_ind
		,oth_ovrtn_ind
		,appdecnmkr_user_id
		,appdecnmkr_user_nm
		,appdecnmkr_user_role
		,appdecndt
		,apptype
		,apprcvddt
		,appoutcome
		,appmcrprevreviewfmd
		,appissuetype
		,hce_category
		,prim_srvc_cat
		,prim_srvc_sub_cat
		,business_segment
		,entity
		,medicare_id
		,member_dob
		,member_sex
		,member_state
		,member_id
		,purchaser_id
		,subscriber_id
		,create_dt
		,avtar_mtch_ind
		,case_id
		,case_category_cd
		,svc_setting
		,notif_recd_dttm
		,notif_yrmonth
		,svc_seq_id
		,svc_seq_nbr
		,proc_cd
		,prim_proc_ind
		,prim_diag_cd
		,icd_ver_cd
		,prim_proc_last_decn
		,svc_freq
		,svc_freq_typ_cd
		,proc_unit_cnt
		,svc_crmk_cd
		,svc_start_dt
		,svc_end_dt
		,svc_cat_cd
		,svc_cat_dtl_cd
		,plc_of_svc_cd
		,plc_of_svc_drv_cd
		,case_status_cd
		,case_status_rsn_cd
		,appeal
		,palist
		,prim_svc_palist
		,pa_program
		,case_init_cur_decn_dttm
		,case_init_svc_cur_decn_dttm
		,adrcase_cancelled_ind
		,casedrv_cancelled_ind
		,serv_cancelled_ind
		,servdrv_cancelled_ind
		,ab_excl
		,adv_det_rate_exclusion
		,servdrv_prov_key
		,case_cur_svc_cat_dtl_cd
		,case_init_decn_cd
		,case_svc_init_decn_cd
		,case_decn_stat_cd
		,case_svc_decn_stat_cd
		,case_prov_par_status_cd
		,admit_cat_cd
		,auth_typ_cd
		,channel_cd
		,svcdecn_curr_gap_otcm_cd
		,svcdecn_curr_gap_otcm_cd_desc
		,admit_dt_act
		,admit_dt_exp
		,dschg_dt_act
		,dschg_dt_exp
		,bcrt_void_ind
		,ocm_migration
		,mnr_hce_drv_par_status
		,so_prov_id
		,so_prov_clm_id
		,so_prov_par_status_ind
		,so_prov_typ_f
		,sj_prov_id
		,sj_prov_clm_id
		,sj_prov_par_status_ind
		,sj_prov_typ_f
		,drv_cse_rf_prov_clm_id
		,drv_cse_rf_prov_key
		,drv_cse_rf_par_status
		,rf_prov_id
		,rf_prov_clm_id
		,rf_prov_par_status_ind
		,rf_prov_typ_f
		,pc_prov_id
		,pc_prov_clm_id
		,pc_prov_par_status_ind
		,pc_prov_typ_f
		,fa_prov_id
		,fa_prov_clm_id
		,fa_prov_par_status_ind
		,fa_prov_typ_f
		,at_prov_id
		,at_prov_clm_id
		,at_prov_par_status_ind
		,at_prov_typ_f
		,ad_prov_id
		,ad_prov_clm_id
		,ad_prov_par_status_ind
		,ad_prov_typ_f
		,b_case_id
		,c_case_id
		,fin_source_name
		,migration_source
		,fin_product_level_3
		,tfm_include_flag
		,global_cap
		,nce_tadm_dec_risk_type
		,fin_contractpbp
		,fin_contract_nbr
		,fin_pbp
		,fin_submarket
		,fin_market
		,fin_region
		,fin_state
		,fin_plan_level_2
		,fin_g_i
		,fin_brand
		,group_number
		,aco
		,aco_network
		,group_name
		,fin_segment_name
		,fin_tfm_product
		,fin_mbi_hicn_fnl
		,mbi_match_flag
		,sgr_source_name
		,fin_tfm_product_new
		,fin_ps9_business_unit
		,fin_ps9_location
		,fin_ps9_operating_unit
		,fin_ps9_product
		,initialfulladr_cases
		--,initialpartialadr_cases
		,persistentfulladr_cases
		--,persistentpartialadr_cases
		,initial_dnl_decn_userid
		,initial_dnl_decn_user_role
		,initial_dnl_decn_dttm
		,latest_dnl_decn_userid
		,latest_dnl_decn_user_role
		,latest_dnl_decn_dttm
		,md_escalation_ind
		,icm_md_reviewed_ind
		,prim_diag_ahrq_genl_catgy_cd
		,prim_diag_ahrq_genl_catgy_desc
		,prim_diag_ahrq_diag_dtl_catgy_cd
		,prim_diag_ahrq_diag_dtl_catgy_desc
		,240_dx_md_escltn_ind
		,ili_dx_ind
		,covid_dx_ind
		,24_adj_dx_retain_ind
		,admit_exp_month
		,admit_act_month
		,dschg_exp_month
		,dschg_act_month
		,admit_exp_qtr
		,admit_act_qtr
		,dschg_exp_qtr
		,dschg_act_qtr
		,los
		,admission_date
		,case when transplant_flag='Y' and admit_cat_cd<>'33 - Transplant' and 
			trans_cat_count>0 then 'N' else transplant_flag end as transplant_flag
		,trans_cat_count
		,transplantdate
		,transplant_type
		,case when transplant_flag='Y' and admit_cat_cd<>'33 - Transplant' and 
			trans_cat_count>0 then 1 else 0 end as medsurg_overlap_ind
		,member_appeal_ind
		--,appeal_ind
		,member_appeal_ovtn_ind
		--,Appeal_ovrtn_Ind
from HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_trsjoin_1;


--SELECT * from HCE_OPS_STAGE.HCEOPS_hce_adr_avtar_like_25_26_f_trsjoin_1_test


--select
--to_char(a.dschg_dt_act ,'yyyyMM') dischrgdt
--,count(distinct case_id) unq_case_cnt
--,count(distinct (case when p2p_match_ind ='Y' then case_id end)) p2p_case_cnt
--,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--,count(distinct (case when icm_md_reviewed_ind=1 then case_id end )) ICM_MD_review_case_cnt
--,count(DISTINCT(CASE WHEN member_appeal_ovtn_ind=1 then case_id end)) member_appeal_ovtn_cnt
--,count(DISTINCT(CASE WHEN appeal_ovrtn_ind=1 then case_id end)) appeal_ovtn_cnt
--,count(DISTINCT(CASE WHEN member_appeal_ind=1 then case_id end)) member_appeal_cnt
--,count(DISTINCT(CASE WHEN Appeal_Ind=1 then case_id end)) appeal_cnt
--from
--HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F a
--where
--svc_setting ='Inpatient' --Inpatient Services
--and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--and admit_cat_cd in ('17 - Medical','30 - Surgical')
--and fin_product_level_3<>'INSTITUTIONAL'
--and tfm_include_flag=1
--and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--group by
--to_char(a.dschg_dt_act ,'yyyyMM');
--




--
--
--
select
to_char(admit_dt_act ,'MM/dd/yyyy') admtdt
,count(distinct case_id) unq_case_cnt
,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
,count(distinct (case when icm_md_reviewed_ind=1 then case_id end)) as MD_Reviewed_cnt
,count(DISTINCT(CASE WHEN member_appeal_ovtn_ind=1 then case_id end)) member_appeal_ovtn_cnt
,count(DISTINCT(CASE WHEN appeal_ovrtn_ind=1 then case_id end)) appeal_ovtn_cnt
,count(DISTINCT(CASE WHEN member_appeal_ind=1 then case_id end)) member_appeal_cnt
,count(DISTINCT(CASE WHEN Appeal_Ind=1 then case_id end)) appeal_cnt
from
HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F
where
svc_setting ='Inpatient' --Inpatient Services
and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
and admit_cat_cd in ('17 - Medical','30 - Surgical')
and fin_product_level_3<>'INSTITUTIONAL'
and tfm_include_flag=1
and ((global_cap='NA' and fin_source_name = 'COSMOS') )
and to_char(admit_dt_act ,'yyyy') in ('2026')
and to_char(admit_dt_act ,'MM/dd/yyyy') >= '02/09/2026'
group by
to_char(admit_dt_act ,'MM/dd/yyyy');
--
--




select
to_char(a.dschg_dt_act ,'yyyyMM') dischrgdt
,count(distinct case_id) unq_case_cnt
,count(distinct (case when p2p_match_ind ='Y' then case_id end)) p2p_case_cnt
,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
,count(distinct (case when icm_md_reviewed_ind=1 then case_id end )) ICM_MD_review_case_cnt
from
HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F a
where
svc_setting ='Inpatient' --Inpatient Services
and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
and admit_cat_cd in ('17 - Medical','30 - Surgical')
and fin_product_level_3<>'INSTITUTIONAL'
and tfm_include_flag=1
and ((global_cap='NA' and fin_source_name = 'COSMOS') )
group by
to_char(a.dschg_dt_act ,'yyyyMM')

union ALL 
select
to_char(a.dschg_dt_act ,'yyyyMM') dischrgdt
,count(distinct case_id) unq_case_cnt
,count(distinct (case when p2p_match_ind ='Y' then case_id end)) p2p_case_cnt
,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
,count(distinct (case when icm_md_reviewed_ind=1 then case_id end )) ICM_MD_review_case_cnt
from
HCE_OPS_ARCHV.HCE_ADR_AVTAR_Like_2024_F a
where
svc_setting ='Inpatient' --Inpatient Services
and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
and admit_cat_cd in ('17 - Medical','30 - Surgical')
and fin_product_level_3<>'INSTITUTIONAL'
and tfm_include_flag=1
and ((global_cap='NA' and fin_source_name = 'COSMOS') )
group by
to_char(a.dschg_dt_act ,'yyyyMM')
UNION all
select
to_char(a.dschg_dt_act ,'yyyyMM') dischrgdt
,count(distinct case_id) unq_case_cnt
,count(distinct (case when p2p_match_ind ='Y' then case_id end)) p2p_case_cnt
,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
,count(distinct (case when icm_md_reviewed_ind=1 then case_id end )) ICM_MD_review_case_cnt
from
HCE_OPS_ARCHV.HCE_ADR_AVTAR_Like_2023_F a
where
svc_setting ='Inpatient' --Inpatient Services
and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
and admit_cat_cd in ('17 - Medical','30 - Surgical')
and fin_product_level_3<>'INSTITUTIONAL'
and tfm_include_flag=1
and ((global_cap='NA' and fin_source_name = 'COSMOS') )
group by
to_char(a.dschg_dt_act ,'yyyyMM');

--
--
--select
--to_char(admit_dt_act ,'MM/dd/yyyy') admtdt
--,count(distinct case_id) unq_case_cnt
--,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
--,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--,count(distinct (case when icm_md_reviewed_ind=1 then case_id end)) as MD_Reviewed_cnt
--,count(DISTINCT(CASE WHEN member_appeal_ovtn_ind=1 then case_id end)) member_appeal_ovtn_cnt
--,count(DISTINCT(CASE WHEN appeal_ovrtn_ind=1 then case_id end)) appeal_ovtn_cnt
--,count(DISTINCT(CASE WHEN member_appeal_ind=1 then case_id end)) member_appeal_cnt
--,count(DISTINCT(CASE WHEN Appeal_Ind=1 then case_id end)) appeal_cnt
--from
--HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F
--where
--svc_setting ='Inpatient' --Inpatient Services
--and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--and admit_cat_cd in ('17 - Medical','30 - Surgical')
--and fin_product_level_3<>'INSTITUTIONAL'
--and tfm_include_flag=1
--and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--and to_char(admit_dt_act ,'yyyy') in ('2026')
--and to_char(admit_dt_act ,'MM/dd/yyyy') >= '02/09/2026'
--group by
--to_char(admit_dt_act ,'MM/dd/yyyy');
--


--select
--to_char(admit_dt_act ,'MM/dd/yyyy') admtdt
--,count(distinct case_id) unq_case_cnt
--,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
--,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--,count(distinct (case when icm_md_reviewed_ind=1 then case_id end)) as MD_Reviewed_cnt
--,count(DISTINCT(CASE WHEN member_appeal_ovtn_ind=1 then case_id end)) member_appeal_ovtn_cnt
--,count(DISTINCT(CASE WHEN appeal_ovrtn_ind=1 then case_id end)) appeal_ovtn_cnt
--,count(DISTINCT(CASE WHEN member_appeal_ind=1 then case_id end)) member_appeal_cnt
--,count(DISTINCT(CASE WHEN Appeal_Ind=1 then case_id end)) appeal_cnt
--from
--HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F
--where
--svc_setting ='Inpatient' --Inpatient Services
--and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--and admit_cat_cd in ('17 - Medical','30 - Surgical')
--and fin_product_level_3<>'INSTITUTIONAL'
--and tfm_include_flag=1
--and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--and to_char(admit_dt_act ,'yyyy') in ('2026')
--and to_char(admit_dt_act ,'MM/dd/yyyy') >= '02/09/2026'
--group by
--to_char(admit_dt_act ,'MM/dd/yyyy');

















































