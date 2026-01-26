
--Step 0.a: Update to the new date & if a monthly claims run; update tre copy cosmos tab 
/*Every week*/
--Find and change date for notifications + claims
@set notifications_date = 01142026
@set claims_date = 12242025
--1/14/25: done--

--Step 0.b: check to see if current month membership is available
select count(*) from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202601; --make sure this IS CURRENT MONTH-- 
@set membership_month = 202512
--1/14/25: check again--

--monthly ish
--Step 0.c Uncomment out most recent roster month from Completion Step 9: tmp_1m.ec_ip_mm_2026 IF 0.B SHOWS NEXT MONTH MEMBERSHIP AVAILABLE
--Don't forget to update roster month in Notification Completion Model
--1/14/25: check again--

/*Monthly claims update*/
--Step 0.d: change claims month
--Change Tre Copy Table: tadm_tre_cpy.glxy_ip_admit_f_202512
@set claims_month = 202512
--1/14/25: done--

--Step 1: Check that AvTar was Updated with this query to check latest date (should be day of or day before run)
select max(admit_dt_act) from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_f 
where 	
       svc_setting ='Inpatient' --Inpatient Services
       and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
       and admit_cat_cd  in ('17 - Medical','30 - Surgical')			
       and fin_brand in ('M&R','C&S')
       and TO_VARCHAR(admit_dt_act, 'MM/dd/yyyy') is not null 
       and TO_VARCHAR(admit_dt_act  ,'yyyy') in ('2026')	;	
    

--Step 2.1: Getting remaining TRS cases that did not match to Notifications
drop table if exists tmp_1m.ec_avtar_23_trs;
create table tmp_1m.ec_avtar_23_trs as
select 
	0 as p2p_full_evertouched_cnt
	,0 as p2p_full_ovtn
	,'0' as p2p_match_ind
	,0 as mcr_reconsideration_ind
	,0 as mcr_evertouched_decn_ind
	,0 as mcr_ovtrn_ind
	,0 as mcr_uphelp_ind
	,0 as rvsl_ind
	,'0' as rvsl_decn_userid
	,cast(rvsl_decn_dttm as timestamp) as rvsl_decn_dttm
	,'0' as rvsl_decn_user_role
	,0 as mcr_rvsls
	,0 as rvsl_bed_decn_mtch_ind
	,0 as rvsl_srv_decn_mtch_ind
	,0 as appeal_ind
	,0 as appeal_ovrtn_ind
	,0 as oth_ovrtn_ind
	,'0' as appdecnmkr_user_id
	,'0' as appdecnmkr_user_nm
	,'0' as appdecnmkr_user_role
	,cast(appdecndt as timestamp) as appdecndt 
	,'0' as appoutcome
	,'0' as appmcrprevreviewfmd
	,'0' as appissuetype
	,'0' as hce_category
	,'0' as prim_srvc_cat
	,'0' as prim_srvc_sub_cat
	,'0' as business_segment
	,'0' as entity
	,a.fin_mbi_hicn_fnl as medicare_id
	,cast(a.dob as timestamp) as member_dob
	,a.fin_gender as member_sex
	,'0' as member_state
	,'0' as member_id
	,'0' as purchaser_id
	,'0' as subscriber_id
	,CAST(create_dt AS date) as create_dt  
	,0 as avtar_mtch_ind
	,concat(a.fin_mbi_hicn_fnl,a.transplantdate,a.programlvl2) as case_id
	,'0' as case_category_cd
	,'0' as svc_setting
	,cast(notif_recd_dttm as timestamp) as notif_recd_dttm 
	,'0' as notif_yrmonth
	,'0' as svc_seq_id
	,0 as svc_seq_nbr
	,'0' as proc_cd
	,'0' as prim_proc_ind
	,'0' as prim_diag_cd
	,'0' as icd_ver_cd
	,'0' as prim_proc_last_decn
	,0 as svc_freq
	,'0' as svc_freq_typ_cd
	,0 as proc_unit_cnt
	,'0' as svc_crmk_cd
	,cast(svc_start_dt AS date) as svc_start_dt
	,cast(svc_end_dt AS date) as svc_end_dt
	,'0' as svc_cat_cd
	,'0' as svc_cat_dtl_cd
	,'0' as plc_of_svc_cd
	,'0' as plc_of_svc_drv_cd
	,'0' as case_status_cd
	,'0' as case_status_rsn_cd
	,'0' as appeal
	,'0' as palist
	,'0' as prim_svc_palist
	,'0' as pa_program
	,cast(case_init_cur_decn_dttm as timestamp) as case_init_cur_decn_dttm
	,cast(case_init_svc_cur_decn_dttm as timestamp) as case_init_svc_cur_decn_dttm
	,'0' as adrcase_cancelled_ind
	,'0' as casedrv_cancelled_ind
	,'0' as serv_cancelled_ind
	,'0' as servdrv_cancelled_ind
	,'0' as ab_excl
	,'0' as adv_det_rate_exclusion
	,'0' as servdrv_prov_key
	,'0' as case_cur_svc_cat_dtl_cd
	,'0' as case_init_decn_cd
	,'0' as case_svc_init_decn_cd
	,'0' as case_decn_stat_cd
	,'0' as case_svc_decn_stat_cd
	,'0' as case_prov_par_status_cd
	,'0' as admit_cat_cd
	,'0' as auth_typ_cd
	,'0' as channel_cd
	,cast(a.admission_date AS date) as admit_dt_act
	,CAST(admit_dt_exp AS date) as admit_dt_exp
	,TRY_TO_DATE(a.discharge_date) as dschg_dt_act
	,cast(dschg_dt_exp AS date) as dschg_dt_exp
	,0 as bcrt_void_ind
	,'0' as ocm_migration
	,'0' as mnr_hce_drv_par_status
	,'0' as so_prov_id
	,'0' as so_prov_clm_id
	,'0' as so_prov_par_status_ind
	,0 as so_prov_typ_f
	,'0' as sj_prov_id
	,'0' as sj_prov_clm_id
	,'0' as sj_prov_par_status_ind
	,0 as sj_prov_typ_f
	,'0' as drv_cse_rf_prov_clm_id
	,'0' as drv_cse_rf_prov_key
	,'0' as drv_cse_rf_par_status
	,'0' as rf_prov_id
	,'0' as rf_prov_clm_id
	,'0' as rf_prov_par_status_ind
	,0 as rf_prov_typ_f
	,'0' as pc_prov_id
	,'0' as pc_prov_clm_id
	,'0' as pc_prov_par_status_ind
	,0 as pc_prov_typ_f
	,'0' as fa_prov_id
	,'0' as fa_prov_clm_id
	,'0' as fa_prov_par_status_ind
	,0 as fa_prov_typ_f
	,'0' as at_prov_id
	,'0' as at_prov_clm_id
	,'0' as at_prov_par_status_ind
	,0 as at_prov_typ_f
	,'0' as ad_prov_id
	,'0' as ad_prov_clm_id
	,'0' as ad_prov_par_status_ind
	,0 as ad_prov_typ_f
	,'0' as b_case_id
	,'0' as c_case_id
	,a.fin_source_name
	,a.migration_source
	,a.fin_product_level_3
	,a.tfm_include_flag
	,a.global_cap
	,a.nce_tadm_dec_risk_type
	,a.fin_contractpbp
	,'0' as fin_contract_nbr
	,'0' as fin_pbp
	,'0' as fin_submarket
	,a.fin_market
	,'0' as fin_region
	,a.fin_state
	,'0' as fin_plan_level_2
	,a.fin_g_i
	,a.fin_brand
	,'0' as group_number
	,'0' as aco
	,'0' as aco_network
	,'0' as group_name
	,'0' as fin_segment_name
	,'0' as fin_tfm_product
	,a.fin_mbi_hicn_fnl
	,0 as mbi_match_flag
	,a.sgr_source_name
	,a.fin_tfm_product_new 
	,a.fin_ps9_business_unit
	,a.fin_ps9_location
	,a.fin_ps9_operating_unit
	,a.fin_ps9_product
	,0 as initialfulladr_cases
	--,0 as initialpartialadr_cases
	,0 as persistentfulladr_cases
	--,0 as persistentpartialadr_cases
	,'0' as initial_dnl_decn_userid
	,'0' as initial_dnl_decn_user_role
	,cast(initial_dnl_decn_dttm as timestamp) as initial_dnl_decn_dttm
	,'0' as latest_dnl_decn_userid
	,'0' as latest_dnl_decn_user_role
	,cast(latest_dnl_decn_dttm as timestamp) as latest_dnl_decn_dttm
	,0 as md_escalation_ind
	,0 as icm_md_reviewed_ind
	,prim_diag_ahrq_genl_catgy_cd
	,prim_diag_ahrq_genl_catgy_desc
	,prim_diag_ahrq_diag_dtl_catgy_cd
	,prim_diag_ahrq_diag_dtl_catgy_desc
	,0 as `240_dx_md_escltn_in`
	,0 as ili_dx_ind
	,0 as covid_dx_ind
	,0 as `24_adj_dx_retain_ind`
	,'9999-99-99' as admit_exp_month
	,'9999-99-99' as admit_act_month
	,'9999-99-99' as dschg_exp_month
	,'9999-99-99' as dschg_act_month
	,'9999-99-99' as admit_exp_qtr
	,'9999-99-99' as admit_act_qtr
	,'9999-99-99' as dschg_exp_qtr
	,'9999-99-99' as dschg_act_qtr
	,0 as los
	,a.admission_date
	,'Y' as transplant_flag
	,0 as trans_cat_count
	,a.transplantdate
	,a.programlvl2 as transplant_type
	,0 as medsurg_overlap_ind
FROM
	HCE_OPS_FNL.HCEOPS_TRS_DATA_SET_FNL as a
left outer join
	HCE_OPS_ARCHV.hce_adr_avtar_like_2023_f as b
on  b.transplant_flag='Y'
and a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where b.fin_mbi_hicn_fnl is null and year(TRY_TO_DATE(a.transplantdate))=2023
;

--Step 2.2: Union non-notification TRS to rest of notifications
drop table if exists tmp_1m.ec_avtar_23_trs_combined;
create table tmp_1m.ec_avtar_23_trs_combined as
select 
	* from HCE_OPS_ARCHV.hce_adr_avtar_like_2023_f
union all select
	* from tmp_1m.ec_avtar_23_trs
	;

--Step 2.3: Adding variable to split PAC from IPA 
drop table if exists tmp_1m.ec_avtar_23_1_trs;
create table tmp_1m.ec_avtar_23_1_trs as
select 
	a.*
	,TRY_TO_DATE(admission_date) as trs_admit_dt
	,TRY_TO_DATE(transplantdate) as trs_transplant_dt
	,case when a.transplant_flag='Y' then 'Transplant'
		when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 'Medical'
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 'Surgical'
	 	when  a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('17 - Long Term Care','42 - Long Term Acute Care') 
	 		then 'LTAC'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and (a.case_cur_svc_cat_dtl_cd in ('31 - Skilled Nursing','46 - PAT Skilled Nursing') 
	 		or substr(a.plc_of_svc_cd,1,2) in ('31','16')) then 'SNF'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('35 - Therapy Services') and 
	 		substr(a.plc_of_svc_cd,1,2) in ('61','6') then 'AIR'
	 	else 'NA' end as IP_type
	 ,case 	when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1
		else 0 end as loc_flag
from tmp_1m.ec_avtar_23_trs_combined as a 
;

--Step 2.4: Create a date field that works for PAC & IPA (IPA cares about only closed cases while PAC cares about open and closed)
drop table if exists tmp_1m.ec_avtar_23_2_trs;
create table tmp_1m.ec_avtar_23_2_trs as
select     
	*
	,case when ip_type in ('Medical','Surgical','Transplant') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('Transplant') and admit_dt_act is null and admission_date is not null then trs_admit_dt
		when ip_type in ('Transplant')and admit_dt_act is null and admission_date is null then trs_transplant_dt
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is null then admit_dt_exp else null end as hcedt 
from tmp_1m.ec_avtar_23_1_trs
;


--Step 2.5: Add week & hce_month variable
drop table if exists tmp_1m.ec_avtar_23_3_trs;
create table tmp_1m.ec_avtar_23_3_trs as
select 
	a.*
	,concat(lpad(year(a.hcedt),4,0),lpad(month(a.hcedt),2,0)) as hce_admit_month
	,cast(hcedt as date) as hce_dt
	,c.week as admit_week
from tmp_1m.ec_avtar_23_2_trs as a 
left join tmp_2y.ec_loc_week_assign as c 
on a.hcedt=c.date
;

--Step 3.1: Getting remaining TRS cases that did not match to Notifications
drop table if exists tmp_1m.ec_avtar_24_trs;
create table tmp_1m.ec_avtar_24_trs as
select 
	0 as p2p_full_evertouched_cnt
	,0 as p2p_full_ovtn
	,'0' as p2p_match_ind
	,0 as mcr_reconsideration_ind
	,0 as mcr_evertouched_decn_ind
	,0 as mcr_ovtrn_ind
	,0 as mcr_uphelp_ind
	,0 as rvsl_ind
	,'0' as rvsl_decn_userid
	,cast(rvsl_decn_dttm as timestamp) as rvsl_decn_dttm
	,'0' as rvsl_decn_user_role
	,0 as mcr_rvsls
	,0 as rvsl_bed_decn_mtch_ind
	,0 as rvsl_srv_decn_mtch_ind
	,0 as appeal_ind
	,0 as appeal_ovrtn_ind
	,0 as oth_ovrtn_ind
	,'0' as appdecnmkr_user_id
	,'0' as appdecnmkr_user_nm
	,'0' as appdecnmkr_user_role
	,cast(appdecndt as timestamp) as appdecndt 
	,'0' as appoutcome
	,'0' as appmcrprevreviewfmd
	,'0' as appissuetype
	,'0' as hce_category
	,'0' as prim_srvc_cat
	,'0' as prim_srvc_sub_cat
	,'0' as business_segment
	,'0' as entity
	,a.fin_mbi_hicn_fnl as medicare_id
	,cast(a.dob as timestamp) as member_dob
	,a.fin_gender as member_sex
	,'0' as member_state
	,'0' as member_id
	,'0' as purchaser_id
	,'0' as subscriber_id
	,cast(create_dt as date) as create_dt  
	,0 as avtar_mtch_ind
	,concat(a.fin_mbi_hicn_fnl,a.transplantdate,a.programlvl2) as case_id
	,'0' as case_category_cd
	,'0' as svc_setting
	,cast(notif_recd_dttm as timestamp) as notif_recd_dttm 
	,'0' as notif_yrmonth
	,'0' as svc_seq_id
	,0 as svc_seq_nbr
	,'0' as proc_cd
	,'0' as prim_proc_ind
	,'0' as prim_diag_cd
	,'0' as icd_ver_cd
	,'0' as prim_proc_last_decn
	,0 as svc_freq
	,'0' as svc_freq_typ_cd
	,0 as proc_unit_cnt
	,'0' as svc_crmk_cd
	,cast(svc_start_dt as date) as svc_start_dt
	,cast(svc_end_dt as date) as svc_end_dt
	,'0' as svc_cat_cd
	,'0' as svc_cat_dtl_cd
	,'0' as plc_of_svc_cd
	,'0' as plc_of_svc_drv_cd
	,'0' as case_status_cd
	,'0' as case_status_rsn_cd
	,'0' as appeal
	,'0' as palist
	,'0' as prim_svc_palist
	,'0' as pa_program
	,cast(case_init_cur_decn_dttm as timestamp) as case_init_cur_decn_dttm
	,cast(case_init_svc_cur_decn_dttm as timestamp) as case_init_svc_cur_decn_dttm
	,'0' as adrcase_cancelled_ind
	,'0' as casedrv_cancelled_ind
	,'0' as serv_cancelled_ind
	,'0' as servdrv_cancelled_ind
	,'0' as ab_excl
	,'0' as adv_det_rate_exclusion
	,'0' as servdrv_prov_key
	,'0' as case_cur_svc_cat_dtl_cd
	,'0' as case_init_decn_cd
	,'0' as case_svc_init_decn_cd
	,'0' as case_decn_stat_cd
	,'0' as case_svc_decn_stat_cd
	,'0' as case_prov_par_status_cd
	,'0' as admit_cat_cd
	,'0' as auth_typ_cd
	,'0' as channel_cd
	,cast(a.admission_date as date) as admit_dt_act
	,cast(admit_dt_exp as date) as admit_dt_exp
	,cast(a.discharge_date as date) as dschg_dt_act
	,cast(dschg_dt_exp as date) as dschg_dt_exp
	,0 as bcrt_void_ind
	,'0' as ocm_migration
	,'0' as mnr_hce_drv_par_status
	,'0' as so_prov_id
	,'0' as so_prov_clm_id
	,'0' as so_prov_par_status_ind
	,0 as so_prov_typ_f
	,'0' as sj_prov_id
	,'0' as sj_prov_clm_id
	,'0' as sj_prov_par_status_ind
	,0 as sj_prov_typ_f
	,'0' as drv_cse_rf_prov_clm_id
	,'0' as drv_cse_rf_prov_key
	,'0' as drv_cse_rf_par_status
	,'0' as rf_prov_id
	,'0' as rf_prov_clm_id
	,'0' as rf_prov_par_status_ind
	,0 as rf_prov_typ_f
	,'0' as pc_prov_id
	,'0' as pc_prov_clm_id
	,'0' as pc_prov_par_status_ind
	,0 as pc_prov_typ_f
	,'0' as fa_prov_id
	,'0' as fa_prov_clm_id
	,'0' as fa_prov_par_status_ind
	,0 as fa_prov_typ_f
	,'0' as at_prov_id
	,'0' as at_prov_clm_id
	,'0' as at_prov_par_status_ind
	,0 as at_prov_typ_f
	,'0' as ad_prov_id
	,'0' as ad_prov_clm_id
	,'0' as ad_prov_par_status_ind
	,0 as ad_prov_typ_f
	,'0' as b_case_id
	,'0' as c_case_id
	,a.fin_source_name
	,a.migration_source
	,a.fin_product_level_3
	,a.tfm_include_flag
	,a.global_cap
	,a.nce_tadm_dec_risk_type
	,a.fin_contractpbp
	,'0' as fin_contract_nbr
	,'0' as fin_pbp
	,'0' as fin_submarket
	,a.fin_market
	,'0' as fin_region
	,a.fin_state
	,'0' as fin_plan_level_2
	,a.fin_g_i
	,a.fin_brand
	,'0' as group_number
	,'0' as aco
	,'0' as aco_network
	,'0' as group_name
	,'0' as fin_segment_name
	,'0' as fin_tfm_product
	,a.fin_mbi_hicn_fnl
	,0 as mbi_match_flag
	,a.sgr_source_name
	,a.fin_tfm_product_new 
	,a.fin_ps9_business_unit
	,a.fin_ps9_location
	,a.fin_ps9_operating_unit
	,a.fin_ps9_product
	,0 as initialfulladr_cases
	--,0 as initialpartialadr_cases
	,0 as persistentfulladr_cases
	--,0 as persistentpartialadr_cases
	,'0' as initial_dnl_decn_userid
	,'0' as initial_dnl_decn_user_role
	,cast(initial_dnl_decn_dttm as timestamp) as initial_dnl_decn_dttm
	,'0' as latest_dnl_decn_userid
	,'0' as latest_dnl_decn_user_role
	,cast(latest_dnl_decn_dttm as timestamp) as latest_dnl_decn_dttm
	,0 as md_escalation_ind
	,0 as icm_md_reviewed_ind
	,prim_diag_ahrq_genl_catgy_cd
	,prim_diag_ahrq_genl_catgy_desc
	,prim_diag_ahrq_diag_dtl_catgy_cd
	,prim_diag_ahrq_diag_dtl_catgy_desc
	,0 as `240_dx_md_escltn_in`
	,0 as ili_dx_ind
	,0 as covid_dx_ind
	,0 as `24_adj_dx_retain_ind`
	,'9999-99-99' as admit_exp_month
	,'9999-99-99' as admit_act_month
	,'9999-99-99' as dschg_exp_month
	,'9999-99-99' as dschg_act_month
	,'9999-99-99' as admit_exp_qtr
	,'9999-99-99' as admit_act_qtr
	,'9999-99-99' as dschg_exp_qtr
	,'9999-99-99' as dschg_act_qtr
	,0 as los
	,a.admission_date
	,'Y' as transplant_flag
	,0 as trans_cat_count
	,a.transplantdate
	,a.programlvl2 as transplant_type
	,0 as medsurg_overlap_ind
FROM
	HCE_OPS_FNL.HCEOPS_TRS_DATA_SET_FNL as a
left outer join
	HCE_OPS_ARCHV.hce_adr_avtar_like_2024_f as b 
on  b.transplant_flag='Y'
and a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where b.fin_mbi_hicn_fnl is null and year(TRY_TO_DATE(a.transplantdate))=2024
;



--Step 3.2: Union non-notification TRS to rest of notifications
drop table if exists tmp_1m.ec_avtar_24_trs_combined;
create table tmp_1m.ec_avtar_24_trs_combined as
select 
	* from HCE_OPS_ARCHV.hce_adr_avtar_like_2024_f 
union all select
	* from tmp_1m.ec_avtar_24_trs
	;


--Step 3.3:Run this every week: Add week to the table with recent data that Pradeepa emails about 
drop table if exists tmp_1m.ec_avtar_24_1_trs;
create table tmp_1m.ec_avtar_24_1_trs as
select 
	a.*
	,TRY_TO_DATE(admission_date) as trs_admit_dt
	,TRY_TO_DATE(transplantdate) as trs_transplant_dt
	,case when a.transplant_flag='Y' then 'Transplant'
		when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 'Medical'
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 'Surgical'
	 	when  a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('17 - Long Term Care','42 - Long Term Acute Care') 
	 		then 'LTAC'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and (a.case_cur_svc_cat_dtl_cd in ('31 - Skilled Nursing','46 - PAT Skilled Nursing') 
	 		or substr(a.plc_of_svc_cd,1,2) in ('31','16')) then 'SNF'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('35 - Therapy Services') and 
	 		substr(a.plc_of_svc_cd,1,2) in ('61','6') then 'AIR'
	 	else 'NA' end as IP_type
	,case when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1
		else 0 end as loc_flag
from tmp_1m.ec_avtar_24_trs_combined as a 
;

--Step 3.4: Create a date field that works for PAC & IPA (IPA cares about only closed cases while PAC cares about open and closed)
drop table if exists tmp_1m.ec_avtar_24_2_trs;
create table tmp_1m.ec_avtar_24_2_trs as
select     
	*
	,case when ip_type in ('Medical','Surgical','Transplant') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('Transplant') and admit_dt_act is null and admission_date is not null then trs_admit_dt
		when ip_type in ('Transplant')and admit_dt_act is null and admission_date is null then trs_transplant_dt
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is null then admit_dt_exp else null end as hcedt
from tmp_1m.ec_avtar_24_1_trs
;

--Step 3.5: Add week & hce_month variable
drop table if exists tmp_1m.ec_avtar_24_3_trs;
create table tmp_1m.ec_avtar_24_3_trs as
select 
	a.*
	,concat(lpad(year(a.hcedt),4,0),lpad(month(a.hcedt),2,0)) as hce_admit_month
	,cast(hcedt as date) as hce_dt
	,c.week as admit_week
from tmp_1m.ec_avtar_24_2_trs as a 
left join tmp_2y.ec_loc_week_assign as c 
on a.hcedt=c.date 
;

--Step 4.1: Getting remaining TRS cases that did not match to Notifications
drop table if exists tmp_1m.ec_avtar_25_26_trs;
create table tmp_1m.ec_avtar_25_26_trs as
select 
	0 as p2p_full_evertouched_cnt
	,0 as p2p_full_ovtn
	,'0' as p2p_match_ind
	,0 as mcr_reconsideration_ind
	,0 as mcr_evertouched_decn_ind
	,0 as mcr_ovtrn_ind
	,0 as mcr_uphelp_ind
	,0 as rvsl_ind
	,'0' as rvsl_decn_userid
	,cast(rvsl_decn_dttm as timestamp) as rvsl_decn_dttm
	,'0' as rvsl_decn_user_role
	,0 as mcr_rvsls
	,0 as rvsl_bed_decn_mtch_ind
	,0 as rvsl_srv_decn_mtch_ind
	,0 as appeal_ind
	,0 as appeal_ovrtn_ind
	,0 as oth_ovrtn_ind
	,'0' as appdecnmkr_user_id
	,'0' as appdecnmkr_user_nm
	,'0' as appdecnmkr_user_role
	,cast(appdecndt as timestamp) as appdecndt 
	,'0' as appoutcome
	,'0' as appmcrprevreviewfmd
	,'0' as appissuetype
	,'0' as hce_category
	,'0' as prim_srvc_cat
	,'0' as prim_srvc_sub_cat
	,'0' as business_segment
	,'0' as entity
	,a.fin_mbi_hicn_fnl as medicare_id
	,cast(a.dob as timestamp) as member_dob
	,a.fin_gender as member_sex
	,'0' as member_state
	,'0' as member_id
	,'0' as purchaser_id
	,'0' as subscriber_id
	,cast(create_dt as date) as create_dt  
	,0 as avtar_mtch_ind
	,concat(a.fin_mbi_hicn_fnl,a.transplantdate,a.programlvl2) as case_id
	,'0' as case_category_cd
	,'0' as svc_setting
	,cast(notif_recd_dttm as timestamp) as notif_recd_dttm 
	,'0' as notif_yrmonth
	,'0' as svc_seq_id
	,0 as svc_seq_nbr
	,'0' as proc_cd
	,'0' as prim_proc_ind
	,'0' as prim_diag_cd
	,'0' as icd_ver_cd
	,'0' as prim_proc_last_decn
	,0 as svc_freq
	,'0' as svc_freq_typ_cd
	,0 as proc_unit_cnt
	,'0' as svc_crmk_cd
	,cast(svc_start_dt as date) as svc_start_dt
	,cast(svc_end_dt as date) as svc_end_dt
	,'0' as svc_cat_cd
	,'0' as svc_cat_dtl_cd
	,'0' as plc_of_svc_cd
	,'0' as plc_of_svc_drv_cd
	,'0' as case_status_cd
	,'0' as case_status_rsn_cd
	,'0' as appeal
	,'0' as palist
	,'0' as prim_svc_palist
	,'0' as pa_program
	,cast(case_init_cur_decn_dttm as timestamp) as case_init_cur_decn_dttm
	,cast(case_init_svc_cur_decn_dttm as timestamp) as case_init_svc_cur_decn_dttm
	,'0' as adrcase_cancelled_ind
	,'0' as casedrv_cancelled_ind
	,'0' as serv_cancelled_ind
	,'0' as servdrv_cancelled_ind
	,'0' as ab_excl
	,'0' as adv_det_rate_exclusion
	,'0' as servdrv_prov_key
	,'0' as case_cur_svc_cat_dtl_cd
	,'0' as case_init_decn_cd
	,'0' as case_svc_init_decn_cd
	,'0' as case_decn_stat_cd
	,'0' as case_svc_decn_stat_cd
	,'0' as case_prov_par_status_cd
	,'0' as admit_cat_cd
	,'0' as auth_typ_cd
	,'0' as channel_cd
	,'' as svcdecn_curr_gap_otcm_cd /*added in on 8/6/25 to account for two new fields added to avtar table*/
	,'' as category_description /*added in on 8/6/25 to account for two new fields added to avtar table*/
	,cast(a.admission_date as date) as admit_dt_act
	,cast(admit_dt_exp as date) as admit_dt_exp
	,cast(a.discharge_date as date) as dschg_dt_act
	,cast(dschg_dt_exp as date) as dschg_dt_exp
	,0 as bcrt_void_ind
	,'0' as ocm_migration
	,'0' as mnr_hce_drv_par_status
	,'0' as so_prov_id
	,'0' as so_prov_clm_id
	,'0' as so_prov_par_status_ind
	,0 as so_prov_typ_f
	,'0' as sj_prov_id
	,'0' as sj_prov_clm_id
	,'0' as sj_prov_par_status_ind
	,0 as sj_prov_typ_f
	,'0' as drv_cse_rf_prov_clm_id
	,'0' as drv_cse_rf_prov_key
	,'0' as drv_cse_rf_par_status
	,'0' as rf_prov_id
	,'0' as rf_prov_clm_id
	,'0' as rf_prov_par_status_ind
	,0 as rf_prov_typ_f
	,'0' as pc_prov_id
	,'0' as pc_prov_clm_id
	,'0' as pc_prov_par_status_ind
	,0 as pc_prov_typ_f
	,'0' as fa_prov_id
	,'0' as fa_prov_clm_id
	,'0' as fa_prov_par_status_ind
	,0 as fa_prov_typ_f
	,'0' as at_prov_id
	,'0' as at_prov_clm_id
	,'0' as at_prov_par_status_ind
	,0 as at_prov_typ_f
	,'0' as ad_prov_id
	,'0' as ad_prov_clm_id
	,'0' as ad_prov_par_status_ind
	,0 as ad_prov_typ_f
	,'0' as b_case_id
	,'0' as c_case_id
	,a.fin_source_name
	,a.migration_source
	,a.fin_product_level_3
	,a.tfm_include_flag
	,a.global_cap
	,a.nce_tadm_dec_risk_type
	,a.fin_contractpbp
	,'0' as fin_contract_nbr
	,'0' as fin_pbp
	,'0' as fin_submarket
	,a.fin_market
	,'0' as fin_region
	,a.fin_state
	,'0' as fin_plan_level_2
	,a.fin_g_i
	,a.fin_brand
	,'0' as group_number
	,'0' as aco
	,'0' as aco_network
	,'0' as group_name
	,'0' as fin_segment_name
	,'0' as fin_tfm_product
	,a.fin_mbi_hicn_fnl
	,0 as mbi_match_flag
	,a.sgr_source_name
	,a.fin_tfm_product_new 
	,a.fin_ps9_business_unit
	,a.fin_ps9_location
	,a.fin_ps9_operating_unit
	,a.fin_ps9_product
	,0 as initialfulladr_cases
	--,0 as initialpartialadr_cases--
	,0 as persistentfulladr_cases
	--,0 as persistentpartialadr_cases--
	,'0' as initial_dnl_decn_userid
	,'0' as initial_dnl_decn_user_role
	,cast(initial_dnl_decn_dttm as timestamp) as initial_dnl_decn_dttm
	,'0' as latest_dnl_decn_userid
	,'0' as latest_dnl_decn_user_role
	,cast(latest_dnl_decn_dttm as timestamp) as latest_dnl_decn_dttm
	,0 as md_escalation_ind
	,0 as icm_md_reviewed_ind
	,prim_diag_ahrq_genl_catgy_cd
	,prim_diag_ahrq_genl_catgy_desc
	,prim_diag_ahrq_diag_dtl_catgy_cd
	,prim_diag_ahrq_diag_dtl_catgy_desc
	,0 as `240_dx_md_escltn_in`
	,0 as ili_dx_ind
	,0 as covid_dx_ind
	,0 as `24_adj_dx_retain_ind`
	,'9999-99-99' as admit_exp_month
	,'9999-99-99' as admit_act_month
	,'9999-99-99' as dschg_exp_month
	,'9999-99-99' as dschg_act_month
	,'9999-99-99' as admit_exp_qtr
	,'9999-99-99' as admit_act_qtr
	,'9999-99-99' as dschg_exp_qtr
	,'9999-99-99' as dschg_act_qtr
	,0 as los
	,a.admission_date
	,'Y' as transplant_flag
	,0 as trans_cat_count
	,a.transplantdate
	,a.programlvl2 as transplant_type
	,0 as medsurg_overlap_ind
FROM
	HCE_OPS_FNL.HCEOPS_TRS_DATA_SET_FNL as a
left outer join
	HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F as b
on  b.transplant_flag='Y'
and a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where b.fin_mbi_hicn_fnl is null and year(TRY_TO_DATE(a.transplantdate))>2024
;

--Step 4.2: Union non-notification TRS to rest of notifications
drop table if exists tmp_1m.ec_avtar_25_26_trs_combined;
create table tmp_1m.ec_avtar_25_26_trs_combined as
select 
	* from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_F
union all select
	* from tmp_1m.ec_avtar_25_26_trs
	;


--Step 4.3 :Run this every week: Add week to the table with recent data that Pradeepa emails about 
drop table if exists tmp_1m.ec_avtar_25_26_1;
create table tmp_1m.ec_avtar_25_26_1 as
select 
	a.*
	,TRY_TO_DATE(admission_date) as trs_admit_dt
	,TRY_TO_DATE(transplantdate) as trs_transplant_dt
	,case when a.transplant_flag='Y' then 'Transplant'
		when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 'Medical'
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 'Surgical'
	 	when  a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('17 - Long Term Care','42 - Long Term Acute Care') 
	 		then 'LTAC'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and (a.case_cur_svc_cat_dtl_cd in ('31 - Skilled Nursing','46 - PAT Skilled Nursing') 
	 		or substr(a.plc_of_svc_cd,1,2) in ('31','16')) then 'SNF'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('35 - Therapy Services') and 
	 		substr(a.plc_of_svc_cd,1,2) in ('61','6') then 'AIR'
	 	else 'NA' end as IP_type
	 ,case 	when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1
		else 0 end as loc_flag
from tmp_1m.ec_avtar_25_26_trs_combined as a 
;


--Step 4.4: Create a date field that works for PAC & IPA (IPA cares about only closed cases while PAC cares about open and closed)
drop table if exists tmp_1m.ec_avtar_25_26_2;
create table tmp_1m.ec_avtar_25_26_2 as
select     
	*
	,case when ip_type in ('Medical','Surgical','Transplant') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('Transplant') and admit_dt_act is null and admission_date is not null then trs_admit_dt
		when ip_type in ('Transplant')and admit_dt_act is null and admission_date is null then trs_transplant_dt
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is null then admit_dt_exp else null end as hcedt
from tmp_1m.ec_avtar_25_26_1
;

--Step 4.5: Add week & hce_month variable
drop table if exists tmp_1m.ec_avtar_25_26_3;
create table tmp_1m.ec_avtar_25_26_3 as
select 
	a.*
	,concat(lpad(year(a.hcedt),4,0),lpad(month(a.hcedt),2,0)) as hce_admit_month
	,cast(hcedt as date) as hce_dt
	,c.week as admit_week
from tmp_1m.ec_avtar_25_26_2 as a 
left join tmp_2y.ec_loc_week_assign as c 
on a.hcedt=c.date 
;



--Step 5: union together all needed notifications from the AvTar Report after Pradeepa sends the weekly email - update date of run! 
--Note: Respiratory AND leading indicator flags need to be based source of truth table tmp_1y.hce_resp_2025 and have periods in the ICDs unlike claims
drop table if exists tmp_1m.kn_ip_dataset_01072026_trs; 
create table tmp_1m.kn_ip_dataset_01072026_trs as 
select 
	admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,TO_VARCHAR(hce_dt,'yyyy') as admit_year
	,admit_dt_act
	,admit_dt_exp
	,dschg_dt_act
	,'Auths' as service_month
	,case when admit_dt_act is not null then concat(year(admit_dt_act),lpad(month(admit_dt_act),2,0)) else concat(year(admit_dt_exp),lpad(month(admit_dt_exp),2,0))
		end as create_mth
	,'Auths' as component
	,entity 
	,IP_type
	,loc_flag
	,svc_setting
	,case_cur_svc_cat_dtl_cd
	,migration_source
	,case when migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	,case when fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
	,fin_tfm_product_new
	,tfm_include_flag
	,global_cap
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_brand
	,fin_g_i
	,fin_product_level_3
	,fin_plan_level_2
	,case when fin_brand ='M&R' then fin_market
		when fin_brand='C&S' then fin_state end as fin_market
	,fin_contractpbp
	,group_number 
	,group_name
	,BUSINESS_SEGMENT
	,case when admit_dt_act is not null and dschg_dt_act is not null then 'DISCHARGED' when admit_dt_act is not null and dschg_dt_act is null then 'OPEN' else null end as do_ind
	,mnr_hce_drv_par_status as par_nonpar
	,substr(fa_prov_id,2,9) as prov_tin
	,case when ((global_cap='NA' and (sgr_source_name = 'COSMOS' or sgr_source_name='CSP')) OR
		(sgr_source_name = 'NICE' AND (nce_tadm_dec_risk_type='FFS' or nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end as capitated
	,case when los<=1 then '1'
		when los=2 then '2'
		when los=3 then '3'
		when los between 4 and 5 then '4-5'
		when los between 6 and 10 then '6-10'
		when los between 11 and 30 then '11-30'
		when los >30 then '31+' else 'NA' end as los_categories
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff('day',admit_dt_act,current_date())
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff('day',admit_dt_exp,current_date())
		else los end as los_exp
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF('day',admit_dt_act,current_date()) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff('day',admit_dt_act,dschg_dt_act)
		end as los_act
	,case when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83') then 'ILI' else 'NA' end as respiratory_flag 
     ,case when ip_type = 'Transplant' then 'Transplant' 
     	when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83') then 'ILI'
    	when admit_cat_cd in ('17 - Medical') then 'Medical'
    	when admit_cat_cd in ('30 - Surgical')	then 'Surgical'	
    	when IP_type in ('LTAC') then 'LTAC'
    	when IP_type in ('SNF') then 'SNF' 
    	when IP_type in ('AIR') then 'AIR' else 'Medical' end as ipa_li_split 
    ,prim_diag_ahrq_genl_catgy_desc 
  --  ,prim_diag_ahrq_diag_dtl_catgy_desc 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end as leading_ind_pop
 	,CASE WHEN fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end as MnR_NICE_FFS_Flag
 	,case when (fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
 	,case when fin_brand='M&R' and migration_source='OAH' then 1 else 0 end as MnR_OAH_flag 
 	,case when (fin_brand='C&S' and migration_source='OAH') then 1 
 		WHEN (TO_VARCHAR(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_Flag 
	,ocm_migration 
	,case when appeal_ind=1 and initialfulladr_cases=1 then 1 else 0 end as appealed_cases
	,case when appeal_ovrtn_ind=1 then 1 else 0 end as overturned_cases
	,case when initialfulladr_cases=1 and appeal_ind=1 and md_escalation_ind=1 then 1 else 0 end as md_rev_appeals
	,case when auth_typ_cd='1 - PreService' then 1 else 0 end as pre_auth_cases
	,case when initialfulladr_cases=1 and md_escalation_ind=1 and appeal_ovrtn_ind=1 and appeal_ind=1 then 1 else 0 end as md_review_overturn
	,count(distinct case when case_svc_init_decn_cd='AD - Fully Adverse Determination' then case_id end) as first_adverse 
	,count(distinct case when case_svc_init_decn_cd<>'FA - Fully Approved' then case_id end) as first_not_approved_srvc 
	,count(distinct case when case_init_decn_cd<>'FA - Fully Approved' then case_id end) as first_not_approved_case 
	,count(distinct case_id) as case_count					
    ,count(distinct (case when initialfulladr_cases=1 then case_id end)) as Initial_ADR_cnt				
    ,count(distinct (case when persistentfulladr_cases=1 then case_id end)) as Persistent_ADR_cnt					
    ,count(distinct (case when icm_md_reviewed_ind=1 then case_id end)) as MD_Reviewed_cnt
    ,count(distinct (case when initialfulladr_cases=1 AND Appeal_ind=1 then case_id  end )) as Appeal_case_cnt
    ,count(distinct (case when Appeal_ovrtn_Ind=1 then case_id  end )) as Appeal_Ovrtn_case_cnt
    ,count(distinct (case when mcr_reconsideration_ind=1 then case_id  end )) as MCR_Reconsideration_case_cnt
    ,count(distinct (case when MCR_Ovtrn_ind=1  then case_id  end )) as MCR_Ovrtn_case_cnt
    ,count(distinct (case when P2P_full_evertouched_cnt=1 then case_id  end )) as P2P_case_cnt
    ,count(distinct (case when P2P_full_ovtn=1  then case_id  end )) as P2P_Ovrtn_case_cnt
    ,count(distinct (case when P2P_full_ovtn=0 and Appeal_ovrtn_Ind=0 and MCR_Ovtrn_ind=0 and initialfulladr_cases=1 and  persistentfulladr_cases=0 then case_id end)) as Other_ovtrns
	,0 as membership
from tmp_1m.ec_avtar_25_26_3
where 	
 	   fin_brand in ('M&R','C&S')
       and ((IP_type in ('Medical','Surgical','Transplant') and TO_VARCHAR(admit_dt_act, 'MM/dd/yyyy') is not null) or IP_type in ('LTAC','SNF','AIR'))
 group by 
admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,TO_VARCHAR(hce_dt,'yyyy') 
	,admit_dt_act
	,admit_dt_exp
	,dschg_dt_act 
	,case when admit_dt_act is not null then concat(year(admit_dt_act),lpad(month(admit_dt_act),2,0)) else concat(year(admit_dt_exp),lpad(month(admit_dt_exp),2,0)) end 
	,entity 
	,IP_type
	,loc_flag
	,svc_setting
	,case_cur_svc_cat_dtl_cd
	,migration_source
	,case when migration_source='OAH' then 'OAH' else 'Non-OAH' end 
	,case when fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end 
	,fin_tfm_product_new
	,tfm_include_flag
	,global_cap
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_brand
	,fin_g_i
	,fin_product_level_3
	,fin_plan_level_2
	,case when fin_brand ='M&R' then fin_market
		when fin_brand='C&S' then fin_state end 
	,fin_contractpbp
	,group_number
	,group_name 
	,BUSINESS_SEGMENT
	,case when admit_dt_act is not null and dschg_dt_act is not null then 'DISCHARGED' when admit_dt_act is not null and dschg_dt_act is null then 'OPEN' else null end
	,mnr_hce_drv_par_status 
	,substr(fa_prov_id,2,9)
	,case when ((global_cap='NA' and (sgr_source_name = 'COSMOS' or sgr_source_name='CSP')) OR
		(sgr_source_name = 'NICE' AND (nce_tadm_dec_risk_type='FFS' or nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end 
	,case when los<=1 then '1'
		when los=2 then '2'
		when los=3 then '3'
		when los between 4 and 5 then '4-5'
		when los between 6 and 10 then '6-10'
		when los between 11 and 30 then '11-30'
		when los >30 then '31+' else 'NA' end 
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff('day',admit_dt_act,current_date())
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff('day',admit_dt_exp,current_date())
		else los end 
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF('day',admit_dt_act,current_date()) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff('day',admit_dt_act,dschg_dt_act)
		end 
--    ,ili_dx_ind 
--    ,covid_dx_ind 
	 ,case when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83') then 'ILI' else 'NA' end
     ,case when ip_type = 'Transplant' then 'Transplant' 
     	when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83') then 'ILI'
    	when admit_cat_cd in ('17 - Medical') then 'Medical'
    	when admit_cat_cd in ('30 - Surgical')	then 'Surgical'	
    	when IP_type in ('LTAC') then 'LTAC'
    	when IP_type in ('SNF') then 'SNF' 
    	when IP_type in ('AIR') then 'AIR' else 'Medical' end
    ,prim_diag_ahrq_genl_catgy_desc 
 --   ,prim_diag_ahrq_diag_dtl_catgy_desc 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end
 	,CASE WHEN fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end 
 	,case when (fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end 
 	,case when fin_brand='M&R' and migration_source='OAH' then 1 else 0 end 
 	,case when (fin_brand='C&S' and migration_source='OAH') then 1 
 		WHEN (TO_VARCHAR(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end 
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end 
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (YEAR(hce_dt) ='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end
 	,ocm_migration
 	,case when appeal_ind=1 and initialfulladr_cases=1 then 1 else 0 end
	,case when appeal_ovrtn_ind=1 then 1 else 0 end 
	,case when initialfulladr_cases=1 and appeal_ind=1 and md_escalation_ind=1 then 1 else 0 end
	,case when auth_typ_cd='1 - PreService' then 1 else 0 end 
	,case when initialfulladr_cases=1 and md_escalation_ind=1 and appeal_ovrtn_ind=1 and appeal_ind=1 then 1 else 0 end 
 union all select 
	admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,TO_VARCHAR(hce_dt,'yyyy') as admit_year
	,admit_dt_act
	,admit_dt_exp
	,dschg_dt_act
	,'Auths' as service_month
	,case when admit_dt_act is not null then concat(year(admit_dt_act),lpad(month(admit_dt_act),2,0)) else concat(year(admit_dt_exp),lpad(month(admit_dt_exp),2,0))
		end as create_mth
	,'Auths' as component
	,entity 
	,IP_type
	,loc_flag
	,svc_setting
	,case_cur_svc_cat_dtl_cd
	,migration_source
	,case when migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	,case when fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
	,fin_tfm_product_new
	,tfm_include_flag
	,global_cap
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_brand
	,fin_g_i
	,fin_product_level_3
	,fin_plan_level_2
	,case when fin_brand ='M&R' then fin_market
		when fin_brand='C&S' then fin_state end as fin_market
	,fin_contractpbp
	,group_number 
	,group_name
	,BUSINESS_SEGMENT
	,case when admit_dt_act is not null and dschg_dt_act is not null then 'DISCHARGED' when admit_dt_act is not null and dschg_dt_act is null then 'OPEN' else null end as do_ind
	,mnr_hce_drv_par_status as par_nonpar
	,substr(fa_prov_id,2,9) as prov_tin
	,case when ((global_cap='NA' and (sgr_source_name = 'COSMOS' or sgr_source_name='CSP')) OR
		(sgr_source_name = 'NICE' AND (nce_tadm_dec_risk_type='FFS' or nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end as capitated
	,case when los<=1 then '1'
		when los=2 then '2'
		when los=3 then '3'
		when los between 4 and 5 then '4-5'
		when los between 6 and 10 then '6-10'
		when los between 11 and 30 then '11-30'
		when los >30 then '31+' else 'NA' end as los_categories
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff('day',admit_dt_act,current_date())
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff('day',admit_dt_exp,current_date())
		else los end as los_exp
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF('day',admit_dt_act,current_date()) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff('day',admit_dt_act,dschg_dt_act)
		end as los_act
	,case when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI' else 'NA' end as respiratory_flag 
    ,case when ip_type = 'Transplant' then 'Transplant' 
     	when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI'
    	when admit_cat_cd in ('17 - Medical') then 'Medical'
    	when admit_cat_cd in ('30 - Surgical')	then 'Surgical'	
    	when IP_type in ('LTAC') then 'LTAC'
    	when IP_type in ('SNF') then 'SNF' 
    	when IP_type in ('AIR') then 'AIR' else 'Medical' end  as ipa_li_split 
    ,prim_diag_ahrq_genl_catgy_desc 
  --  ,prim_diag_ahrq_diag_dtl_catgy_desc 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end as leading_ind_pop
 	,CASE WHEN fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end as MnR_NICE_FFS_Flag
 	,case when (fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
 	,case when fin_brand='M&R' and migration_source='OAH' then 1 else 0 end as MnR_OAH_flag 
 	,case when (fin_brand='C&S' and migration_source='OAH') then 1 
 		WHEN (TO_VARCHAR(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	,ocm_migration 
	,case when appeal_ind=1 and initialfulladr_cases=1 then 1 else 0 end as appealed_cases
	,case when appeal_ovrtn_ind=1 then 1 else 0 end as overturned_cases
	,case when initialfulladr_cases=1 and appeal_ind=1 and md_escalation_ind=1 then 1 else 0 end as md_rev_appeals
	,case when auth_typ_cd='1 - PreService' then 1 else 0 end as pre_auth_cases
	,case when initialfulladr_cases=1 and md_escalation_ind=1 and appeal_ovrtn_ind=1 and appeal_ind=1 then 1 else 0 end as md_review_overturn
	,count(distinct case when case_svc_init_decn_cd='AD - Fully Adverse Determination' then case_id end) as first_adverse 
	,count(distinct case when case_svc_init_decn_cd<>'FA - Fully Approved' then case_id end) as first_not_approved_srvc 
	,count(distinct case when case_init_decn_cd<>'FA - Fully Approved' then case_id end) as first_not_approved_case 
	,count(distinct case_id) as case_count					
    ,count(distinct (case when initialfulladr_cases=1 then case_id end)) as Initial_ADR_cnt				
    ,count(distinct (case when persistentfulladr_cases=1 then case_id end)) as Persistent_ADR_cnt					
    ,count(distinct (case when icm_md_reviewed_ind=1 then case_id end)) as MD_Reviewed_cnt
    ,count(distinct (case when initialfulladr_cases=1 AND Appeal_ind=1 then case_id  end )) as Appeal_case_cnt
    ,count(distinct (case when Appeal_ovrtn_Ind=1 then case_id  end )) as Appeal_Ovrtn_case_cnt
    ,count(distinct (case when mcr_reconsideration_ind=1 then case_id  end )) as MCR_Reconsideration_case_cnt
    ,count(distinct (case when MCR_Ovtrn_ind=1  then case_id  end )) as MCR_Ovrtn_case_cnt
    ,count(distinct (case when P2P_full_evertouched_cnt=1 then case_id  end )) as P2P_case_cnt
    ,count(distinct (case when P2P_full_ovtn=1  then case_id  end )) as P2P_Ovrtn_case_cnt
    ,count(distinct (case when P2P_full_ovtn=0 and Appeal_ovrtn_Ind=0 and MCR_Ovtrn_ind=0 and initialfulladr_cases=1 and  persistentfulladr_cases=0 then case_id end)) as Other_ovtrns
	,0 as membership
from tmp_1m.ec_avtar_24_3_trs
where 	
 	   fin_brand in ('M&R','C&S')
       and ((IP_type in ('Medical','Surgical','Transplant') and TO_VARCHAR(admit_dt_act, 'MM/dd/yyyy') is not null) or IP_type in ('LTAC','SNF','AIR'))
       and (TO_VARCHAR(hce_dt,'yyyy') in ('2023') OR  TO_VARCHAR(hce_dt,'yyyyMM')='202401')
 group by 
admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,TO_VARCHAR(hce_dt,'yyyy') 
	,admit_dt_act
	,admit_dt_exp
	,dschg_dt_act 
	,case when admit_dt_act is not null then concat(year(admit_dt_act),lpad(month(admit_dt_act),2,0)) else concat(year(admit_dt_exp),lpad(month(admit_dt_exp),2,0)) end 
	,entity 
	,IP_type
	,loc_flag
	,svc_setting
	,case_cur_svc_cat_dtl_cd
	,migration_source
	,case when migration_source='OAH' then 'OAH' else 'Non-OAH' end 
	,case when fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end 
	,fin_tfm_product_new
	,tfm_include_flag
	,global_cap
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_brand
	,fin_g_i
	,fin_product_level_3
	,fin_plan_level_2
	,case when fin_brand ='M&R' then fin_market
		when fin_brand='C&S' then fin_state end
	,fin_contractpbp
	,group_number
	,group_name 
	,BUSINESS_SEGMENT
	,case when admit_dt_act is not null and dschg_dt_act is not null then 'DISCHARGED' when admit_dt_act is not null and dschg_dt_act is null then 'OPEN' else null end
	,mnr_hce_drv_par_status 
	,substr(fa_prov_id,2,9)
	,case when ((global_cap='NA' and (sgr_source_name = 'COSMOS' or sgr_source_name='CSP')) OR
		(sgr_source_name = 'NICE' AND (nce_tadm_dec_risk_type='FFS' or nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end 
	,case when los<=1 then '1'
		when los=2 then '2'
		when los=3 then '3'
		when los between 4 and 5 then '4-5'
		when los between 6 and 10 then '6-10'
		when los between 11 and 30 then '11-30'
		when los >30 then '31+' else 'NA' end 
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff('day',admit_dt_act,current_date())
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff('day',admit_dt_exp,current_date())
		else los end 
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF('day',admit_dt_act,current_date()) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff('day',admit_dt_act,dschg_dt_act)
		end 
--    ,ili_dx_ind 
--    ,covid_dx_ind 
    ,case when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI' else 'NA' end
    ,case when ip_type = 'Transplant' then 'Transplant' 
     	when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI'
    	when admit_cat_cd in ('17 - Medical') then 'Medical'
    	when admit_cat_cd in ('30 - Surgical')	then 'Surgical'	
    	when IP_type in ('LTAC') then 'LTAC'
    	when IP_type in ('SNF') then 'SNF' 
    	when IP_type in ('AIR') then 'AIR' else 'Medical' end 
    ,prim_diag_ahrq_genl_catgy_desc 
 --   ,prim_diag_ahrq_diag_dtl_catgy_desc 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end
 	,CASE WHEN fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end 
 	,case when (fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end 
 	,case when fin_brand='M&R' and migration_source='OAH' then 1 else 0 end 
 	,case when (fin_brand='C&S' and migration_source='OAH') then 1 
 		WHEN (TO_VARCHAR(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end 
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end 
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end
 	,ocm_migration
 	,case when appeal_ind=1 and initialfulladr_cases=1 then 1 else 0 end
	,case when appeal_ovrtn_ind=1 then 1 else 0 end 
	,case when initialfulladr_cases=1 and appeal_ind=1 and md_escalation_ind=1 then 1 else 0 end
	,case when auth_typ_cd='1 - PreService' then 1 else 0 end 
	,case when initialfulladr_cases=1 and md_escalation_ind=1 and appeal_ovrtn_ind=1 and appeal_ind=1 then 1 else 0 end 
 union all select 
	000000 as admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,TO_VARCHAR(hce_dt,'yyyy') as admit_year
	,admit_dt_act
	,admit_dt_exp
	,dschg_dt_act
	,'Auths' as service_month
	,case when admit_dt_act is not null then concat(year(admit_dt_act),lpad(month(admit_dt_act),2,0)) else concat(year(admit_dt_exp),lpad(month(admit_dt_exp),2,0)) 
		end as create_mth
	,'Auths' as component
	,entity 
	,IP_type
	,loc_flag
	,svc_setting
	,case_cur_svc_cat_dtl_cd
	,migration_source
	,case when migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	,case when fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
	,fin_tfm_product_new
	,tfm_include_flag
	,global_cap
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_brand
	,fin_g_i
	,fin_product_level_3
	,fin_plan_level_2
	,case when fin_brand ='M&R' then fin_market
		when fin_brand='C&S' then fin_state end as fin_market
	,fin_contractpbp
	,group_number
	,group_name 
	,BUSINESS_SEGMENT
	,case when admit_dt_act is not null and dschg_dt_act is not null then 'DISCHARGED' when admit_dt_act is not null and dschg_dt_act is null then 'OPEN' else null end as do_ind
	,mnr_hce_drv_par_status as par_nonpar
	,substr(fa_prov_id,2,9) as prov_tin
	,case when ((global_cap='NA' and (sgr_source_name = 'COSMOS' or sgr_source_name='CSP')) OR
		(sgr_source_name = 'NICE' AND (nce_tadm_dec_risk_type='FFS' or nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end as capitated
	,case when los<=1 then '1'
		when los=2 then '2'
		when los=3 then '3'
		when los between 4 and 5 then '4-5'
		when los between 6 and 10 then '6-10'
		when los between 11 and 30 then '11-30'
		when los >30 then '31+' else 'NA' end as los_categories
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff('day',admit_dt_act,current_date())
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff('day',admit_dt_exp,current_date())
		else los end as los_exp
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF('day',admit_dt_act,current_date()) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff('day',admit_dt_act,dschg_dt_act)
		end as los_act
--   ,ili_dx_ind 
--    ,covid_dx_ind 
    ,case when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI' else 'NA' end as respiratory_flag 
    ,case when ip_type = 'Transplant' then 'Transplant' 
     	when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI'
    	when admit_cat_cd in ('17 - Medical') then 'Medical'
    	when admit_cat_cd in ('30 - Surgical')	then 'Surgical'	
    	when IP_type in ('LTAC') then 'LTAC'
    	when IP_type in ('SNF') then 'SNF' 
    	when IP_type in ('AIR') then 'AIR' else 'Medical' end  as ipa_li_split 
    ,prim_diag_ahrq_genl_catgy_desc 
  --  ,prim_diag_ahrq_diag_dtl_catgy_desc 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end as leading_ind_pop
 	,CASE WHEN fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end as MnR_NICE_FFS_Flag
 	,case when (fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
 	,case when fin_brand='M&R' and migration_source='OAH' then 1 else 0 end as MnR_OAH_flag 
 	,case when (fin_brand='C&S' and migration_source='OAH') then 1 
 		WHEN (TO_VARCHAR(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag	
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	,'NA' as ocm_migration
	,case when appeal_ind=1 and initialfulladr_cases=1 then 1 else 0 end as appealed_cases
	,case when appeal_ovrtn_ind=1 then 1 else 0 end as overturned_cases
	,case when initialfulladr_cases=1 and appeal_ind=1 and md_escalation_ind=1 then 1 else 0 end as md_rev_appeals
	,case when auth_typ_cd='1 - PreService' then 1 else 0 end as pre_auth_cases
	,case when initialfulladr_cases=1 and md_escalation_ind=1 and appeal_ovrtn_ind=1 and appeal_ind=1 then 1 else 0 end as md_review_overturn
	,count(distinct case when case_svc_init_decn_cd='AD - Fully Adverse Determination' then case_id end) as first_adverse 
	,count(distinct case when case_svc_init_decn_cd<>'FA - Fully Approved' then case_id end) as first_not_approved_srvc 
	,count(distinct case when case_init_decn_cd<>'FA - Fully Approved' then case_id end) as first_not_approved_case 
	,count(distinct case_id) as case_count					
    ,count(distinct (case when initialfulladr_cases=1 then case_id end)) as Initial_ADR_cnt				
    ,count(distinct (case when persistentfulladr_cases=1 then case_id end)) as Persistent_ADR_cnt					
    ,count(distinct (case when icm_md_reviewed_ind=1 then case_id end)) as MD_Reviewed_cnt
    ,count(distinct (case when initialfulladr_cases=1 AND Appeal_ind=1 then case_id  end )) as Appeal_case_cnt
    ,count(distinct (case when Appeal_ovrtn_Ind=1 then case_id  end )) as Appeal_Ovrtn_case_cnt
    ,count(distinct (case when mcr_reconsideration_ind=1 then case_id  end )) as MCR_Reconsideration_case_cnt
    ,count(distinct (case when MCR_Ovtrn_ind=1  then case_id  end )) as MCR_Ovrtn_case_cnt
    ,count(distinct (case when P2P_full_evertouched_cnt=1 then case_id  end )) as P2P_case_cnt
    ,count(distinct (case when P2P_full_ovtn=1  then case_id  end )) as P2P_Ovrtn_case_cnt
    ,count(distinct (case when P2P_full_ovtn=0 and Appeal_ovrtn_Ind=0 and MCR_Ovtrn_ind=0 and initialfulladr_cases=1 and  persistentfulladr_cases=0 then case_id end)) as Other_ovtrns
	,0 as membership
from tmp_1m.ec_avtar_23_3_trs
where 	
		fin_brand in ('M&R','C&S')
       and ((IP_type in ('Medical','Surgical','Transplant') and TO_VARCHAR(admit_dt_act, 'MM/dd/yyyy') is not null) or IP_type in ('LTAC','SNF','AIR'))
--       and TO_VARCHAR(admit_dt_act  ,'yyyy') in ('2022')		
 group by 
--		000000 as admit_week
	hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,TO_VARCHAR(hce_dt,'yyyy') 
	,admit_dt_act
	,admit_dt_exp
	,dschg_dt_act
	,case when admit_dt_act is not null then concat(year(admit_dt_act),lpad(month(admit_dt_act),2,0)) else concat(year(admit_dt_exp),lpad(month(admit_dt_exp),2,0)) end 
	,entity 
	,IP_type
	,loc_flag
	,svc_setting
	,case_cur_svc_cat_dtl_cd
	,migration_source
	,case when migration_source='OAH' then 'OAH' else 'Non-OAH' end 
	,case when fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end 
	,fin_tfm_product_new
	,tfm_include_flag
	,global_cap
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_brand
	,fin_g_i
	,fin_product_level_3
	,fin_plan_level_2	
	,case when fin_brand ='M&R' then fin_market
		when fin_brand='C&S' then fin_state end 
	,fin_contractpbp
	,group_number
	,group_name 
	,BUSINESS_SEGMENT
	,case when admit_dt_act is not null and dschg_dt_act is not null then 'DISCHARGED' when admit_dt_act is not null and dschg_dt_act is null then 'OPEN' else null end 
	,mnr_hce_drv_par_status 
	,substr(fa_prov_id,2,9) 
	,case when ((global_cap='NA' and (sgr_source_name = 'COSMOS' or sgr_source_name='CSP')) OR
		(sgr_source_name = 'NICE' AND (nce_tadm_dec_risk_type='FFS' or nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end 
	,case when los<=1 then '1'
		when los=2 then '2'
		when los=3 then '3'
		when los between 4 and 5 then '4-5'
		when los between 6 and 10 then '6-10'
		when los between 11 and 30 then '11-30'
		when los >30 then '31+' else 'NA' end 
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff('day',admit_dt_act,current_date())
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff('day',admit_dt_exp,current_date())
		else los end 
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF('day',admit_dt_act,current_date()) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff('day',admit_dt_act,dschg_dt_act)
		end 
--    ,ili_dx_ind 
--    ,covid_dx_ind 
    ,case when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI' else 'NA' end
	,case when ip_type = 'Transplant' then 'Transplant' 
     	when prim_diag_cd in ('B97.29', 'J02.48', 'U07.1', 'J12.82','J12.81') then 'COVID-19'
    	when prim_diag_cd in ('079.99','382.9','460','461.9','465.8','465.9','466.0','466.19','486','487.0','487.1','487.8','488','488.0','488.01','488.02','488.09','488.1','488.11','488.12',
    		'488.19','490','780.6','780.60','786.2','B97.10','B97.89','H66.90','H66.91','H66.92','H66.93','J00','J01.90','J01.91','J06.9','J09','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10',
    		'J10.0','J10.00','J10.01','J10.08','J10.1','J10.2','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11.0','J11.00','J11.08','J11.1','J11.2','J11.8','J11.81','J11.82','J11.83','J11.89',
    		'J12.2','J12.89','J12.9','J18.0','J18.1','J18.2','J18.8','J18.9','J20.0','J20.1','J20.2','J20.3','J20.4','J20.6','J20.7','J20.8','J20.9','J22','J40','J80','J98.8','R05','R05.1','R05.2',
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83','A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI'
    	when admit_cat_cd in ('17 - Medical') then 'Medical'
    	when admit_cat_cd in ('30 - Surgical')	then 'Surgical'	
    	when IP_type in ('LTAC') then 'LTAC'
    	when IP_type in ('SNF') then 'SNF' 
    	when IP_type in ('AIR') then 'AIR' else 'Medical' end
    ,prim_diag_ahrq_genl_catgy_desc 
 -- ,prim_diag_ahrq_diag_dtl_catgy_desc 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end 
 	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end 
 	,CASE WHEN fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end 
 	,case when (fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end 
 	,case when fin_brand='M&R' and migration_source='OAH' then 1 else 0 end 
 	,case when (fin_brand='C&S' and migration_source='OAH') then 1 
 		WHEN (TO_VARCHAR(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end 
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end 
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end
 	,case when appeal_ind=1 and initialfulladr_cases=1 then 1 else 0 end 
	,case when appeal_ovrtn_ind=1 then 1 else 0 end
	,case when initialfulladr_cases=1 and appeal_ind=1 and md_escalation_ind=1 then 1 else 0 end 
	,case when auth_typ_cd='1 - PreService' then 1 else 0 end 
	,case when initialfulladr_cases=1 and md_escalation_ind=1 and appeal_ovrtn_ind=1 and appeal_ind=1 then 1 else 0 end 
 --	,ocm_migration
 ;


--Step 6: Adding in Other Needed Variables & Swing Bed based on PAC Provider list 
drop table if exists tmp_1m.kn_ip_dataset_01072026_2_trs; 
create table tmp_1m.kn_ip_dataset_01072026_2_trs as 
select 
	a.*
	,case when a.IP_type='SNF' and b.class='IP_SWGBED' then 1 
		else 0 end as swgbed
	,case when a.fin_product_level_3<>'INSTITUTIONAL' AND a.TFM_INCLUDE_FLAG=1 AND a.CAPITATED=0 AND a.BUSINESS_SEGMENT='MnR' then 'M&R'
		WHEN a.fin_product_level_3='DUAL' AND a.TFM_INCLUDE_FLAG=0 AND a.CAPITATED=0 AND (a.MIGRATION_SOURCE<>'OAH' or a.migration_source is null) AND a.BUSINESS_SEGMENT='CnS' 
			then 'C&S' else 'Other' end as MR_CS_Other
from tmp_1m.kn_ip_dataset_01072026_trs as a
left join tmp_1y.hk_snf_swgbed_tins2 as b
on a.prov_tin=b.prov_tin
;


--Step 7: Adding in a IPA/PAC split now that SWGBED is split out 
drop table if exists tmp_1m.kn_ip_dataset_01072026_3_trs; 
create table tmp_1m.kn_ip_dataset_01072026_3_trs as 
select 
	*
	,case when swgbed=1 then 'Swing Bed'
		when IP_Type in ('LTAC','SNF','AIR') then IP_type else ipa_li_split end as admit_type
	,case when swgbed=1 then 'PAC'
		when IP_type in ('LTAC','SNF','AIR') then 'PAC'
		when IP_type in ('Medical','Surgical','Transplant') then 'IPA' else 'NA' end as IPA_PAC_flag
from  tmp_1m.kn_ip_dataset_01072026_2_trs
;


--Step 8: Roll up before join to MM 
drop table if exists tmp_1m.kn_ip_dataset_01072026_4_trs; 
create table tmp_1m.kn_ip_dataset_01072026_4_trs as 
select 	
	a.admit_week
	,a.hce_admit_month
--	,a.admit_act_qtr
	,a.admit_year
	,'Auths' as fst_srvc_month
	,'' as adjd_yrmonth
	,a.component
	,a.entity
	,a.ip_type
	,a.loc_flag
	,a.svc_setting
	,a.case_cur_svc_cat_dtl_cd
	,a.migration_source
	,a.total_oah_flag
	,a.institutional_flag
	,a.fin_tfm_product_new
	,a.tfm_include_flag
	,a.global_cap
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type
	,a.fin_brand
	,a.fin_g_i
	,a.fin_product_level_3
	,a.fin_plan_level_2
	,a.fin_market
	,a.fin_contractpbp
	,a.group_number
	,a.group_name
	,a.do_ind
	,a.par_nonpar
	,a.prov_tin
	,d.collection as Hospital_Group
	,a.capitated
	,a.los_categories
	,a.los_exp
	,0 as length_of_stay
--	,a.ili_dx_ind
--	,a.covid_dx_ind
	,a.respiratory_flag
	,a.ipa_li_split
--	,a.prim_diag_ahrq_genl_catgy_desc
	,a.mnr_cosmos_ffs_flag
	,a.leading_ind_pop
	,a.mnr_nice_ffs_flag
	,a.mnr_total_ffs_flag
	,a.mnr_oah_flag
	,a.cns_oah_flag
	,a.mnr_dual_flag
	,a.cns_dual_flag
	,a.ocm_migration
	,a.swgbed
	,a.mr_cs_other
	,a.admit_type
	,a.ipa_pac_flag
	,a.first_adverse
	,a.first_not_approved_srvc
	,a.first_not_approved_case
	,a.md_review_overturn
	,sum(a.appealed_cases) as appealed_cases
	,sum(a.overturned_cases) as overturned_cases
	,sum(a.md_rev_appeals) as md_rev_appeals
	,sum(a.pre_auth_cases) as pre_auth_cases
	,sum(a.case_count) as case_count
	,sum(a.Initial_ADR_cnt) as Initial_ADR_cnt
	,sum(a.persistent_adr_cnt) as persistent_adr_cnt
	,sum(a.md_reviewed_cnt) as md_reviewed_cnt
	,sum(a.appeal_case_cnt) as appeal_case_cnt
	,sum(a.appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
	,sum(a.mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
	,sum(a.mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
	,sum(a.p2p_case_cnt) as p2p_case_cnt
	,sum(a.p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
	,sum(a.other_ovtrns) as other_ovtrns
	,sum(a.membership) as membership
	,0 as days
	,0 as frank_days
	,0 as admits
	,0 as allowed
	,0 as netpaid
	,0 as franky_paid
	,0 as franky_admits
	,0 as franky_allw
from tmp_1m.kn_ip_dataset_01072026_3_trs as a
left join tmp_1y.tin_collection as d
on a.prov_tin = d.tin 
group by 
		a.admit_week
	,a.hce_admit_month
--	,a.admit_act_qtr
	,a.admit_year
	,a.component
	,a.entity
	,a.ip_type
	,a.loc_flag
	,a.svc_setting
	,a.case_cur_svc_cat_dtl_cd
	,a.migration_source
	,a.total_oah_flag
	,a.institutional_flag
	,a.fin_tfm_product_new
	,a.tfm_include_flag
	,a.global_cap
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type
	,a.fin_brand
	,a.fin_g_i
	,a.fin_product_level_3
	,a.fin_plan_level_2
	,a.fin_market
	,a.fin_contractpbp
	,a.group_number
	,a.group_name
	,a.do_ind
	,a.par_nonpar
	,a.prov_tin
	,d.collection
	,a.capitated
	,a.los_categories
	,a.los_exp
--	,a.ili_dx_ind
--	,a.covid_dx_ind
	,a.respiratory_flag
	,a.ipa_li_split
--	,a.prim_diag_ahrq_genl_catgy_desc
	,a.mnr_cosmos_ffs_flag
	,a.leading_ind_pop
	,a.mnr_nice_ffs_flag
	,a.mnr_total_ffs_flag
	,a.mnr_oah_flag
	,a.cns_oah_flag
	,a.mnr_dual_flag
	,a.cns_dual_flag
	,a.ocm_migration
	,a.swgbed
	,a.mr_cs_other
	,a.admit_type
	,a.ipa_pac_flag
	,a.first_adverse
	,a.first_not_approved_srvc
	,a.first_not_approved_case
	,a.md_review_overturn
;

--Step 9: Pulling Member Months
drop table if exists tmp_1m.kn_ip_dataset_01072026_mm; 
create table tmp_1m.kn_ip_dataset_01072026_mm as 
select 
	000000 as fin_inc_week
	,a.fin_inc_month
--	,a.fin_inc_qtr
	,a.fin_inc_year 
	,'MM' as fst_srvc_month
	,'MM' as adjd_yrmonth
	,'Membership' as component
	,'MM' as entity
	,'MM' as ip_type
	,1 as loc_flag
	,'MM' as svc_setting
	,'MM' as case_cur_svc_cat_dtl_cd
	,a.migration_source
	,case when a.migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	,case when a.fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
	,a.fin_tfm_product_new
	,a.tfm_include_flag
	,a.global_cap
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type
	,a.fin_brand
	,a.fin_g_i
	,a.fin_product_level_3
	,a.fin_plan_level_2
	,case when a.fin_brand ='M&R' then a.fin_market
		when a.fin_brand='C&S' then a.fin_state end as fin_market
	,a.fin_contractpbp
	,a.tadm_group_nbr_consist 
	,b.group_name
	,'MM' as do_ind
	,'MM' as par_nonpar
	,'MM' as prov_tin
	,'MM' as Hospital_Group
	,case when ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP')) OR (a.sgr_source_name = 'NICE' AND (a.nce_tadm_dec_risk_type='FFS' 
		or a.nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end as capitated
	,'MM' as los_categories
	,0 as los_exp
	,0 as length_of_stay
--	,0 as ili_dx_ind 
--  ,0 as covid_dx_ind 
	,'MM' as respiratory_flag
	,'MM' as ipa_li_split
--	,'MM' as prim_diag_ahrq_genl_catgy_desc 
	,CASE WHEN a.fin_brand='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.fin_product_level_3 <>'INSTITUTIONAL' AND a.tfm_include_flag=1 
		THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 
		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end as leading_ind_pop
	,CASE WHEN a.fin_brand='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end as MnR_NICE_FFS_Flag
	,case when (a.fin_brand='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
		OR (a.fin_brand='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
	,case when a.fin_brand='M&R' and a.migration_source='OAH' then 1 else 0 end as MnR_OAH_flag
 	,case when (a.fin_brand='C&S' and a.migration_source='OAH') then 1 
 		WHEN (a.fin_inc_year='2024' AND a.fin_brand='C&S' AND a.GLOBAL_CAP = 'NA' AND a.SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND a.MIGRATION_SOURCE = 'OAH' AND a.FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag
	,case when a.fin_brand='M&R' and a.fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	,CASE WHEN ((a.fin_brand in('C&S') and a.migration_source <> 'OAH' and a.global_cap = 'NA' and a.fin_product_level_3='DUAL' AND
		a.SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (a.fin_inc_year ='2024' AND a.fin_brand in ('C&S')
		AND a.GLOBAL_CAP = 'NA' AND a.SGR_SOURCE_NAME IN ('COSMOS','CSP') AND a.MIGRATION_SOURCE = 'OAH' AND a.FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	,'NA' as ocm_migration
	,0 as swgbed
	,case when a.fin_product_level_3<>'INSTITUTIONAL' AND a.TFM_INCLUDE_FLAG=1 AND ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP'))
			OR (a.sgr_source_name = 'NICE' AND (a.nce_tadm_dec_risk_type='FFS' or a.nce_tadm_dec_risk_type='PHYSICIAN'))) AND a.fin_brand='M&R' then 'M&R'  
		WHEN a.fin_product_level_3='DUAL' AND a.TFM_INCLUDE_FLAG=0 AND ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP')) OR 
			(a.sgr_source_name = 'NICE' AND (a.nce_tadm_dec_risk_type='FFS' or a.nce_tadm_dec_risk_type='PHYSICIAN'))) AND (a.MIGRATION_SOURCE<>'OAH' 
			or a.migration_source is null) AND a.fin_brand='C&S' then 'C&S' else 'Other' end as MR_CS_Other
	,'MM' as admit_type
	,'MM' as ipa_pac_flag
	,0 as first_adverse
	,0 as first_not_approved_srvc
	,0 as first_not_approved_case
	,0 as md_review_overturn
	,0 as appealed_cases
	,0 as overturned_cases
	,0 as md_rev_appeals
	,0 as pre_auth_cases
	,0 as case_count
	,0 as Initial_ADR_cnt
	,0 as Persistent_ADR_cnt
	,0 as MD_Reviewed_cnt
	,0 as Appeal_case_cnt
	,0 as Appeal_Ovrtn_case_cnt
	,0 as MCR_Reconsideration_case_cnt
	,0 as MCR_Ovrtn_case_cnt
	,0 as P2P_case_cnt
	,0 as P2P_Ovrtn_case_cnt
	,0 as Other_ovtrns
	,SUM(a.fin_member_cnt) as membership
	,0 as days
	,0 as frank_days
	,0 as admits
	,0 as allowed
	,0 as netpaid
	,0 as franky_paid
	,0 as franky_admits
	,0 as franky_allw
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_${membership_month} as a
left join fichsrv.group_crosswalk as b
		on a.tadm_group_nbr_consist = b.group_number  
		and a.fin_inc_year = b.year
where fin_inc_year in ('2023','2024','2025','2026')
group by 
		a.fin_inc_month
--	,a.fin_inc_qtr
	,a.fin_inc_year 
	,a.migration_source
	,case when a.migration_source='OAH' then 'OAH' else 'Non-OAH' end
	,case when a.fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end 
	,a.fin_tfm_product_new
	,a.tfm_include_flag
	,a.global_cap
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type
	,a.fin_brand
	,a.fin_g_i
	,a.fin_product_level_3
	,a.fin_plan_level_2
	,case when a.fin_brand ='M&R' then a.fin_market
		when a.fin_brand='C&S' then a.fin_state end 
	,a.fin_contractpbp
	,a.tadm_group_nbr_consist 
	,b.group_name
	,case when ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP')) OR (a.sgr_source_name = 'NICE' AND 
		(a.nce_tadm_dec_risk_type='FFS' or a.nce_tadm_dec_risk_type='PHYSICIAN'))) then 0 else 1 end
	,CASE WHEN a.fin_brand='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.fin_product_level_3 <>'INSTITUTIONAL' AND a.tfm_include_flag=1 
		THEN 1 else 0 end 
	,CASE WHEN fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1 AND 
		fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end 
	,CASE WHEN a.fin_brand='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end 
	,case when (a.fin_brand='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.fin_product_level_3 <>'INSTITUTIONAL' AND  
		tfm_include_flag=1) OR (a.fin_brand='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end
	,case when a.fin_brand='M&R' and a.migration_source='OAH' then 1 else 0 end 
 	,case when (a.fin_brand='C&S' and a.migration_source='OAH') then 1 
 		WHEN (a.fin_inc_year='2024' AND a.fin_brand='C&S' AND a.GLOBAL_CAP = 'NA' AND a.SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND a.MIGRATION_SOURCE = 'OAH' AND a.FIN_STATE = 'MD') THEN 0 else 0 end 
	,case when a.fin_brand='M&R' and a.fin_product_level_3='DUAL' then 1 else 0 end 
	,CASE WHEN ((a.fin_brand in('C&S') and a.migration_source <> 'OAH' and a.global_cap = 'NA' and a.fin_product_level_3='DUAL' AND
		a.SGR_SOURCE_NAME in('COSMOS','CSP') ) OR (a.fin_inc_year ='2024' AND a.fin_brand in ('C&S')
		AND a.GLOBAL_CAP = 'NA' AND a.SGR_SOURCE_NAME IN ('COSMOS','CSP') AND a.MIGRATION_SOURCE = 'OAH' AND a.FIN_STATE = 'MD')) then 1 else 0 end
	,case when a.fin_product_level_3<>'INSTITUTIONAL' AND a.TFM_INCLUDE_FLAG=1 AND ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP'))
			OR (a.sgr_source_name = 'NICE' AND (a.nce_tadm_dec_risk_type='FFS' or a.nce_tadm_dec_risk_type='PHYSICIAN'))) AND a.fin_brand='M&R' then 'M&R'  
		WHEN a.fin_product_level_3='DUAL' AND a.TFM_INCLUDE_FLAG=0 AND ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP')) OR 
			(a.sgr_source_name = 'NICE' AND (a.nce_tadm_dec_risk_type='FFS' or a.nce_tadm_dec_risk_type='PHYSICIAN'))) AND (a.MIGRATION_SOURCE<>'OAH' 
				or a.migration_source is null) AND a.fin_brand='C&S' then 'C&S' else 'Other' end 
;


--Step 10: Combine notifications and membership
drop table if exists tmp_1m.kn_ip_dataset_notif_01072026_trs;
create table tmp_1m.kn_ip_dataset_notif_01072026_trs as				
SELECT	
	*
	from tmp_1m.kn_ip_dataset_01072026_4_trs
union all select 
	* from tmp_1m.kn_ip_dataset_01072026_mm
	; 



/************************************************************************************************************************************************************/
--------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------If this is a weekly update, proceed to STEP 28----------------------------------------------------------------
------------------------------------------------If this is a monthly claims update, proceed to STEP 11--------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------
/************************************************************************************************************************************************************/

/*

--Step 11.1: Grabbing archived 2022 
drop table if exists tmp_1y.ec_ip_dataset_claims_cosmos_2022;
create table tmp_1y.ec_ip_dataset_claims_cosmos_2022 as				
SELECT
--CLAIM-RELATED FIELDS
	a.SITE_CLM_AUD_NBR
	,a.COMPONENT
	,a.ADMITID
	,a.SUB_AUD_NBR
	,a.DTL_LN_NBR
	,a.CLM_REC_CD
	,a.EVENTKEY
	,a.TADM_ADMIT_TYPE
	,a.TADM_MDC
	,a.ADMIT_QTR
	,a.ADMIT_YR_MONTH
	,a.ADMIT_START_DT
	,a.ADMIT_END_DT
	,a.FST_SRVC_DT
	,a.ERLY_SRVC_QTR
	,a.ERLY_SRVC_DT
	,a.fst_srvc_month
	,a.DSCHRG_STS_CD
	,a.CATGY_ROL_UP_2_DESC
	,a.BRAND_FNL 
	,a.CLM_DNL_F
	,a.admit_rnk
	,c.week as admit_week
--CLAIM ADJUDICATION FIELDS
	,a.BIL_RECV_DT
	,a.ADJD_QTR
	,a.ADJD_DT
	,a.CLM_PD_DT
	,a.FNL_RSN_CD_SYS_ID
	,a.CLM_LVL_RSN_CD_SYS_ID
	,a.SRVC_LVL_RSN_CD_SYS_ID
--DEMOGRAPHIC FIELDS
	,a.PLAN_LEVEL_1_FNL
	,a.PLAN_LEVEL_2_FNL
	,a.PRODUCT_LEVEL_1_FNL
	,a.PRODUCT_LEVEL_2_FNL
	,a.PRODUCT_LEVEL_3_FNL
	,a.REGION_FNL
	,a.MARKET_FNL
	,a.FIN_SUBMARKET
	,a.GLOBAL_CAP
	,a.GROUP_IND_FNL
	,a.TFM_INCLUDE_FLAG
	,a.MIGRATION_SOURCE
	,a.GROUPNUMBER
	,a.SEGMENT_NAME_FNL
	,a.CONTRACTPBP_FNL
	,a.CONTRACT_FNL
--PATIENT FIELDS
	,a.GAL_MBI_HICN_FNL
	,a.BTH_DT
	,a.GDR_CD
--PROVIDER FIELDS
	,a.MPIN
	,a.SRVC_PROV_ID
	,a.PROV_TIN
	,a.FULL_NM
	,a.PROV_PRTCP_STS_CD
	,a.CLM_PL_OF_SRVC_DESC
	,a.COS_PROV_SPCL_CD
--SERVICE FIELDS
	,a.FNL_DRG_CD
	,a.CLM_ADMIT_TYPE
	,a.FNL_ADMIT_TYP
	,a.PROC_CD
	,a.icd_2
	,a.icd_3
	,a.icd_4
	,a.icd_5
	,case when a.pd_dn_ol_tadm_admit_type  IN ('IP_HSP','IP_MATNB','IP_MEDSURGICU','IP_NICUEXTSTAY','IP_TRANS') then 'IPA'
		when a.pd_dn_ol_tadm_admit_type  in ('IP_LTAC','IP_REHAB','IP_SNF','IP_REHAB','IP_SWGBED') then 'PAC'
		else 'Other' end as ipa_pac_flag 
	,a.denial_f
	,case when b.drg_med_surg_txt in ('UNKNOWN','**','MED') then 'MEDICAL' 
		when a.fnl_drg_cd is null then 'MEDICAL' 
		when b.drg_med_surg_txt in ('SURG','SURGICAL') then 'SURGICAL' else b.drg_med_surg_txt end as med_surg
	,case when primary_diag_cd in ('B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09X1','J09X2','J09X3','J09X9','J1000','J1001','J1008','J101','J102','J1081','J1082',
	    	'J1083','J1089','J1100','J1108','J111','J112','J1181','J1182','J1183','J1189','J122','J129','J188','J189','J200','J201','J202','J203','J204','J205','J206','J207','J208','J209','J40','R05',
	    	'R502','R509','R5081','R6883','J181','R052','R053','R054','R058','R059','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410',
	    	'J411','J418') then 1 
    	when icd_2 in ('B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09X1','J09X2','J09X3','J09X9','J1000','J1001','J1008','J101','J102','J1081','J1082',
	    	'J1083','J1089','J1100','J1108','J111','J112','J1181','J1182','J1183','J1189','J122','J129','J188','J189','J200','J201','J202','J203','J204','J205','J206','J207','J208','J209','J40','R05',
	    	'R502','R509','R5081','R6883','J181','R052','R053','R054','R058','R059','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410',
	    	'J411','J418') then 1 else 0 end as ili_dx_ind 
    ,case when primary_diag_cd in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
    	when primary_diag_cd in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
	    	'78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
	    	'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
	    	'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410',
	    	'J411','J418') then 'ILI'
    	when icd_2 in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
    	when icd_2 in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
	    	'78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
	    	'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
	    	'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410',
	    	'J411','J418') then 'ILI'
    		 else 'NA' end as respiratory_flag 
    ,case when a.migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
    ,case when a.product_level_3_fnl ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
    ,datediff(DAY,a.pd_dn_ol_admit_start_dt,a.pd_dn_ol_admit_end_dt) as los 
	,a.PRIMARY_DIAG_CD
	,a.AHRQ_DIAG_GENL_CATGY_DESC
	,a.AHRQ_DIAG_DTL_CATGY_DESC
	,a.RVNU_CD
	,a.ICD_VER_CD
	,a.tfm_product_new_fnl
--COUNTING FIELDS
	,a.ADMIT_IPRVUWGT
	,a.SBMT_CHRG_AMT
	,a.ALLW_AMT_FNL
	,a.NET_PD_AMT_FNL
	,a.TADM_ADMITS
	,a.TADM_QTYDAYS
	,a.pd_dn_ol_admitid 
	,a.pd_dn_ol_admit_start_dt 
	,a.pd_dn_ol_admit_end_dt 
	,a.pd_dn_ol_tadm_admit_type 
	,a.ip_status_code
	,case when dateadd(day,-10, last_day(a.adjd_dt))>=a.adjd_dt then TO_VARCHAR(a.adjd_dt,'yyyyMM') else TO_VARCHAR(add_months(a.adjd_dt,1),'yyyyMM') end as adjd_yrmonth
	,TO_VARCHAR(a.pd_dn_ol_admit_start_dt,'yyyyMM') as pd_dn_ol_admit_yrmonth
from tadm_tre_cpy.glxy_ip_admit_f_${claims_month} as a
left join tmp_2y.ec_glxy_drg_code as b
	on a.fnl_drg_cd = b.drg_cd
	and a.ADMIT_START_DT between b.drg_row_eff_dt and b.drg_row_end_dt
left join tmp_2y.ec_loc_week_assign as c 
	on a.pd_dn_ol_admit_start_dt =c.date 
where year(a.pd_dn_ol_admit_start_dt)='2022'
;

--QA Check that only 2022 is in the table above 
select distinct year(pd_dn_ol_admit_start_dt) from tmp_1y.ec_ip_dataset_claims_cosmos_2022;
*/

--Step 11: Inital COSMOS Pull that will go through Frankenstein runout process
drop table if exists tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date};
create table tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date} as				
SELECT
/*CLAIM-RELATED FIELDS*/
	a.SITE_CLM_AUD_NBR
	,a.COMPONENT
	,a.ADMITID
	,a.SUB_AUD_NBR
	,a.DTL_LN_NBR
	,a.CLM_REC_CD
	,a.EVENTKEY
	,a.TADM_ADMIT_TYPE
	,a.TADM_MDC
	,a.ADMIT_QTR
	,a.ADMIT_YR_MONTH
	,a.ADMIT_START_DT
	,a.ADMIT_END_DT
	,a.FST_SRVC_DT
	,a.ERLY_SRVC_QTR
	,a.ERLY_SRVC_DT
	,a.fst_srvc_month
	,a.DSCHRG_STS_CD
	,a.CATGY_ROL_UP_2_DESC
	,a.BRAND_FNL 
	,a.CLM_DNL_F
	,a.admit_rnk
	,c.week as admit_week
/*CLAIM ADJUDICATION FIELDS*/
	,a.BIL_RECV_DT
	,a.ADJD_QTR
	,a.ADJD_DT
	,a.CLM_PD_DT
	,a.FNL_RSN_CD_SYS_ID
	,a.CLM_LVL_RSN_CD_SYS_ID
	,a.SRVC_LVL_RSN_CD_SYS_ID

/*DEMOGRAPHIC FIELDS*/
	,a.PLAN_LEVEL_1_FNL
	,a.PLAN_LEVEL_2_FNL
	,a.PRODUCT_LEVEL_1_FNL
	,a.PRODUCT_LEVEL_2_FNL
	,a.PRODUCT_LEVEL_3_FNL
	,a.REGION_FNL
	,a.MARKET_FNL
	,a.FIN_SUBMARKET
	,a.GLOBAL_CAP
	,a.GROUP_IND_FNL
	,a.TFM_INCLUDE_FLAG
	,a.MIGRATION_SOURCE
	,a.GROUPNUMBER
	,a.SEGMENT_NAME_FNL
	,a.CONTRACTPBP_FNL
	,a.CONTRACT_FNL

/*PATIENT FIELDS*/
	,a.GAL_MBI_HICN_FNL
	,a.BTH_DT
	,a.GDR_CD

/*PROVIDER FIELDS*/
	,a.MPIN
	,a.SRVC_PROV_ID
	,a.PROV_TIN
	,a.FULL_NM
	,a.PROV_PRTCP_STS_CD
	,a.CLM_PL_OF_SRVC_DESC
	,a.COS_PROV_SPCL_CD

/*SERVICE FIELDS*/
	,a.FNL_DRG_CD
	,a.CLM_ADMIT_TYPE
	,a.FNL_ADMIT_TYP
	,a.PROC_CD
	,a.icd_2
	,a.icd_3
	,a.icd_4
	,a.icd_5
	,case when a.pd_dn_ol_tadm_admit_type  IN ('IP_HSP','IP_MATNB','IP_MEDSURGICU','IP_NICUEXTSTAY','IP_TRANS') then 'IPA'
		when a.pd_dn_ol_tadm_admit_type  in ('IP_LTAC','IP_REHAB','IP_SNF','IP_REHAB','IP_SWGBED') then 'PAC'
		else 'Other' end as ipa_pac_flag 

	,a.denial_f
	,case when b.drg_med_surg_txt in ('UNKNOWN','**','MED','MEDICAL') then 'MEDICAL' 
		when a.fnl_drg_cd is null then 'MEDICAL' 
		when b.drg_med_surg_txt in ('SURG','SURGICAL') then 'SURGICAL' else b.drg_med_surg_txt end as med_surg
	,case when primary_diag_cd in ('B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09X1','J09X2','J09X3','J09X9','J1000','J1001','J1008','J101','J102','J1081','J1082',
	    	'J1083','J1089','J1100','J1108','J111','J112','J1181','J1182','J1183','J1189','J122','J129','J188','J189','J200','J201','J202','J203','J204','J205','J206','J207','J208','J209','J40','R05',
	    	'R502','R509','R5081','R6883','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410',
	    	'J411','J418') then 1 
    	when icd_2 in ('B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09X1','J09X2','J09X3','J09X9','J1000','J1001','J1008','J101','J102','J1081','J1082',
	    	'J1083','J1089','J1100','J1108','J111','J112','J1181','J1182','J1183','J1189','J122','J129','J188','J189','J200','J201','J202','J203','J204','J205','J206','J207','J208','J209','J40','R05',
	    	'R502','R509','R5081','R6883','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410',
	    	'J411','J418') then 1 else 0 end as ili_dx_ind 
    ,case when primary_diag_cd in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
    	when primary_diag_cd in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
	    	'78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
	    	'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
	    	'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883','A3700',
	    	'A3701','A3710','A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 'ILI'
    	when icd_2 in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
    	when icd_2 in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
	    	'78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
	    	'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
	    	'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883',
	    	'A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 'ILI'
    		 else 'NA' end as respiratory_flag 
    ,case when a.migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
    ,case when a.product_level_3_fnl ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
    ,datediff('day' ,a.pd_dn_ol_admit_start_dt, a.pd_dn_ol_admit_end_dt) as los 
	,a.PRIMARY_DIAG_CD
	,a.AHRQ_DIAG_GENL_CATGY_DESC
	,a.AHRQ_DIAG_DTL_CATGY_DESC
	,a.RVNU_CD
	,a.ICD_VER_CD
	,a.tfm_product_new_fnl
/*COUNTING FIELDS*/
	,a.ADMIT_IPRVUWGT
	,a.SBMT_CHRG_AMT
	,a.ALLW_AMT_FNL
	,a.NET_PD_AMT_FNL
	,a.TADM_ADMITS
	,a.TADM_QTYDAYS
	,a.pd_dn_ol_admitid 
	,a.pd_dn_ol_admit_start_dt 
	,a.pd_dn_ol_admit_end_dt 
	,a.pd_dn_ol_tadm_admit_type 
	,a.ip_status_code
	,case when dateadd(day,-10, last_day(a.adjd_dt))>=a.adjd_dt then TO_VARCHAR(a.adjd_dt,'yyyyMM') else TO_VARCHAR(add_months(a.adjd_dt,1),'yyyyMM') end as adjd_yrmonth
	,TO_VARCHAR(a.pd_dn_ol_admit_start_dt,'yyyyMM') as pd_dn_ol_admit_yrmonth
from tadm_tre_cpy.glxy_ip_admit_f_${claims_month} as a
left join tmp_2y.ec_glxy_drg_code as b
	on a.fnl_drg_cd = b.drg_cd
	and a.ADMIT_START_DT between b.drg_row_eff_dt and b.drg_row_end_dt
left join tmp_2y.ec_loc_week_assign as c 
	on a.pd_dn_ol_admit_start_dt =c.date
where year(a.pd_dn_ol_admit_start_dt)>'2022'
;



--QA Check that 2023 and on is in the table above 
select distinct year(pd_dn_ol_admit_start_dt) from tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date};



--Step 11.5: Stack 2022 table with current table before sending through Franky
drop table if exists tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date}_2;
create table tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date}_2 as				
SELECT
	* 
	from tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date}
	union all 
	select 
	*
	from tmp_1y.ec_ip_dataset_claims_cosmos_2022
;


--Step 12: First step of Franky, rolling up and pulling a subset of inital pull 
drop table if exists tmp_1m.ec_ip_dataset_claims_franky;
create table tmp_1m.ec_ip_dataset_claims_franky as
select 
--	ADMIT_START_DT
--	,admit_end_dt 
	admit_week
	,gal_mbi_hicn_fnl
--	,eventkey
--	,primary_diag_cd 
--	,icd_2
--	,icd_3
--	,icd_4
--	,icd_5
--	,fnl_drg_cd
--	,tadm_admit_type
--	,admit_yr_month 
--	,admit_qtr 
	,'Claims' as entity 
	,migration_source
	,product_level_3_fnl 
	,tfm_product_new_fnl 
	,tfm_include_flag
	,global_cap
	,'COSMOS' as sgr_source_name
	,'COMSOS' as nce_tadm_dec_risk_type 
	,group_ind_fnl 
	,PROV_TIN
	,market_fnl 
	,GROUPNUMBER
	,plan_level_2_fnl
	,brand_fnl
	,adjd_dt 
--	,admitid
	,max(case when prov_prtcp_sts_cd='P' then 'Par'
		when prov_prtcp_sts_cd='N' then 'Non-Par'
		when prov_prtcp_sts_cd ='D' then 'Non-Par' else prov_prtcp_sts_cd end) as prov_prtcp_sts_cd
	,CONTRACTPBP_FNL
	,ipa_pac_flag
--	,denial_f
	,max(case when total_oah_flag ='OAH' then 'OAH'
		else 'Non-OAH' end) as total_oah_flag
	,institutional_flag
--	,los
	,pd_dn_ol_admitid 
	,pd_dn_ol_admit_start_dt 
	,pd_dn_ol_admit_end_dt 
	,pd_dn_ol_tadm_admit_type 
--	,ip_status_code
	,adjd_yrmonth
	,pd_dn_ol_admit_yrmonth
	,max(case when med_surg in ('SURGICAL') then 'SURGICAL' else 'MEDICAL' end) med_surg
    ,min(case when respiratory_flag='COVID-19' then 'COVID-19' 
    		when respiratory_flag='ILI' then 'ILI' else 'NA' end) respiratory_flag
    ,sum(ALLW_AMT_FNL) as ALLW_AMT_FNL
	,sum(NET_PD_AMT_FNL) as NET_PD_AMT_FNL
	,sum(TADM_ADMITS) as TADM_ADMITS
--	,sum(days) as days
from tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date}_2 as a
group by 
--	ADMIT_START_DT
--	,admit_end_dt 
	admit_week
	,gal_mbi_hicn_fnl
--	,eventkey
--	,primary_diag_cd 
--	,icd_2
--	,icd_3
--	,icd_4
--	,icd_5
--	,fnl_drg_cd
--	,tadm_admit_type
--	,admit_yr_month 
--	,admit_qtr 
	,'Claims'
	,migration_source
	,product_level_3_fnl 
	,tfm_product_new_fnl 
	,tfm_include_flag
	,global_cap
	,'COSMOS'
	,'COMSOS'
	,group_ind_fnl 
	,PROV_TIN
	,market_fnl 
	,GROUPNUMBER
	,plan_level_2_fnl
	,brand_fnl
	,adjd_dt 
--	,admitid
	,CONTRACTPBP_FNL
	,ipa_pac_flag
--	,denial_f
	,institutional_flag
--	,los
	,pd_dn_ol_admitid 
	,pd_dn_ol_admit_start_dt 
	,pd_dn_ol_admit_end_dt 
	,pd_dn_ol_tadm_admit_type 
--	,ip_status_code
	,adjd_yrmonth
	,pd_dn_ol_admit_yrmonth
;



--Step 13: pull paid/denied needed fields to pull Franky net paid
drop table if exists tmp_1m.ec_ip_dataset_claims_franky1;
create table tmp_1m.ec_ip_dataset_claims_franky1 as
select 
--	admitid
--	,admit_start_dt
--	,admit_end_dt
	pd_dn_ol_admitid 
	,pd_dn_ol_admit_start_dt 
	,pd_dn_ol_admit_end_dt 
	,pd_dn_ol_tadm_admit_type 
--	,ip_status_code 
	,adjd_dt 
	,adjd_yrmonth
	,pd_dn_ol_admit_yrmonth
	,gal_mbi_hicn_fnl 
--	,sum(net_pd_amt_fnl) as net_pd_amt_fnl 
	,sum(ALLW_AMT_FNL) as ALLW_AMT_FNL
--	,sum(days) as days
 from tmp_1m.ec_ip_dataset_claims_franky
 group by 
 --	 admitid
--	,admit_start_dt
--	,admit_end_dt
	pd_dn_ol_admitid 
	,pd_dn_ol_admit_start_dt 
	,pd_dn_ol_admit_end_dt 
	,pd_dn_ol_tadm_admit_type 
--	,ip_status_code 
	,adjd_dt 
	,adjd_yrmonth
	,pd_dn_ol_admit_yrmonth
	,gal_mbi_hicn_fnl 
 ;

--Step 14:Creating runout adjust snapshot
drop table if exists tmp_1m.ec_ip_dataset_claims_franky2 ;
create table tmp_1m.ec_ip_dataset_claims_franky2 as
select *
	,sum(ALLW_AMT_FNL) OVER (partition by gal_mbi_hicn_fnl,pd_dn_ol_admitid,pd_dn_ol_admit_yrmonth order by adjd_dt  ROWS BETWEEN UNBOUNDED PRECEDING AND Current row  )  as runout_snapshot_allw
	,datediff('day',pd_dn_ol_admit_start_dt,pd_dn_ol_admit_end_dt)+1 as days1 
from tmp_1m.ec_ip_dataset_claims_franky1
;

--Step 15: If the running total of paid dollars is >0, then we count give it a 1
drop table if exists tmp_1m.ec_ip_dataset_claims_franky3 ;
create table tmp_1m.ec_ip_dataset_claims_franky3 as
select gal_mbi_hicn_fnl
	,pd_dn_ol_admitid	
	,pd_dn_ol_admit_yrmonth	
	,pd_dn_ol_admit_start_dt	
	,pd_dn_ol_admit_end_dt	
	,pd_dn_ol_tadm_admit_type	
	,adjd_dt	
	,adjd_yrmonth	
	,allw_amt_fnl
	,runout_snapshot_allw
	,case when runout_snapshot_allw>0 then 1 else 0 end as runout_snapshot_units
	,case when runout_snapshot_allw>0 then days1 else 0 end as runout_snapshot_days
from tmp_1m.ec_ip_dataset_claims_franky2
;             



--Step 16: Create lag unit count of ruse in final franky logic
drop table if exists tmp_1m.ec_ip_dataset_claims_franky4 ;
CREATE TABLE tmp_1m.ec_ip_dataset_claims_franky4 as
SELECT *
	,lag(runout_snapshot_units) over (partition by gal_mbi_hicn_fnl,pd_dn_ol_admitid,pd_dn_ol_admit_yrmonth order by adjd_dt) AS lag_units
	,lag(runout_snapshot_days) over (partition by gal_mbi_hicn_fnl,pd_dn_ol_admitid,pd_dn_ol_admit_yrmonth order by adjd_dt) AS lag_days
FROM tmp_1m.ec_ip_dataset_claims_franky3
;

--Step 17:Final Franky Logic - getting in-the-moment unit count
drop table if exists tmp_1m.ec_ip_dataset_claims_franky5 ;
CREATE TABLE tmp_1m.ec_ip_dataset_claims_franky5 as
select gal_mbi_hicn_fnl
	,pd_dn_ol_admitid	
	,pd_dn_ol_admit_yrmonth	
	,pd_dn_ol_admit_start_dt	
	,pd_dn_ol_admit_end_dt	
	,pd_dn_ol_tadm_admit_type	
	,adjd_dt	
	,adjd_yrmonth	
	,allw_amt_fnl
	,runout_snapshot_allw
	,runout_snapshot_days
	,dense_rank() over ( partition by gal_mbi_hicn_fnl,pd_dn_ol_admitid,adjd_yrmonth order by adjd_dt desc ) mem_admit_latest
	,case when lag_units is not null and runout_snapshot_units < lag_units then -1
			when runout_snapshot_units + lag_units =2 then 0
			else runout_snapshot_units end as new_runout_snapshot_units
	,case when lag_days is not null then 0
		when lag_days is null then runout_snapshot_days
		end as new_runout_snapshot_days
	,lag_days
--	,case when lag_units_allw is not null and runout_snapshot_units_allw < lag_units_allw then -1
--			when runout_snapshot_units_allw + lag_units_allw =2 then 0
--			else runout_snapshot_units_allw end as new_runout_snapshot_units_allw
	,lag_units as lag_units_for_qa
from tmp_1m.ec_ip_dataset_claims_franky4
;



--Step 18:Roll Franky Logic up
drop table if exists tmp_1m.ec_ip_dataset_claims_franky6 ;
CREATE TABLE tmp_1m.ec_ip_dataset_claims_franky6 as
select gal_mbi_hicn_fnl
	,pd_dn_ol_admitid
	,pd_dn_ol_admit_start_dt	
	,pd_dn_ol_admit_end_dt
	,adjd_dt
	,adjd_yrmonth
	,sum(new_runout_snapshot_units) as units_franky
	,sum(allw_amt_fnl) as allw_franky
	,sum(new_runout_snapshot_days) as days_franky
from tmp_1m.ec_ip_dataset_claims_franky5
group by gal_mbi_hicn_fnl
	,pd_dn_ol_admitid
	,pd_dn_ol_admit_start_dt	
	,pd_dn_ol_admit_end_dt
	,adjd_dt
	,adjd_yrmonth
;


--Step 19: Join Back to Big Claims Table from first step of Franky
drop table if exists tmp_1m.ec_ip_dataset_claims_franky7 ;
CREATE TABLE tmp_1m.ec_ip_dataset_claims_franky7 as
select a.*
	,b.units_franky
	,b.allw_franky
	,b.days_franky
from tmp_1m.ec_ip_dataset_claims_franky as a
left join tmp_1m.ec_ip_dataset_claims_franky6 as b
	on a.gal_mbi_hicn_fnl = b.gal_mbi_hicn_fnl
	and a.pd_dn_ol_admitid = b.pd_dn_ol_admitid --696264.62	73
	and a.pd_dn_ol_admit_start_dt = b.pd_dn_ol_admit_start_dt
	and a.pd_dn_ol_admit_end_dt = b.pd_dn_ol_admit_end_dt
	and a.adjd_dt = b.adjd_dt
;



--Step 20: Cutting off admits with no admitID 
drop table if exists tmp_1m.ec_ip_dataset_claims_franky8_${claims_date} ;
CREATE TABLE tmp_1m.ec_ip_dataset_claims_franky8_${claims_date} as
select 
	*
	,d.collection as Hospital_Group
from tmp_1m.ec_ip_dataset_claims_franky7  as a 
left join tmp_1y.tin_collection as d 
	on a.prov_tin = d.tin
where a.pd_dn_ol_admitid is not null
;

--QA for frankie:  
--Test Volumes (Want these to match to ensure that rejoining to detailed admit table doesn't dupe:
select sum(allw_franky) as net_pd_franky, sum(units_franky) as units_franky , sum(days_franky) as days_franky from tmp_1m.ec_ip_dataset_claims_franky6;
--NOVEMBER
-- Paid: 68,716,656,301.25
-- Units: 4,995,767
--DECEMBER
-- Paid: 70,624,690,826.05
-- Units: 5,122,903
--JANUARY
-- Paid: 72,267,482,661.31
-- Units: 5,238,959
--FEBRUARY
-- Paid: 73,640,186,893.02
-- Units: 5,338,064
--MARCH
-- Paid: 80,337,165,615.57
-- Units: 5,555,249
--APRIL
-- Paid: 82,354,587,077.30
-- Units: 5,685,626
--MAY
-- Paid: 84,739,473,072.09
-- Units: 5,843,965
--JUNE
-- Paid: 87,050,010,546.60
-- Units: 5,990,785
--JULY
-- Paid: 88,906,947,782.00	
-- Units: 6,114,204
--AUGUST
-- Paid: 91,560,740,248.71
-- Units: 6,285,956
-- Days: 54,748,243
--SEPTEMBER
-- Paid: 93,678,334,649.97
-- Units: 6,420,769
-- Days: 55,889,236
--OCTOBER
-- Paid: 95,970,964,466.46
-- Units: 6,572,434
-- Days: 57,216,203
		


select sum(allw_franky) as net_pd_franky, sum(units_franky) as units_franky , sum(days_franky) as days_franky from tmp_1m.ec_ip_dataset_claims_franky7;
--NOVEMBER
-- Paid: 68,718,031,259.42
-- Units: 4,995,792
--DECEMBER
-- Paid: 70,630,111,492.91
-- Units: 5,123,027
--JANUARY
-- Paid: 72,272,920,314.23
-- Units: 5,239,087
--FEBRUARY
-- Paid: 73,646,184,533.03
-- Units: 5,338,202
--MARCH
-- Paid: 80,343,792,969.62
-- Units: 5,555,398
--APRIL
-- Paid: 82,361,403,576.27
-- Units: 5,685,783
--MAY
-- Paid: 84,745,964,215.87
-- Units: 5,844,101
--JUNE
-- Paid: 87,057,043,087.29
-- Units: 5,990,932
--JULY
-- Paid: 88,913,865,737.46	
-- Units: 6,114,352
--AUGUST
-- Paid: 91,568,402,113.13
-- Units: 6,286,103
-- Days: 54,753,731
--SEPTEMBER
-- Paid: 93,685,975,716.23
-- Units: 6,420,915
-- Days: 55,894,729	
-- OCTOBER
-- Paid: 95,977,733,957.69
-- Units: 6,572,497
-- Days: 57,221,737


--Step 21: COSMOS After Franky adjustment
drop table if exists tmp_1m.ec_ip_dataset_claims_cosmos_franky; 
create table tmp_1m.ec_ip_dataset_claims_cosmos_franky as 
select
	a.admit_week
	,a.pd_dn_ol_tadm_admit_type as tadm_admit_type
	,a.pd_dn_ol_admit_yrmonth as admit_yr_month
	,a.migration_source
	,a.product_level_3_fnl
	,a.tfm_product_new_fnl
	,a.tfm_include_flag
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type
	,a.group_ind_fnl
	,a.market_fnl
	,a.groupnumber
	,b.group_name
	,a.plan_level_2_fnl
	,a.BRAND_FNL
	,a.global_cap
	,a.adjd_dt
--	,a.perc_offset
	,case when dateadd(DAY,-10,last_day(a.adjd_dt))>=a.adjd_dt THEN TO_VARCHAR(a.adjd_dt,'yyyyMM') else TO_VARCHAR(add_months(a.adjd_dt,1),'yyyyMM') end as adjd_yrmonth
	,a.pd_dn_ol_admitid as admitid
	,a.prov_prtcp_sts_cd	
	,a.PROV_TIN
	,a.Hospital_Group
	,a.contractpbp_fnl
	,a.ipa_pac_flag
	,a.med_surg
	,a.respiratory_flag
	,case when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('COVID-19') then 'COVID-19'
    	when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('ILI') then 'ILI'
    	when a.ipa_pac_flag ='IPA' and a.pd_dn_ol_tadm_admit_type='IP_TRANS' then 'Transplant'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'MEDICAL' then 'Medical'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'SURGICAL' then 'Surgical' 
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_REHAB' then 'AIR' 
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_LTAC' then 'LTAC' 
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_SNF' then 'SNF'
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_SWGBED' then 'Swing Bed' 
    	when a.pd_dn_ol_tadm_admit_type='MHCDIP' then 'Other' else 'Other' end as ipa_li_split 
	,a.total_oah_flag
	,a.institutional_flag 
	,datediff('day',a.pd_dn_ol_admit_start_dt,a.pd_dn_ol_admit_end_dt)+1 as los_clms 
	,case when a.global_cap='NA' then 0 else 1 end as capitated
	,CASE WHEN a.BRAND_FNL='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.product_level_3_fnl <>'INSTITUTIONAL' AND a.tfm_include_flag=1 THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
 	,CASE WHEN a.BRAND_FNL='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.product_level_3_fnl <>'INSTITUTIONAL' AND a.tfm_include_flag=1 
 		AND a.tfm_product_new_fnl in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end as leading_ind_pop
 	,CASE WHEN a.BRAND_FNL='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end as MnR_NICE_FFS_Flag
 	,case when (a.BRAND_FNL='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (a.BRAND_FNL='M&R' AND sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
 	,case when a.BRAND_FNL='M&R' and a.migration_source='OAH' then 1 else 0 end as MnR_OAH_flag 
 	,case when a.BRAND_FNL='C&S' and a.migration_source='OAH' then 1 else 0 end as CnS_OAH_flag
 	,case when a.BRAND_FNL='M&R' and a.product_level_3_fnl='DUAL' then 1 else 0 end as MnR_Dual_flag
 	,case when (a.BRAND_FNL='C&S' and a.global_cap = 'NA' and a.product_level_3_fnl='DUAL' AND a.SGR_SOURCE_NAME in('COSMOS','NICE','CSP') ) then 1 else 0 end as CnS_Dual_flag
--	,count(distinct concat(a.dt,a.pd_dn_ol_admitid)) as length_of_stay
	,0 as frank_netpaid
	,sum(a.units_franky) as frank_admits
	,sum(a.allw_franky) as frank_allowed
	,sum(a.days_franky) as frank_days
	,sum(a.allw_amt_fnl) as allowed
	,sum(a.net_pd_amt_fnl) as netpaid
	,sum(a.tadm_admits) as admits
	,0 as days
from tmp_1m.ec_ip_dataset_claims_franky8_${claims_date} as a 
left join fichsrv.group_crosswalk as b
		on a.groupnumber = b.group_number  
		and substr(a.pd_dn_ol_admit_yrmonth,1,4)=b.year
group by 
	a.admit_week
	,a.pd_dn_ol_tadm_admit_type
	,a.pd_dn_ol_admit_yrmonth--	,a.fst_srvc_month
	,a.migration_source
	,a.product_level_3_fnl
	,a.tfm_product_new_fnl
	,a.tfm_include_flag
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type
	,a.group_ind_fnl
	,a.market_fnl
	,a.groupnumber
	,b.group_name
	,a.plan_level_2_fnl
	,a.BRAND_FNL
	,a.global_cap
	,a.adjd_dt
--	,a.perc_offset
	,case when dateadd(DAY,-10,last_day(a.adjd_dt))>=a.adjd_dt then TO_VARCHAR(a.adjd_dt,'yyyyMM') else TO_VARCHAR(add_months(a.adjd_dt,1),'yyyyMM') end 
	,a.pd_dn_ol_admitid
	,a.prov_prtcp_sts_cd
	,a.PROV_TIN
	,a.Hospital_Group
	,a.contractpbp_fnl
	,a.ipa_pac_flag
	,a.med_surg
	,a.respiratory_flag
	,case when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('COVID-19') then 'COVID-19'
    	when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('ILI') then 'ILI'
    	when a.ipa_pac_flag ='IPA' and a.pd_dn_ol_tadm_admit_type='IP_TRANS' then 'Transplant'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'MEDICAL' then 'Medical'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'SURGICAL' then 'Surgical' 
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_REHAB' then 'AIR' 
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_LTAC' then 'LTAC' 
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_SNF' then 'SNF'
    	when a.ipa_pac_flag ='PAC' and a.pd_dn_ol_tadm_admit_type='IP_SWGBED' then 'Swing Bed' 
    	when a.pd_dn_ol_tadm_admit_type='MHCDIP' then 'Other' else 'Other' end
	,a.total_oah_flag
	,a.institutional_flag
	,datediff('day' ,a.pd_dn_ol_admit_start_dt,a.pd_dn_ol_admit_end_dt)+1 
	,case when a.global_cap='NA' then 0 else 1 end 
	,CASE WHEN a.BRAND_FNL='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.product_level_3_fnl <>'INSTITUTIONAL' AND a.tfm_include_flag=1 THEN 1 else 0 end 
 	,CASE WHEN a.BRAND_FNL='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.product_level_3_fnl <>'INSTITUTIONAL' AND a.tfm_include_flag=1 
 		AND a.tfm_product_new_fnl in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end 
 	,CASE WHEN a.BRAND_FNL='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end 
 	,case when (a.BRAND_FNL='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (a.BRAND_FNL='M&R' AND sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end 
 	,case when a.BRAND_FNL='M&R' and a.migration_source='OAH' then 1 else 0 end
 	,case when a.BRAND_FNL='C&S' and a.migration_source='OAH' then 1 else 0 end 
 	,case when a.BRAND_FNL='M&R' and a.product_level_3_fnl='DUAL' then 1 else 0 end 
 	,case when (a.BRAND_FNL='C&S' and a.global_cap = 'NA' and a.product_level_3_fnl='DUAL' AND a.SGR_SOURCE_NAME in('COSMOS','NICE','CSP') ) then 1 else 0 end 
	;



--Step 22: Create claims dataset for service month conversion
drop table if exists tmp_1m.ec_ip_dataset_claims_triangle_${claims_date}; 
create table tmp_1m.ec_ip_dataset_claims_triangle_${claims_date} as 
SELECT
	000000 as admit_week
	,admit_yr_month
	,'0000' as admit_year
	,fst_srvc_month -- rename in notification
	,'Triangle' as adjd_yrmonth
	,'Triangle' as component
	,'Triangle' as entity
	,'Triangle' as tadm_admit_type
	,0 as loc_flag
	,'Triangle' as svc_setting
	,'Triangle' as case_cur_svc_cat_dtl_cd
	,'Triangle' as migration_source
	,'Triangle' as total_oah_flag
	,'Triangle' as institutional_flag
	,tfm_product_new_fnl
	,0 as tfm_include_flag
	,'Triangle' as global_cap
	,'Triangle' as sgr_source_name
	,'Triangle' as nce_tadm_dec_risk_type
	,'Triangle' as BRAND_FNL
	,'Triangle' as group_ind_fnl
	,'Triangle' as product_level_3_fnl
	,'Triangle' as plan_level_2_fnl
	,'Triangle' as market_fnl
	,'Triangle' as contractpbp_fnl
	,'Triangle' as groupnumber
	,'Triangle' as group_name
	,'Triangle' as do_ind
	,'Triangle' as prov_prtcp_sts_cd
	,'Triangle' as prov_tin -- not needed for claims anywhere so placeholder for union
	,'Triangle' as hospital_group
	,0 as capitated
	,'Triangle' as los_categories
	,0 as los_clms
	,0 as length_of_stay
--	,0 as ili_dx_ind
--	,0 as covid_dx_ind -- both these ind not needed
	,'Triangle' as respiratory_flag
	,'Triangle' as ipa_li_split
	,0 as MnR_COSMOS_FFS_Flag
 	,CASE WHEN BRAND_FNL='M&R' AND global_cap='NA' AND product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND tfm_product_new_fnl in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end as leading_ind_pop
 	,0 as MnR_NICE_FFS_Flag
 	,0 as MnR_TOTAL_FFS_FLAG
 	,0 as MnR_OAH_flag 
 	,0 as CnS_OAH_flag
 	,0 as MnR_Dual_flag
 	,0 as CnS_Dual_flag 
	,'Triangle' as ocm_migration
	,0 as swgbed
 	,'Triangle' as mr_cs_other
	,case when ipa_pac_flag ='IPA' and respiratory_flag in ('COVID-19') then 'COVID-19'
    	when ipa_pac_flag ='IPA' and respiratory_flag in ('ILI') then 'ILI'
    	when ipa_pac_flag ='IPA' and tadm_admit_type='IP_TRANS' then 'Transplant'
    	when ipa_pac_flag ='IPA' and med_surg = 'MEDICAL' then 'Medical'
    	when ipa_pac_flag ='IPA' and med_surg = 'SURGICAL' then 'Surgical' 
		when tadm_admit_type='IP_REHAB' then 'AIR'
		when tadm_admit_type='IP_LTAC' then 'LTAC' 
		when tadm_admit_type='IP_SNF' then 'SNF'
		when tadm_admit_type='IP_SWGBED' then 'Swing Bed'
		when tadm_admit_type='MHCDIP' then 'Other' else 'Other' end as admit_type
	,ipa_pac_flag
	,0 as first_adverse
	,0 as first_not_approved_srvc
	,0 as first_not_approved_case
	,0 as md_review_overturn
	,0 as appealed_cases
	,0 as overturned_cases
	,0 as md_rev_appeals
	,0 as pre_auth_cases
	,0 as case_count
	,0 as Initial_ADR_cnt
	,0 as persistent_adr_cnt
	,0 as md_reviewed_cnt
	,0 as appeal_case_cnt
	,0 as appeal_ovrtn_case_cnt
	,0 as mcr_reconsideration_case_cnt
	,0 as mcr_ovrtn_case_cnt
	,0 as p2p_case_cnt
	,0 as p2p_ovrtn_case_cnt
	,0 as other_ovtrns
	,0 as membership
	,0 as days
	,0 as frank_days
	,0 as admits
	,sum(allw_amt_fnl) as allowed
	,0 as netpaid
	,0 as franky_paid
	,0 as franky_admits
	,0 as frank_allowed
from tmp_1m.ec_ip_dataset_claims_cosmos_${claims_date}_2 
group by 
	admit_yr_month
	,fst_srvc_month -- rename in notification
	,adjd_yrmonth
	,tfm_product_new_fnl
	,CASE WHEN BRAND_FNL='M&R' AND global_cap='NA' AND  product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND tfm_product_new_fnl in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end
	,ipa_pac_flag
	,case when ipa_pac_flag ='IPA' and respiratory_flag in ('COVID-19') then 'COVID-19'
    	when ipa_pac_flag ='IPA' and respiratory_flag in ('ILI') then 'ILI'
    	when ipa_pac_flag ='IPA' and tadm_admit_type='IP_TRANS' then 'Transplant'
    	when ipa_pac_flag ='IPA' and med_surg = 'MEDICAL' then 'Medical'
    	when ipa_pac_flag ='IPA' and med_surg = 'SURGICAL' then 'Surgical' 
		when tadm_admit_type='IP_REHAB' then 'AIR'
		when tadm_admit_type='IP_LTAC' then 'LTAC' 
		when tadm_admit_type='IP_SNF' then 'SNF'
		when tadm_admit_type='IP_SWGBED' then 'Swing Bed'
		when tadm_admit_type='MHCDIP' then 'Other' else 'Other' end
;
	

--Step 23: NICE pull
drop table if exists tmp_1m.ec_ip_dataset_claims_nice; 
create table tmp_1m.ec_ip_dataset_claims_nice as 
SELECT
/*CLAIM-RELATED FIELDS*/
	a.CLM_AUD_NBR
	,a.COMPONENT
	,a.ADMITID
	,a.DTL_LN_NBR
	,a.EVENTKEY
	,a.TADM_ADMIT_TYPE
	,a.TADM_MDC
	,a.ADMIT_QTR
	,a.ADMIT_YR_MONTH
	,a.ADMIT_START_DT
	,a.ADMIT_END_DT
	,a.FST_SRVC_DT
	,a.fst_srvc_month
	,a.ERLY_SRVC_QTR
	,a.ERLY_SRVC_DT
	,a.DSCHRG_STS_CD
	,a.BRAND_FNL 
	,c.week as admit_week
/*CLAIM ADJUDICATION FIELDS*/
	,a.BIL_RECV_DT
	,a.ADJD_QTR
	,a.ADJD_DT
	,a.CLM_PD_DT
/*DEMOGRAPHIC FIELDS*/
	,a.PLAN_LEVEL_1_FNL
	,a.PLAN_LEVEL_2_FNL
	,a.PRODUCT_LEVEL_1_FNL
	,a.PRODUCT_LEVEL_2_FNL
	,a.PRODUCT_LEVEL_3_FNL
	,a.REGION_FNL
	,a.MARKET_FNL
	,a.FIN_SUBMARKET_FNL
	,a.PMGDEC_FNL
	,a.DEC_RISK_TYPE_FNL
	,a.GROUP_IND_FNL
	,a.TFM_INCLUDE_FLAG
	,a.GROUPNUMBER
	,a.SEGMENT_NAME_FNL
	,a.CONTRACTPBP_FNL
	,a.CONTRACT_FNL
/*PATIENT FIELDS*/
	,a.MBI_HICN_FNL
	,a.BTH_DT
	,a.GDR_CD
/*PROVIDER FIELDS*/
	,a.MPIN
	,a.SRVC_PROV_ID
	,a.PROV_TIN
	,a.FULL_NM
	,a.PROV_PRTCP_STS_CD
/*SERVICE FIELDS*/
	,a.FNL_DRG_CD
	,a.CLM_ADMIT_TYPE
	,a.FNL_ADMIT_TYP
	,a.PROC_CD
	,a.icd_2
	,a.icd_3
	,a.icd_4
	,a.icd_5
	,a.PRIMARY_DIAG_CD
	,a.AHRQ_DIAG_GENL_CATGY_DESC
	,a.AHRQ_DIAG_DTL_CATGY_DESC
	,case when a.tadm_admit_type IN ('IP_HSP','IP_MATNB','IP_MEDSURGICU','IP_NICUEXTSTAY','IP_TRANS') then 'IPA'
		when a.tadm_admit_type in ('IP_LTAC','IP_REHAB','IP_SNF','IP_REHAB','IP_SWGBED') then 'PAC'
		else 'Other' end as ipa_pac_flag 
	,a.denial_f
	,case when b.servicecatg IN ('UNKNOWN','**') then 'MEDICAL'
		WHEN b.servicecatg IN ('MED','MEDICAL') THEN 'MEDICAL'
		WHEN b.servicecatg IN ('SURG','SURGICAL') THEN 'SURGICAL'
		when a.fnl_drg_cd is null then 'MEDICAL' else b.servicecatg end as med_surg
	,case when primary_diag_cd in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
    	when primary_diag_cd in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
	    	'78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
	    	'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
	    	'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883','A3700',
	    	'A3701','A3710','A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 'ILI'
    	when icd_2 in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
    	when icd_2 in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
	    	'78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
	    	'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
	    	'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883','A3700','A3701','A3710',
	    	'A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 'ILI'
    		 else 'NA' end as respiratory_flag 
    	,case when primary_diag_cd in ('B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09X1','J09X2','J09X3','J09X9','J1000','J1001','J1008','J101','J102','J1081','J1082',
	    	'J1083','J1089','J1100','J1108','J111','J112','J1181','J1182','J1183','J1189','J122','J129','J188','J189','J200','J201','J202','J203','J204','J205','J206','J207','J208','J209','J40','R05',
	    	'R502','R509','R5081','R6883','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 1 
    	when icd_2 in ('B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09X1','J09X2','J09X3','J09X9','J1000','J1001','J1008','J101','J102','J1081','J1082',
	    	'J1083','J1089','J1100','J1108','J111','J112','J1181','J1182','J1183','J1189','J122','J129','J188','J189','J200','J201','J202','J203','J204','J205','J206','J207','J208','J209','J40','R05',
	    	'R502','R509','R5081','R6883','J181','R052','R053','R054','R058','R059','A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 1 else 0 end as ili_dx_ind 
    ,case when a.product_level_3_fnl ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
    ,datediff('day',a.ADMIT_START_DT,a.ADMIT_END_DT)+1 as los_clms 
	,a.tfm_product_fnl
	,a.RVNU_CD
	,a.ICD_VER_CD
/*COUNTING FIELDS*/
	,a.FNL_IPRVUWGT
	,a.SBMT_CHRG_AMT
	,a.ALLW_AMT_FNL
	,a.NET_PD_AMT_FNL
	,a.TADM_ADMITS
	,a.TADM_QTYDAYS
from fichsrv.nice_ip as a
left join fichsrv.tadm_glxy_drg_code as b
	on a.fnl_drg_cd = b.drg_cd
left join tmp_2y.ec_loc_week_assign as c 
on a.admit_start_dt =c.date
;
SELECT DISTINCT servicecatg FROM fichsrv.tadm_glxy_drg_code

--Step 24: Facets pull for C&S Duals 
drop table if exists tmp_1m.ec_ip_dataset_claims_facet;
create table tmp_1m.ec_ip_dataset_claims_facet as
select 
	a.admit_start_dt
    ,a.admit_end_dt
    ,d.week as admit_week
    ,a.gal_mbi_hicn_fnl
    ,a.eventkey
    ,a.primary_diag_cd
    ,a.icd_2
    ,a.icd_3
    ,a.icd_4
    ,a.icd_5
    ,a.fnl_drg_cd
    ,a.tadm_admit_type
    ,a.admit_yr_month
    ,a.fst_srvc_month
    ,a.admit_qtr
    ,'Claims' as entity
    ,b.migration_source
    ,b.fin_product_level_3 as product_level_3_fnl
    ,b.fin_tfm_product_new as tfm_product_new_fnl
    ,b.tfm_include_flag
    ,b.global_cap
    ,'CSP' as sgr_source_name
    ,'CSP' as nce_tadm_dec_risk_type
    ,b.fin_g_i as group_ind_fnl
    ,b.fin_market as market_fnl
    ,b.tadm_group_nbr_consist as groupnumber
    ,b.fin_plan_level_2 as plan_level_2_fnl
    ,b.fin_brand as brand_fnl
    ,a.adjd_dt
    ,a.admitid
    ,a.prov_prtcp_sts_cd
    ,b.fin_contractpbp as contractpbp_fnl
    ,case when a.tadm_admit_type IN ('IP_HSP','IP_MATNB','IP_MEDSURGICU','IP_NICUEXTSTAY','IP_TRANS') then 'IPA'
          when a.tadm_admit_type in ('IP_LTAC','IP_REHAB','IP_SNF','IP_REHAB','IP_SWGBED') then 'PAC'
          else 'Other' end as ipa_pac_flag
    ,a.denial_f
	,case when b.servicecatg IN ('UNKNOWN','**') then 'MEDICAL'
		WHEN b.servicecatg IN ('MED','MEDICAL') THEN 'MEDICAL'
		WHEN b.servicecatg IN ('SURG','SURGICAL') THEN 'SURGICAL'
		when a.fnl_drg_cd is null then 'MEDICAL' else b.servicecatg end as med_surg
    ,case when a.primary_diag_cd in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
          when a.primary_diag_cd in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
               '78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
               'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
               'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883',
               'A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 'ILI'
          when a.icd_2 in ('B9729', 'J0248', 'U071', 'J1282','J1281') then 'COVID-19'
          when a.icd_2 in ('07999','3829','460','4619','4658','4659','4660','46619','486','4870','4871','4878','488','4880','48801','48802','48809','4881','48811','48812','48819','490','7806',
                '78060','7862','B9710','B9789','H6690','H6691','H6692','H6693','J00','J0190','J0191','J069','J09','J09X','J09X1','J09X2','J09X3','J09X9','J10','J100','J1000','J1001','J1008','J101','J102',
                'J108','J1081','J1082','J1083','J1089','J11','J110','J1100','J1108','J111','J112','J118','J1181','J1182','J1183','J1189','J122','J1289','J129','J180','J181','J182','J188','J189','J200',
                'J201','J202','J203','J204','J206','J207','J208','J209','J22','J40','J80','J988','R05','R051','R052','R053','R054','R058','R059','R502','R5081','R509','R6883',
                'A3700','A3701','A3710','A3711','A3780','A3781','A3790','J120','J410','J411','J418') then 'ILI'
          else 'NA' end as respiratory_flag
    ,case when b.fin_product_level_3 ='INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
    ,case when b.migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
    ,datediff('day',a.ADMIT_START_DT,a.ADMIT_END_DT)+1 as los_clms
    ,case when b.global_cap='NA' then 0 else 1 end as capitated
    ,AHRQ_DIAG_GENL_CATGY_DESC
    ,AHRQ_DIAG_DTL_CATGY_DESC
    ,tadm_admits 
    ,net_pd_amt_fnl 
    ,allw_amt_fnl 
    ,tadm_qtydays
    
from FICHSRV.SMART_IP as a
left join fichsrv.tre_membership as b
    on a.gal_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
    and a.admit_yr_month = b.fin_inc_month
left join fichsrv.tadm_glxy_drg_code as c
    on a.fnl_drg_cd = c.drg_cd
left join tmp_2y.ec_loc_week_assign as d 
	on a.admit_start_dt =d.date
;

--Step 25: Union of Franky COSMOS & NICE & SMART pulls 
drop table if exists tmp_1m.ec_ip_dataset_claims_trs; 
create table tmp_1m.ec_ip_dataset_claims_trs as 
select
	a.admit_week
	,a.tadm_admit_type
	,a.admit_yr_month 
	,'000000' as service_month
	,a.adjd_yrmonth
	,'Claims' as entity 
	,a.migration_source
	,a.product_level_3_fnl 
	,a.tfm_product_new_fnl 
	,a.tfm_include_flag
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type 
	,a.group_ind_fnl 
	,a.market_fnl 
	,a.GROUPNUMBER
	,b.group_name
	,a.plan_level_2_fnl
	,a.BRAND_FNL
	,a.adjd_dt 
	,a.admitid
	,a.prov_prtcp_sts_cd 
	,a.PROV_TIN
	,a.Hospital_group
	,a.CONTRACTPBP_FNL
	,a.ipa_pac_flag
	,a.med_surg
	,a.respiratory_flag
	,a.ipa_li_split 
	,a.total_oah_flag
	,a.institutional_flag
	,a.los_clms
	,0 as length_of_stay
	,a.capitated
	,a.global_cap
	,a.admits
	,a.netpaid
	,a.allowed
	,a.days
	,a.frank_days
	,a.frank_netpaid
	,a.frank_admits
	,a.frank_allowed
from tmp_1m.ec_ip_dataset_claims_cosmos_franky as a 
left join fichsrv.group_crosswalk as b
		on a.GROUPNUMBER = b.group_number  
		and substring(a.admit_yr_month,1,4) = b.year
union all 
select 
	a.admit_week
	,a.tadm_admit_type
	,a.admit_yr_month 
	,'000000' as service_month
--	,a.fst_srvc_month
	,'000000' as adjd_yrmonth
	,'Claims' as entity 
	,'NICE' as migration_source
	,a.product_level_3_fnl 
	,a.tfm_product_fnl as tfm_product_new_fnl 
	,a.tfm_include_flag
	,'NICE' as sgr_source_name
	,a.dec_risk_type_fnl as nce_tadm_dec_risk_type
	,a.group_ind_fnl 
	,a.market_fnl 
	,a.GROUPNUMBER
	,b.group_name
	,a.plan_level_2_fnl
	,a.BRAND_FNL
	,a.adjd_dt 
	,a.admitid
	,case when a.prov_prtcp_sts_cd='P' then 'Par'
		when a.prov_prtcp_sts_cd='N' then 'Non-Par'
		when a.prov_prtcp_sts_cd ='D' then 'Non-Par' else prov_prtcp_sts_cd end
		,a.PROV_TIN
	,'NICE' as hospital_group
	,a.CONTRACTPBP_FNL
	,a.ipa_pac_flag
	,a.med_surg
	,a.respiratory_flag
	,case when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('COVID-19') then 'COVID-19'
    	when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('ILI') then 'ILI'
    	when a.ipa_pac_flag ='IPA' and a.tadm_admit_type='IP_TRANS' then 'Transplant'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'MEDICAL' then 'Medical'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'SURGICAL' then 'Surgical' 
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_REHAB' then 'AIR' 
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_LTAC' then 'LTAC' 
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_SNF' then 'SNF'
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_SWGBED' then 'Swing Bed' 
    	when a.tadm_admit_type='MHCDIP' then 'Other' else 'Other' end as ipa_li_split 
	,'Non-OAH' as total_oah_flag
	,a.institutional_flag
	,datediff('day',a.admit_start_dt,a.admit_end_dt)+1 as los_clms
	,0 as length_of_stay
	,case when a.dec_risk_type_fnl in ('FFS','PHYSICIAN') then 0 else 1 end as capitated
	,'NICE' as global_cap
	,a.tadm_admits 
	,a.net_pd_amt_fnl 
	,a.allw_amt_fnl 
	,a.tadm_qtydays
	,0 as frank_days
	,0 as frank_netpaid
	,0 as frank_admits
	,0 as frank_allowed
from tmp_1m.ec_ip_dataset_claims_nice as a
left join fichsrv.group_crosswalk as b
		on a.GROUPNUMBER = b.group_number  
		and substring(a.admit_yr_month,1,4) = b.year
union all 
select 
	a.admit_week
	,a.tadm_admit_type
	,a.admit_yr_month 
	,'000000' as service_month
--	,a.fst_srvc_month
	,'000000' as adjd_yrmonth
	,a.entity 
	,a.migration_source
	,a.product_level_3_fnl 
	,a.tfm_product_new_fnl 
	,a.tfm_include_flag
	,a.sgr_source_name
	,a.nce_tadm_dec_risk_type
	,a.group_ind_fnl 
	,a.market_fnl 
	,a.GROUPNUMBER
	,b.group_name
	,a.plan_level_2_fnl
	,a.BRAND_FNL
	,a.adjd_dt 
	,a.admitid
	,case when a.prov_prtcp_sts_cd='P' then 'Par'
		when a.prov_prtcp_sts_cd='N' then 'Non-Par'
		when a.prov_prtcp_sts_cd ='D' then 'Non-Par' else a.prov_prtcp_sts_cd end
	,'' as PROV_TIN
	,'' as hospital_group
	,a.CONTRACTPBP_FNL
	,a.ipa_pac_flag
	,a.med_surg
	,a.respiratory_flag
	,case when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('COVID-19') then 'COVID-19'
    	when a.ipa_pac_flag ='IPA' and a.respiratory_flag in ('ILI') then 'ILI'
    	when a.ipa_pac_flag ='IPA' and a.tadm_admit_type='IP_TRANS' then 'Transplant'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'MEDICAL' then 'Medical'
    	when a.ipa_pac_flag ='IPA' and a.med_surg = 'SURGICAL' then 'Surgical' 
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_REHAB' then 'AIR' 
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_LTAC' then 'LTAC' 
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_SNF' then 'SNF'
    	when a.ipa_pac_flag ='PAC' and a.tadm_admit_type='IP_SWGBED' then 'Swing Bed' 
    	when a.tadm_admit_type='MHCDIP' then 'Other' else 'Other' end as ipa_li_split 
	,a.total_oah_flag
	,a.institutional_flag
	,datediff('day',a.admit_start_dt,a.admit_end_dt)+1 as los_clms
	,0 as length_of_stay
	,a.capitated
	,a.global_cap
	,a.tadm_admits 
	,a.net_pd_amt_fnl 
	,a.allw_amt_fnl 
	,a.tadm_qtydays
	,0 as frank_days
	,0 as frank_netpaid
	,0 as frank_admits
	,0 as frank_allowed
from tmp_1m.ec_ip_dataset_claims_facet as a
left join fichsrv.group_crosswalk as b
		on a.groupnumber = b.group_number  
		and substring(a.admit_yr_month,1,4) = b.year
;


--Step 26: Claims Roll Up to union with notification for leading indicator dataset 
drop table if exists tmp_1m.ec_ip_dataset_claims_fnl_trs_${claims_date}; 
create table tmp_1m.ec_ip_dataset_claims_fnl_trs_${claims_date} as 
select	
	admit_week
	,admit_yr_month
	,'0000' as admit_year
	,service_month
--	,fst_srvc_month
	,adjd_yrmonth
	,'Claims' as component
	,'Claims' as entity
	,tadm_admit_type
	,0 as loc_flag
	,'Claims' as svc_setting
	,'Claims' as case_cur_svc_cat_dtl_cd
	,migration_source
	,total_oah_flag
	,institutional_flag
	,tfm_product_new_fnl
	,tfm_include_flag
	,global_cap
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,BRAND_FNL
	,group_ind_fnl
	,product_level_3_fnl
	,plan_level_2_fnl
	,market_fnl
	,contractpbp_fnl
	,groupnumber
	,group_name
	,'Claims' as do_ind
	,prov_prtcp_sts_cd
	,prov_tin
	,hospital_group
	,capitated
	,'NA' as los_categories
	,los_clms
	,sum(length_of_stay) as length_of_stay
--	,0 as ili_dx_ind
--	,0 as covid_dx_ind -- both these ind not needed
	,respiratory_flag
	,ipa_li_split
	,CASE WHEN BRAND_FNL='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
 	,CASE WHEN BRAND_FNL='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND tfm_product_new_fnl in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end as leading_ind_pop
 	,CASE WHEN BRAND_FNL='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end as MnR_NICE_FFS_Flag
 	,case when (BRAND_FNL='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (BRAND_FNL='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
 	,case when BRAND_FNL='M&R' and migration_source='OAH' then 1 else 0 end as MnR_OAH_flag 
 	,case when BRAND_FNL='C&S' and migration_source='OAH' then 1 else 0 end as CnS_OAH_flag
 	,case when BRAND_FNL='M&R' and product_level_3_fnl='DUAL' then 1 else 0 end as MnR_Dual_flag
 	,case when BRAND_FNL='C&S' and global_cap = 'NA' and product_level_3_fnl='DUAL' AND SGR_SOURCE_NAME in('COSMOS','NICE','CSP') then 1 else 0 end as CnS_Dual_flag
	,'NA' as ocm_migration
	,0 as swgbed
 	,case when BRAND_FNL='M&R' then 'M&R' else 'C&S' end as mr_cs_other
	,case when ipa_pac_flag='IPA' then ipa_li_split
		when tadm_admit_type='IP_REHAB' then 'AIR'
		when tadm_admit_type='IP_LTAC' then 'LTAC' 
		when tadm_admit_type='IP_SNF' then 'SNF'
		when tadm_admit_type='IP_SWGBED' then 'Swing Bed'
		when tadm_admit_type='MHCDIP' then 'Other' else 'Other' end as admit_type
	,ipa_pac_flag
	,0 as first_adverse
	,0 as first_not_approved_srvc
	,0 as first_not_approved_case
	,0 as md_review_overturn
	,0 as appealed_cases
	,0 as overturned_cases
	,0 as md_rev_appeals
	,0 as pre_auth_cases
	,0 as case_count
	,0 as Initial_ADR_cnt
	,0 as persistent_adr_cnt
	,0 as md_reviewed_cnt
	,0 as appeal_case_cnt
	,0 as appeal_ovrtn_case_cnt
	,0 as mcr_reconsideration_case_cnt
	,0 as mcr_ovrtn_case_cnt
	,0 as p2p_case_cnt
	,0 as p2p_ovrtn_case_cnt
	,0 as other_ovtrns
	,0 as membership
	,sum(days) as days
	,sum(frank_days) as frank_days
	,sum(admits) as admits
	,sum(allowed) as allowed
	,sum(netpaid) as netpaid
	,sum(frank_netpaid) as franky_paid
	,sum(frank_admits) as franky_admits
	,sum(frank_allowed) as frank_allowed
from  tmp_1m.ec_ip_dataset_claims_trs
group by 
	admit_week
	,admit_yr_month
--	,admit_qtr
	,service_month
--	,fst_srvc_month
--	,perc_offset
	,adjd_yrmonth
	,tadm_admit_type
	,migration_source
	,total_oah_flag
	,institutional_flag
	,tfm_product_new_fnl
	,tfm_include_flag
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,global_cap
	,BRAND_FNL 
	,group_ind_fnl
	,product_level_3_fnl
	,plan_level_2_fnl
	,market_fnl
	,contractpbp_fnl
	,groupnumber
	,group_name
	,prov_prtcp_sts_cd
	,prov_tin
	,hospital_group
	,capitated
	,los_clms
	,respiratory_flag
	,ipa_li_split
	,CASE WHEN BRAND_FNL='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1 THEN 1 else 0 end 
 	,CASE WHEN BRAND_FNL='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1 
 		AND tfm_product_new_fnl in ('HMO','PPO','NPPO','DUAL_CHRONIC') then 1 else 0 end
 	,CASE WHEN BRAND_FNL='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end 
 	,case when (BRAND_FNL='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND product_level_3_fnl <>'INSTITUTIONAL' AND tfm_include_flag=1) 
 		OR (BRAND_FNL='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end 
 	,case when BRAND_FNL='M&R' and migration_source='OAH' then 1 else 0 end 
 	,case when BRAND_FNL='C&S' and migration_source='OAH' then 1 else 0 end
 	,case when BRAND_FNL='M&R' and product_level_3_fnl='DUAL' then 1 else 0 end 
 	,case when BRAND_FNL='C&S' and global_cap = 'NA' and product_level_3_fnl='DUAL' AND SGR_SOURCE_NAME in('COSMOS','NICE','CSP') then 1 else 0 end 
 	 ,case when BRAND_FNL='M&R' then 'M&R' else 'C&S' end 
	,case when ipa_pac_flag='IPA' then ipa_li_split
		when tadm_admit_type='IP_REHAB' then 'AIR'
		when tadm_admit_type='IP_LTAC' then 'LTAC' 
		when tadm_admit_type='IP_SNF' then 'SNF'
		when tadm_admit_type='IP_SWGBED' then 'Swing Bed'
		when tadm_admit_type='MHCDIP' then 'Other' else 'Other' end
	,ipa_pac_flag
	;



--Step 27: Union of claims & notifications/membership 
drop table if exists tmp_1m.ec_ip_dataset_all_${notifications_date}_trs;
create table tmp_1m.ec_ip_dataset_all_${notifications_date}_trs as				
SELECT	
	*
	from tmp_1m.kn_ip_dataset_notif_01072026_trs
union all select 
	* 
	from tmp_1m.ec_ip_dataset_claims_fnl_trs_${claims_date} 
union all select 
	* 
	from tmp_1m.ec_ip_dataset_claims_triangle_${claims_date} 
	; 

SELECT sum(franky_allw), hce_admit_month FROM tmp_1m.ec_ip_dataset_all_${notifications_date}_trs GROUP BY hce_admit_month ORDER BY hce_admit_month asc;
--checking that most recent claims month is populating--

--QA check for newest week (should be one higher than what is written below, also make sure to change and save the code when you are checking this)
select max(admit_week) from tmp_1m.ec_ip_dataset_all_${notifications_date}_TRS where ipa_pac_flag ='IPA' and loc_flag=1;
--6/18/25: 202525
--6/25/25: 202526
--7/2/25: 202527
--7/9/25: 202528
--7/16/25: 202529
--7/23/25: 202530
--7/30/25: 202531
--8/6/25: 202532
--8/20/25: 202534
--8/27/25: 202535
--9/3/25: 202536
--9/10/25: 202537
--9/17/25: 202538
--9/24/25: 202539
--10/1/25: 202540
--10/8/25: 202541
--10/15/25: 202542
--10/22/25: 202543
--10/29/25: 202544
--11/5/25: 202545
--11/12/25: 202546
--11/19/25: 202547
--11/26/25: 202548
--12/3/25: 202549
--12/10/25: 202550
--12/17/25: 202551
--12/24/25: 202552
--1/7/26: 202602

/************************************************************************************************************************************************************/
--------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------End of Model Build ---------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------
/************************************************************************************************************************************************************/

/*STEPS 28+29 FOR PAC DELETED*/

--Step 30: LOC Valuation Pull & export 
drop table if exists tmp_1m.kn_ip_dataset_loc_01072026 ;
create table tmp_1m.kn_ip_dataset_loc_01072026 as
select 
	admit_week
	,hce_admit_month as admit_act_month
--	,admit_act_qtr
	,total_oah_flag
	,institutional_flag
	,fin_tfm_product_new
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_market
	,fin_brand
	,group_name
	,los_categories
	,respiratory_flag
	,mnr_cosmos_ffs_flag
	,leading_ind_pop
	,mnr_nice_ffs_flag
	,mnr_total_ffs_flag
	,mnr_oah_flag
	,cns_oah_flag
	,mnr_dual_flag
	,cns_dual_flag
	,ocm_migration
	,component
	,sum(case_count) as case_count
	,sum(Initial_ADR_cnt) as Initial_ADR_cnt
	,sum(persistent_adr_cnt) as persistent_adr_cnt
	,sum(md_reviewed_cnt) as md_reviewed_cnt
	,sum(appeal_case_cnt) as appeal_case_cnt
	,sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
	,sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
	,sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
	,sum(p2p_case_cnt) as p2p_case_cnt
	,sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
	,sum(other_ovtrns) as other_ovtrns
	,sum(membership) as membership
from tmp_1m.kn_ip_dataset_notif_01072026_trs
where ipa_pac_flag in ('IPA','MM') 
	and hce_admit_month > '202212'
	and loc_flag=1
group by  
	admit_week
	,hce_admit_month
--	,admit_act_qtr
	,total_oah_flag
	,institutional_flag
	,fin_tfm_product_new
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_market
	,fin_brand
	,group_name
	,los_categories
	,respiratory_flag
	,mnr_cosmos_ffs_flag
	,leading_ind_pop
	,mnr_nice_ffs_flag
	,mnr_total_ffs_flag
	,mnr_oah_flag
	,cns_oah_flag
	,mnr_dual_flag
	,cns_dual_flag
	,ocm_migration
	,component
;



--Step 31: Leading Indicator Export
drop table if exists tmp_1m.ec_ip_dataset_LI_${notifications_date}_trs ;
create table tmp_1m.ec_ip_dataset_LI_${notifications_date}_trs as
select 	
	admit_week
	,hce_admit_month
	,adjd_yrmonth
--	,admit_act_qtr
	,fst_srvc_month AS service_month
--	,perc_offset
--	,total_oah_flag
--	,institutional_flag
	,fin_tfm_product_new
--	,sgr_source_name
--	,nce_tadm_dec_risk_type
--	,fin_market
--	,group_name
--	,los_categories
--	,respiratory_flag
	,case when admit_type in ('Transplant') then 'Surgical'
		else admit_type end as admit_type
	,ipa_pac_flag
--	,mnr_cosmos_ffs_flag
--	,leading_ind_pop
--	,mnr_nice_ffs_flag
--	,mnr_total_ffs_flag
--	,mnr_oah_flag
--	,cns_oah_flag
--	,mnr_dual_flag
--	,cns_dual_flag
--	,ocm_migration
	,component
	,sum(case_count) as case_count
	,sum(Initial_ADR_cnt) as Initial_ADR_cnt
	,sum(persistent_adr_cnt) as persistent_adr_cnt
--	,sum(md_reviewed_cnt) as md_reviewed_cnt
--	,sum(appeal_case_cnt) as appeal_case_cnt
--	,sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
--	,sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
--	,sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
--	,sum(p2p_case_cnt) as p2p_case_cnt
--	,sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
--	,sum(other_ovtrns) as other_ovtrns
	,sum(membership) as membership
	,sum(allowed) as allowed
--	,sum(netpaid) as netpaid
	,sum(admits) as admits
	,0 as days
	,sum(frank_days) as franky_days
	,sum(franky_paid) as franky_paid
	,sum(franky_admits) as franky_admits
	,sum(franky_allw) as franky_allowed
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
where leading_ind_pop =1 
	and admit_type not in ('Other')
	and hce_admit_month > '202112'
	and ipa_pac_flag in ('IPA','PAC','MM')
group by 
	admit_week
	,hce_admit_month
	,adjd_yrmonth
--	,admit_act_qtr
	,fst_srvc_month
--	,perc_offset
--	,total_oah_flag
--	,institutional_flag
	,fin_tfm_product_new
--	,sgr_source_name
--	,nce_tadm_dec_risk_type
--	,fin_market
--	,group_name
--	,los_categories
--	,respiratory_flag
	,case when admit_type in ('Transplant') then 'Surgical'
		else admit_type end
	,ipa_pac_flag
--	,mnr_cosmos_ffs_flag
--	,leading_ind_pop
--	,mnr_nice_ffs_flag
--	,mnr_total_ffs_flag
--	,mnr_oah_flag
--	,cns_oah_flag
--	,mnr_dual_flag
--	,cns_dual_flag
--	,ocm_migration
	,component
	;

--Step 32: Getting all combos of admit month, service month with adjd month
drop table if exists tmp_1m.ec_ip_dataset_LI_${notifications_date}_1_trs ;
create table tmp_1m.ec_ip_dataset_LI_${notifications_date}_1_trs as
select DISTINCT 	
	a.fin_tfm_product_new
	,a.admit_type
	,a.component
	,a.ipa_pac_flag
	,a.hce_admit_month
	,a.service_month
	,b.ADJD_Month as adjd_yrmonth
from tmp_1m.ec_ip_dataset_LI_${notifications_date}_trs as a 
left join TMP_2Y.EC_FRANKY_EXTRAP_2026 as b
on a.hce_admit_month=b.hce_month
;


--Step 33: Uninoning on  current adjdmonth onto final export 
drop table if exists tmp_1m.ec_ip_dataset_LI_${notifications_date}_2_trs ;
create table tmp_1m.ec_ip_dataset_LI_${notifications_date}_2_trs as
select 
	admit_week
	,hce_admit_month
	,adjd_yrmonth
	,service_month
	,fin_tfm_product_new
	,admit_type
	,ipa_pac_flag
	,component
	,case_count
	,Initial_ADR_cnt
	,persistent_adr_cnt
	,membership
	,allowed
	,admits
	,days
	,franky_days
	,franky_paid
	,franky_admits
	,franky_allowed
from tmp_1m.ec_ip_dataset_LI_${notifications_date}_trs
union all select 
	000000 as admit_week
	,hce_admit_month
	,cast(adjd_yrmonth as string) as adjd_yrmonth
	,service_month
	,fin_tfm_product_new
	,admit_type
	,ipa_pac_flag
	,component
	,0 as case_count
	,0 as Initial_ADR_cnt
	,0 as persistent_adr_cnt
	,0 as membership
	,0 as allowed
	,0 as admits
	,0 as days
	,0 as franky_days
	,0 as franky_paid
	,0 as franky_admits
	,0 as franky_allowed
from tmp_1m.ec_ip_dataset_LI_${notifications_date}_1_trs
;

--Step 34: final Roll up for export 
drop table if exists tmp_1m.ec_ip_dataset_LI_${notifications_date}_3_trs ;
create table tmp_1m.ec_ip_dataset_LI_${notifications_date}_3_trs as
select 
	admit_week
	,hce_admit_month
	,try_cast(adjd_yrmonth as int) as adjd_yrmonth
	,service_month
	,fin_tfm_product_new
	,admit_type
	,ipa_pac_flag
	,component
	,sum(case_count) as case_count
	,sum(Initial_ADR_cnt) as Initial_ADR_cnt
	,sum(persistent_adr_cnt) as persistent_adr_cnt
	,sum(membership) as membership
	,sum(allowed) as allowed
	,sum(admits) as admits
	,sum(days) as days
	,sum(franky_days) as franky_days
	,sum(franky_paid) as franky_paid
	,sum(franky_admits) as franky_admits
	,sum(franky_allowed) as franky_allowed
from tmp_1m.ec_ip_dataset_LI_${notifications_date}_2_trs
group by 
	admit_week
	,hce_admit_month
	,adjd_yrmonth
	,service_month
	,fin_tfm_product_new
	,admit_type
	,ipa_pac_flag
	,component
	;

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
--Completion Dataset

--Completion Step 1: Weekly IPA Notifications, for LOC Valuation and LI
drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_1;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_1 as
select 
	'Weekly Notifs' as comp_type
	,admit_week
    ,'' as hce_admit_month
    ,'' AS service_month
    ,'' as adjd_yrmonth
    ,'' as tfm_product_new_fnl
    ,ipa_pac_flag
    ,CASE WHEN admit_type = 'Surgical' THEN 'Surgical' 
    	WHEN admit_type IN ('AIR','LTAC','SNF','Swing Bed') THEN 'PAC' ELSE 'Other IPA' end as admit_type /*newly added in dec 2025*/
    ,'' as cap_status
    ,sum(case_count) as case_count
    ,sum() as 
    ,sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
    ,sum(persistent_adr_cnt) as persistent_adr_cnt
    ,0 as membership
    ,0 as franky_admits
    ,0 as franky_allowed
    ,0 as franky_days
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
where ipa_pac_flag in ('IPA','PAC')
     and component = 'Auths'
-- Will need to make sure transplants don't get included into this when we add them in .... But don't we need that for the LI piece....
group by 
	admit_week
    ,ipa_pac_flag
    ,CASE WHEN admit_type = 'Surgical' THEN 'Surgical' 
    	WHEN admit_type IN ('AIR','LTAC','SNF','Swing Bed') THEN 'PAC' ELSE 'Other IPA' end
;

--Completion Step 2: MM for Weekly IPA Notifications
drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_2a;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_2a as
select distinct 
	admit_week
    ,hce_admit_month
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
;

drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_2b;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_2b as
select 
	hce_admit_month
    ,sum(membership) as membership
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
where component = 'Membership'
group by hce_admit_month
;

drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_2;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_2 as
select 
	'Week MM' as comp_type
    ,b.admit_week
    ,a.hce_admit_month
    ,'' AS service_month
    ,'' as adjd_yrmonth
    ,'' as tfm_product_new_fnl
    ,'' as ipa_pac_flag
    ,'' as admit_type
    ,'' as cap_status
    ,0 as case_count
    ,0 as 
    ,0 as p2p_ovrtn_case_cnt
    ,0 as persistent_adr_cnt
    ,a.membership
    ,0 as franky_admits
    ,0 as franky_allowed
    ,0 as franky_days
from tmp_1m.ec_ip_dataset_comp_${notifications_date}_2b as a
left join tmp_1m.ec_ip_dataset_comp_${notifications_date}_2a as b
     on a.hce_admit_month = b.hce_admit_month
where b.admit_week > 0
;

--Completion Step 3: Monthly Membership
drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_3;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_3 as
select 'Month MM' as comp_type
	,0 as admit_week
	,hce_admit_month
	,'' AS service_month
    ,'' as adjd_yrmonth
	,fin_tfm_product_new
	,'' as ipa_pac_flag
    ,'' as admit_type
    ,'' as cap_status
    ,0 as case_count
    ,0 as 
    ,0 as p2p_ovrtn_case_cnt
    ,0 as persistent_adr_cnt
    ,sum(membership) as membership
    ,0 as franky_admits
    ,0 as franky_allowed
    ,0 as franky_days
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
where component = 'Membership'
	and leading_ind_pop =1
group by hce_admit_month
	,fin_tfm_product_new
;

--Completion Step 4: Claims Completion Factors --
drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_4;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_4 as
select 'Claims' as comp_type
    ,0 as admit_week
    ,hce_admit_month
    ,'' AS service_month
    ,adjd_yrmonth
    ,fin_tfm_product_new
    ,ipa_pac_flag
    ,case when admit_type in ('Transplant') then 'Surgical'
		else admit_type end as admit_type
    ,'' as cap_status
    ,0 as case_count
    ,0 as 
    ,0 as p2p_ovrtn_case_cnt
    ,0 as persistent_adr_cnt
    ,0 as membership
    ,sum(franky_admits) as franky_admits
    ,sum(franky_allw) as franky_allowed
    ,sum(frank_days) as franky_days
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
where leading_ind_pop =1 
    and admit_type not in ('Other')
	and hce_admit_month > '202112'
	and adjd_yrmonth not in ('','MM')
	and component = 'Claims'
group by 
	hce_admit_month
    ,adjd_yrmonth
    ,fin_tfm_product_new
    ,ipa_pac_flag
    ,admit_type
;

----Completion Step 5: Reserve Adjustments - % Cap
drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_5;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_5 AS
select 'Non-Cap' as comp_type
	,0 as admit_week
	,hce_admit_month
	,fst_srvc_month as service_month
	,adjd_yrmonth
	,fin_tfm_product_new
	,ipa_pac_flag
	,'' as admit_type
	,case when global_cap = 'NA' then 'N' else 'Y' end as cap_status
	,0 as case_count
    ,0 as 
    ,0 as p2p_ovrtn_case_cnt
    ,0 as persistent_adr_cnt
    ,0 as membership
    ,0 as franky_admits
	,sum(franky_allw) as franky_allowed
	,0 as franky_days
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
	where component = 'Claims' 
	--Leading Indicator Pop without the Cap Filter--
		and fin_brand='M&R'
		and sgr_source_name='COSMOS'
		and fin_product_level_3 <>'INSTITUTIONAL'
		AND tfm_include_flag=1
		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
		and admit_type <> 'Other'
		and (hce_admit_month > '202306')
group by hce_admit_month
	,fst_srvc_month
	,adjd_yrmonth
	,fin_tfm_product_new
	,ipa_pac_flag
	,case when global_cap = 'NA' then 'N' else 'Y' end
;

----Completion Step 6: Reserve Adjustments - % MH
drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_6;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_6 AS
select 'MH' as comp_type
	,0 as admit_week
	,hce_admit_month
	,fst_srvc_month as service_month
	,adjd_yrmonth
	,fin_tfm_product_new
	,ipa_pac_flag
	,'' as admit_type
	,'' as cap_status
	,0 as case_count
    ,0 as 
    ,0 as p2p_ovrtn_case_cnt
    ,0 as persistent_adr_cnt
    ,0 as membership
    ,0 as franky_admits
	,sum(franky_allw) as franky_allowed
	,0 as franky_days
from tmp_1m.ec_ip_dataset_all_${notifications_date}_trs
	where component = 'Claims' 
	--Leading Indicator Pop without the Cap Filter--
		and fin_brand='M&R'
		and sgr_source_name='COSMOS'
		and fin_product_level_3 <>'INSTITUTIONAL'
		AND tfm_include_flag=1
		AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
		and (hce_admit_month > '202306') -- OR fst_srvc_month > '202206')
group by hce_admit_month
	,fst_srvc_month
	,adjd_yrmonth
	,fin_tfm_product_new
	,ipa_pac_flag
;

----Completion Step 7: 2024 Roster Table (ARCHIVED)
--drop table if exists tmp_2y.ec_ip_mm_2024;
--create table tmp_2y.ec_ip_mm_2024 as
--select distinct fin_mbi_hicn_fnl
--	,'202401' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202401 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
--	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202402' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202402 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202403' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202403 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
--union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202404' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202404 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202405' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202405 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202406' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202406 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202407' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202407 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202408' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202408 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202409' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202409 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202410' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202410 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
--	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202411' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202411 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
-- union all	
-- select distinct fin_mbi_hicn_fnl
--	,'202412' as roster_month
--	,fin_inc_month as enroll_month
--	,fin_tfm_product_new
--from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202412 as a
--where fin_inc_year in ('2024')
--	and fin_brand='M&R'
--	AND global_cap='NA'
--	AND sgr_source_name='COSMOS'
--	AND fin_product_level_3 <>'INSTITUTIONAL'
--	AND tfm_include_flag=1 
-- 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
--;

----Completion Step 8: 2025 Roster Table
--1/5/26: completed
/*
drop table if exists tmp_2y.ec_ip_mm_2025;
create table tmp_2y.ec_ip_mm_2025 as
select distinct fin_mbi_hicn_fnl
	,'202501' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202501 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202502' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202502 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202503' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202503 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202504' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202504 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
union all	
select distinct fin_mbi_hicn_fnl
	,'202505' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
union all	
select distinct fin_mbi_hicn_fnl
	,'202506' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202506 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202507' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202507 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202508' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202508 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202509' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202509 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202510' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202510 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202511' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202511 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202512' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202512 as a
where fin_inc_year in ('2024','2025')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
;
*/

----Completion Step 9: 2026 Roster Table (uncomment each new roster month)
 /*
drop table if exists tmp_1m.ec_ip_mm_2026;
create table tmp_1m.ec_ip_mm_2026 as
select distinct fin_mbi_hicn_fnl
	,'202601' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202601 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202602' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202602 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202603' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202603 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202604' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202604 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
union all	
select distinct fin_mbi_hicn_fnl
	,'202605' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202605 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
union all	
select distinct fin_mbi_hicn_fnl
	,'202606' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202606 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202607' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202607 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202608' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202608 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202609' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202609 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202610' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202610 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202611' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202611 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
 union all	
 select distinct fin_mbi_hicn_fnl
	,'202612' as roster_month
	,fin_inc_month as enroll_month
	,fin_tfm_product_new
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202612 as a
where fin_inc_year in ('2025','2026')
	and fin_brand='M&R'
	AND global_cap='NA'
	AND sgr_source_name='COSMOS'
	AND fin_product_level_3 <>'INSTITUTIONAL'
	AND tfm_include_flag=1 
 	AND fin_tfm_product_new in ('HMO','PPO','NPPO','DUAL_CHRONIC')
*/
;


drop table if exists tmp_1m.ec_ip_mm_stack;
create table tmp_1m.ec_ip_mm_stack as
select * from tmp_2y.ec_ip_mm_2025
union all
select * from tmp_1m.ec_ip_mm_2026
;

drop table if exists tmp_1m.ec_ip_mm_joiner;
create table tmp_1m.ec_ip_mm_joiner as
select fin_mbi_hicn_fnl
	,enroll_month
	,fin_tfm_product_new
	,min(roster_month) as roster_month_joiner
from tmp_1m.ec_ip_mm_stack
group by fin_mbi_hicn_fnl
	,enroll_month
	,fin_tfm_product_new
;

drop table if exists tmp_1m.ec_ip_mm_leaver;
create table tmp_1m.ec_ip_mm_leaver as
select fin_mbi_hicn_fnl
	,enroll_month
	,fin_tfm_product_new
	,max(roster_month) as roster_month_leaver
from tmp_1m.ec_ip_mm_stack
group by fin_mbi_hicn_fnl
	,enroll_month
	,fin_tfm_product_new
;


drop table if exists tmp_1m.ec_ip_mm_joiner2;
create table tmp_1m.ec_ip_mm_joiner2 as
select 'Roster Joiner' as comp_type
	,0 as admit_week
	,enroll_month as hce_admit_month
	,roster_month_joiner as service_month
	,'' as adjd_yr_month
	,fin_tfm_product_new
	,'' as ipa_pac_flag
	,'' as admit_type
	,'' as cap_status
	,0 as case_count
   	,0 as 
    ,0 as p2p_ovrtn_case_cnt
    ,0 as persistent_adr_cnt
    ,count(distinct fin_mbi_hicn_fnl) as membership
    ,0 as franky_admits
	,0 as franky_allowed
	,0 as franky_days
from tmp_1m.ec_ip_mm_joiner
group by enroll_month
	,roster_month_joiner
	,fin_tfm_product_new
;

drop table if exists tmp_1m.ec_ip_mm_leaver2;
create table tmp_1m.ec_ip_mm_leaver2 as
select 'Roster Leaver' as comp_type
	,0 as admit_week
	,enroll_month as hce_admit_month
	,roster_month_leaver as service_month
	,'' as adjd_yr_month
	,fin_tfm_product_new
	,'' as ipa_pac_flag
	,'' as admit_type
	,'' as cap_status
	,0 as case_count
   	,0 as 
    ,0 as p2p_ovrtn_case_cnt
    ,0 as persistent_adr_cnt
    ,count(distinct fin_mbi_hicn_fnl) as membership
    ,0 as franky_admits
	,0 as franky_allowed
	,0 as franky_days
from tmp_1m.ec_ip_mm_leaver
group by enroll_month
	,roster_month_leaver
	,fin_tfm_product_new
;

drop table if exists tmp_1m.ec_ip_dataset_comp_${notifications_date}_fnl;
create table tmp_1m.ec_ip_dataset_comp_${notifications_date}_fnl as
	select * from tmp_1m.ec_ip_dataset_comp_${notifications_date}_1
union all
	select * from tmp_1m.ec_ip_dataset_comp_${notifications_date}_2
union all
	select * from tmp_1m.ec_ip_dataset_comp_${notifications_date}_3
union all
	select * from tmp_1m.ec_ip_dataset_comp_${notifications_date}_4
union all
	select * from tmp_1m.ec_ip_dataset_comp_${notifications_date}_5
union all
	select * from tmp_1m.ec_ip_dataset_comp_${notifications_date}_6
union all
	select * from tmp_1m.ec_ip_mm_joiner2
union all
	select * from tmp_1m.ec_ip_mm_leaver2
;


---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
--EXPORT DATA TABLES:

--IN SAS BELOW

--libname HCX_EC "/hpsasfin/int/projects/hcemrn/ec/prod/data/";
/*
data hcx_ec.LOC_IP_11_26_25 (compress=yes); 
set TMP_1M.EC_IP_DATASET_LOC_11262025
;run;

data hcx_ec.ec_ip_dataset_11_26_2025 (compress=yes); 
set TMP_1M.EC_IP_DATASET_LI_11262025_3_TRS
;run;

data hcx_ec.ec_ip_dataset_comp_11262025_fnl (compress=yes); 
set TMP_1M.EC_IP_DATASET_COMP_11262025_FNL 
;run;
*/
 



