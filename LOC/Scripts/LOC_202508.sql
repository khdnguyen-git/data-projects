
--Step 0.a: Update to the new date & if a monthly claims run; update tre copy cosmos tab 
/*Every week*/
--Find and change date: _09242025
--IMPORTANT: MAKE SURE CLAIMS TABLES IN STEP 27 REFLECT OLD DATE IF NO CLAIMS UPDATE
--MAKE SURE THERE IS NO SPACE AFTER DATE OR ELSE IT WILL NOT WORK
--9/24/25: done--
--9/5/2025: August Claims should be 09032025 until we recieve September claims (due a modeling change) then go back to normal update cadence

--Step 0.b: check to see if current month membership is available
select count(*) from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202509; --make sure this IS CURRENT MONTH-- 
--"from tadm_tre_cpy.gl_rstd_gpsgalnce_f_[CURRENT MONTH ENROLLMENT] /**/ as a"
--9/24/25: done; it's September enrollment table now--
--ONLY STEP 9 SHOULD BE CHANGING; CHECK CODE AT VERY BOTTOM TO MAKE SURE ENROLLMENT TABLE PULL NAMES ARE NOT CHANGING--

--monthly ish
--Step 0.c Uncomment out most recent roster month from Completion Step 8: tmp_1m.kn_ip_mm_2025 IF 0.B SHOWS NEXT MONTH MEMBERSHIP AVAILABLE
--Don't forget to update roster month in Notification Completion Model
--CHECK THAT STEP 0.B DID NOT OVERRIDE ANY CODE FOR ROSTER MONTH
--9/24/25: done; no new enrollment table; uncommented September section last week--


/*Monthly claims update*/
--Step 0.d: change claims month
--Find and change Tre Copy Table: tadm_tre_cpy.glxy_ip_admit_f_202508
--check that step 27 is reflecting current claims table in union
--9/24/25: done; no new claims yet--




--Step 1: Check that AvTar was Updated with this query to check latest date (should be day of or day before run)
select max(admit_dt_act) from hce_proj_bd.HCE_ADR_AVTAR_Like_24_25_F
where 	
       svc_setting ='Inpatient' --Inpatient Services
       and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
       and admit_cat_cd  in ('17 - Medical','30 - Surgical')			
       and fin_brand in ('M&R','C&S')
       and DATE_FORMAT(admit_dt_act, 'MM/dd/yyyy') is not null 
       and DATE_FORMAT(admit_dt_act  ,'yyyy') in ('2025')	;	


--Step 2.1: Getting remaining TRS cases that did not match to Notifications
drop table tmp_1m.kn_avtar_22_trs;
create table tmp_1m.kn_avtar_22_trs stored as orc as
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
	,0 as initialpartialadr_cases
	,0 as persistentfulladr_cases
	,0 as persistentpartialadr_cases
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
	tmp_1m.TRS_DATA_SET_FNL as a
left outer join
	hce_proj_bd.hce_adr_avtar_like_2022_f as b
on  b.transplant_flag='Y'
and a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where b.fin_mbi_hicn_fnl is null and year(a.transplantdate)=2022
;



select * from hce_proj_bd.hce_adr_avtar_like_2023_f

--Step 2.2: Union non-notification TRS to rest of notifications
drop table tmp_1m.kn_avtar_22_trs_combined;
create table tmp_1m.kn_avtar_22_trs_combined stored as orc as
select 
	* from hce_proj_bd.hce_adr_avtar_like_2022_f
union all select
	* from tmp_1m.kn_avtar_22_trs
	;

--Step 2.3: Adding variable to split PAC from IPA 
drop table tmp_1m.kn_avtar_22_1_trs;
create table tmp_1m.kn_avtar_22_1_trs stored as orc as
select 
	a.*
	,cast(admission_date as date) as trs_admit_dt
	,cast(transplantdate as date) as trs_transplant_dt
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
from tmp_1m.kn_avtar_22_trs_combined as a 
;

--Step 2.4: Create a date field that works for PAC & IPA (IPA cares about only closed cases while PAC cares about open and closed)
drop table tmp_1m.kn_avtar_22_2_trs;
create table tmp_1m.kn_avtar_22_2_trs stored as orc as
select     
	*
	,case when ip_type in ('Medical','Surgical','Transplant') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('Transplant') and admit_dt_act is null and admission_date is not null then trs_admit_dt
		when ip_type in ('Transplant')and admit_dt_act is null and admission_date is null then trs_transplant_dt
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is null then admit_dt_exp else null end as hcedt 
from tmp_1m.kn_avtar_22_1_trs
;


--Step 2.5: Add week & hce_month variable
drop table tmp_1m.kn_avtar_22_3_trs;
create table tmp_1m.kn_avtar_22_3_trs stored as orc as
select 
	a.*
	,concat(lpad(year(a.hcedt),4,0),lpad(month(a.hcedt),2,0)) as hce_admit_month
	,cast(hcedt as date) as hce_dt
	,c.week as admit_week
from tmp_1m.kn_avtar_22_2_trs as a 
left join tmp_2y.ec_loc_week_assign as c 
on a.hcedt=c.`date` 
;

--Step 3.1: Getting remaining TRS cases that did not match to Notifications
drop table tmp_1m.kn_avtar_23_trs;
create table tmp_1m.kn_avtar_23_trs stored as orc as
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
	,0 as initialpartialadr_cases
	,0 as persistentfulladr_cases
	,0 as persistentpartialadr_cases
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
	tmp_1m.TRS_DATA_SET_FNL as a
left outer join
	hce_proj_bd.hce_adr_avtar_like_2023_f as b
on  b.transplant_flag='Y'
and a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where b.fin_mbi_hicn_fnl is null and year(a.transplantdate)=2023
;



--Step 3.2: Union non-notification TRS to rest of notifications
drop table tmp_1m.kn_avtar_23_trs_combined;
create table tmp_1m.kn_avtar_23_trs_combined stored as orc as
select 
	* from hce_proj_bd.hce_adr_avtar_like_2023_f
union all select
	* from tmp_1m.kn_avtar_23_trs
	;


--Step 3.3:Run this every week: Add week to the table with recent data that Pradeepa emails about 
drop table tmp_1m.kn_avtar_23_1_trs;
create table tmp_1m.kn_avtar_23_1_trs stored as orc as
select 
	a.*
	,cast(admission_date as date) as trs_admit_dt
	,cast(transplantdate as date) as trs_transplant_dt
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
from tmp_1m.kn_avtar_23_trs_combined as a 
;

--Step 3.4: Create a date field that works for PAC & IPA (IPA cares about only closed cases while PAC cares about open and closed)
drop table tmp_1m.kn_avtar_23_2_trs;
create table tmp_1m.kn_avtar_23_2_trs stored as orc as
select     
	*
	,case when ip_type in ('Medical','Surgical','Transplant') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('Transplant') and admit_dt_act is null and admission_date is not null then trs_admit_dt
		when ip_type in ('Transplant')and admit_dt_act is null and admission_date is null then trs_transplant_dt
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is null then admit_dt_exp else null end as hcedt
from tmp_1m.kn_avtar_23_1_trs
;

--Step 3.5: Add week & hce_month variable
drop table tmp_1m.kn_avtar_23_3_trs;
create table tmp_1m.kn_avtar_23_3_trs stored as orc as
select 
	a.*
	,concat(lpad(year(a.hcedt),4,0),lpad(month(a.hcedt),2,0)) as hce_admit_month
	,cast(hcedt as date) as hce_dt
	,c.week as admit_week
from tmp_1m.kn_avtar_23_2_trs as a 
left join tmp_2y.ec_loc_week_assign as c 
on a.hcedt=c.`date` 
;

--Step 4.1: Getting remaining TRS cases that did not match to Notifications
drop table tmp_1m.kn_avtar_24_25_trs;
create table tmp_1m.kn_avtar_24_25_trs stored as orc as
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
	,0 as initialpartialadr_cases
	,0 as persistentfulladr_cases
	,0 as persistentpartialadr_cases
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
	tmp_1m.TRS_DATA_SET_FNL as a
left outer join
	hce_proj_bd.hce_adr_avtar_like_24_25_f as b
on  b.transplant_flag='Y'
and a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where b.fin_mbi_hicn_fnl is null and year(a.transplantdate)>2023
;

--Step 4.2: Union non-notification TRS to rest of notifications
drop table tmp_1m.kn_avtar_24_25_trs_combined;
create table tmp_1m.kn_avtar_24_25_trs_combined stored as orc as
select 
	* from hce_proj_bd.hce_adr_avtar_like_24_25_f
union all select
	* from tmp_1m.kn_avtar_24_25_trs
	;

--Step 4.3 :Run this every week: Add week to the table with recent data that Pradeepa emails about 
drop table tmp_1m.kn_avtar_24_25_1;
create table tmp_1m.kn_avtar_24_25_1 stored as orc as
select 
	a.*
	,cast(admission_date as date) as trs_admit_dt
	,cast(transplantdate as date) as trs_transplant_dt
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
from tmp_1m.kn_avtar_24_25_trs_combined as a 
;


--Step 4.4: Create a date field that works for PAC & IPA (IPA cares about only closed cases while PAC cares about open and closed)
drop table tmp_1m.kn_avtar_24_25_2;
create table tmp_1m.kn_avtar_24_25_2 stored as orc as
select     
	*
	,case when ip_type in ('Medical','Surgical','Transplant') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('Transplant') and admit_dt_act is null and admission_date is not null then trs_admit_dt
		when ip_type in ('Transplant')and admit_dt_act is null and admission_date is null then trs_transplant_dt
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is not null then admit_dt_act
		when ip_type in ('LTAC','AIR','SNF') and admit_dt_act is null then admit_dt_exp else null end as hcedt
from tmp_1m.kn_avtar_24_25_1
;

--Step 4.5: Add week & hce_month variable
drop table tmp_1m.kn_avtar_24_25_3;
create table tmp_1m.kn_avtar_24_25_3 stored as orc as
select 
	a.*
	,concat(lpad(year(a.hcedt),4,0),lpad(month(a.hcedt),2,0)) as hce_admit_month
	,cast(hcedt as date) as hce_dt
	,c.week as admit_week
from tmp_1m.kn_avtar_24_25_2 as a 
left join tmp_2y.ec_loc_week_assign as c 
on a.hcedt=c.`date` 
;



--Step 5: union together all needed notifications from the AvTar Report after Pradeepa sends the weekly email - update date of run! 
--Note: Respiratory AND leading indicator flags need to be based source of truth table tmp_1y.hce_resp_2024 and have periods in the ICDs unlike claims
drop table tmp_1m.kn_ip_dataset_09242025_trs; 
create table tmp_1m.kn_ip_dataset_09242025_trs stored as orc as 
select 
	admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,DATE_FORMAT(hce_dt,'yyyy') as admit_year
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
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff(current_date(),admit_dt_act)
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff(current_date(),admit_dt_exp)
		else los end as los_exp
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF(current_date(),admit_dt_act) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff(dschg_dt_act,admit_dt_act)
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
 		WHEN (DATE_FORMAT(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
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
    ,count(distinct (case when initialfulladr_cases=1 then case_id end)) as Intital_ADR_cnt				
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
from tmp_1m.kn_avtar_24_25_3
where 	
 	   fin_brand in ('M&R','C&S')
       and ((IP_type in ('Medical','Surgical','Transplant') and DATE_FORMAT(admit_dt_act, 'MM/dd/yyyy') is not null) or IP_type in ('LTAC','SNF','AIR'))
 group by 
admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,DATE_FORMAT(hce_dt,'yyyy') 
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
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff(current_date(),admit_dt_act)
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff(current_date(),admit_dt_exp)
		else los end 
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF(current_date(),admit_dt_act) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff(dschg_dt_act,admit_dt_act)
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
 		WHEN (DATE_FORMAT(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end 
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end 
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (YEAR(hce_dt) ='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
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
	,DATE_FORMAT(hce_dt,'yyyy') as admit_year
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
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff(current_date(),admit_dt_act)
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff(current_date(),admit_dt_exp)
		else los end as los_exp
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF(current_date(),admit_dt_act) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff(dschg_dt_act,admit_dt_act)
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
 		WHEN (DATE_FORMAT(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
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
    ,count(distinct (case when initialfulladr_cases=1 then case_id end)) as Intital_ADR_cnt				
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
from tmp_1m.kn_avtar_23_3_trs
where 	
 	   fin_brand in ('M&R','C&S')
       and ((IP_type in ('Medical','Surgical','Transplant') and DATE_FORMAT(admit_dt_act, 'MM/dd/yyyy') is not null) or IP_type in ('LTAC','SNF','AIR'))
       and (DATE_FORMAT(hce_dt,'yyyy') in ('2023') OR  date_format(hce_dt,'yyyyMM')='202401')
 group by 
admit_week
	,hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,DATE_FORMAT(hce_dt,'yyyy') 
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
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff(current_date(),admit_dt_act)
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff(current_date(),admit_dt_exp)
		else los end 
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF(current_date(),admit_dt_act) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff(dschg_dt_act,admit_dt_act)
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
 		WHEN (DATE_FORMAT(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end 
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end 
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
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
	,DATE_FORMAT(hce_dt,'yyyy') as admit_year
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
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff(current_date(),admit_dt_act)
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff(current_date(),admit_dt_exp)
		else los end as los_exp
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF(current_date(),admit_dt_act) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff(dschg_dt_act,admit_dt_act)
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
 		WHEN (DATE_FORMAT(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag	
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
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
    ,count(distinct (case when initialfulladr_cases=1 then case_id end)) as Intital_ADR_cnt				
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
from tmp_1m.kn_avtar_22_3_trs
where 	
		fin_brand in ('M&R','C&S')
       and ((IP_type in ('Medical','Surgical','Transplant') and DATE_FORMAT(admit_dt_act, 'MM/dd/yyyy') is not null) or IP_type in ('LTAC','SNF','AIR'))
--       and DATE_FORMAT(admit_dt_act  ,'yyyy') in ('2022')		
 group by 
--		000000 as admit_week
	hce_dt
	,hce_admit_month
	,admit_act_month
	,admit_act_qtr
	,DATE_FORMAT(hce_dt,'yyyy') 
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
	,case when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is not null then datediff(current_date(),admit_dt_act)
		when (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999') and admit_dt_act is null then datediff(current_date(),admit_dt_exp)
		else los end 
	,case when admit_dt_act is null then 0 
		when admit_dt_act is not null and (year(dschg_dt_act)='9999' or year(dschg_dt_exp)='9999')
		then DATEDIFF(current_date(),admit_dt_act) 
		when admit_dt_act is not null and dschg_dt_act is not null and year(dschg_dt_act)<>'9999' then datediff(dschg_dt_act,admit_dt_act)
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
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83''A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI'
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
 		WHEN (DATE_FORMAT(hce_dt,'yyyy')='2024' AND fin_brand='C&S' AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 0 else 0 end 
 	,case when fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end 
	,CASE WHEN ((business_segment = 'CnS' and fin_brand in('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (YEAR(hce_dt)='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end
 	,case when appeal_ind=1 and initialfulladr_cases=1 then 1 else 0 end 
	,case when appeal_ovrtn_ind=1 then 1 else 0 end
	,case when initialfulladr_cases=1 and appeal_ind=1 and md_escalation_ind=1 then 1 else 0 end 
	,case when auth_typ_cd='1 - PreService' then 1 else 0 end 
	,case when initialfulladr_cases=1 and md_escalation_ind=1 and appeal_ovrtn_ind=1 and appeal_ind=1 then 1 else 0 end 
 --	,ocm_migration
 ;


--Step 6: Adding in Other Needed Variables & Swing Bed based on PAC Provider list 
drop table tmp_1m.kn_ip_dataset_09242025_2_trs; 
create table tmp_1m.kn_ip_dataset_09242025_2_trs stored as orc as 
select 
	a.*
	,case when a.IP_type='SNF' and b.class='IP_SWGBED' then 1 
		else 0 end as swgbed
	,case when a.fin_product_level_3<>'INSTITUTIONAL' AND a.TFM_INCLUDE_FLAG=1 AND a.CAPITATED=0 AND a.BUSINESS_SEGMENT='MnR' then 'M&R'
		WHEN a.fin_product_level_3='DUAL' AND a.TFM_INCLUDE_FLAG=0 AND a.CAPITATED=0 AND (a.MIGRATION_SOURCE<>'OAH' or a.migration_source is null) AND a.BUSINESS_SEGMENT='CnS' 
			then 'C&S' else 'Other' end as MR_CS_Other
from tmp_1m.kn_ip_dataset_09242025_trs as a
left join tmp_1y.hk_snf_swgbed_tins2 as b
on a.prov_tin=b.prov_tin
;


--Step 7: Adding in a IPA/PAC split now that SWGBED is split out 
drop table tmp_1m.kn_ip_dataset_09242025_3_trs; 
create table tmp_1m.kn_ip_dataset_09242025_3_trs stored as orc as 
select 
	*
	,case when swgbed=1 then 'Swing Bed'
		when IP_Type in ('LTAC','SNF','AIR') then IP_type else ipa_li_split end as admit_type
	,case when swgbed=1 then 'PAC'
		when IP_type in ('LTAC','SNF','AIR') then 'PAC'
		when IP_type in ('Medical','Surgical','Transplant') then 'IPA' else 'NA' end as IPA_PAC_flag
from  tmp_1m.kn_ip_dataset_09242025_2_trs
;


--Step 8: Roll up before join to MM 
drop table tmp_1m.kn_ip_dataset_09242025_4_trs; 
create table tmp_1m.kn_ip_dataset_09242025_4_trs stored as orc as 
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
	,sum(a.intital_adr_cnt) as intital_adr_cnt
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
from tmp_1m.kn_ip_dataset_09242025_3_trs as a
left join tadm_proj_cosmos.tin_collection as d
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
drop table tmp_1m.kn_ip_dataset_09242025_mm; 
create table tmp_1m.kn_ip_dataset_09242025_mm stored as orc as 
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
		a.SGR_SOURCE_NAME in('COSMOS','CSP') AND a.fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (a.fin_inc_year ='2024' AND a.fin_brand in ('C&S')
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
	,0 as Intital_ADR_cnt
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
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202509 /**/ as a /*MAKE SURE THIS IS ENROLLMENT TABLE FOR CURRENT MONTH*/
left join fichsrv.group_crosswalk as b
		on a.tadm_group_nbr_consist = b.group_number  
		and a.fin_inc_year = b.`year`
where fin_inc_year in ('2022','2023','2024','2025')
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
		a.SGR_SOURCE_NAME in('COSMOS','CSP') AND a.fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (a.fin_inc_year ='2024' AND a.fin_brand in ('C&S')
		AND a.GLOBAL_CAP = 'NA' AND a.SGR_SOURCE_NAME IN ('COSMOS','CSP') AND a.MIGRATION_SOURCE = 'OAH' AND a.FIN_STATE = 'MD')) then 1 else 0 end
	,case when a.fin_product_level_3<>'INSTITUTIONAL' AND a.TFM_INCLUDE_FLAG=1 AND ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP'))
			OR (a.sgr_source_name = 'NICE' AND (a.nce_tadm_dec_risk_type='FFS' or a.nce_tadm_dec_risk_type='PHYSICIAN'))) AND a.fin_brand='M&R' then 'M&R'  
		WHEN a.fin_product_level_3='DUAL' AND a.TFM_INCLUDE_FLAG=0 AND ((a.global_cap='NA' and (a.sgr_source_name = 'COSMOS' or a.sgr_source_name='CSP')) OR 
			(a.sgr_source_name = 'NICE' AND (a.nce_tadm_dec_risk_type='FFS' or a.nce_tadm_dec_risk_type='PHYSICIAN'))) AND (a.MIGRATION_SOURCE<>'OAH' 
				or a.migration_source is null) AND a.fin_brand='C&S' then 'C&S' else 'Other' end 
;


--Step 10: Combine notifications and membership
drop table tmp_1m.kn_ip_dataset_notif_09242025_trs;
create table tmp_1m.kn_ip_dataset_notif_09242025_trs as				
SELECT	
	*
	from tmp_1m.kn_ip_dataset_09242025_4_trs
union all select 
	* from tmp_1m.kn_ip_dataset_09242025_mm
	; 

--Step 30: LOC Valuation Pull & export 
drop table tmp_1m.kn_ip_dataset_loc_09242025 ;
create table tmp_1m.kn_ip_dataset_loc_09242025 stored as orc as
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
	,sum(intital_adr_cnt) as intital_adr_cnt
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
from tmp_1m.kn_ip_dataset_notif_09242025_trs
where ipa_pac_flag in ('IPA','MM') 
	and hce_admit_month > '202112'
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


---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
--EXPORT DATA TABLES:

--IN SAS BELOW

--tmp_1m.kn_ip_dataset_loc_09242025

--
--libname HCX_EC "/hpsasfin/int/projects/hcemrn/ec/prod/data/";

--/*LOC valuation*/
--data LOC_IP_9_24_25 (compress=yes); /*CHANGE TO CURRENT DATE*/
--set tmp_1m.kn_ip_dataset_loc_09242025
--;run;










