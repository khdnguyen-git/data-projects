/*==============================================================================
 * Re-running LOC mm portion to add mbi
 * For 2024+ only
 *==============================================================================*/
drop table if exists tmp_1m.kn_ip_dataset_09172025_mm_check;
create table tmp_1m.kn_ip_dataset_09172025_mm_check stored as orc as 
select 
	a.fin_mbi_hicn_fnl
	, 000000 as fin_inc_week
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
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202508 /**/ as a /*MAKE SURE THIS IS MOST RECENT ENROLLMENT TABLE*/
left join fichsrv.group_crosswalk as b
		on a.tadm_group_nbr_consist = b.group_number  
		and a.fin_inc_year = b.`year`
where fin_inc_year in ('2024','2025')
group by 
	a.fin_mbi_hicn_fnl
	, a.fin_inc_month
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

drop table if exists tmp_1m.kn_nice_physician_mbi;
create table tmp_1m.kn_nice_physician_mbi as
select distinct 
	a.fin_mbi_hicn_fnl as mbi
	, a.fin_inc_month
	, a.global_cap
	, a.migration_source
	, a.fin_brand
	, a.tfm_include_flag
	, a.fin_product_level_3
	, a.nce_tadm_dec_risk_type
	, a.fin_g_i
	, a.fin_market
from tmp_1m.kn_ip_dataset_09172025_mm_check as a
where MnR_NICE_FFS_Flag = 1
	and nce_tadm_dec_risk_type = 'PHYSICIAN'
	and fin_inc_month >= '202401'
;

drop table if exists tmp_1m.kn_nice_physician_auth_loc;
create table tmp_1m.kn_nice_physician_auth_loc as
select
	a.mbi
	, b.case_id
	, b.member_id
	, b.subscriber_id
	, b.avtar_mtch_ind
	, b.case_category_cd
	, b.svc_setting
	, b.notif_recd_dttm
	, b.notif_yrmonth
	, b.prim_srvc_cat
	, b.business_segment
	, b.entity
	, b.hce_category
	, b.appeal_ind
	, b.appeal_ovrtn_ind
	, b.oth_ovrtn_ind
	, b.p2p_full_evertouched_cnt
	, b.p2p_full_ovtn
	, b.p2p_match_ind
	, b.mcr_reconsideration_ind
	, b.mcr_evertouched_decn_ind
	, b.mcr_ovtrn_ind
	, b.mcr_uphelp_ind
	, b.svc_seq_id
	, b.proc_cd
	, b.prim_proc_ind
	, b.prim_diag_cd
	, b.proc_unit_cnt
	, b.case_decn_stat_cd
	, a.global_cap
	, a.migration_source
	, a.fin_brand
	, a.tfm_include_flag
	, a.fin_product_level_3
	, a.nce_tadm_dec_risk_type
	, a.fin_g_i
	, a.fin_market
	, b.initialfulladr_cases
	, b.persistentfulladr_cases
	, b.transplant_flag
from tmp_1m.kn_nice_physician_mbi as a
left join tmp_1m.kn_avtar_24_25_3 as b
	on a.mbi = b.fin_mbi_hicn_fnl
	and b.fin_brand in ('M&R','C&S')
	and notif_yrmonth >= '202401'
	and loc_flag = 1
;

select count(*) from tmp_1m.kn_nice_physician_auth_loc;
-- 1214225

select notif_yrmonth, count(distinct case_id) from tmp_1m.kn_nice_physician_auth_loc
group by notif_yrmonth
order by notif_yrmonth; 
--notif_yrmonth	_c1
--[NULL]	0
--202401	1,222
--202402	1,097
--202403	1,235
--202404	1,385
--202405	1,345
--202406	1,212
--202407	1,298
--202408	1,238
--202409	1,259
--202410	1,356
--202411	1,278
--202412	1,346
--202501	1,173
--202502	1,081
--202503	1,194
--202504	1,145
--202505	1,091
--202506	1,199
--202507	1,350
--202508	1,116
--202509	611



drop table if exists tmp_1m.kn_nice_physician_auth_epal ;
create table tmp_1m.kn_nice_physician_auth_epal as
select
	a.mbi
	, b.case_id
	, b.member_id
	, b.subscriber_id
	, b.avtar_mtch_ind
	, b.case_category_cd
	, b.svc_setting
	, b.notif_recd_dttm
	, b.notif_yrmonth
	, b.prim_srvc_cat
	, b.business_segment
	, b.entity
	, b.hce_category
	, b.appeal_ind
	, b.appeal_ovrtn_ind
	, b.oth_ovrtn_ind
	, b.p2p_full_evertouched_cnt
	, b.p2p_full_ovtn
	, b.p2p_match_ind
	, b.mcr_reconsideration_ind
	, b.mcr_evertouched_decn_ind
	, b.mcr_ovtrn_ind
	, b.mcr_uphelp_ind
	, b.svc_seq_id
	, b.proc_cd
	, b.prim_proc_ind
	, b.prim_diag_cd
	, b.proc_unit_cnt
	, b.case_decn_stat_cd
	, a.global_cap
	, a.migration_source
	, a.fin_brand
	, a.tfm_include_flag
	, a.fin_product_level_3
	, a.nce_tadm_dec_risk_type
	, a.fin_g_i
	, a.fin_market
	, b.initialfulladr_cases
	, b.persistentfulladr_cases
	, b.transplant_flag
from tmp_1m.kn_nice_physician_mbi as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
	on a.mbi = b.fin_mbi_hicn_fnl
	and b.business_segment not in ('EnI','ERR','null') 
	and b.avtar_mtch_ind = 1
	and b.pa_program not in ('Not EPAL-Prime','Non-EPAL')
	and b.notif_yrmonth >= '202401' ;

select distinct pa_program from hce_proj_bd.hce_adr_avtar_like_24_25_f
where pa_program not in ('Not EPAL-Prime','Non-EPAL')

select notif_yrmonth, count(distinct case_id) as auth_EPAL from tmp_1m.kn_nice_physician_auth_epal
group by notif_yrmonth;

--notif_yrmonth	_c1
--[NULL]	0
--202401	1,175
--202402	1,113
--202403	914
--202404	1,004
--202405	1,122
--202406	1,034
--202407	1,180
--202408	1,273
--202409	1,419
--202410	1,229
--202411	1,448
--202412	1,222
--202501	902
--202502	1,582
--202503	769
--202504	751
--202505	728
--202506	1,407
--202507	1,381
--202508	1,299
--202509	619

drop table if exists tmp_1m.kn_nice_physician_loc_vs_epal ;
create table tmp_1m.kn_nice_physician_loc_vs_epal as
select
	'LOC' as program
	, notif_yrmonth
	, case_decn_stat_cd
	, global_cap
	, migration_source
	, fin_brand
	, tfm_include_flag
	, fin_product_level_3
	, nce_tadm_dec_risk_type
	, fin_g_i
	, fin_market
	, initialfulladr_cases
	, persistentfulladr_cases
	, count(distinct case_id) as n_auth
from tmp_1m.kn_nice_physician_auth_loc
group by 
	notif_yrmonth
	, case_decn_stat_cd
	, global_cap
	, migration_source
	, fin_brand
	, tfm_include_flag
	, fin_product_level_3
	, nce_tadm_dec_risk_type
	, fin_g_i
	, fin_market
	, initialfulladr_cases
	, persistentfulladr_cases
union all
select
	'EPAL' as program
	, notif_yrmonth
	, case_decn_stat_cd
	, global_cap
	, migration_source
	, fin_brand
	, tfm_include_flag
	, fin_product_level_3
	, nce_tadm_dec_risk_type
	, fin_g_i
	, fin_market
	, initialfulladr_cases
	, persistentfulladr_cases
	, count(distinct case_id) as n_auth
from tmp_1m.kn_nice_physician_auth_epal
group by 
	notif_yrmonth
	, case_decn_stat_cd
	, global_cap
	, migration_source
	, fin_brand
	, tfm_include_flag
	, fin_product_level_3
	, nce_tadm_dec_risk_type
	, fin_g_i
	, fin_market
	, initialfulladr_cases
	, persistentfulladr_cases
;

select * from tmp_1m.kn_nice_physician_loc_vs_epal;