
select sum(member_appeal_ind), sum(member_appeal_ovtn_ind) from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where ((fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
and admit_act_month = '202601'


select count(distinct case_id)
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where ((fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
	or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
	and admit_act_month = '202601'
	and member_appeal_ind = 1
	and svc_setting = 'Inpatient'
	and plc_of_svc_cd = '21 - Acute Hospital'
	and admit_cat_cd in ('17 - Medical', '30 - Surgical')
-- 40

select count(distinct case_id)
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where ((fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
	and admit_act_month = '202601'
	and member_appeal_ovtn_ind = 1
	and svc_setting = 'Inpatient'
	and plc_of_svc_cd = '21 - Acute Hospital'
-- 17

	
select count(distinct case_id)
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
--or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
	and admit_act_month = '202601'
	and member_appeal_ind = 1 and member_appeal_ovtn_ind = 0
	and svc_setting = 'Inpatient'
	and plc_of_svc_cd = '21 - Acute Hospital'
-- 24
	
	
select APPEAL_OVRTN_IND, MEMBER_APPEAL_OVTN_IND, OTH_OVRTN_IND, P2P_FULL_OVTN, MCR_OVTRN_IND
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
--or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
	and admit_act_month = '202601'
	and member_appeal_ind = 1 and member_appeal_ovtn_ind = 0
	and svc_setting = 'Inpatient'
	and plc_of_svc_cd = '21 - Acute Hospital'
order by 1 desc, 2 desc, 3 desc, 4 desc, 5 desc
-- 24
	

SELECT distinct
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'TMP_7D'
  AND table_name = 'HCE_ADR_AVTAR_LIKE_25_26_F_MA'
  AND (column_name ILIKE '%OVTN%' or column_name ilike '%OVRTN%' or column_name ilike '%OVTRN%')
ORDER BY column_name;


	

select distinct admit_act_month from tmp_7d.hce_adr_avtar_like_25_26_f_ma
order by admit_act_month desc






select distinct 
	gal_sbscr_nbr
	, admit_yr_month
	, bil_recv_dt
	, max(readmit_ind) over (partition by gal_sbscr_nbr, admit_yr_month order by admit_yr_month) as readmit_ind
from tmp_1m.kn_mcr_readmits_base_202512
where gal_sbscr_nbr in (
'00963759263'
)
order by admit_yr_month

select distinct
	mcr_member_id
	, mcr_work_item_id_cleaned
	, site_clm_aud_nbr
	, mcr_received_month
	, to_char(bil_recv_dt, 'yyyyMM') as bil_recv_month
	, admit_yr_month
	, admit_start_dt
	, admit_end_dt
	, pd_dn_ol_admitid 
	, indexadmit_ind
	, readmit_master_admitid
	, readmit_ind
from tmp_1m.kn_mcr_join_mnr_readmit_2024_202512
where gal_sbscr_nbr = '00963759263'
order by bil_recv_month, admit_yr_month, pd_dn_ol_admitid, indexadmit_ind desc



select min(APPDECNDT), max(APPDECNDT) from tmp_7d.hce_adr_avtar_like_25_26_f_ma


select
	*
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where APPDECNDT >= '2025-12-01'
and
case_id in 
('289564085'
, '289570147'
, '289583565'
, '289651251'
, '289652772'
, '289808558'
, '289818220'
, '289827994'
, '289873389'
, '290053280'
, '290140994'
, '290179142'
, '290362662'
, '290366690'
, '290407582'
, '290410612'
, '290420185'
, '290422303'
, '290437441'
, '290464780'
, '290500312'
, '290510595'
, '290524492'
, '290561046'
, '290574197'
, '290596141'
, '290599178'
, '290607068'
, '290609114')


select count(distinct case_id)
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where to_char(APPDECNDT, 'yyyyMM') >= '202601'
and member_appeal_ind = 1
and (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
--or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
	and svc_setting = 'Inpatient'
	and plc_of_svc_cd = '21 - Acute Hospital'

select count(distinct case_id)
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where admit_act_month = '202601'
and member_appeal_ind = 1



create or replace table tmp_1m.kn_loc_od_appeal_hcemi1_hcemnr0 as 
with extract as (
select
	case_id
	, member_appeal_ind
	, member_appeal_ovtn_ind
	, p2p_full_evertouched_cnt
	, p2p_full_ovtn
	, p2p_match_ind
	, mcr_reconsideration_ind
	, mcr_evertouched_decn_ind
	, mcr_ovtrn_ind
	, mcr_uphelp_ind
	, rvsl_ind
	, rvsl_decn_userid
	, to_char(rvsl_decn_dttm, 'yyyy-mm-dd') as rvsl_decn_dt
	, rvsl_decn_user_role
	, mcr_rvsls
	, rvsl_bed_decn_mtch_ind
	, rvsl_srv_decn_mtch_ind
	, appeal_ind
	, appeal_ovrtn_ind
	, oth_ovrtn_ind
	, to_char(initial_dnl_decn_dttm, 'yyyy-mm-dd') as initial_dnl_decn_dt
	, to_char(latest_dnl_decn_dttm, 'yyyy-mm-dd') as latest_dnl_decn_dt
	, to_char(appdecndt, 'yyyy-mm-dd') as app_decn_dt
	, appoutcome
	, appissuetype
	, fin_brand
	, sgr_source_name
	, tfm_include_flag
	, fin_source_name
	, migration_source
	, hce_category
	, business_segment
	, entity
	, avtar_mtch_ind
	, case_category_cd
	, svc_setting
	, proc_cd
	, prim_proc_ind
	, prim_proc_last_decn
	, svc_crmk_cd
	, to_char(svc_start_dt, 'yyyy-mm-dd') as svc_start_dt
	, to_char(svc_end_dt, 'yyyy-mm-dd') as svc_end_dt
	, svc_cat_cd
	, svc_cat_dtl_cd
	, plc_of_svc_cd
	, plc_of_svc_drv_cd
	, appeal
	, palist
	, prim_svc_palist
	, pa_program
	, case_init_decn_cd
	, case_svc_init_decn_cd
	, case_decn_stat_cd
	, case_svc_decn_stat_cd
	, case_prov_par_status_cd
	, admit_cat_cd
	, auth_typ_cd
	, to_char(admit_dt_act, 'yyyy-mm-dd') as admit_dt_act
	, to_char(admit_dt_exp, 'yyyy-mm-dd') as admit_dt_exp
	, to_char(dschg_dt_exp, 'yyyy-mm-dd') as dschg_dt_exp
	, case when fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
	, case when fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1) 
		or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_FFS_FLAG
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 1 else 0 end as MnR_Dual_flag
	, case when ((fin_brand in ('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and 
		sgr_source_name in ('COSMOS','CSP')) or (substr(admit_act_month, 1, 4) = '2024' and fin_brand in ('C&S')
	and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as ISNP_flag
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where APPDECNDT >= '2025-12-01'
and case_id in 
('289827994'
, '289873389'
, '290053280'
, '290407582'
, '290410612'
, '290420185'
, '290437441'
, '290464780'
, '290500312'
, '290510595'
, '290524492'
, '290574197'
, '290596141'
, '290609114')
)
select
	*
	, case when total_oah_flag = 'OAH' then 'OAH'
			when isnp_flag = 1 then 'ISNP'
			when mnr_ffs_flag = 1 then 'M&R FFS'
			when mnr_cosmos_ffs_flag = 1 then 'M&R COSMOS FFS'
			when mnr_nice_ffs_flag = 1 then 'M&R NICE FFS'
			when cns_dual_flag = 1 then 'C&S DUAL'
		else 'Others'
	end as population
from extract
order by case_id
;















create or replace table tmp_1m.kn_loc_od_appeal_hcemi0_hcemnr1 as 
with extract as (
select
	case_id
	, member_appeal_ind
	, member_appeal_ovtn_ind
	, p2p_full_evertouched_cnt
	, p2p_full_ovtn
	, p2p_match_ind
	, mcr_reconsideration_ind
	, mcr_evertouched_decn_ind
	, mcr_ovtrn_ind
	, mcr_uphelp_ind
	, rvsl_ind
	, rvsl_decn_userid
	, to_char(rvsl_decn_dttm, 'yyyy-mm-dd') as rvsl_decn_dt
	, rvsl_decn_user_role
	, mcr_rvsls
	, rvsl_bed_decn_mtch_ind
	, rvsl_srv_decn_mtch_ind
	, appeal_ind
	, appeal_ovrtn_ind
	, oth_ovrtn_ind
	, to_char(initial_dnl_decn_dttm, 'yyyy-mm-dd') as initial_dnl_decn_dt
	, to_char(latest_dnl_decn_dttm, 'yyyy-mm-dd') as latest_dnl_decn_dt
	, to_char(appdecndt, 'yyyy-mm-dd') as app_decn_dt
	, appoutcome
	, appissuetype
	, fin_brand
	, sgr_source_name
	, tfm_include_flag
	, fin_source_name
	, migration_source
	, hce_category
	, business_segment
	, entity
	, avtar_mtch_ind
	, case_category_cd
	, svc_setting
	, proc_cd
	, prim_proc_ind
	, prim_proc_last_decn
	, svc_crmk_cd
	, to_char(svc_start_dt, 'yyyy-mm-dd') as svc_start_dt
	, to_char(svc_end_dt, 'yyyy-mm-dd') as svc_end_dt
	, svc_cat_cd
	, svc_cat_dtl_cd
	, plc_of_svc_cd
	, plc_of_svc_drv_cd
	, appeal
	, palist
	, prim_svc_palist
	, pa_program
	, case_init_decn_cd
	, case_svc_init_decn_cd
	, case_decn_stat_cd
	, case_svc_decn_stat_cd
	, case_prov_par_status_cd
	, admit_cat_cd
	, auth_typ_cd
	, to_char(admit_dt_act, 'yyyy-mm-dd') as admit_dt_act
	, to_char(admit_dt_exp, 'yyyy-mm-dd') as admit_dt_exp
	, to_char(dschg_dt_exp, 'yyyy-mm-dd') as dschg_dt_exp
	, case when fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
	, case when fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1) 
		or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_FFS_FLAG
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 1 else 0 end as MnR_Dual_flag
	, case when ((fin_brand in ('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and 
		sgr_source_name in ('COSMOS','CSP')) or (substr(admit_act_month, 1, 4) = '2024' and fin_brand in ('C&S')
	and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as ISNP_flag
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where APPDECNDT >= '2025-12-01'
	and case_id in 
('289789701'
, '289945805'
, '290092464'
, '290185194'
, '290362352'
, '290405067'
, '290424992'
, '290429339'
, '290434408'
, '290440607'
, '290478841'
, '290516775'
, '290525723'
, '290571415'
, '290580674'
, '290630892'
, '290653703'
, '290664790'
, '290682611')
)
select
	*
	, case when total_oah_flag = 'OAH' then 'OAH'
			when isnp_flag = 1 then 'ISNP'
			when mnr_ffs_flag = 1 then 'M&R FFS'
			when mnr_cosmos_ffs_flag = 1 then 'M&R COSMOS FFS'
			when mnr_nice_ffs_flag = 1 then 'M&R NICE FFS'
			when cns_dual_flag = 1 then 'C&S DUAL'
		else 'Others'
	end as population
from extract
order by case_id
;

select * from tmp_1m.kn_loc_od_appeal_hcemi0_hcemnr1


select 
	column_name
	, data_type
from information_schema.columns
where table_schema = 'TMP_7D'
and table_name = 'HCE_ADR_AVTAR_LIKE_25_26_F_MA'
order by ordinal_position;

select * from information_schema.tables
where table_schema = 'TMP_7D'
and table_name = 'HCE_ADR_AVTAR_LIKE_25_26_F_MA'




create or replace table tmp_1m.kn_loc_od_appeal_hcemi1 as 
with extract as (
select
	case_id
	, member_appeal_ind
	, member_appeal_ovtn_ind
	, p2p_full_evertouched_cnt
	, p2p_full_ovtn
	, p2p_match_ind
	, mcr_reconsideration_ind
	, mcr_evertouched_decn_ind
	, mcr_ovtrn_ind
	, mcr_uphelp_ind
	, rvsl_ind
	, rvsl_decn_userid
	, to_char(rvsl_decn_dttm, 'yyyy-mm-dd') as rvsl_decn_dt
	, rvsl_decn_user_role
	, mcr_rvsls
	, rvsl_bed_decn_mtch_ind
	, rvsl_srv_decn_mtch_ind
	, appeal_ind
	, appeal_ovrtn_ind
	, oth_ovrtn_ind
	, to_char(initial_dnl_decn_dttm, 'yyyy-mm-dd') as initial_dnl_decn_dt
	, to_char(latest_dnl_decn_dttm, 'yyyy-mm-dd') as latest_dnl_decn_dt
	, to_char(appdecndt, 'yyyy-mm-dd') as app_decn_dt
	, appoutcome
	, appissuetype
	, fin_brand
	, sgr_source_name
	, tfm_include_flag
	, fin_source_name
	, migration_source
	, hce_category
	, business_segment
	, entity
	, avtar_mtch_ind
	, case_category_cd
	, svc_setting
	, proc_cd
	, prim_proc_ind
	, prim_proc_last_decn
	, svc_crmk_cd
	, to_char(svc_start_dt, 'yyyy-mm-dd') as svc_start_dt
	, to_char(svc_end_dt, 'yyyy-mm-dd') as svc_end_dt
	, svc_cat_cd
	, svc_cat_dtl_cd
	, plc_of_svc_cd
	, plc_of_svc_drv_cd
	, appeal
	, palist
	, prim_svc_palist
	, pa_program
	, case_init_decn_cd
	, case_svc_init_decn_cd
	, case_decn_stat_cd
	, case_svc_decn_stat_cd
	, case_prov_par_status_cd
	, admit_cat_cd
	, auth_typ_cd
	, to_char(admit_dt_act, 'yyyy-mm-dd') as admit_dt_act
	, to_char(admit_dt_exp, 'yyyy-mm-dd') as admit_dt_exp
	, to_char(dschg_dt_exp, 'yyyy-mm-dd') as dschg_dt_exp
	, case when fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
	, case when fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1) 
		or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_FFS_FLAG
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 1 else 0 end as MnR_Dual_flag
	, case when ((fin_brand in ('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and 
		sgr_source_name in ('COSMOS','CSP')) or (substr(admit_act_month, 1, 4) = '2024' and fin_brand in ('C&S')
	and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as ISNP_flag
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where APPDECNDT >= '2025-12-01'
	and case_id in 
('289570147'
, '289583565'
, '289651251'
, '289802740'
, '289810293'
, '289818095'
, '289827994'
, '289873389'
, '289935566'
, '290059315'
, '290065976'
, '290214338'
, '290221507'
, '290222326'
, '290226854'
, '290236467'
, '290374504'
, '290407582'
, '290410612'
, '290412638'
, '290415593'
, '290464780'
, '290510595'
, '290599178'
, '290607068')
)
select
	*
	, case when total_oah_flag = 'OAH' then 'OAH'
			when isnp_flag = 1 then 'ISNP'
			when mnr_ffs_flag = 1 then 'M&R FFS'
			when mnr_cosmos_ffs_flag = 1 then 'M&R COSMOS FFS'
			when mnr_nice_ffs_flag = 1 then 'M&R NICE FFS'
			when cns_dual_flag = 1 then 'C&S DUAL'
		else 'Others'
	end as population
from extract
order by case_id
;


select member_appeal_ind, population, count(distinct case_id)
from tmp_1m.kn_loc_od_appeal_hcemi1
group by 1, 2





case when case_id in
('289564085'
, '289570147'
, '289583565'
, '289651251'
, '289652772'
, '289802740'
, '289808558'
, '289810293'
, '289818095'
, '289818220'
, '289827994'
, '289828577'
, '289873389'
, '289935566'
, '290039767'
, '290053280'
, '290059315'
, '290065976'
, '290140994'
, '290143579'
, '290179142'
, '290189537'
, '290194529'
, '290214338'
, '290219161'
, '290221507'
, '290222326'
, '290222854'
, '290226854'
, '290236467'
, '290362662'
, '290366690'
, '290374504'
, '290407582'
, '290410612'
, '290412638'
, '290415593'
, '290420185'
, '290422303'
, '290437441'
, '290464780'
, '290500312'
, '290510595'
, '290524492'
, '290561046'
, '290574197'
, '290596141'
, '290599178'
, '290607068'
, '290609114')
then 1 else 0 
end as hcemi_member_appeal_ind


select
	table_name
	, column_name
	, ordinal_position
	, is_nullable
	, data_type
from information_schema.columns
where table_schema = 'TMP_7D'
	and table_name = 'HCE_ADR_AVTAR_LIKE_25_26_F_MA'
	and (column_name ilike '%ovtn%' or column_name ilike '%ot%' or column_name ilike '%ovtrn%')
;

select * from tmp_7d.HCE_ADR_AVTAR_LIKE_25_26_F_MA



select population from tmp_1m.kn_loc_od_appeal_01282026;

create or replace table tmp_1m.kn_loc_od_appeal_01282026_v2 as 
with extract as (
select
	case_id
	, member_appeal_ind as hcemnr_member_appeal_ind
	, case when case_id in
	('289558919'
, '289564085'
, '289570147'
, '289583565'
, '289651251'
, '289652772'
, '289729836'
, '289773063'
, '289789701'
, '289802740'
, '289808558'
, '289810293'
, '289818095'
, '289818220'
, '289827994'
, '289828577'
, '289829886'
, '289860173'
, '289873389'
, '289877766'
, '289913662'
, '289921555'
, '289935566'
, '289944481'
, '289945805'
, '289958381'
, '289985078'
, '289987234'
, '290013237'
, '290039767'
, '290053280'
, '290059315'
, '290065976'
, '290069379'
, '290074849'
, '290092464'
, '290128348'
, '290139172'
, '290140994'
, '290143579'
, '290179142'
, '290189537'
, '290194529'
, '290214338'
, '290218460'
, '290219161'
, '290221507'
, '290222326'
, '290222854'
, '290226854'
, '290236467'
, '290243448'
, '290313986'
, '290315462'
, '290318049'
, '290320787'
, '290354580'
, '290361126'
, '290362352'
, '290362662'
, '290366690'
, '290374504'
, '290402344'
, '290405067'
, '290407582'
, '290410612'
, '290412638'
, '290415593'
, '290420185'
, '290422303'
, '290423560'
, '290424992'
, '290434084'
, '290434408'
, '290437441'
, '290440607'
, '290442063'
, '290447705'
, '290449986'
, '290452999'
, '290460094'
, '290464780'
, '290478841'
, '290500312'
, '290505489'
, '290506715'
, '290510595'
, '290524492'
, '290525723'
, '290526290'
, '290550368'
, '290561046'
, '290570333'
, '290574197'
, '290579072'
, '290579543'
, '290580674'
, '290583217'
, '290594447'
, '290596141'
, '290597835'
, '290599178'
, '290600469'
, '290604661'
, '290607068'
, '290609114'
, '290630892'
, '290650759'
, '290653703'
, '290669986'
, '290675312'
, '290681649'
, '290682611'
, '290685078'
, '290695069'
, '290710198'
, '290737396'
, '290759466'
, '290773556'
, '290774518'
, '290788369'
, '290806824'
, '290822197'
, '290825141'
, '290827874'
, '290841960'
, '290843282'
, '290849226'
, '290853236'
, '290859749'
, '290862309'
, '290869373'
, '290892134'
, '290898672'
, '290900403'
, '290910029'
, '290914551'
, '290915316'
, '290932646'
, '290936099'
, '290941441'
, '290957012'
, '290989021'
, '291120724'
, '289610497'
, '289642774'
, '289646939'
, '289669416'
, '289744572')
	then 1 else 0
	end as hcemi_member_appeal_ind
	, member_appeal_ovtn_ind as hcemnr_member_appeal_ovtn_ind
	, case when case_id in 
('289558919'
, '289564085'
, '289570147'
, '289583565'
, '289651251'
, '289652772'
, '289729836'
, '289773063'
, '289789701'
, '289802740'
, '289808558'
, '289818095'
, '289818220'
, '289827994'
, '289828577'
, '289873389'
, '289877766'
, '289913662'
, '289921555'
, '289935566'
, '289944481'
, '289945805'
, '289958381'
, '289985078'
, '289987234'
, '290013237'
, '290039767'
, '290053280'
, '290059315'
, '290065976'
, '290069379'
, '290074849'
, '290092464'
, '290128348'
, '290139172'
, '290140994'
, '290143579'
, '290179142'
, '290189537'
, '290194529'
, '290214338'
, '290218460'
, '290221507'
, '290222854'
, '290226854'
, '290236467'
, '290243448'
, '290313986'
, '290320787'
, '290354580'
, '290361126'
, '290362352'
, '290362662'
, '290366690'
, '290374504'
, '290402344'
, '290407582'
, '290410612'
, '290412638'
, '290415593'
, '290420185'
, '290422303'
, '290423560'
, '290424992'
, '290434084'
, '290440607'
, '290452999'
, '290460094'
, '290464780'
, '290500312'
, '290506715'
, '290510595'
, '290524492'
, '290525723'
, '290526290'
, '290550368'
, '290561046'
, '290574197'
, '290579072'
, '290579543'
, '290580674'
, '290583217'
, '290594447'
, '290597835'
, '290599178'
, '290600469'
, '290604661'
, '290607068'
, '290609114'
, '290650759'
, '290653703'
, '290669986'
, '290675312'
, '290681649'
, '290682611'
, '290685078'
, '290695069'
, '290710198'
, '290737396'
, '290759466'
, '290773556'
, '290774518'
, '290788369'
, '290806824'
, '290822197'
, '290825141'
, '290827874'
, '290841960'
, '290843282'
, '290849226'
, '290853236'
, '290859749'
, '290862309'
, '290869373'
, '290892134'
, '290898672'
, '290900403'
, '290910029'
, '290914551'
, '290915316'
, '290932646'
, '290936099'
, '290941441'
, '290957012'
, '290989021'
, '291120724'
, '289642774'
, '289669416'
, '289744572')
then 1 else 0 end as hcemi_member_appeal_ovtn_ind
	, p2p_full_evertouched_cnt
	, p2p_full_ovtn
	, p2p_match_ind
	, mcr_reconsideration_ind
	, mcr_evertouched_decn_ind
	, mcr_ovtrn_ind
	, mcr_uphelp_ind
	, initialfulladr_cases
	, persistentfulladr_cases
	, case when initialfulladr_cases = 1 and persistentfulladr_cases = 0 then 1
	else 0
	end as overturned_ind
	, rvsl_ind
	, rvsl_decn_userid
	, to_char(rvsl_decn_dttm, 'yyyy-mm-dd') as rvsl_decn_dt
	, rvsl_decn_user_role
	, mcr_rvsls
	, rvsl_bed_decn_mtch_ind
	, rvsl_srv_decn_mtch_ind
	, appeal_ind
	, appeal_ovrtn_ind
	, oth_ovrtn_ind
	, to_char(initial_dnl_decn_dttm, 'yyyy-mm-dd') as initial_dnl_decn_dt
	, to_char(latest_dnl_decn_dttm, 'yyyy-mm-dd') as latest_dnl_decn_dt
	, to_char(appdecndt, 'yyyy-mm-dd') as app_decn_dt
	, appoutcome
	, appissuetype
	, fin_brand
	, sgr_source_name
	, tfm_include_flag
	, fin_source_name
	, fin_product_level_3
	, migration_source
	, hce_category
	, business_segment
	, global_cap
	, nce_tadm_dec_risk_type
	, entity
	, avtar_mtch_ind
	, case_category_cd
	, svc_setting
	, proc_cd
	, prim_proc_ind
	, prim_proc_last_decn
	, svc_crmk_cd
	, to_char(svc_start_dt, 'yyyy-mm-dd') as svc_start_dt
	, to_char(svc_end_dt, 'yyyy-mm-dd') as svc_end_dt
	, svc_cat_cd
	, svc_cat_dtl_cd
	, plc_of_svc_cd
	, plc_of_svc_drv_cd
	, appeal
	, palist
	, prim_svc_palist
	, pa_program
	, case_init_decn_cd
	, case_svc_init_decn_cd
	, case_decn_stat_cd
	, case_svc_decn_stat_cd
	, case_prov_par_status_cd
	, admit_cat_cd
	, auth_typ_cd
	, admit_act_month
	, to_char(admit_dt_act, 'yyyy-mm-dd') as admit_dt_act
	, to_char(admit_dt_exp, 'yyyy-mm-dd') as admit_dt_exp
	, to_char(dschg_dt_act, 'yyyy-mm-dd') as dschg_dt_act
	, to_char(dschg_dt_exp, 'yyyy-mm-dd') as dschg_dt_exp
	, case when ((fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
      	 	or  (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
     then 1 else 0 end as MnR_FFS_flag
	, case when ((fin_brand in ('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and 
		sgr_source_name in ('COSMOS','CSP')) or (substr(admit_act_month, 1, 4) = '2024' and fin_brand in ('C&S')
	and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when (fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL') then 1 else 0 end as ISNP_flag
	, case when (svc_setting = 'Inpatient' and plc_of_svc_cd = '21 - Acute Hospital' and admit_cat_cd in ('17 - Medical', '30 - Surgical')) then 1 
		else 0 
	end as loc_flag
from tmp_1m.hce_adr_avtar_like_25_26_f_ma
where admit_act_month >= '202512'
)
select
	*
	, case when MnR_FFS_flag = 1 then 'M&R FFS'
		   when cns_dual_flag = 1 then 'C&S Dual'
		   when total_OAH_flag = 'OAH' then 'OAH'
		   when ISNP_flag = 1 then 'ISNP'
	else 'Others'
	end as population
from extract
where loc_flag = 1
order by case_id
;

select hcemi_member_appeal_ind, hcemnr_member_appeal_ind, count(distinct case_id) from tmp_1m.kn_loc_od_appeal_01282026
where population != 'Others'
	and admit_act_month = '202601'
group by 1,2

select population from tmp_1m.kn_loc_od_appeal_01282026

--
--select hcemnr_member_appeal_ind, hcemi_member_appeal_ind, population, loc_flag, count(distinct case_id)
--from tmp_1m.kn_loc_od_appeal
--group by 1,2,3,4;

select population, count(distinct case_id)
from tmp_1m.kn_loc_od_appeal_01282026
where admit_act_month = '202601'
	and loc_flag = 1 
	and hcemnr_member_appeal_ind = 1
group by 1
order by count(distinct case_id) desc;
--M&R FFS	153
--Others	151
--OAH	26
--C&S Dual	10
--ISNP	4

select population, count(distinct case_id)
from tmp_1m.kn_loc_od_appeal
where admit_act_month = '202601'
	and loc_flag = 1 
	and hcemnr_member_appeal_ind = 1
group by 1;


select count(*) from tmp_1m.kn_loc_od_appeal where loc_flag = 1


select admit_act_month from tmp_7d.hce_adr_avtar_like_25_26_f_ma

select dschg_dt_act from tmp_7d.hce_adr_avtar_like_25_26_f_ma

select
	table_name
	, row_count
	, created
	, last_altered
	, last_ddl
	, last_ddl_by
from information_schema.tables
where table_schema ilike 'tmp_7d'
	and table_name ilike 'hce_adr_avtar_like_25_26_f_ma'
;


select * from fichsrv.cosmos_op


select * from fichsrv.tre_membership

select * from hce_ops_fnl.HCE_ADR_AVTAR_LIKE_25_26_F 


select count(distinct fin_mbi_hicn_fnl) from fichsrv.tre_membership
where global_cap = 'NA' and fin_brand = 'M&R' and tfm_include_flag = 1
and fin_inc_month = '202601'

select max(fin_inc_month) from fichsrv.TRE_MEMBERSHIP;



select 
	*
from tmp_1m.kn_loc_od_appeal_202601
where admit_act_month = 202601
	and population != 'Others'
	and initialfulladr_cases = 1 and persistentfulladr_cases = 0
	and hcemnr_member_appeal_ind = 1
	and oth_ovrtn_ind = 1
;

select distinct
	 appoutcome
	, apptype
from HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag


select
	table_name
	, row_count
	, created
	, last_altered
	, last_ddl
	, last_ddl_by
from information_schema.tables
where table_schema = 'TMP_7D'
	and table_name = 'HCE_ADR_AVTAR_LIKE_25_26_F_MA'
;


select
	table_name
	, column_name
	, ordinal_position
	, is_nullable
	, data_type
from information_schema.columns
where table_schema = 'TMP_1M'
	and table_name = 'HCE_ADR_AVTAR_LIKE_25_26_F_MA'
order by column_name
;

with cases as (
select
	case_id
	, admit_dt_act as admit_dt_actual
	, initialfulladr_cases
	, hcemi_member_appeal_ind
	, hcemnr_member_appeal_ind
	, appissuetype
from tmp_1m.kn_loc_od_appeal_01282026
where admit_act_month = 202601 and population != 'Others'
and hcemnr_member_appeal_ind = 1 and initialfulladr_cases = 0
)
/*select count(distinct case_id) from cases*/
select distinct * from cases
;



where case_id in 
('289862168'	
, '290035705'
, '289980875'
, '289330282'
, '290194156'
, '289951815'
, '289961215'
, '290843427'
, '289915734'
, '290364519'
)


select authcaseid, * from HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag
where authcaseid in 
('290035705'
, '289330282'
, '290194156'
, '289951815'
, '289961215'
) 



select authcaseid, * from HCE_OPS_STAGE.HCEOPS_ALL_APPEALS_XWALK_USRROLE_Tag
where authcaseid in 
('290843427'
, '289980875'
, '289862168'
, '289915734'
, '290364519'
) 





select
	table_name
	, row_count
	, created
	, last_altered
	, last_ddl
	, last_ddl_by
from information_schema.tables
where table_schema = 'TMP_1M'
	and table_name = 'HCE_ADR_AVTAR_LIKE_25_26_F_MA'
;            
                   



select count(*) from tmp_1m.kn_loc_od_appeal_02032026 

create or replace table tmp_1m.kn_loc_od_appeal_02032026 as 
with extract as (
select
	case_id
	, member_appeal_ind as hcemnr_member_appeal_ind
	, case when case_id in
	('289558919'
,'289564085'
,'289570147'
,'289583565'
,'289610497'
,'289610547'
,'289634597'
,'289641340'
,'289642774'
,'289646939'
,'289649673'
,'289651251'
,'289652772'
,'289669416'
,'289728572'
,'289729836'
,'289744572'
,'289773063'
,'289781079'
,'289789701'
,'289802740'
,'289808558'
,'289810293'
,'289818095'
,'289818220'
,'289827994'
,'289828577'
,'289829886'
,'289833614'
,'289841045'
,'289841696'
,'289860173'
,'289873389'
,'289877766'
,'289898646'
,'289913662'
,'289921421'
,'289921555'
,'289924878'
,'289935566'
,'289939662'
,'289944481'
,'289945805'
,'289951572'
,'289954109'
,'289957549'
,'289958321'
,'289958381'
,'289963474'
,'289966262'
,'289985078'
,'289987234'
,'290000428'
,'290006598'
,'290013237'
,'290033924'
,'290037550'
,'290039767'
,'290053280'
,'290059315'
,'290065976'
,'290069379'
,'290074849'
,'290081721'
,'290092464'
,'290112825'
,'290128348'
,'290139172'
,'290140994'
,'290143579'
,'290160233'
,'290179142'
,'290185194'
,'290185630'
,'290186558'
,'290189537'
,'290194529'
,'290214338'
,'290218460'
,'290219161'
,'290221507'
,'290222326'
,'290222854'
,'290226854'
,'290236467'
,'290243448'
,'290243728'
,'290244159'
,'290257853'
,'290266873'
,'290283686'
,'290313986'
,'290315462'
,'290318049'
,'290320787'
,'290332650'
,'290354580'
,'290361126'
,'290362023'
,'290362352'
,'290362662'
,'290366690'
,'290374504'
,'290400623'
,'290402344'
,'290405067'
,'290407582'
,'290410612'
,'290412638'
,'290415593'
,'290420185'
,'290422303'
,'290423560'
,'290424992'
,'290425841'
,'290426054'
,'290427890'
,'290433444'
,'290434084'
,'290434408'
,'290437068'
,'290437441'
,'290440607'
,'290442063'
,'290447705'
,'290449986'
,'290452999'
,'290460094'
,'290464780'
,'290464920'
,'290478841'
,'290487665'
,'290500312'
,'290502657'
,'290505489'
,'290506715'
,'290510595'
,'290516775'
,'290524492'
,'290525723'
,'290526290'
,'290550368'
,'290561046'
,'290567295'
,'290570333'
,'290574197'
,'290579072'
,'290579543'
,'290580674'
,'290583217'
,'290594447'
,'290596141'
,'290597835'
,'290599178'
,'290600469'
,'290603392'
,'290604661'
,'290607068'
,'290609114'
,'290629176'
,'290630892'
,'290639154'
,'290650759'
,'290653703'
,'290664790'
,'290669986'
,'290675312'
,'290681649'
,'290682611'
,'290685078'
,'290695069'
,'290703868'
,'290710198'
,'290737396'
,'290759466'
,'290772989'
,'290773556'
,'290774518'
,'290788369'
,'290792919'
,'290793527'
,'290798772'
,'290806824'
,'290817122'
,'290817771'
,'290822197'
,'290825141'
,'290827874'
,'290841960'
,'290843282'
,'290849226'
,'290853236'
,'290853432'
,'290859749'
,'290862309'
,'290869373'
,'290872416'
,'290877429'
,'290882350'
,'290892134'
,'290893245'
,'290896317'
,'290898672'
,'290900403'
,'290902510'
,'290902962'
,'290909062'
,'290910029'
,'290914551'
,'290915316'
,'290932646'
,'290936099'
,'290937565'
,'290941441'
,'290953282'
,'290955404'
,'290957012'
,'290958720'
,'290960123'
,'290960607'
,'290963240'
,'290971653'
,'290976033'
,'290989021'
,'290998124'
,'291001316'
,'291005265'
,'291013448'
,'291015891'
,'291024319'
,'291026145'
,'291028499'
,'291036059'
,'291041919'
,'291043787'
,'291063063'
,'291065541'
,'291071120'
,'291073209'
,'291074910'
,'291090697'
,'291093413'
,'291094111'
,'291098515'
,'291103680'
,'291110570'
,'291111414'
,'291111463'
,'291117339'
,'291120724'
,'291132142'
,'291132808'
,'291145529'
,'291156595'
,'291180199'
,'291185512'
,'291186268'
,'291186432'
,'291194398'
,'291196130'
,'291200166'
,'291200940'
,'291219610'
,'291223185'
,'291235414'
,'291235503'
,'291248300'
,'291251055'
,'291257664'
,'291265354'
,'291265923'
,'291265927'
,'291266371'
,'291267975'
,'291271219'
,'291274475'
,'291278291'
,'291284728'
,'291285441'
,'291286249'
,'291287082'
,'291292043'
,'291292668'
,'291299818'
,'291300662'
,'291302198'
,'291304612'
,'291304988'
,'291305088'
,'291305332'
,'291305845'
,'291307599'
,'291309281'
,'291309680'
,'291312981'
,'291314161'
,'291319514'
,'291324291'
,'291330051'
,'291333740'
,'291334024'
,'291345328'
,'291355974'
,'291357721'
,'291358437'
,'291366635'
,'291368693'
,'291381209'
,'291383703'
,'291384510'
,'291384992'
,'291402075'
,'291402386'
,'291402972'
,'291406367'
,'291411670'
,'291420690'
,'291426871'
,'291427045'
,'291454426'
,'291456075'
,'291488865'
,'291495782'
,'291496291'
,'291511809'
,'291512000'
,'291530100'
,'291548059'
,'291583086')
	then 1 else 0
	end as hcemi_member_appeal_ind
	, member_appeal_ovtn_ind as hcemnr_member_appeal_ovtn_ind
	, case when case_id in 
('289558919'
,'289564085'
,'289570147'
,'289583565'
,'289610547'
,'289634597'
,'289641340'
,'289642774'
,'289651251'
,'289652772'
,'289669416'
,'289728572'
,'289729836'
,'289744572'
,'289773063'
,'289781079'
,'289789701'
,'289802740'
,'289808558'
,'289818095'
,'289818220'
,'289827994'
,'289828577'
,'289833614'
,'289841045'
,'289841696'
,'289873389'
,'289877766'
,'289898646'
,'289913662'
,'289921421'
,'289921555'
,'289924878'
,'289935566'
,'289939662'
,'289944481'
,'289945805'
,'289951572'
,'289954109'
,'289957549'
,'289958321'
,'289958381'
,'289963474'
,'289966262'
,'289985078'
,'289987234'
,'290000428'
,'290006598'
,'290013237'
,'290037550'
,'290039767'
,'290053280'
,'290059315'
,'290065976'
,'290069379'
,'290074849'
,'290081721'
,'290092464'
,'290128348'
,'290139172'
,'290140994'
,'290143579'
,'290160233'
,'290179142'
,'290185194'
,'290185630'
,'290189537'
,'290194529'
,'290214338'
,'290218460'
,'290221507'
,'290222854'
,'290226854'
,'290236467'
,'290243448'
,'290243728'
,'290244159'
,'290257853'
,'290266873'
,'290283686'
,'290313986'
,'290320787'
,'290332650'
,'290354580'
,'290361126'
,'290362023'
,'290362352'
,'290362662'
,'290366690'
,'290374504'
,'290400623'
,'290402344'
,'290407582'
,'290410612'
,'290412638'
,'290415593'
,'290420185'
,'290422303'
,'290423560'
,'290424992'
,'290426054'
,'290433444'
,'290434084'
,'290437068'
,'290440607'
,'290452999'
,'290460094'
,'290464780'
,'290487665'
,'290500312'
,'290502657'
,'290506715'
,'290510595'
,'290524492'
,'290525723'
,'290526290'
,'290550368'
,'290561046'
,'290574197'
,'290579072'
,'290579543'
,'290580674'
,'290583217'
,'290594447'
,'290597835'
,'290599178'
,'290600469'
,'290603392'
,'290604661'
,'290607068'
,'290609114'
,'290629176'
,'290639154'
,'290650759'
,'290653703'
,'290664790'
,'290669986'
,'290675312'
,'290681649'
,'290682611'
,'290685078'
,'290695069'
,'290710198'
,'290737396'
,'290759466'
,'290772989'
,'290773556'
,'290774518'
,'290788369'
,'290798772'
,'290806824'
,'290822197'
,'290825141'
,'290827874'
,'290841960'
,'290843282'
,'290849226'
,'290853236'
,'290859749'
,'290862309'
,'290869373'
,'290872416'
,'290877429'
,'290882350'
,'290892134'
,'290893245'
,'290896317'
,'290898672'
,'290900403'
,'290902510'
,'290902962'
,'290910029'
,'290914551'
,'290915316'
,'290932646'
,'290936099'
,'290941441'
,'290955404'
,'290957012'
,'290958720'
,'290960607'
,'290963240'
,'290971653'
,'290989021'
,'290998124'
,'291001316'
,'291013448'
,'291015891'
,'291026145'
,'291028499'
,'291036059'
,'291041919'
,'291043787'
,'291063063'
,'291071120'
,'291074910'
,'291090697'
,'291094111'
,'291103680'
,'291110570'
,'291111463'
,'291117339'
,'291120724'
,'291132142'
,'291132808'
,'291156595'
,'291180199'
,'291185512'
,'291186268'
,'291186432'
,'291194398'
,'291196130'
,'291200166'
,'291200940'
,'291223185'
,'291235414'
,'291235503'
,'291251055'
,'291257664'
,'291265923'
,'291265927'
,'291274475'
,'291278291'
,'291284728'
,'291285441'
,'291286249'
,'291287082'
,'291292043'
,'291292668'
,'291299818'
,'291300662'
,'291302198'
,'291304988'
,'291305088'
,'291305332'
,'291305845'
,'291307599'
,'291309281'
,'291309680'
,'291312981'
,'291319514'
,'291330051'
,'291333740'
,'291334024'
,'291345328'
,'291355974'
,'291357721'
,'291358437'
,'291366635'
,'291368693'
,'291381209'
,'291383703'
,'291384510'
,'291384992'
,'291402075'
,'291402972'
,'291406367'
,'291411670'
,'291420690'
,'291426871'
,'291427045'
,'291456075'
,'291488865'
,'291495782'
,'291496291'
,'291511809'
,'291512000'
,'291530100'
,'291548059'
,'291583086')
then 1 else 0 end as hcemi_member_appeal_ovtn_ind
	, p2p_full_evertouched_cnt
	, p2p_full_ovtn
	, p2p_match_ind
	, mcr_reconsideration_ind
	, mcr_evertouched_decn_ind
	, mcr_ovtrn_ind
	, mcr_uphelp_ind
	, initialfulladr_cases
	, persistentfulladr_cases
	, case when initialfulladr_cases = 1 and persistentfulladr_cases = 0 then 1
	else 0
	end as overturned_ind
	, rvsl_ind
	, rvsl_decn_userid
	, to_char(rvsl_decn_dttm, 'yyyy-mm-dd') as rvsl_decn_dt
	, rvsl_decn_user_role
	, mcr_rvsls
	, rvsl_bed_decn_mtch_ind
	, rvsl_srv_decn_mtch_ind
	, appeal_ind
	, appeal_ovrtn_ind
	, oth_ovrtn_ind
	, to_char(initial_dnl_decn_dttm, 'yyyy-mm-dd') as initial_dnl_decn_dt
	, to_char(latest_dnl_decn_dttm, 'yyyy-mm-dd') as latest_dnl_decn_dt
	, to_char(appdecndt, 'yyyy-mm-dd') as app_decn_dt
	, appoutcome
	, appissuetype
	, fin_brand
	, sgr_source_name
	, tfm_include_flag
	, fin_source_name
	, fin_product_level_3
	, migration_source
	, hce_category
	, business_segment
	, global_cap
	, nce_tadm_dec_risk_type
	, entity
	, avtar_mtch_ind
	, case_category_cd
	, svc_setting
	, proc_cd
	, prim_proc_ind
	, prim_proc_last_decn
	, svc_crmk_cd
	, to_char(svc_start_dt, 'yyyy-mm-dd') as svc_start_dt
	, to_char(svc_end_dt, 'yyyy-mm-dd') as svc_end_dt
	, svc_cat_cd
	, svc_cat_dtl_cd
	, plc_of_svc_cd
	, plc_of_svc_drv_cd
	, appeal
	, palist
	, prim_svc_palist
	, pa_program
	, case_init_decn_cd
	, case_svc_init_decn_cd
	, case_decn_stat_cd
	, case_svc_decn_stat_cd
	, case_prov_par_status_cd
	, admit_cat_cd
	, auth_typ_cd
	, admit_act_month
	, to_char(admit_dt_act, 'yyyy-mm-dd') as admit_dt_act
	, to_char(admit_dt_exp, 'yyyy-mm-dd') as admit_dt_exp
	, to_char(dschg_dt_act, 'yyyy-mm-dd') as dschg_dt_act
	, to_char(dschg_dt_exp, 'yyyy-mm-dd') as dschg_dt_exp
	, case when ((fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
      	 	or  (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')))
     then 1 else 0 end as MnR_FFS_flag
	, case when ((fin_brand in ('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and 
		sgr_source_name in ('COSMOS','CSP')) or (substr(admit_act_month, 1, 4) = '2024' and fin_brand in ('C&S')
	and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when (fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL') then 1 else 0 end as ISNP_flag
	, case when (svc_setting = 'Inpatient' and plc_of_svc_cd = '21 - Acute Hospital' and admit_cat_cd in ('17 - Medical', '30 - Surgical')) then 1 
		else 0 
	end as loc_flag
from tmp_7d.hce_adr_avtar_like_25_26_f_ma
where admit_act_month >= '202512'
)
select
	*
	, case when MnR_FFS_flag = 1 then 'M&R FFS'
		   when cns_dual_flag = 1 then 'C&S Dual'
		   when total_OAH_flag = 'OAH' then 'OAH'
		   when ISNP_flag = 1 then 'ISNP'
	else 'Others'
	end as population
from extract
where loc_flag = 1
order by case_id
;

select distinct
	case_id
	, initialfulladr_cases
	, admit_act_month
	, admit_dt_act
	, app_decn_dt
from tmp_1m.kn_loc_od_appeal_02032026
where hcemnr_member_appeal_ind = 1 and hcemi_member_appeal_ind = 0
	and loc_flag = 1
	and population != 'Others'
	and app_decn_dt >= '2026-01-01'
	and initialfulladr_cases = 1 
	
	


	
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               