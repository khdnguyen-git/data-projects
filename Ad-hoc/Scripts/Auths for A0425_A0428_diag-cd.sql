select * from hce_proj_bd.hce_adr_avtar_like_24_25_f
where business_segment not in ('EnI','ERR','null')
and medicare_id is not null
and proc_cd in ('A0425','A0428')
and notif_yrmonth between '202401' and '202512' limit 200

--select * from fichsrv.tadm_glxy_diagnosis_code limit 50

drop table tmp_7d.esb_NonEmergent_codes_2023
select * from tmp_7d.esb_NonEmergent_codes_2023 limit 50

select count(*) from tmp_7d.esb_NonEmergent_codes_2023

drop table tmp_7d.esb_NonEmergent_codes_2023;
create table tmp_7d.esb_NonEmergent_codes_2023 STORED AS ORC as
select 
	case_id,
	medicare_id,
	avtar_mtch_ind,
	create_dt,
	SUBSTRING(notif_recd_dttm,1,10) as Notif_date,
	substring(Notif_yrmonth,1,4) as Notif_Year,
	Notif_yrmonth,
	prim_srvc_sub_cat,
	prim_diag_cd,
	entity,
	case_category_cd,
	channel_cd,
	proc_cd,
	prim_srvc_cat,
	prim_proc_ind,
	pa_program,
	case_decn_stat_cd,
	business_segment,
	migration_source,
	fin_brand,
	fin_source_name,
	sgr_source_name,
	nce_tadm_dec_risk_type,
	tfm_include_flag,
	global_cap,
	fin_market,
	fin_state,
	fin_plan_level_2,
	fin_product_level_3,
	fin_g_i,
	group_number,
	group_name,
	case when case_decn_stat_cd = 'PD-Partially Adverse Determination' then 1 else 0 end as Partially_adverse,
	case when case_decn_stat_cd = 'AD-Fully Adverse Determination' or case_decn_stat_cd = 'AD - Fully Adverse Determination' then 1 else 0 end as fully_adverse	
from hce_proj_bd.hce_adr_avtar_like_2023_f as a
where business_segment not in ('EnI','ERR','null')
and medicare_id is not null
and proc_cd in ('A0425','A0428')
and notif_yrmonth between '202301' and '202312';


drop table tmp_7d.esb_NonEmergent_codes_24_25;
create table tmp_7d.esb_NonEmergent_codes_24_25 STORED AS ORC as
select 
	case_id,
	medicare_id,
	avtar_mtch_ind,
	create_dt,
	SUBSTRING(notif_recd_dttm,1,10) as Notif_date,
	substring(Notif_yrmonth,1,4) as Notif_Year,
	Notif_yrmonth,
	prim_srvc_sub_cat,
	prim_diag_cd,
	entity,
	case_category_cd,
	channel_cd,
	proc_cd,
	prim_srvc_cat,
	prim_proc_ind,
	pa_program,
	case_decn_stat_cd,
	business_segment,
	migration_source,
	fin_brand,
	fin_source_name,
	sgr_source_name,
	nce_tadm_dec_risk_type,
	tfm_include_flag,
	global_cap,
	fin_market,
	fin_state,
	fin_plan_level_2,
	fin_product_level_3,
	fin_g_i,
	group_number,
	group_name,
	case when case_decn_stat_cd = 'PD-Partially Adverse Determination' then 1 else 0 end as Partially_adverse,
	case when case_decn_stat_cd = 'AD-Fully Adverse Determination' or case_decn_stat_cd = 'AD - Fully Adverse Determination' then 1 else 0 end as fully_adverse	
from hce_proj_bd.hce_adr_avtar_like_24_25_f as a
where business_segment not in ('EnI','ERR','null')
and medicare_id is not null
and proc_cd in ('A0425','A0428')
and notif_yrmonth between '202401' and '202512'
;

drop table tmp_7d.esb_NonEmergent_codes_all_a;
create table tmp_7d.esb_NonEmergent_codes_all_a STORED AS ORC as 
select * from tmp_7d.esb_NonEmergent_codes_2023
union all
select * from tmp_7d.esb_NonEmergent_codes_24_25;

select count(*) from tmp_7d.esb_NonEmergent_codes_all_a; 
-- 49,230
describe tmp_7d.esb_NonEmergent_codes_all_a

--select count(*) from tmp_7d.esb_NonEmergent_codes_b
--drop table tmp_7d.esb_NonEmergent_codes_all_b
drop table tmp_7d.esb_NonEmergent_codes_all_b;
create table tmp_7d.esb_NonEmergent_codes_all_b STORED AS ORC as
select 
	case_id,
	medicare_id,
	avtar_mtch_ind,
	create_dt,
	Notif_date,
	Notif_yrmonth,
	prim_srvc_sub_cat,
	prim_diag_cd,
	entity,
	case_category_cd,
	channel_cd,
	proc_cd,
	prim_srvc_cat,
	prim_proc_ind,
	pa_program,
	case_decn_stat_cd,
	business_segment,
	migration_source,
	fin_brand,
	fin_source_name,
	sgr_source_name,
	nce_tadm_dec_risk_type,
	tfm_include_flag,
	global_cap,
	fin_market,
	fin_state,
	fin_plan_level_2,
	fin_product_level_3,
	fin_g_i,
	group_number,
	group_name,
	Partially_adverse,
	fully_adverse,
	
CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL' THEN 1 else 0 end as MnR_COSMOS_FFS_Flag,

CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type = 'FFS' THEN 1 else 0 end as MnR_NICE_FFS_Flag,

CASE WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL') 
       OR (business_segment = 'MnR' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG,

CASE WHEN (Notif_Year ='2024' AND business_segment = 'CnS' AND fin_brand in ('M&R','C&S') AND GLOBAL_CAP = 'NA' AND
	SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') then 0 
	WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND migration_source='OAH')
	OR (business_segment = 'CnS' AND fin_brand='C&S' and migration_source='OAH') then 1 else 0 end as OAH_FLAG,
	
CASE WHEN ((business_segment = 'CnS' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
	SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (Notif_Year ='2024' AND business_segment = 'CnS' AND fin_brand in ('M&R','C&S')
	AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_Flag,	
	
CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag,

CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='INSTITUTIONAL' then 1 else 0 end as ISNP_flag,

	count(*) as n
from tmp_7d.esb_NonEmergent_codes_all_a

where proc_cd <> 'null'          --some null proc cases are removed.   
and fin_plan_level_2 <> 'PFFS'      --PFFS does not require PA, but some acitivity was found.  Not material to remove. 
group by
	case_id,
	medicare_id,
	avtar_mtch_ind,
	create_dt,
	Notif_date,
	Notif_yrmonth,
	prim_srvc_sub_cat,
	prim_diag_cd,
	entity,
	case_category_cd,
	channel_cd,
	proc_cd,
	prim_srvc_cat,
	prim_proc_ind,
	pa_program,
	case_decn_stat_cd,
	business_segment,
	migration_source,
	fin_brand,
	fin_source_name,
	sgr_source_name,
	nce_tadm_dec_risk_type,
	tfm_include_flag,
	global_cap,
	fin_market,
	fin_state,
	fin_plan_level_2,
	fin_product_level_3,
	fin_g_i,
	group_number,
	group_name,
	Partially_adverse,
	fully_adverse,
	CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL' THEN 1 else 0 end,

CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type = 'FFS' THEN 1 else 0 end ,

CASE WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL') 
       OR (business_segment = 'MnR' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS')) then 1 else 0 end ,

CASE WHEN (Notif_Year ='2024' AND business_segment = 'CnS' AND fin_brand in ('M&R','C&S') AND GLOBAL_CAP = 'NA' AND
	SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') then 0 
	WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND migration_source='OAH')
	OR (business_segment = 'CnS' AND fin_brand='C&S' and migration_source='OAH') then 1 else 0 end ,
	
CASE WHEN ((business_segment = 'CnS' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
	SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (Notif_Year ='2024' AND business_segment = 'CnS' AND fin_brand in ('M&R','C&S')
	AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end ,	
	
CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end ,

CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='INSTITUTIONAL' then 1 else 0 end


select 
	proc_cd
	, sum(n)
from tmp_7d.esb_NonEmergent_codes_all_b
where prim_proc_ind = 'Y' and avtar_mtch_ind = 1
group by 
	proc_cd
