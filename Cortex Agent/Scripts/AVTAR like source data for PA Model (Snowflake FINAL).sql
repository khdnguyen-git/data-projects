	--select * from HCE_OPS_FNL.hce_adr_avtar_like_24_25_f 
--where 
--business_segment not in ('EnI','ERR','null') 
--and avtar_mtch_ind = 1
--and pa_program not in ('Not EPAL-Prime','Non-EPAL')
--and notif_yrmonth between '202401' and '202512'
--and sgr_source_name is not null limit 100


-----------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.pa_refresh23;
create table tmp_1m.pa_refresh23 as

select 
a.business_segment,
a.medicare_id,
a.create_dt,
a.notif_recd_dttm,
a.Notif_yrmonth,
a.entity,
a.proc_cd,
a.pa_program,
a.case_init_decn_cd,
a.case_decn_stat_cd,
a.case_id,
a.migration_source,
a.fin_brand,
a.fin_source_name,
a.sgr_source_name,
a.nce_tadm_dec_risk_type,
a.tfm_include_flag,
a.global_cap,
a.fin_market,
a.fin_state,
a.fin_plan_level_2,
a.fin_product_level_3,
a.fin_g_i,
a.group_number,
a.group_name,
'' as hce_cohort
from VING_PRD_TREND_DB.HCE_OPS_FNL.HCE_ADR_AVTAR_LIKE_2023_F_1 as a   --ARCHIVE TABLE, MAY NEED TO CHANGE IN NEXT REFRESH--
where 
business_segment not in ('EnI','ERR','null') 
--and entity <> 'PHS'
and avtar_mtch_ind = 1
--and prim_svc_palist = 'Y'
and pa_program not in ('Not EPAL-Prime','Non-EPAL')
and notif_yrmonth between '202301' and '202312'   
;
--select count(*) from tmp_1m.pa_refresh212;

-----------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.pa_refresh24;
create table tmp_1m.pa_refresh24 as

select 
a.business_segment,
a.medicare_id,
a.create_dt,
a.notif_recd_dttm,
a.Notif_yrmonth,
a.entity,
a.proc_cd,
a.pa_program,
a.case_init_decn_cd,
a.case_decn_stat_cd,
a.case_id,
a.migration_source,
a.fin_brand,
a.fin_source_name,
a.sgr_source_name,
a.nce_tadm_dec_risk_type,
a.tfm_include_flag,
a.global_cap,
a.fin_market,
a.fin_state,
a.fin_plan_level_2,
a.fin_product_level_3,
a.fin_g_i,
a.group_number,
a.group_name,
b.hce_cohort

from VING_PRD_TREND_DB.HCE_OPS_FNL.HCE_ADR_AVTAR_LIKE_24_25_F as a

LEFT join tmp_1y.HCE_COHORT_2023_2024_6 AS b    --MAY NEED TO CHANGE NEXT REFRESH--
--left join tmp_1y.2023_2024_HCE_COHORT_6 as b
on b.fin_mbi_hicn_fnl = a.medicare_id

where 
a.business_segment not in ('EnI','ERR','null') 
--and entity <> 'PHS'
and a.avtar_mtch_ind = 1
--and prim_svc_palist = 'Y'
and a.pa_program not in ('Not EPAL-Prime','Non-EPAL')
and a.notif_yrmonth between '202401' and '202412'  
;
-----------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.pa_refresh25;
create table tmp_1m.pa_refresh25 as

select 
a.business_segment,
a.medicare_id,
a.create_dt,
a.notif_recd_dttm,
a.Notif_yrmonth,
a.entity,
a.proc_cd,
a.pa_program,
a.case_init_decn_cd,
a.case_decn_stat_cd,
a.case_id,
a.migration_source,
a.fin_brand,
a.fin_source_name,
a.sgr_source_name,
a.nce_tadm_dec_risk_type,
a.tfm_include_flag,
a.global_cap,
a.fin_market,
a.fin_state,
a.fin_plan_level_2,
a.fin_product_level_3,
a.fin_g_i,
a.group_number,
a.group_name,
b.hce_cohort

from VING_PRD_TREND_DB.HCE_OPS_FNL.HCE_ADR_AVTAR_LIKE_25_26_F as a

LEFT join HCE_OPS_FNL.HCEOPS_2024_2025_HCE_COHORT_6 AS b    --MAY NEED TO CHANGE NEXT REFRESH--
--left join tmp_1y.2024_2025_HCE_COHORT_6 as b    
on b.fin_mbi_hicn_fnl = a.medicare_id

where 
a.business_segment not in ('EnI','ERR','null') 
--and entity <> 'PHS'
and a.avtar_mtch_ind = 1
--and prim_svc_palist = 'Y'
and a.pa_program not in ('Not EPAL-Prime','Non-EPAL')
and a.notif_yrmonth between '202501' and '202512'   -------------CHANGE THIS DATE--------------
;

-----------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.pa_refresh26;
create table tmp_1m.pa_refresh26 as

select 
a.business_segment,
a.medicare_id,
a.create_dt,
a.notif_recd_dttm,
a.Notif_yrmonth,
a.entity,
a.proc_cd,
a.pa_program,
a.case_init_decn_cd,
a.case_decn_stat_cd,
a.case_id,
a.migration_source,
a.fin_brand,
a.fin_source_name,
a.sgr_source_name,
a.nce_tadm_dec_risk_type,
a.tfm_include_flag,
a.global_cap,
a.fin_market,
a.fin_state,
a.fin_plan_level_2,
a.fin_product_level_3,
a.fin_g_i,
a.group_number,
a.group_name,
b.hce_cohort

from VING_PRD_TREND_DB.HCE_OPS_FNL.HCE_ADR_AVTAR_LIKE_25_26_F as a

LEFT join HCE_OPS_FNL.HCEOPS_2024_2025_HCE_COHORT_6 AS b    --MAY NEED TO CHANGE NEXT REFRESH--
--left join tmp_1y.2024_2025_HCE_COHORT_6 as b    
on b.fin_mbi_hicn_fnl = a.medicare_id

where 
a.business_segment not in ('EnI','ERR','null') 
--and entity <> 'PHS'
and a.avtar_mtch_ind = 1
--and prim_svc_palist = 'Y'
and a.pa_program not in ('Not EPAL-Prime','Non-EPAL')
and a.notif_yrmonth between '202601' and '202612'   -------------CHANGE THIS DATE--------------
;
--drop table if exists tmp_1m.pa_refresh_c;
create table tmp_1m.pa_refresh_c as 

select * from tmp_1m.pa_refresh26
union all
select * from tmp_1m.pa_refresh25
union all
select * from tmp_1m.pa_refresh24
union all
select * from tmp_1m.pa_refresh23
--union all
--select * from tmp_1m.pa_refresh22
--union all 
--select * from tmp_1m.pa_refresh21
--union all 
--select * from tmp_1m.pa_refresh20
--union all 
--select * from tmp_1m.pa_refresh19
--union all 
--select * from tmp_1m.pa_refresh18

--SELECT * FROM tmp_1y.HCE_COHORT_2023_2024_6
--HCE_OPS_FNL.HCEOPS_2024_2025_HCE_COHORT_6
;
-----------------------------------------------------------------------------------------------------------------------------
--select count(*) from tmp_1m.pa_refresh_c;

--QA CHECK--
--select distinct pa_program from tmp_1m.pa_refresh_c
--IF THE RESULTS SHOW DIFFERENT THAN BELOW, THEN STOP AND EVALUATE WHY--
	--ECS--
	--NaviHealth--
	--Optum--
	--Optum&ECS--
	--OrthoNet--
	--UCS--
	--eviCore--
	--eviCore&ECS--

--QA CHECK--
--select distinct case_decn_stat_cd from tmp_1m.pa_refresh_c
--IF THE RESULTS SHOW DIFFERENT THAN BELOW, THEN STOP AND EVALUATE WHY--
	--AD - Fully Adverse Determination--
	--FA - Fully Approved--
	--FM - Fully Mixed--
	--FP - Fully Pended--
	--PA - Partially Approved--
	--PD - Partially Adverse Determination--
	--PM - Partially Mixed--

--QA CHECK--
--select entity, count(*) from tmp_1m.pa_refresh_c group by entity
--IF THE RESULTS SHOW DIFFERENT THAN BELOW, THEN STOP AND EVALUATE WHY--
	--COSMOS--
	--NICE--
-----------------------------------------------------------------------------------------------------------------------------
--select * from tmp_1m.pa_refresh_c2  limit 50
--drop table if exists tmp_1m.pa_refresh_c2;
create table tmp_1m.pa_refresh_c2 as 

select 
	medicare_id
	,create_dt
	,notif_recd_dttm
	,substring(Notif_yrmonth,1,4) as Notif_Year
	,Notif_yrmonth
	,business_segment
	,case when entity = 'PHS' then 'NICE' else 'COSMOS' end as Entityname
	,entity
	,proc_cd
	--,pa_program
	,case_decn_stat_cd
	,case_id
	,migration_source
	,fin_brand
	,fin_source_name
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,tfm_include_flag
	,global_cap
	,fin_market
	,fin_state
	,fin_plan_level_2
	,fin_product_level_3
	,fin_g_i
	,group_number
	,group_name
	,hce_cohort
	
	,case when case_init_decn_cd in ('AD - Fully Adverse Determination','FM - Fully Mixed','PM - Partially Mixed','PD - Partially Adverse Determination') then 1 else 0 end as initial_adverse
	,case when case_decn_stat_cd in ('FM - Fully Mixed','PD - Partially Adverse Determination','PM - Partially Mixed') then 1 else 0 end as partial_adverse
	,case when case_decn_stat_cd in ('AD-Fully Adverse Determination','AD - Fully Adverse Determination') then 1 else 0 end as fully_adverse
	
--,case when hce_cohort in ('NTM','NTU') then 'New Member' else 'Retained' end as hce_cohort
--,case when case_decn_stat_cd = 'PD-Partially Adverse Determination' then 1 else 0 end as Partially_adverse
--,case when case_decn_stat_cd = 'AD-Fully Adverse Determination' or case_decn_stat_cd = 'AD - Fully Adverse Determination' then 1 else 0 end as fully_adverse
--,case when case_decn_stat_cd = 'FM-Fully Mixed' or case_decn_stat_cd = 'FM - Fully Mixed' then 1 else 0 end as fully_mixed
--,case when case_decn_stat_cd = 'FA-Fully Approved' then 1 else 0 end as fully_approved
--,case when case_decn_stat_cd = 'FP-Fully Pended' then 1 else 0 end as fully_pended
--,case when case_decn_stat_cd = 'PA-Partially Approved' then 1 else 0 end as partially_approved
--,case when case_decn_stat_cd = 'PM-Partially Mixed' then 1 else 0 end as partially_mixed
,1 as case_cnt

from tmp_1m.pa_refresh_c

--left join fichsrv.tre_membership as b
--on a.medicare_id = b.fin_mbi_hicn_fnl
--and a.Notif_yrmonth = b.fin_inc_month
;

-----------------------------------------------------------------------------------------------------------------------------
--select 
--sum(OAH_Flag) as orig_OAH
--,sum(CnS_OAH_Flag_MD) as OAH_MD
--,sum(CnS_OAH_Flag_combined) as OAH_wo_MD
--,sum(CnS_Dual_flag) as orig_Cns_DUAL
--,sum(CnS_Dual_Flag_modif) as orig_Cns_DUAL_modif
--,sum(CnS_Dual_flag_States) as CnS_Dual_StateVol
--,sum(CnS_Dual_flag_ExclStates) as CnS_Dual_woState
--,sum(CnS_Dual_flag_NEW) as new_CnS_Dual
--,sum(CnS_Dual_flag_NEW2) as new_CnS_Dual2
--,sum(CnS_Dual_flag_NEW_wo_MD) as new_CnS_Dual_wo_MD
--from tmp_1m.pa_refresh_c3_TEST_v5

--drop table if exists tmp_1m.pa_refresh_c3; 
create table tmp_1m.pa_refresh_c3 as select 

Notif_yrmonth,
business_segment,
entityname,
entity,
--pa_program,
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
hce_cohort,

CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL' THEN 1 else 0 end as MnR_COSMOS_FFS_Flag,

CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type = 'FFS' THEN 1 else 0 end as MnR_NICE_FFS_Flag,

CASE WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL') 
       OR (business_segment = 'MnR' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG,

--CASE WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND migration_source='OAH')
--	OR (business_segment = 'CnS' AND fin_brand='C&S' and migration_source='OAH') then 1 else 0 end as OAH_FLAG, --(ORIGINAL)-- 

--CASE WHEN (Notif_Year ='2024' AND business_segment = 'CnS' AND fin_brand in ('M&R','C&S') AND GLOBAL_CAP = 'NA' AND
--	SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') THEN 1 else 0 end as CnS_OAH_Flag_MD,
	
CASE WHEN (Notif_Year ='2024' AND business_segment = 'CnS' AND fin_brand in ('M&R','C&S') AND GLOBAL_CAP = 'NA' AND
	SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') then 0 
	WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND migration_source='OAH')
	OR (business_segment = 'CnS' AND fin_brand='C&S' and migration_source='OAH') then 1 else 0 end as OAH_FLAG,
	
--CASE WHEN (business_segment = 'CnS' AND fin_brand='M&R' AND fin_product_level_3='DUAL' AND migration_source <> 'OAH' AND global_cap='NA') 
--	OR (business_segment = 'CnS' AND fin_brand='C&S' AND fin_product_level_3='DUAL'  AND migration_source <> 'OAH' AND global_cap='NA') THEN 1 else 0 end as CnS_Dual_Flag,  --(ORIGINAL)--

--CASE WHEN (business_segment = 'CnS' AND fin_brand='M&R' AND fin_product_level_3='DUAL' AND migration_source <> 'OAH' AND global_cap='NA' AND SGR_SOURCE_NAME in ('COSMOS','CSP')) 
	--OR (business_segment = 'CnS' AND fin_brand='C&S' AND fin_product_level_3='DUAL'  AND migration_source <> 'OAH' AND global_cap='NA' AND SGR_SOURCE_NAME in ('COSMOS','CSP')) THEN 1 else 0 end as CnS_Dual_Flag_modif,  --(ORIGINAL)--
	
--CASE WHEN (business_segment = 'CnS' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
--	SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state IN ('OK','NC','NM','NV','OH','TX')) then 1 else 0 end as CnS_Dual_flag_States,

--CASE WHEN (business_segment = 'CnS' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
--	SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) then 1 else 0 end as CnS_Dual_flag_ExclStates,

CASE WHEN ((business_segment = 'CnS' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
	SGR_SOURCE_NAME in('COSMOS','CSP')) OR (Notif_Year ='2024' AND business_segment = 'CnS' AND fin_brand in ('M&R','C&S')
	AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_Flag,	

--CASE WHEN (Notif_Year ='2024' AND business_segment = 'CnS'  AND fin_brand in ('M&R','C&S') AND GLOBAL_CAP = 'NA' AND
--	SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') then 1  
--	WHEN (business_segment = 'CnS' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
--	SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX'))  then 1 else 0 end as CnS_Dual_flag_NEW2,	
	
CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag,

CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='INSTITUTIONAL' then 1 else 0 end as ISNP_flag,

proc_cd,
case_cnt,
initial_adverse,
Partial_adverse,
fully_adverse

from tmp_1m.pa_refresh_c2

where proc_cd <> 'null'          --some null proc cases are removed.   
and fin_plan_level_2 <> 'PFFS'      --PFFS does not require PA, but some acitivity was found.  Not material to remove. 
;

-----------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.pa_refresh_c4;
create table tmp_1m.pa_refresh_c4 as select 

Notif_yrmonth,
--pa_program,
--business_segment,
--entityname,
--entity,
--migration_source,
--fin_brand,
--fin_source_name,
--nce_tadm_dec_risk_type,
--tfm_include_flag,
--global_cap,
fin_market,
fin_state,
--fin_plan_level_1,
--fin_product_level_3,
fin_g_i,
--sj_prov_id,	
--rf_prov_id,
--MnR_Dual_flag,
hce_cohort,
CASE WHEN MnR_COSMOS_FFS_Flag = 1 THEN 'MnR_COSMOS_FFS'
	WHEN MnR_NICE_FFS_Flag = 1 THEN 'MnR_NICE_FFS'
	WHEN CnS_Dual_Flag = 1 THEN 'CnS_Dual' 
	WHEN ISNP_flag = 1 THEN 'ISNP'
	WHEN OAH_flag = 1 THEN 'OAH' ELSE 'REMOVE' END AS POPULATION_FLAG,
	
proc_cd,
case_cnt,
initial_adverse,
Partial_adverse,
fully_adverse

from tmp_1m.pa_refresh_c3;

-----------------------------------------------------------------------------------------------------------------------------
--STANDARD OUTPUT FOR NORMAL PA MODEL--
-----------------------------------------------------------------------------------------------------------------------------
--select count(*) from tmp_1m.pa_refresh_c5_v2
--where mnr_dual_flag = 1

--drop table if exists tmp_1m.pa_refresh_c5_v2;
create table tmp_1m.pa_refresh_c5_v2 as select 

Notif_yrmonth,
--pa_program,
--business_segment,
--entityname,
--fin_brand,
--fin_market,
--fin_plan_level_1,
--fin_product_level_3,
fin_g_i,
--MnR_Dual_flag,
POPULATION_FLAG,
proc_cd,
--hce_cohort,
sum(case_cnt) as Total_Auths,
sum(Initial_adverse) as Initial_deny,
sum(Partial_adverse) as Partial_deny,
sum(fully_adverse) as Fully_deny

from tmp_1m.pa_refresh_c4

where Population_Flag <> 'REMOVE' --this will remove any auths from C&S members we don't want to include in affordability reporting--

and proc_cd not in ('T1019','S5125','S5170','S5130','S5161','S5150','T2033','S5135','T1005','T2031','S5102','S5126','T2040','T2029','S5140','T1023','S5160','S5165','T1017','T2021','T4541',
'T2025','T4527','S5121','T4526','T2016','T2030','T4535','S5190','S5101','T4528','T4523','S5100','T2003','T4524','T1002','T4522','T1001','S5120','S5105','T1505','T1020','S0317','T4525','S5185',
'S5151','T1003','T4537','T2038','T4544','S5136','T4543','S0315','T2032','T2002','T1028','T1999','T4540','T5999','T4534','T4521','T2039','T4542','T2024','S9470','S5199','T2042','T1004',
'T2017','S0316','T2035','T4532')  --THESE PROCS ARE NOT IN THE AVTAR FOR CNS_DUALS.  REMOVING THESE BETTER ALIGN WITH AVTAR, EVEN THOUGH THE AVTAR MATCH IND = 1--

group by
Notif_yrmonth,
--pa_program,
--business_segment,
--entityname,
--fin_brand,
--fin_market,
--fin_plan_level_1,
--fin_product_level_3,
fin_g_i,
--MnR_Dual_flag,
POPULATION_FLAG,
--OAH_FLAG,
proc_cd
--hce_cohort
;

-----------------------------------------------------------------------------------------------------------------------------
--STOP HERE--
-----------------------------------------------------------------------------------------------------------------------------

drop table if exists tmp_1m.pa_refresh23;
drop table if exists tmp_1m.pa_refresh24;
drop table if exists tmp_1m.pa_refresh25;
drop table if exists tmp_1m.pa_refresh26;
drop table if exists tmp_1m.pa_refresh_c;
drop table if exists tmp_1m.pa_refresh_c2;
drop table if exists tmp_1m.pa_refresh_c3;
drop table if exists tmp_1m.pa_refresh_c4;
drop table if exists tmp_1m.pa_refresh_c5_v2;

-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------
--STANDARD OUTPUT FOR ENHANCED PA MODEL--
-----------------------------------------------------------------------------------------------------------------------------
--select count(*) from tmp_1m.pa_refresh_c4
--drop table if exists tmp_1m.pa_refresh_c4b;
create table tmp_1m.pa_refresh_c4b  as select 

Notif_yrmonth,
--business_segment,
--entityname,
--entity,
--migration_source,
--fin_brand,
--fin_source_name,
--nce_tadm_dec_risk_type,
--tfm_include_flag,
--global_cap,
fin_market,
--fin_plan_level_1,
--fin_product_level_3,
fin_g_i,
--MnR_Dual_flag,
group_number,
group_name,

CASE WHEN MnR_COSMOS_FFS_Flag = 1 THEN 'MnR_COSMOS_FFS'
	WHEN MnR_NICE_FFS_Flag = 1 THEN 'MnR_NICE_FFS'
	WHEN CnS_Dual_Flag = 1 THEN 'CnS_Dual'
	WHEN ISNP_Flag = 1 THEN 'ISNP'
	WHEN OAH_flag = 1 THEN 'OAH' ELSE 'REMOVE' END AS POPULATION_FLAG,
	
proc_cd,
case_cnt,
fully_adverse,
Partially_adverse

from tmp_1m.pa_refresh_c3;

------------------------------------------------------------------------------------------------------------------------
--create table tmp_1m.pa_refresh_c5b_test as select * from tmp_1m.pa_refresh_c5b where Notif_yrmonth like '2023%' 
--drop table if exists tmp_1m.pa_refresh_c5b;
create table tmp_1m.pa_refresh_c5b  as select 

Notif_yrmonth,
--business_segment,
--entityname,
--fin_brand,
fin_market,
--fin_plan_level_1,
--fin_product_level_3,
fin_g_i,
group_number,
group_name,
--MnR_Dual_flag,
POPULATION_FLAG,
proc_cd,
sum(case_cnt) as Total_Auths,
--sum(Partially_adverse) as Partial_deny,
sum(fully_adverse) as Fully_deny

from tmp_1m.pa_refresh_c4b

where Population_Flag <> 'REMOVE' --this will remove any auths from C&S members we don't want to include in affordability reporting--

group by
Notif_yrmonth,
--business_segment,
--entityname,
--fin_brand,
fin_market,
--fin_plan_level_1,
--fin_product_level_3,
fin_g_i,
group_number,
group_name,
--MnR_Dual_flag,
POPULATION_FLAG,
--OAH_FLAG,
proc_cd
;

---------------------------------------------------------------------------------------------------------------------
--EVAL POPULATION FILTERS--
---------------------------------------------------------------------------------------------------------------------

--drop table if exists tmp_1m.pa_refresh_c3_eval
create table tmp_1m.pa_refresh_c3_eval  as
select
business_segment,
fin_brand,
fin_source_name,
migration_source,
global_cap,
tfm_include_flag,
nce_tadm_dec_risk_type,
fin_product_level_3,
MnR_COSMOS_FFS_Flag,
MnR_TOTAL_FFS_FLAG,
OAH_Flag,
CnS_Dual_Flag,
MnR_Dual_flag,
ISNP_flag,
sum(case_cnt),
count(*)
from tmp_1m.pa_refresh_c3
where Notif_yrmonth like '%2023%'
group by
business_segment,
fin_brand,
fin_source_name,
migration_source,
global_cap,
tfm_include_flag,
nce_tadm_dec_risk_type,
fin_product_level_3,
MnR_COSMOS_FFS_Flag,
MnR_TOTAL_FFS_FLAG,
OAH_Flag,
CnS_Dual_Flag,
MnR_Dual_flag,
ISNP_flag
























-----------------------------------------------------------------------------------------------------------------------------
--TEST NAVI HH DATA--
-----------------------------------------------------------------------------------------------------------------------------
select 

population_flag,
fin_market,
sum(Total_Auths) as Auths

from tmp_1m.pa_refresh_c5

where Notif_yrmonth in ('202301','202302','202303','202304','202305','202306','202307','202308','202309','202310','202311','202312')
and population_flag in ('CnS_Dual','MnR_COSMOS_FFS','MnR_NICE_FFS')
and proc_cd in 
('G0151',
'G0152',
'G0153',
'G0155',
'G0156',
'G0157',
'G0159',
'G0161',
'G0162',
'G0299',
'G0300',
'G0493',
'G0495',
'S9122',
'S9123',
'S9124',
'S9127',
'S9128',
'S9129',
'S9131')

group by
population_flag,
fin_market

--------------------------------------------------------------------------------------------
select 
proc_cd,
 count(distinct case_id) as auths
 
from HCE_OPS_FNL.hce_adr_avtar_like_23_24_f

where 
--business_segment not in ('EnI','ERR','null')
avtar_mtch_ind = 1
--and pa_program = 'NaviHealth'   --using this makes the auths go down.  New total 84,027 
and pa_program not in ('Not EPAL-Prime','Non-EPAL')
and notif_yrmonth between '202301' and '202312'
and business_segment = 'CnS'  --excluding this doesn't change much.  New total 98,762 vs 98,603
and fin_brand = 'C&S'
and global_cap='NA'
and fin_product_level_3='DUAL'
and migration_source <> 'OAH'
and proc_cd in 
('G0151',
'G0152',
'G0153',
'G0155',
'G0156',
'G0157',
'G0159',
'G0161',
'G0162',
'G0299',
'G0300',
'G0493',
'G0495',
'S9122',
'S9123',
'S9124',
'S9127',
'S9128',
'S9129',
'S9131')

group by proc_cd


CASE WHEN business_segment = 'CnS' AND fin_brand='C&S' AND global_cap='NA' AND fin_product_level_3='DUAL' AND migration_source <> 'OAH' THEN 1 else 0 end as CnS_Dual_Flag,

