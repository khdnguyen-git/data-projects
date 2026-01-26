-- hce_proj_bd (HIVE) -> hce_ops_fnl (Snowflake)
-- stored as orc as -> as
-- drop table -> drop table if exists


-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
--VERSION 2: QUERY ALL LINES WHERE A SKIN SUB CODE IS PRESENT ON THE CASE, PULL ALL CASE LINES---
-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.ess_PreD_skinsub_monitor_cases;
create table tmp_1m.ess_PreD_skinsub_monitor_cases as
select distinct
case_id
from hce_ops_fnl.hce_adr_avtar_like_24_25_f
where business_segment <> 'EnI'
and pa_program in ('Not EPAL-Prime')
and notif_yrmonth between '202401' and '202512'
--and prim_proc_ind = 'Y' --NORMALLY INCLUDED FOR PRE-D TO GET TO THE PRIMARY PROC ON THE CASE--
--and medicare_id in
and proc_cd in
('A2001','A2002','A2004','A2005','A2006','A2007','A2008','A2009','A2010','A2011','A2012','A2013','A2014','A2015','A2016','A2017','A2018','A2019','A2021','A4100','Q4100','Q4110',
'Q4111','Q4112','Q4114','Q4115','Q4117','Q4118','Q4121','Q4122','Q4123','Q4125','Q4126','Q4127','Q4130','Q4132','Q4133','Q4134','Q4135','Q4136','Q4137','Q4138','Q4139','Q4140',
'Q4141','Q4142','Q4143','Q4145','Q4146','Q4147','Q4148','Q4149','Q4150','Q4151','Q4152','Q4153','Q4154','Q4155','Q4156','Q4157','Q4158','Q4159','Q4160','Q4161','Q4162','Q4163',
'Q4164','Q4165','Q4166','Q4167','Q4168','Q4169','Q4170','Q4171','Q4173','Q4174','Q4175','Q4176','Q4177','Q4178','Q4179','Q4180','Q4181','Q4182','Q4183','Q4184','Q4185','Q4186',
'Q4187','Q4188','Q4189','Q4190','Q4191','Q4192','Q4193','Q4194','Q4195','Q4196','Q4197','Q4198','Q4199','Q4200','Q4201','Q4202','Q4203','Q4204','Q4205','Q4206','Q4208','Q4209',
'Q4211','Q4212','Q4213','Q4214','Q4215','Q4216','Q4217','Q4218','Q4219','Q4220','Q4221','Q4222','Q4224','Q4225','Q4226','Q4227','Q4229','Q4230','Q4231','Q4232','Q4233','Q4234',
'Q4235','Q4236','Q4237','Q4238','Q4239','Q4240','Q4241','Q4242','Q4245','Q4246','Q4247','Q4248','Q4249','Q4250','Q4251','Q4252','Q4253','Q4254','Q4255','Q4256','Q4257','Q4258',
'Q4259','Q4260','Q4261','Q4262','Q4263','Q4264','Q4265','Q4266','Q4267','Q4268','Q4269','Q4270','Q4271','Q4272','Q4273','Q4274','Q4275','Q4276','Q4278','Q4279','Q4280','Q4281',
'Q4282','Q4283','Q4284','Q4287','Q4288','Q4289','Q4290','Q4291','Q4292','Q4293','Q4294','Q4295','Q4296','Q4297','Q4298','Q4299','Q4300','Q4301','Q4302','Q4303','Q4304','Q4310',
'Q4318','Q4326','Q4331','Q4332', 'Q4101','Q4102','Q4104','Q4105','Q4116','Q4124','Q4128','Q4210','Q4277','Q4309','Q4313','Q4319','Q4341');
;
---------------------------------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.ess_PreD_skinsub_monitor_a;
--select count (*) from tmp_1m.ess_PreD_skinsub_monitor_a limit 50

create table tmp_1m.ess_PreD_skinsub_monitor_a as
select
a.case_id,
medicare_id,
-- create_dt,
SUBSTRING(notif_recd_dttm,1,10) as Notif_date,
Notif_yrmonth,
entity,
case_category_cd,
channel_cd,
proc_cd,
prim_srvc_cat,
prim_proc_ind,
pa_program,
case_decn_stat_cd,
--business_segment,
--global_cap,
fin_g_i,
--fin_brand,
--fin_source_name,
--migration_source,
--tfm_include_flag,
--nce_tadm_dec_risk_type,
--fin_product_level_3,
fin_market,
sj_prov_id,
rf_prov_id,
--fin_tfm_product,
--fin_tfm_product_new,
--CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND fin_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL' THEN 1 else 0 end as MnR_COSMOS_FFS_Flag,
--CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND fin_source_name='NICE' AND nce_tadm_dec_risk_type = 'FFS' THEN 1 else 0 end as MnR_NICE_FFS_Flag,
CASE WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND global_cap='NA' AND fin_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL')
OR (business_segment = 'MnR' AND fin_brand='M&R' AND fin_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG,
--CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' AND migration_source='OAH' THEN 1 else 0 end as MnR_OAH_Flag,
--CASE WHEN business_segment = 'CnS' AND fin_brand='C&S' AND migration_source='OAH' THEN 1 else 0 end as CnS_OAH_Flag,
CASE WHEN (business_segment = 'MnR' AND fin_brand='M&R' AND migration_source='OAH')
OR (business_segment = 'CnS' AND fin_brand='C&S' AND migration_source='OAH') THEN 1 else 0 end as OAH_Flag,
CASE WHEN business_segment = 'CnS' AND fin_brand='C&S' AND global_cap='NA' AND fin_product_level_3='DUAL' AND migration_source <> 'OAH' THEN 1 else 0 end as CnS_Dual_Flag,
CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='INSTITUTIONAL' then 1 else 0 end as ISNP_flag,
CASE WHEN business_segment = 'MnR' AND fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag,
CASE WHEN fin_market in ('AR','CO','DC','DE','FL','KY','LA','MD','MS','NJ','NM','OH','OK','PA','PR','TX','VI') THEN 'LCD' ELSE 'NON-LCD' END as LCD_flag
from hce_ops_fnl.hce_adr_avtar_like_24_25_f as a
inner join tmp_1m.ess_PreD_skinsub_monitor_cases b
on b.case_id = a.case_id;

-----------------------------------------------------------------------------------------------------------------------------
--drop table if exists tmp_1m.ess_PreD_skinsub_monitor_b;
create table tmp_1m.ess_PreD_skinsub_monitor_b as
select
case_id,
medicare_id,
Notif_date,
Notif_yrmonth,
entity,
case_category_cd,
channel_cd,
proc_cd,
prim_srvc_cat,
prim_proc_ind,
pa_program,
case_decn_stat_cd,
fin_g_i,
fin_market,
sj_prov_id,
rf_prov_id,
'1' as CaseCnt,
CASE WHEN MnR_TOTAL_FFS_FLAG = 1 THEN 'MnR_FFS'
WHEN CnS_Dual_Flag = 1 THEN 'CnS_Dual'
WHEN ISNP_flag = 1 THEN 'INSTITUTIONAL'
WHEN OAH_flag = 1 THEN 'OAH' ELSE 'REMOVE' END AS POPULATION_FLAG,
LCD_FLAG
from tmp_1m.ess_PreD_skinsub_monitor_a;

-------------------------------------------------------------------------------------------------------------------------
--select count(*) from tmp_1m.ess_PreD_skinsub_monitor_f;
--drop table if exists tmp_1m.ess_PreD_skinsub_monitor_f;
create table tmp_1m.ess_PreD_skinsub_monitor_f as
select
case_id,
medicare_id,
Notif_date,
Notif_yrmonth,
entity,
case_category_cd,
channel_cd,
proc_cd,
prim_srvc_cat,
prim_proc_ind,
pa_program,
case_decn_stat_cd,
fin_g_i,
fin_market,
sj_prov_id,
rf_prov_id,
POPULATION_FLAG,
LCD_FLAG,
CaseCnt
from tmp_1m.ess_PreD_skinsub_monitor_b
where POPULATION_FLAG <> 'REMOVE'
and proc_cd is not null;


