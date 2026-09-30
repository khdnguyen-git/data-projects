--Rolling Bed Day Decision to case levl
-- Limit data to actual Admit and Discharge data to calculate Initial LOS FULL denial or Partial
--drop table hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_11 ;
--create table  hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_11 as	
--select 
--	a.case_id 
--	,b.bed_decn_from_dttm
--	,b.bed_decn_end_dttm 
--	,b.bedday_persistent_fulladr_ind
--	,b.bedday_inital_adr_flag
--	,b.initial_dnl_user_ind
--	,b.initial_dnl_decn_dttm
--	,b.initial_dnl_decn_usrid
--	,b.latest_dnl_user_ind
--	,b.latest_dnl_decn_dttm
--	,b.latest_dnl_decn_usrid
--	,b.initial_dnl_decn_user_role
--	,b.latest_dnl_decn_user_role
--	,b.case_key 
----,Case when b.case_key is NULL then c.inital_adr_flag else b.cas_initial_full_adr_flag end as case_initial_full_adr_flag
----,Case when b.case_key is NULL then c.latest_adr_flag else b.case_persistent_full_ADR_Flag end as case_persistent_full_ADR_Flag
--from 
--pa_operations.HCE_ADR_AVTAR_Like_25_26_F_1 a  
--Inner join
-- 	hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_10 b
--on a.case_id = SUBSTRING(b.case_key,5)
--and ( a.admit_dt_act<= bed_decn_from_dttm and  bed_decn_end_dttm<a.dschg_dt_act )
--;
--
--
--
----desc formatted  hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_8;--21500424            
--
--drop table hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_12;
--create table hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_12 as
--select 
--	a.case_key
--	,min(bed_decn_from_dttm) bed_decn_from_dttm
--	,max(bed_decn_end_dttm) bed_decn_end_dttm
--	,max(case when bedday_persistent_fulladr_ind<>'2-PersistentDenied' then 1 else 0 end) Non_persistent
--	,max(case when bedday_inital_adr_flag<>'2-Denied' then 1 else 0 end ) Initial_non_ADR
--	,max(case when bedday_inital_adr_flag='2-Denied' then 1 else 0 end ) Initial_atleastONE_ADR
--	,min(case when initial_dnl_user_ind=1 then initial_dnl_decn_usrid end ) initial_dnl_decn_usrid
--	,min(case when initial_dnl_user_ind=1 then initial_dnl_decn_user_role end ) initial_dnl_decn_user_role
--	,min(case when initial_dnl_user_ind=1 then initial_dnl_decn_dttm end ) initial_dnl_decn_dttm
--	,max(case when latest_dnl_user_ind=1 then latest_dnl_decn_usrid end ) latest_dnl_decn_usrid
--	,max(case when latest_dnl_user_ind=1 then latest_dnl_decn_user_role end ) latest_dnl_decn_user_role
--	,max(case when latest_dnl_user_ind=1 then latest_dnl_decn_dttm end ) latest_dnl_decn_dttm
--from 
--	 hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_11 a
--group by 
--	case_key
--;
--
--
--drop table hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_13 ;
--create table hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_13 as
--select 
--	a.case_key
--	,bed_decn_from_dttm
--	,bed_decn_end_dttm
--	,DATEDIFF(bed_decn_end_dttm,bed_decn_from_dttm) DECN_LOS
--	,case when Initial_atleastONE_ADR=1 and Initial_non_ADR=0 then 'InitialFullADR'
--		when Initial_atleastONE_ADR=1 and Initial_non_ADR=1 then 'InitialPartialADR'
--		when Initial_atleastONE_ADR=0 and Initial_non_ADR=1 then 'InitialFullApporved'
--	end as Cas_Initial_full_ADR_Flag
--	,case when Initial_atleastONE_ADR=1 and Initial_non_ADR=0 and Non_persistent=0 then 'PersistentFullADR' end case_persistent_full_ADR_Flag
--	,case when Initial_atleastONE_ADR=1 and Initial_non_ADR=1 and Non_persistent=0 then 'PersistentPatailADR' end case_persistent_Partial_ADR_Flag
--	,initial_dnl_decn_usrid
--	,initial_dnl_decn_user_role
--	,initial_dnl_decn_dttm
--	,latest_dnl_decn_usrid
--	,latest_dnl_decn_user_role
--	,latest_dnl_decn_dttm
--	,Non_persistent
--	,Initial_non_ADR
--	,Initial_atleastONE_ADR
--from 
--	 hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_12 a
--;


drop TABLE IF exists hce_ops_stage.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_a ;
create table hce_ops_stage.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_a as
select 
	a.*
--	,case when ( b.cas_initial_full_adr_flag  in ('InitialFullADR','2-Denied') or (coalesce(cas_initial_full_adr_flag,'NotPresent') not in ('InitialPartialADR') and c.inital_adr_flag ='2-Denied')) then 1 else 0 end as InitialFullADR_cases
--	,case when cas_initial_full_adr_flag in ('InitialPartialADR') then 1 else 0 end as InitialPartialADR_cases
--	,case when (case_persistent_full_ADR_Flag in ('PersistentFullADR','2-Denied') or (coalesce(case_persistent_full_ADR_Flag,'NotPresent') not in ('PersistentPartialADR')  and c.latest_adr_flag ='2-Denied')) then 1 else 0 end as PersistentFullADR_cases
--	,case when case_persistent_full_ADR_Flag in ('PersistentPartialADR') then 1 else 0 end as PersistentPartialADR_cases
	,case when (c.inital_adr_flag ='2-Denied' or  b.inital_adr_flag ='2-Denied') then 1 else 0 end as InitialFullADR_cases
	,case when (c.latest_adr_flag ='2-Denied' or b.latest_adr_flag ='2-Denied')  then 1 else 0 end as PersistentFullADR_cases
--	,COALESCE (b.initial_dnl_decn_usrid,c.initial_dnl_decn_userid) as initial_dnl_decn_userid
--	,COALESCE (b.initial_dnl_decn_user_role,c.Initial_dnl_decn_user_role) as Initial_dnl_decn_user_role
--	,COALESCE (b.initial_dnl_decn_dttm,c.initial_dnl_decn_dttm) as initial_dnl_decn_dttm
--	,COALESCE (b.latest_dnl_decn_usrid,c.latest_dnl_decn_userid) as latest_dnl_decn_userid
--	,COALESCE (b.latest_dnl_decn_user_role,c.Latest_dnl_decn_user_role) as Latest_dnl_decn_user_role
--	,COALESCE (b.latest_dnl_decn_dttm,c.latest_dnl_decn_dttm) as latest_dnl_decn_dttm
	,COALESCE (b.initial_dnl_decn_userid,c.initial_dnl_decn_userid) as initial_dnl_decn_userid
	,COALESCE (b.initial_dnl_decn_user_role,c.Initial_dnl_decn_user_role) as Initial_dnl_decn_user_role
	,COALESCE (b.initial_dnl_decn_dttm,c.initial_dnl_decn_dttm) as initial_dnl_decn_dttm
	,COALESCE (b.latest_dnl_decn_userid,c.latest_dnl_decn_userid) as latest_dnl_decn_userid
	,COALESCE (b.latest_dnl_decn_user_role,c.Latest_dnl_decn_user_role) as Latest_dnl_decn_user_role
	,COALESCE (b.latest_dnl_decn_dttm,c.latest_dnl_decn_dttm) as latest_dnl_decn_dttm
--	
--	,case when a.svc_setting<>'Inpatient' then c.initial_dnl_decn_userid 
--		  when a.svc_setting='Inpatient' then b.initial_dnl_decn_userid 
--	 end as initial_dnl_decn_userid
--	 ,case when a.svc_setting<>'Inpatient' then c.Initial_dnl_decn_user_role 
--		  when a.svc_setting='Inpatient' then b.Initial_dnl_decn_user_role 
--	 end as Initial_dnl_decn_user_role
--	 ,case when a.svc_setting<>'Inpatient' then c.initial_dnl_decn_dttm 
--		  when a.svc_setting='Inpatient' then b.initial_dnl_decn_dttm 
--	 end as initial_dnl_decn_dttm	
--	 ,case when a.svc_setting<>'Inpatient' then c.latest_dnl_decn_userid 
--		  when a.svc_setting='Inpatient' then b.latest_dnl_decn_userid 
--	 end as latest_dnl_decn_userid	
--	 ,case when a.svc_setting<>'Inpatient' then c.Latest_dnl_decn_user_role 
--		  when a.svc_setting='Inpatient' then b.Latest_dnl_decn_user_role 
--	 end as Latest_dnl_decn_user_role		
--	 ,case when a.svc_setting<>'Inpatient' then c.latest_dnl_decn_dttm 
--		  when a.svc_setting='Inpatient' then b.latest_dnl_decn_dttm 
--	 end as latest_dnl_decn_dttm			
	,case when c.case_key is null then 0 else 1 end as serv_PA_decn_case_found_ind
	,case when b.case_key is null then 0 else 1 end as serv_LOC_decn_case_found_ind
from 
	hce_ops_stage.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_1 a  
--left outer join
-- 	hce_ops_stage.HCEOPS_SD_ADR_TRANS_BED_DECN_13  b
--on a.case_id = SUBSTRING(b.case_key,5)                       
left outer JOIN 
	hce_ops_stage.HCEOPS_SD_ADR_TRANS_SERV_DECN_5 b
on a.case_id = SUBSTRING(b.case_key,5)  and svc_setting='Inpatient' and b.svc_seq_nbr=0
left outer JOIN 
	hce_ops_stage.HCEOPS_SD_ADR_TRANS_SERV_DECN_5 c
on a.case_id = SUBSTRING(c.case_key,5)  
AND a.svc_seq_nbr=c.svc_seq_nbr and svc_setting<>'Inpatient'
;

--select svc_setting,case_id,svc_seq_nbr,case_decn_stat_cd,InitialFullADR_cases,PersistentFullADR_cases,case_category_cd,* 
--from hce_ops_stage.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_a_Test where business_segment='MnR' and svc_setting<>'Inpatient' and hce_category is not null
--and initialfulladr_cases=1 and case_category_cd='01 - PriorAuthList EPAL';

--select 
--*
--from
--hce_ops_stage.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_a_Test
--where 
--svc_setting='Inpatient'
--and missing_serv_decn_case=0 and svc_seq_nbr<>0

--desc formatted hce_ops_stage.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F_a;--33307334                        
--desc formatted pa_operations.HCE_ADR_AVTAR_Like_25_26_F_1;--33307334            

--select * from pa_operations.HCE_ADR_AVTAR_Like_25_26_F_a where latest_decn_userid like '%MCR%'

--select * from hce_ops_stage.HCEOPS_HCE_ADR_AVTAR_Like_25_26_F where case_id='270858097'
--
--select * from hce_ops_stage.HCEOPS_SD_ADR_TRANS_SERV_DECN_2_test where case_key='HSR-270858097' 

--drop table tmp_1m.test11;
--create table tmp_1m.test11 as
--select 
--	DATE_FORMAT(a.dschg_dt_act  ,'yyyyMM') dischrgdt
--	,count(distinct case_id) unq_case_cnt
--	,count(case_id) case_cnt
--	,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
--	,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--from  pa_operations.HCE_ADR_AVTAR_Like_25_26_F  a
--where
--	 svc_setting ='Inpatient' --Inpatient Services
--	and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--	and admit_cat_cd  in ('17 - Medical','30 - Surgical')
--	and fin_product_level_3<>'INSTITUTIONAL'
--	and tfm_include_flag=1
--	and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--group by 
--	DATE_FORMAT(a.dschg_dt_act   ,'yyyyMM') 
--union all
--select 
--	DATE_FORMAT(dschg_dt_act  ,'yyyyMM') dischrgdt
--	,count(distinct case_id) unq_case_cnt
--	,count(case_id) case_cnt
--	,count(distinct (case when initialfulladr_cases=1 then case_id end)) Initial_fullADR_case_cnt
--	,count(distinct (case when persistentfulladr_cases=1 then case_id end)) Persistent_fullADR_case_cnt
--from pa_operations.HCE_ADR_AVTAR_Like_2021_F  
----show tables in pa_operations like '*HCE_ADR_AVTAR_Like_*_F' 
--where
--	 svc_setting ='Inpatient' --Inpatient Services
--	and plc_of_svc_cd ='21 - Acute Hospital' -- ACUTE
--	and admit_cat_cd  in ('17 - Medical','30 - Surgical')
--	and fin_product_level_3<>'INSTITUTIONAL'
--	and tfm_include_flag=1
--	and ((global_cap='NA' and fin_source_name = 'COSMOS') )
--group by 
--	DATE_FORMAT(dschg_dt_act,'yyyyMM') 
--;
--
--select * from tmp_1m.test11;
--
--
--drop table  tmp_1m.avtarf_case_total_mnr_join;
--create table tmp_1m.avtarf_case_total_mnr_join stored as orc as
--select a.case_id
--       ,a.dschg_dt_act as admit_dt
--       ,a.initialfulladr_cases
--       ,a.persistentfulladr_cases 
--       ,b.*
--       ,case when a.case_id=b.caseid then 1 else 0 end as case_id_match_flag
--from 
--     tmp_1y.case_total_mnr b
--left join 
--    pa_operations.HCE_ADR_AVTAR_Like_25_26_F a
---- tmp_1m.test11 a
--on
--  a.case_id=b.caseid 
--and 
--  a.dschg_dt_act =b.dischargedt 
--where date_format(b.dischargedt,'yyyy') in ('2022','2023');
--
--
--select * from tmp_1m.avtarf_case_total_mnr_join b where date_format(b.admitdt,'yyyyMM') in ('202202') and case_id is null
--
--select * from  tmp_1m.avtarf_case_total_mnr_join where  initialfulladr_cases<>ini_full_deny_cnt;  --941
--
--select * from  tmp_1m.avtarf_case_total_mnr_join where  persistentfulladr_cases<>curr_full_deny_cnt; --2707
--
--select * from  tmp_1m.avtarf_case_total_mnr_join  where caseid=192702628
--select * from tmp_1y.case_total_mnr b where caseid=194545904  192702628
--
--select * from tmp_1y.case_total_mnr b where caseid=195703722
--select * from pa_operations.HCE_ADR_AVTAR_Like_25_26_F a where case_id =195703722
--select * from pa_operations.HCE_ADR_AVTAR_Like_2021_F a where case_id =195703722
--select * from pa_operations.HCE_ADR_AVTAR_Like_2020_F a where case_id =195703722
--
--2022-02-14 00:00:00.0	2022-02-14 00:00:00.0	2022-02-15 00:00:00.0	2022-02-15 00:00:00.0
--2022-02-14 00:00:00.0	2022-02-14 00:00:00.0	2022-02-15 00:00:00.0	2022-02-15 00:00:00.0
--193224303
--193653913
--193665682
--193669026
--193687065
--193712895
--193723418
--193855580
--193899272
--193907453
--
--drop table  tmp_1y.case_total_mnr_drv;
--create table tmp_1y.case_total_mnr_drv as
--select 
--	c.medicare_id 
--	,c.business_segment adr_business_seg_drv
--	,case when c.case_key  is null then 0 else 1 end mtch_ind
--	,b.* 
--from    
--	tmp_1y.case_total_mnr b
--left outer join 
--	pa_operations.hsr_adr_case_drv_2023_072023 c
--on b.caseid = SUBSTRING(c.case_key,5);
--
--select 
--	mtch_ind
--	,c.business
--	,adr_business_seg_drv
--	,count(distinct caseid) cases 
--from 
--	tmp_1y.case_total_mnr_drv c 
--group by 
--	mtch_ind
--	,c.business
--	,adr_business_seg_drv
--;
--
--mtch_ind	business	adr_business_seg_drv	cases
--0	MnR	[NULL]	18,491
--1	MnR	CnS	36,063
--1	MnR	MnR	1,633,417
