

--Step 5: union together all needed notifications from the AvTar Report after Pradeepa sends the weekly email - update date of run! 
--Note: Respiratory AND leading indicator flags need to be based source of truth table tmp_1y.hce_resp_2024 and have periods in the ICDs unlike claims
drop table tmp_1m.ec_ip_dataset_09242025_trs; 
create table tmp_1m.ec_ip_dataset_09242025_trs stored as orc as 
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
from tmp_1m.ec_avtar_24_25_3
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
		SGR_SOURCE_NAME in('COSMOS','CSP')) OR (YEAR(hce_dt) ='2024' AND business_segment = 'CnS' AND fin_brand in ('C&S')
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
from tmp_1m.ec_avtar_23_3_trs
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
    		'R05.3','R05.4','R05.8','R05.9','R50.2','R50.81','R50.9','R68.83''A37.00','A37.01','A37.10','A37.11','A37.80','A37.81','A37.90','J12.0','J41.0','J41.1','J41.8') then 'ILI'
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
from tmp_1m.ec_avtar_22_3_trs
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
drop table tmp_1m.ec_ip_dataset_09242025_2_trs; 
create table tmp_1m.ec_ip_dataset_09242025_2_trs stored as orc as 
select 
	a.*
	,case when a.IP_type='SNF' and b.class='IP_SWGBED' then 1 
		else 0 end as swgbed
	,case when a.fin_product_level_3<>'INSTITUTIONAL' AND a.TFM_INCLUDE_FLAG=1 AND a.CAPITATED=0 AND a.BUSINESS_SEGMENT='MnR' then 'M&R'
		WHEN a.fin_product_level_3='DUAL' AND a.TFM_INCLUDE_FLAG=0 AND a.CAPITATED=0 AND (a.MIGRATION_SOURCE<>'OAH' or a.migration_source is null) AND a.BUSINESS_SEGMENT='CnS' 
			then 'C&S' else 'Other' end as MR_CS_Other
from tmp_1m.ec_ip_dataset_09242025_trs as a
left join tmp_1y.hk_snf_swgbed_tins2 as b
on a.prov_tin=b.prov_tin
;


--Step 7: Adding in a IPA/PAC split now that SWGBED is split out 
drop table tmp_1m.ec_ip_dataset_09242025_3_trs; 
create table tmp_1m.ec_ip_dataset_09242025_3_trs stored as orc as 
select 
	*
	,case when swgbed=1 then 'Swing Bed'
		when IP_Type in ('LTAC','SNF','AIR') then IP_type else ipa_li_split end as admit_type
	,case when swgbed=1 then 'PAC'
		when IP_type in ('LTAC','SNF','AIR') then 'PAC'
		when IP_type in ('Medical','Surgical','Transplant') then 'IPA' else 'NA' end as IPA_PAC_flag
from  tmp_1m.ec_ip_dataset_09242025_2_trs
;


--Step 8: Roll up before join to MM 
drop table tmp_1m.ec_ip_dataset_09242025_4_trs; 
create table tmp_1m.ec_ip_dataset_09242025_4_trs stored as orc as 
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
from tmp_1m.ec_ip_dataset_09242025_3_trs as a
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
drop table tmp_1m.ec_ip_dataset_09242025_mm; 
create table tmp_1m.ec_ip_dataset_09242025_mm stored as orc as 
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
drop table tmp_1m.ec_ip_dataset_notif_09242025_trs;
create table tmp_1m.ec_ip_dataset_notif_09242025_trs as				
SELECT	
	*
	from tmp_1m.ec_ip_dataset_09242025_4_trs
union all select 
	* from tmp_1m.ec_ip_dataset_09242025_mm
	; 
