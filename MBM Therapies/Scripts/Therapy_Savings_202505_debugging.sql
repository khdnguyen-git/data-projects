
/*Official version, making top edits on Wycliffe's code
1. Define Pilot as the 4 states + Individual population; push the Group to National 
2. Adding ama_pl_of_srvc_cd 49 to the 'Office' category; adding 19, 22, 24 to the OP_REHAB; remove 98943 from Chiro

*/

--select * from fichsrv.tre_membership limit 2;

/*______________[ Membership Detail]___________________________________________________________________________________*/							
DROP table tmp_1m.kn_MBM_dtl;					
create table tmp_1m.kn_MBM_dtl stored as ORC as 					
select 			
	fin_mbi_hicn_fnl			
	,FIN_INC_MONTH			
	,FIN_INC_QTR 		
	,fin_market AS MARKET_FNL		
	,case when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot' else 'National' end	as MBM_DEPLOY_DT
	,fin_g_i AS GROUP_IND_FNL		
	,case when b.migration_source = 'CIP' THEN 'CIP'				
		  when b.migration_source in ('PC','MEDICA') THEN 'SouthFlorida'					
		  when b.fin_product_level_3='DUAL' and b.tfm_include_flag=1 then 'M&R DUALS'					
		  when b.fin_product_level_3='DUAL' and b.tfm_include_flag=0 then 'C&S DUALS'					
		  when b.migration_source = 'NA' and b.fin_g_i = 'I' THEN 'Legacy Individual'					
	      when b.fin_g_i = 'G' THEN 'Group'					
	      else 'OTHERS' end as POPULATION					
	,if(GLOBAL_CAP = 'NA',1,0) GLOBAL_CAP		
	,if(TFM_INCLUDE_FLAG = '1'	,1,0) TFM_INCLUDE	
	,if(FIN_PRODUCT_LEVEL_3 IN ('INSTITUTIONAL'),1,0) INST		
	,if(fin_product_level_2 IN ('PFFS'),1,0) PFFS		
	,if(SPECIAL_NETWORK IN ('ERICKSON'),1,0) ERK			
	,SGR_SOURCE_NAME  		
	, 1  MM		
from fichsrv.tre_membership B 			  --select * from fichsrv.tre_membership limit 2;
where year(fin_incurred_dt) > 2018			
 	  AND b.FIN_BRAND = 'M&R'	
 	  and b.fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
group by 			
	fin_mbi_hicn_fnl			
	,FIN_INC_MONTH			
	,FIN_INC_QTR 		
	,fin_market  		
	,case when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot' else 'National' end	  --caroline edited to show Pilot only take Individual in the 4 states. Groups go to National.
	,fin_g_i  		
    ,case when b.migration_source = 'CIP' THEN 'CIP'				
          when b.migration_source in ('PC','MEDICA') THEN 'SouthFlorida'					
		  when b.fin_product_level_3='DUAL' and b.tfm_include_flag=1 then 'M&R DUALS'					
		  when b.fin_product_level_3='DUAL' and b.tfm_include_flag=0 then 'C&S DUALS'					
		  WHEN b.migration_source = 'NA' and b.fin_g_i = 'I' THEN 'Legacy Individual'					
		  when b.fin_g_i = 'G' THEN 'Group'					
		  else 'OTHERS' end  					
	,if(GLOBAL_CAP = 'NA',1,0)  		 
	,if(TFM_INCLUDE_FLAG = '1'	,1,0)  	
	,if(FIN_PRODUCT_LEVEL_3 IN ('INSTITUTIONAL'),1,0)  		
	,if(fin_product_level_2 IN ('PFFS'),1,0) 		
	,IF(SPECIAL_NETWORK IN ('ERICKSON'),1,0) 			
	,SGR_SOURCE_NAME
;		
		
select count(*) from tmp_1m.kn_MBM_dtl;  --415060257  407463331 399836379   392256742 (removed 2019) 442855070  435558291  428264965  450161568
								
	/*______________[ Membership Summary]___________________________________________________________________________________*/							
DROP TABLE TMP_1m.kn_MBM_MSHP; 							
CREATE TABLE TMP_1m.kn_MBM_MSHP  stored as orc AS 							
	select 							
		 fin_inc_month ep_start_mo							
		,SUBSTRING(MARKET_FNL,0,2) market_fnl							
		,mbm_deploy_dt							
		,group_ind_fnl							
		,population							
		,global_cap							
		,tfm_include							
		,inst							
		,pffs							
		,erk							
		,sgr_source_name							
		,sum(mm) mm 							
		,SUBSTRING(fin_inc_month,0,4) EP_YR 							
		,SUBSTRING(fin_inc_month,5,2) EP_MNTH							
	from tmp_1m.kn_MBM_dtl  a							
	GROUP BY 							
		fin_inc_month							
		,SUBSTRING(MARKET_FNL,0,2)  							
		,mbm_deploy_dt							
		,group_ind_fnl							
		,population							
		,global_cap							
		,tfm_include							
		,inst							
		,pffs							
		,erk							
		,sgr_source_name
;	


--caroline
drop table TMP_1m.kn_MBM_MSHP_sum1;
create table TMP_1m.kn_MBM_MSHP_sum1 stored as orc as
	SELECT 
		'MM' as data_type
		,ep_start_mo
		,'' as visit_mo
		,mbm_deploy_dt as pilot_nat
		,'' as category
		,'' as claim_status
		,0 as visit_ep_lag
		,0 as visit_runout_mo
		,0 as ep_cnt
		,0 as visit_cnt
		,0 as allowed_amt
		,sum(mm) as mms
	FROM TMP_1m.kn_MBM_MSHP
	where population not in ('M&R DUALS', 'C&S DUALS') and global_cap = 1
	group by 
		ep_start_mo
		,mbm_deploy_dt
;
--128 126 124  144 142   select count(*) from TMP_1m.kn_MBM_MSHP_sum1;

select * from TMP_1y.kn_MBM_MSHP_sum1

--_____________[ END OF MEMBERSHIP ]_____________________________________


/*_______________[ bring in LOPA ]_________________________________________________________________________________*/
describe formatted tmp_1y.PA_TRCKNG_op_EVNT_LOPA_DTL
describe formatted tmp_1y.PA_TRCKNG_pr_EVNT_LOPA_DTL


drop table TMP_1m.kn_LOPA_op_1;
create table TMP_1m.kn_LOPA_op_1 as
select  
	case when Include_non_sug_event=1 then MBI_DOS end as Total_Mbi_Dos
	,case when final_LOPA_IND=1 and   mbr_DOS_latest_submission=1 and Include_non_sug_event=1 then MBI_DOS end as still_LOPA_MBI_DOS
	,case when Include_non_sug_event=1  and  (final_LOPA_IND<>1  OR  mbr_DOS_latest_submission<>1)  then MBI_DOS end as Overturn_LOPA_MBI_DOS
	,*
from tmp_1y.PA_TRCKNG_op_EVNT_LOPA_DTL
;	
--2417275  2306199  2164101  1729931 1526791   1338831   select count(*) from TMP_1y.kn_LOPA_op_1  	

drop table TMP_1m.kn_LOPA_op;
create table TMP_1m.kn_LOPA_op as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_LOPA
	,*
from TMP_1m.kn_LOPA_op_1
; 
--2417275  2306199 2164101	select count(*) from TMP_1y.kn_LOPA_op

drop table TMP_1m.kn_LOPA_pr_1;
create table TMP_1m.kn_LOPA_pr_1 as
select  
	MBI_DOS as Total_Mbi_Dos
	,case when final_LOPA_IND=1 and   mbr_DOS_latest_submission=1  then MBI_DOS end as still_LOPA_MBI_DOS
	,case when final_LOPA_IND<>1  OR  mbr_DOS_latest_submission<>1  then MBI_DOS end as Overturn_LOPA_MBI_DOS
	,*
	from tmp_1y.PA_TRCKNG_pr_EVNT_LOPA_DTL
;	
--3714719  3501938 3263702     select count(*) from TMP_1y.kn_LOPA_pr_1

drop table TMP_1m.kn_LOPA_pr;
create table TMP_1m.kn_LOPA_pr as
select 
	case when still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null then mbi_dos else null end as ever_LOPA
	,*
from TMP_1m.kn_LOPA_pr_1
;

--3714719  3501938 3263702	select count(*) from TMP_1y.kn_LOPA_pr

		
/*______________[PR CLAIM PULL]___________________________________________________________________________________*/;	
DROP TABLE TMP_1m.kn_MBM_EPISODE_1; 
 CREATE TABLE TMP_1m.kn_MBM_EPISODE_1 AS 
 SELECT			
			a.GAL_MBI_HICN_FNL AS MBI		
			,a.COMPONENT		
			,a.EVENTKEY AS ID		
			,a.SERVICE_CODE  		
			,a.FST_SRVC_DT AS START_DT		
			,a.FST_SRVC_MONTH AS SERV_MONTH		
			,a.FST_SRVC_QTR HCE_QTR		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as HCTAPaidMonth		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM as PROV_FULL_NM
			,case when b.ever_lopa is not null then 1 else 0 end as LOPA_FLG
			,case when b.still_LOPA_MBI_DOS is not null then 1 else 0 end as still_LOPA
			,case when b.Overturn_LOPA_MBI_DOS is not null then 1 else 0 end as overturn_LOPA
			, 0 APC_PBL_FLG
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY		
			,sum(a.ALLW_AMT_FNL) as ALLOWED		
			,sum(a.NET_PD_AMT_FNL) as PAID		
			,sum(0) as TADM_UTIL		
			,COUNT(DISTINCT a.EVENTKEY) as Visits                     		
			,sum(a.ADJ_SRVC_UNIT_CNT) AS ADJ_SRVC_UNITS		
FROM   FICHSRV.COSMOS_PR a
LEFT JOIN  TMP_1m.kn_LOPA_pr b		   --select * from TMP_1y.kn_LOPA_pr				 
		on concat(a.gal_mbi_hicn_fnl, "_", a.fst_srvc_dt) = b.Total_Mbi_Dos 
        and a.proc_cd = b.proc_cd
        and a.prov_tin = b.prov_tin
WHERE 	a.TFM_INCLUDE_FLAG = 1 			
		AND a.GLOBAL_CAP IN ('NA')			
		AND a.PRODUCT_LEVEL_3_FNL NOT IN ('INSTITUTIONAL', 'DUAL' )		--remove Duals from the data for now; Duals will go live 4/1/2025???	
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND (SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3 or SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> '3') -- REMOVE THIS TO INCLUDE HH CLAIMS 
		AND (a.ama_pl_of_srvc_cd <> 12 or a.ama_pl_of_srvc_cd <> '12')     
		AND (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	OR 
	 	a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
GROUP BY			
			a.GAL_MBI_HICN_FNL		
			,a.COMPONENT		
			,a.EVENTKEY		
			,a.SERVICE_CODE		
			,a.FST_SRVC_DT  		
			,a.FST_SRVC_MONTH  		
			,a.FST_SRVC_QTR  		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM 
			,case when b.ever_lopa is not null then 1 else 0 end
			,case when b.still_LOPA_MBI_DOS is not null then 1 else 0 end
			,case when b.Overturn_LOPA_MBI_DOS is not null then 1 else 0 end
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') )) 		
		ORDER BY a.GAL_MBI_HICN_FNL ASC;
--64354173  62711063  60990327       select count(*) from TMP_1y.kn_MBM_EPISODE_1 	

/*	
--Pulling pr claims for 2018, 2019, 2020  --only need to run once. Ran on 10/8/2024
drop table 	TMP_1y.kn_MBM_EPISODE_1_2018_2020;
CREATE TABLE TMP_1y.kn_MBM_EPISODE_1_2018_2020 AS 
 SELECT			
			a.GAL_MBI_HICN_FNL AS MBI		
			,a.COMPONENT		
			,a.EVENTKEY AS ID		
			,a.SERVICE_CODE  		
			,a.FST_SRVC_DT AS START_DT		
			,a.FST_SRVC_MONTH AS SERV_MONTH		
			,a.FST_SRVC_QTR HCE_QTR		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as HCTAPaidMonth		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM as PROV_FULL_NM
			,0 as LOPA_FLG
			,0 as still_LOPA
			,0 as overturn_LOPA
			,0 APC_PBL_FLG
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY		
			,sum(a.ALLW_AMT_FNL) as ALLOWED		
			,sum(a.NET_PD_AMT_FNL) as PAID		
			,sum(0) as TADM_UTIL		
			,COUNT(DISTINCT a.EVENTKEY) as Visits                     		
			,sum(a.ADJ_SRVC_UNIT_CNT) AS ADJ_SRVC_UNITS		
FROM   tadm_tre_cpy.glxy_pr_f_2018 a
WHERE 	a.TFM_INCLUDE_FLAG = 1 			
		AND a.GLOBAL_CAP IN ('NA')			
		AND a.PRODUCT_LEVEL_3_FNL NOT IN ('INSTITUTIONAL', 'DUAL' )		
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND (SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3 or SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> '3') -- REMOVE THIS TO INCLUDE HH CLAIMS 
		AND (a.ama_pl_of_srvc_cd <> 12 or a.ama_pl_of_srvc_cd <> '12')     
		AND (a.proc_cd in
				('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	     '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
	    		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 		 '98940', '98941', '98942') 
     	     OR 
	 		a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
GROUP BY			
			a.GAL_MBI_HICN_FNL		
			,a.COMPONENT		
			,a.EVENTKEY		
			,a.SERVICE_CODE		
			,a.FST_SRVC_DT  		
			,a.FST_SRVC_MONTH  		
			,a.FST_SRVC_QTR  		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM 
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') )) 		
		--ORDER BY a.GAL_MBI_HICN_FNL ASC
union all
SELECT			
			a.GAL_MBI_HICN_FNL AS MBI		
			,a.COMPONENT		
			,a.EVENTKEY AS ID		
			,a.SERVICE_CODE  		
			,a.FST_SRVC_DT AS START_DT		
			,a.FST_SRVC_MONTH AS SERV_MONTH		
			,a.FST_SRVC_QTR HCE_QTR		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as HCTAPaidMonth		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM as PROV_FULL_NM
			,0 as LOPA_FLG
			,0 as still_LOPA
			,0 as overturn_LOPA
			, 0 APC_PBL_FLG
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY		
			,sum(a.ALLW_AMT_FNL) as ALLOWED		
			,sum(a.NET_PD_AMT_FNL) as PAID		
			,sum(0) as TADM_UTIL		
			,COUNT(DISTINCT a.EVENTKEY) as Visits                     		
			,sum(a.ADJ_SRVC_UNIT_CNT) AS ADJ_SRVC_UNITS		
FROM   tadm_tre_cpy.glxy_pr_f_2019 a
WHERE 	a.TFM_INCLUDE_FLAG = 1 			
		AND a.GLOBAL_CAP IN ('NA')			
		AND a.PRODUCT_LEVEL_3_FNL NOT IN ('INSTITUTIONAL', 'DUAL' )		
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND (SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3 or SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> '3') -- REMOVE THIS TO INCLUDE HH CLAIMS 
		AND (a.ama_pl_of_srvc_cd <> 12 or a.ama_pl_of_srvc_cd <> '12')     
		AND (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	OR 
	 	a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
GROUP BY			
			a.GAL_MBI_HICN_FNL		
			,a.COMPONENT		
			,a.EVENTKEY		
			,a.SERVICE_CODE		
			,a.FST_SRVC_DT  		
			,a.FST_SRVC_MONTH  		
			,a.FST_SRVC_QTR  		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM 
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') )) 		
		--ORDER BY a.GAL_MBI_HICN_FNL ASC
union all
SELECT			
			a.GAL_MBI_HICN_FNL AS MBI		
			,a.COMPONENT		
			,a.EVENTKEY AS ID		
			,a.SERVICE_CODE  		
			,a.FST_SRVC_DT AS START_DT		
			,a.FST_SRVC_MONTH AS SERV_MONTH		
			,a.FST_SRVC_QTR HCE_QTR		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as HCTAPaidMonth		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM as PROV_FULL_NM
			,0 as LOPA_FLG
			,0 as still_LOPA
			,0 as overturn_LOPA
			, 0 APC_PBL_FLG
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY		
			,sum(a.ALLW_AMT_FNL) as ALLOWED		
			,sum(a.NET_PD_AMT_FNL) as PAID		
			,sum(0) as TADM_UTIL		
			,COUNT(DISTINCT a.EVENTKEY) as Visits                     		
			,sum(a.ADJ_SRVC_UNIT_CNT) AS ADJ_SRVC_UNITS		
FROM   tadm_tre_cpy.glxy_pr_f_2020 a
WHERE 	a.TFM_INCLUDE_FLAG = 1 			
		AND a.GLOBAL_CAP IN ('NA')			
		AND a.PRODUCT_LEVEL_3_FNL NOT IN ('INSTITUTIONAL', 'DUAL')	
		AND a.plan_level_2_fnl NOT IN ('PFFS')			
		AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
		AND a.ST_ABBR_CD = a.MARKET_FNL			
		AND a.prov_prtcp_sts_cd = 'P'			
		AND (SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3 or SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> '3') -- REMOVE THIS TO INCLUDE HH CLAIMS 
		AND (a.ama_pl_of_srvc_cd <> 12 or a.ama_pl_of_srvc_cd <> '12')     
		AND (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	OR 
	 	a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') --HH, workers comp, etc.
GROUP BY			
			a.GAL_MBI_HICN_FNL		
			,a.COMPONENT		
			,a.EVENTKEY		
			,a.SERVICE_CODE		
			,a.FST_SRVC_DT  		
			,a.FST_SRVC_MONTH  		
			,a.FST_SRVC_QTR  		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM 
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') )) 		
		--ORDER BY a.GAL_MBI_HICN_FNL ASC
;
--21694518 22395739    select count(*) from TMP_1y.kn_MBM_EPISODE_1_2018_2020
*/	
	
/*______________[OP CLAIM PULL APC ]___(APC = Ambulatory Payment Code)________________________________________________________________________________*/;	
DROP TABLE TMP_1y.kn_MBM_CLAIMS; 
CREATE TABLE TMP_1y.kn_MBM_CLAIMS AS 
select *
	,max(if(instr(clm_rev_rsn_1_10,'00473-') > 0 , 1,0)) over (partition by SITE_CD,CLM_AUD_NBR,SBSCR_NBR) CLM_APC_FLG
	,sum(ALLW_AMT_FNL) over (partition by SITE_CD,CLM_AUD_NBR,SBSCR_NBR) CLM_ALLW_AMNT
from 
	(select *	
	    	,concat(a.clm_rev_rsn_1_cd ,'-',a.clm_rev_rsn_2_cd,'-',a.clm_rev_rsn_3_cd,'-',a.clm_rev_rsn_4_cd,'-',a.clm_rev_rsn_5_cd,'-',a.clm_rev_rsn_6_cd,'-',a.clm_rev_rsn_7_cd,'-',a.clm_rev_rsn_8_cd,'-',a.clm_rev_rsn_9_cd,'-',a.clm_rev_rsn_10_cd, '-') clm_rev_rsn_1_10																																													
     from FICHSRV.COSMOS_OP a
	 where (a.proc_cd in 			
			  ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	   '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
	   		   '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
	 	       '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 	   '98940', '98941', '98942')
		      OR RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		    AND PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	)e ;
--77536654  75713104  73889849     72274650           select count(*) from TMP_1y.kn_MBM_CLAIMS


DROP TABLE TMP_1y.kn_MBM_EPISODE_1b; 
 CREATE TABLE TMP_1y.kn_MBM_EPISODE_1b AS 
		SELECT			
			a.GAL_MBI_HICN_FNL AS MBI		
			,a.COMPONENT		
			,a.EVENTKEY AS ID		
			,a.HCE_SERVICE_CODE SERVICE_CODE  		
			,a.FST_SRVC_DT AS START_DT		
			,a.FST_SRVC_MONTH AS SERV_MONTH		
			,a.FST_SRVC_QTR HCE_QTR		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as HCTAPaidMonth		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM as PROV_FULL_NM
			,case when b.ever_lopa is not null then 1 else 0 end as LOPA_FLG
			,case when b.still_LOPA_MBI_DOS is not null then 1 else 0 end as still_LOPA
			,case when b.Overturn_LOPA_MBI_DOS is not null then 1 else 0 end as overturn_LOPA
			,case when a.CLM_APC_FLG = 1 and  c.rsn_cd in ('208','176','943') then 1 else 0 end as APC_PBL_FLG  	
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY		
			,sum(a.ALLW_AMT_FNL) as ALLOWED		
			,sum(a.NET_PD_AMT_FNL) as PAID		
			,sum(0) as TADM_UTIL		
			,COUNT(DISTINCT a.EVENTKEY) as Visits                     		
			,sum(a.ADJ_SRVC_UNIT_CNT) AS ADJ_SRVC_UNITS		
		FROM  TMP_1y.kn_MBM_CLAIMS a
		left join  TMP_1y.kn_LOPA_op b
		on  concat(a.gal_mbi_hicn_fnl, "_", a.fst_srvc_dt) = b.Total_Mbi_Dos 
            and a.proc_cd = b.proc_cd
            --and a.service_code = b.service_code
            and a.prov_tin = b.prov_tin
        left join fichsrv.TADM_GLXY_REASON_CODE c
        on a.FNL_RSN_CD_SYS_ID = c.RSN_CD_SYS_ID
		WHERE a.TFM_INCLUDE_FLAG = 1 			
					AND a.GLOBAL_CAP IN ('NA')			
					AND a.PRODUCT_LEVEL_3_FNL NOT IN ('INSTITUTIONAL', 'DUAL')			
					AND a.plan_level_2_fnl NOT IN ('PFFS')			
					AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
					--AND ST_ABBR_CD IN ('AR', 'GA', 'NJ', 'SC','CT','NC','PA','NY','AL')			
					AND a.ST_ABBR_CD = a.MARKET_FNL			
					AND a.prov_prtcp_sts_cd = 'P'			
					AND SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3			
					AND a.ama_pl_of_srvc_cd <> 12 			
					AND (a.proc_cd in 			
			  				('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 					 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 					 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 					 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 					 '98940', '98941', '98942') 
		      				OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		      		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')			
		GROUP BY			
			a.GAL_MBI_HICN_FNL		
			,a.COMPONENT		
			,a.EVENTKEY		
			,a.HCE_SERVICE_CODE		
			,a.FST_SRVC_DT  		
			,a.FST_SRVC_MONTH  		
			,a.FST_SRVC_QTR  		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM 
			,case when b.ever_lopa is not null then 1 else 0 end
			,case when b.still_LOPA_MBI_DOS is not null then 1 else 0 end
			,case when b.Overturn_LOPA_MBI_DOS is not null then 1 else 0 end
			,case when a.CLM_APC_FLG = 1 and  c.rsn_cd in ('208','176','943') then 1 else 0 end
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))   		
		ORDER BY a.GAL_MBI_HICN_FNL ASC
; 
--38657890  37706450  36737912  35885437      select count(*) from TMP_1y.kn_MBM_EPISODE_1b

/*
--pulling OP claims for 2018, 2019, 2020 - Only need to run once, ran on 10/8/2024
drop table TMP_1y.kn_MBM_CLAIMS_2018_2020;
CREATE TABLE TMP_1y.kn_MBM_CLAIMS_2018_2020 AS 
select *
	,max(if(instr(clm_rev_rsn_1_10,'00473-') > 0 , 1,0)) over (partition by SITE_CD,CLM_AUD_NBR,SBSCR_NBR) CLM_APC_FLG
	,sum(ALLW_AMT_FNL) over (partition by SITE_CD,CLM_AUD_NBR,SBSCR_NBR) CLM_ALLW_AMNT
from 
	(select GAL_MBI_HICN_FNL ,COMPONENT,EVENTKEY,HCE_SERVICE_CODE SERVICE_CODE,FST_SRVC_DT,FST_SRVC_MONTH ,FST_SRVC_QTR,adjd_dt,MARKET_FNL,GROUP_IND_FNL,PROC_CD,RVNU_CD,PRIMARY_DIAG_CD,AHRQ_DIAG_GENL_CATGY_DESC,AHRQ_DIAG_DTL_CATGY_DESC,PROV_PRTCP_STS_CD,PROV_TIN,FULL_NM,ama_pl_of_srvc_cd ,ALLW_AMT_FNL,NET_PD_AMT_FNL,ADJ_SRVC_UNIT_CNT,TFM_INCLUDE_FLAG ,GLOBAL_CAP,PRODUCT_LEVEL_3_FNL ,plan_level_2_fnl,SPECIAL_NETWORK,ST_ABBR_CD,bil_typ_cd,SITE_CD,CLM_AUD_NBR,SBSCR_NBR,FNL_RSN_CD_SYS_ID
	    	,concat(a.clm_rev_rsn_1_cd ,'-',a.clm_rev_rsn_2_cd,'-',a.clm_rev_rsn_3_cd,'-',a.clm_rev_rsn_4_cd,'-',a.clm_rev_rsn_5_cd,'-',a.clm_rev_rsn_6_cd,'-',a.clm_rev_rsn_7_cd,'-',a.clm_rev_rsn_8_cd,'-',a.clm_rev_rsn_9_cd,'-',a.clm_rev_rsn_10_cd, '-') clm_rev_rsn_1_10																																													
     from tadm_tre_cpy.glxy_op_f_2018 a
	 where (a.proc_cd in 			
			  ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	   '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 	   '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		  	   '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 	   '98940', '98941', '98942') 
		    OR RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		   AND PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
union ALL 
select GAL_MBI_HICN_FNL ,COMPONENT,EVENTKEY,HCE_SERVICE_CODE SERVICE_CODE,FST_SRVC_DT,FST_SRVC_MONTH ,FST_SRVC_QTR,adjd_dt,MARKET_FNL,GROUP_IND_FNL,PROC_CD,RVNU_CD,PRIMARY_DIAG_CD,AHRQ_DIAG_GENL_CATGY_DESC,AHRQ_DIAG_DTL_CATGY_DESC,PROV_PRTCP_STS_CD,PROV_TIN,FULL_NM,ama_pl_of_srvc_cd ,ALLW_AMT_FNL,NET_PD_AMT_FNL,ADJ_SRVC_UNIT_CNT,TFM_INCLUDE_FLAG ,GLOBAL_CAP,PRODUCT_LEVEL_3_FNL ,plan_level_2_fnl,SPECIAL_NETWORK,ST_ABBR_CD,bil_typ_cd,SITE_CD,CLM_AUD_NBR,SBSCR_NBR,FNL_RSN_CD_SYS_ID
	    	,concat(a.clm_rev_rsn_1_cd ,'-',a.clm_rev_rsn_2_cd,'-',a.clm_rev_rsn_3_cd,'-',a.clm_rev_rsn_4_cd,'-',a.clm_rev_rsn_5_cd,'-',a.clm_rev_rsn_6_cd,'-',a.clm_rev_rsn_7_cd,'-',a.clm_rev_rsn_8_cd,'-',a.clm_rev_rsn_9_cd,'-',a.clm_rev_rsn_10_cd, '-') clm_rev_rsn_1_10																																													
from tadm_tre_cpy.glxy_op_f_2019 a
where (a.proc_cd in 			
			  ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	   '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 	   '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		  	   '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 	   '98940', '98941', '98942') 
		      OR RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	   AND PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
union ALL 
select GAL_MBI_HICN_FNL ,COMPONENT,EVENTKEY,HCE_SERVICE_CODE SERVICE_CODE,FST_SRVC_DT,FST_SRVC_MONTH ,FST_SRVC_QTR,adjd_dt,MARKET_FNL,GROUP_IND_FNL,PROC_CD,RVNU_CD,PRIMARY_DIAG_CD,AHRQ_DIAG_GENL_CATGY_DESC,AHRQ_DIAG_DTL_CATGY_DESC,PROV_PRTCP_STS_CD,PROV_TIN,FULL_NM,ama_pl_of_srvc_cd ,ALLW_AMT_FNL,NET_PD_AMT_FNL,ADJ_SRVC_UNIT_CNT,TFM_INCLUDE_FLAG ,GLOBAL_CAP,PRODUCT_LEVEL_3_FNL ,plan_level_2_fnl,SPECIAL_NETWORK,ST_ABBR_CD,bil_typ_cd,SITE_CD,CLM_AUD_NBR,SBSCR_NBR,FNL_RSN_CD_SYS_ID
		    	,concat(a.clm_rev_rsn_1_cd ,'-',a.clm_rev_rsn_2_cd,'-',a.clm_rev_rsn_3_cd,'-',a.clm_rev_rsn_4_cd,'-',a.clm_rev_rsn_5_cd,'-',a.clm_rev_rsn_6_cd,'-',a.clm_rev_rsn_7_cd,'-',a.clm_rev_rsn_8_cd,'-',a.clm_rev_rsn_9_cd,'-',a.clm_rev_rsn_10_cd, '-') clm_rev_rsn_1_10																																													
from tadm_tre_cpy.glxy_op_f_2020 a
where (a.proc_cd in 			
			  ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 	   '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 	   '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		  	   '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 	   '98940', '98941', '98942')
		     OR RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	   AND PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	)e ;
--38195692   38203014             select count(*) from TMP_1y.kn_MBM_CLAIMS_2018_2020


DROP TABLE TMP_1y.kn_MBM_EPISODE_1b_2018_2020; 
 CREATE TABLE TMP_1y.kn_MBM_EPISODE_1b_2018_2020 AS 
		SELECT			
			a.GAL_MBI_HICN_FNL AS MBI		
			,a.COMPONENT		
			,a.EVENTKEY AS ID		
			,a.SERVICE_CODE  		
			,a.FST_SRVC_DT AS START_DT		
			,a.FST_SRVC_MONTH AS SERV_MONTH		
			,a.FST_SRVC_QTR HCE_QTR		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as HCTAPaidMonth		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM as PROV_FULL_NM
			,0 as LOPA_FLG
			,0 as still_LOPA
			,0 as overturn_LOPA
			,case when a.CLM_APC_FLG = 1 and  c.rsn_cd in ('208','176','943') then 1 else 0 end as APC_PBL_FLG  	
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY		
			,sum(a.ALLW_AMT_FNL) as ALLOWED		
			,sum(a.NET_PD_AMT_FNL) as PAID		
			,sum(0) as TADM_UTIL		
			,COUNT(DISTINCT a.EVENTKEY) as Visits                     		
			,sum(a.ADJ_SRVC_UNIT_CNT) AS ADJ_SRVC_UNITS		
		FROM  TMP_1y.kn_MBM_CLAIMS_2018_2020 a
		left join fichsrv.TADM_GLXY_REASON_CODE c
        on a.FNL_RSN_CD_SYS_ID = c.RSN_CD_SYS_ID
		WHERE a.TFM_INCLUDE_FLAG = 1 			
					AND a.GLOBAL_CAP IN ('NA')			
					AND a.PRODUCT_LEVEL_3_FNL NOT IN ('INSTITUTIONAL', 'DUAL')			
					AND a.plan_level_2_fnl NOT IN ('PFFS')			
					AND a.SPECIAL_NETWORK NOT IN ('ERICKSON')			
					--AND ST_ABBR_CD IN ('AR', 'GA', 'NJ', 'SC','CT','NC','PA','NY','AL')			
					AND a.ST_ABBR_CD = a.MARKET_FNL			
					AND a.prov_prtcp_sts_cd = 'P'			
					AND SUBSTRING(coalesce(a.bil_typ_cd,'0'),0,1) <> 3			
					AND a.ama_pl_of_srvc_cd <> 12 			
					AND (a.proc_cd in 			
			  				('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 					 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 					 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 					 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 					 '98940', '98941', '98942') 
		      				OR a.RVNU_CD IN ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
		      		AND a.PROC_CD NOT IN ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')			
		GROUP BY			
			a.GAL_MBI_HICN_FNL		
			,a.COMPONENT		
			,a.EVENTKEY		
			,a.SERVICE_CODE		
			,a.FST_SRVC_DT  		
			,a.FST_SRVC_MONTH  		
			,a.FST_SRVC_QTR  		
			,to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))		
			,a.MARKET_FNL		
			,a.GROUP_IND_FNL		
			,a.PROC_CD		
			,a.RVNU_CD		
			,a.PRIMARY_DIAG_CD		
			,a.AHRQ_DIAG_GENL_CATGY_DESC		
			,a.AHRQ_DIAG_DTL_CATGY_DESC		
			,a.PROV_PRTCP_STS_CD		
			,a.PROV_TIN		
			,a.FULL_NM 
			,case when a.CLM_APC_FLG = 1 and  c.rsn_cd in ('208','176','943') then 1 else 0 end
			,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))   		
		ORDER BY a.GAL_MBI_HICN_FNL ASC
; 
--36737912  32319558    select count(*) from TMP_1y.kn_MBM_EPISODE_1b
*/
	
--union PR and OP claims	
/*
TMP_1y.kn_MBM_EPISODE_1	  (PR 2021 and later)	
TMP_1y.kn_MBM_EPISODE_1b  (OP 2021 and later)
TMP_1y.kn_MBM_EPISODE_1_2018_2020 (PR 2018-2020)
TMP_1y.kn_MBM_EPISODE_1b_2018_2020  (OP 2018-2020)
*/

DROP TABLE TMP_1y.kn_MBM_EPISODE_1c;
create table TMP_1y.kn_MBM_EPISODE_1c as
select * from TMP_1y.kn_MBM_EPISODE_1
union all
select * from TMP_1y.kn_MBM_EPISODE_1_2018_2020
union all
select * from TMP_1y.kn_MBM_EPISODE_1b
union all
select * from TMP_1y.kn_MBM_EPISODE_1b_2018_2020
ORDER BY MBI ASC
;
--141253070  138658520  135969246		select count(*) from TMP_1y.kn_MBM_EPISODE_1c

select * from TMP_1y.kn_MBM_EPISODE_1c limit 2;
select serv_month, sum(allowed) as allowedamt 
from TMP_1y.kn_MBM_EPISODE_1c
where serv_month = '202407'
group by serv_month
--$  81,004,260.74




/*
INSERT INTO TMP_1y.kn_MBM_EPISODE_1			
SELECT * FROM TMP_1y.kn_MBM_EPISODE_1b
;
*/


--select * from TMP_1y.kn_MBM_EPISODE_1 where mbi in ( '7FR1DH1AJ99', '7FR3UP9RH60')
		
/*______________[Add Optum Provider tags & Category definitions ]___________________________________________________________________________________*/;	
DROP TABLE TMP_1y.kn_MBM_EPISODE_2; 
create table TMP_1y.kn_MBM_EPISODE_2 as 			
		select			
		*			
		,IF(DNL_ALLOWED > 0.01, 'Paid', IF(still_LOPA = 1, 'LOPA', IF(APC_PBL_FLG = 1, 'APC-Paid', 'Other Denied' )))CLAIM_STATUS   --CAROLINE updated LOPA flag to use Still_LOPA
		,IF(TIN_NUM IS NULL,0,1) OPTUM_FLG 			
		,case when PROC_CD in ('98940','98941','98942')  			
		     then 'Chiro'			
				when PROC_CD in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026'	
		      ,'97028','97032','97033','97034','97035','97036','97039','97110','97112','97113'			
		      ,'97116','97124','97139','97140','97150','97161','97162','97163','97164','97165'			
		      ,'97166','97167','97168','97530','97532','97533','97535','97537','97542','97545'			
		      ,'97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152'			
		      ,'G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131')			
				     then  'PT-OT'	
				when PROC_CD in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626'	
		        ,'92627','92630','92633','96105','S9128') 			
				     then  'ST'	
		      else 'Other' end 			
				as MBMserv_dtl	
		,case when (market_fnl in ('AR', 'GA', 'NJ', 'SC') and group_ind_fnl = 'I') then   --caroline edited to only keep Individual in Pilot, Groups move to National
					case when  CATEGORY = 'OP_REHAB' then 'Phase-II'
					else
					case when TIN_NUM is NULL then'Phase-II' 
					else 'Phase-I'	end 
					end 
					else 'National'
		end as MBM_DEPLOY_DT	
		from (SELECT a.*, tin_num  			
		      FROM  (SELECT *
					        ,SUM(ALLOWED) OVER (PARTITION BY ID,START_DT,CATEGORY) DNL_ALLOWED
		                    ,MAX(LOPA_FLG) OVER (PARTITION BY ID,START_DT,CATEGORY ) MAX_LOPA_FLG
		             FROM TMP_1y.kn_MBM_EPISODE_1c) a 			
		             left join TMP_1Y.P8001_OPTUM_TIN_2 b 			
		             on prov_tin = tin_num 			
		             and i = 1 ) b 	
	;
--141253070  138658520  135969246		select count(*) from TMP_1y.kn_MBM_EPISODE_2

select serv_month, sum(allowed) as allowedamt 
from TMP_1y.kn_MBM_EPISODE_2
where serv_month = '202406'  --73432103.42  73635236.05  73,514,079.66 (shorter proc_cd list)  76,561,115.88 (original proc_cd list) 
group by serv_month

/*
select   
	mbi , start_dt , proc_cd , count(*) as cnt
from TMP_1y.kn_MBM_EPISODE_2
group by mbi, start_dt, proc_cd 
having count(*) >1

select * 
from TMP_1y.kn_MBM_EPISODE_2
where mbi = '1A09DN3RY47'
      and start_dt = '2022-03-29'
      and proc_cd = '97022'
   
select * 
from FICHSRV.COSMOS_OP
where GAL_MBI_HICN_FNL = '1A09DN3RY47'
      and fst_srvc_dt = '2022-03-29'
      and proc_cd = '97022'
      
select * 
from tmp_1y.PA_TRCKNG_OP_CLM_LOPA
where 

GAL_MBI_HICN_FNL = '1A09DN3RY47'
      and fst_srvc_dt = '2022-03-29'
      and proc_cd = '97022'
      
      
select distinct service_code from tmp_1y.PA_TRCKNG_OP_CLM_LOPA



--select * from TMP_1Y.P8001_OPTUM_TIN_2
select * from TMP_1y.kn_MBM_EPISODE_2 limit 2;
*/


/*
_____________caroline: LOPA Count_________________________
select * from TMP_1y.kn_MBM_EPISODE_2 limit 10;

--sum on DOS Month
drop table tmp_1m.kn_MBM_EPISODE_2_lopa
create table tmp_1m.kn_MBM_EPISODE_2_lopa as
select
component
--,start_dt
,serv_month
,case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as pilot_national
,market_fnl
,group_ind_fnl
,category
,claim_status
,sum(visits) as visit_cnt
,sum(dnl_allowed) as allowed_amt
from TMP_1y.kn_MBM_EPISODE_2
where serv_month > '202109'
group by
component
--,start_dt
,serv_month
,case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end
,market_fnl
,group_ind_fnl
,category
,claim_status

select count(*) from tmp_1m.kn_MBM_EPISODE_2_lopa --39421
select * from tmp_1m.kn_MBM_EPISODE_2_lopa

--sum on DOS Daily for 9/1/2024 and after
drop table tmp_1m.kn_MBM_EPISODE_2_lopa_daily
create table tmp_1m.kn_MBM_EPISODE_2_lopa_daily as
select
component
,start_dt
,serv_month
,case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as pilot_national
,market_fnl
,group_ind_fnl
,category
,claim_status
,sum(visits) as visit_cnt
,sum(dnl_allowed) as allowed_amt
from TMP_1y.kn_MBM_EPISODE_2
where start_dt > '2024-08-31'
group by
component
,start_dt
,serv_month
,case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end
,market_fnl
,group_ind_fnl
,category
,claim_status

select count(*) from tmp_1m.kn_MBM_EPISODE_2_lopa_daily --30032
select * from tmp_1m.kn_MBM_EPISODE_2_lopa_daily

/*______________[Final Agggregation]___________________________________________________________________________________*/
DROP TABLE TMP_1y.kn_MBM_EPISODE_3;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_3 AS SELECT			
			concat(MBI,'-',CATEGORY) MBI		
			,COMPONENT		
			,ID		
			--,SERVICE_CODE		
			,START_DT		
			,SERV_MONTH		
			,HCE_QTR		
			,min(HCTAPaidMonth) as HCTAPaidMonth		
			,MBM_DEPLOY_DT		
			,MARKET_FNL	
			,CLAIM_STATUS
			,CAST(MBMserv_dtl as varchar (10)) AS MBMserv 		
		--	,case when HHserv_dtl in ('RN' ,'PT' ,'OT' ,'Aide','LPN' ,'ST' ,'SW' ) then HHserv_dtl 		
		--		else "Ancillary" end as HHserv	
			--,PROV_PRTCP_STS_CD			
			,CATEGORY	
			,sum(ALLOWED) as ALLOWED		
			,sum(PAID) as PAID		
			,sum(TADM_UTIL) as TADM_UTIL
			,count(distinct concat(id,START_DT)) as Visits 
			,count(Visits) as Vsts                     		
			,sum(ADJ_SRVC_UNITS) AS ADJ_SRVC_UNITS		
		FROM TMP_1y.kn_MBM_EPISODE_2
		WHERE PROV_PRTCP_STS_CD='P'	
		--AND GROUP_IND_FNL = 'I'
			--AND MBMserv_dtl in ('RN' ,'PT' ,'OT' ,'Aide','LPN' ,'ST' ,'SW' ) --Removed Ancillary service codes		
		GROUP BY			
		    concat(MBI,'-',CATEGORY)
			,COMPONENT		
			,ID		
			--,SERVICE_CODE		
			,START_DT		
			,SERV_MONTH		
			,HCE_QTR		
		--	,HCTAPaidMonth		
			,MBM_DEPLOY_DT		
			,MARKET_FNL		
			,CLAIM_STATUS		
			,MBMserv_dtl		
		--	,case when HHserv_dtl in ('RN' ,'PT' ,'OT' ,'Aide','LPN' ,'ST' ,'SW' ) then HHserv_dtl 		
		--		else "Ancillary" end	
			--,PROV_PRTCP_STS_CD		
			,OPTUM_FLG		
			,CATEGORY	
		ORDER BY MBI,MBMserv,START_DT,ID
;
	
--66929218  65797555     64650521  select count(*) from TMP_1y.kn_MBM_EPISODE_3  -- where mbi like '7GT2FY4RA93%'
	
/*______________[Rank-Visits]___________________________________________________________________________________*/
DROP TABLE TMP_1y.kn_MBM_EPISODE_4;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_4 AS  			
SELECT 
	MBI
	,COMPONENT
	,ID
	--,SERVICE_CODE
	,START_DT
	,ROW_NUMBER() OVER (PARTITION BY MBI,MBM_DEPLOY_DT ORDER BY START_DT ) I 
	,SERV_MONTH
	,HCE_QTR
	,HCTAPAIDMONTH
	,MBM_DEPLOY_DT
	,MARKET_FNL
	,CLAIM_STATUS
	,MBMSERV
	--,PROV_PRTCP_STS_CD
	,CATEGORY
	,ALLOWED
	,PAID
	,TADM_UTIL
	,VISITS
	,VSTS
	,ADJ_SRVC_UNITS
FROM  TMP_1y.kn_MBM_EPISODE_3 A
;
--66929218  65797555   64650521    select count(*) from TMP_1y.kn_MBM_EPISODE_4   -- where mbi like '7GT2FY4RA93%'


/*______________[Visits Episode Lag]___________________________________________________________________________________*/
DROP TABLE TMP_1y.kn_MBM_EPISODE_lag;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_lag  AS  
SELECT A.MBI
	,A.COMPONENT
	,A.ID
	,A.START_DT
	,B.START_DT PREV_START_DT
	,datediff(A.START_DT,B.START_DT) Visit_dy_lag
	,if(datediff(A.START_DT,B.START_DT) > 30,1,0) Ep_flag
	,A.I 
	,B.I PREV_I
	,A.SERV_MONTH
	,A.HCE_QTR
	,A.HCTAPAIDMONTH
	,A.MBM_DEPLOY_DT
	,A.MARKET_FNL
	,A.CLAIM_STATUS
	,A.MBMSERV
	,A.CATEGORY
	,A.ALLOWED
	,A.PAID
	,A.TADM_UTIL
	,A.VISITS
	,A.VSTS
	,A.ADJ_SRVC_UNITS
FROM  TMP_1y.kn_MBM_EPISODE_4  A
LEFT JOIN TMP_1y.kn_MBM_EPISODE_4 B 
ON 	A.MBI = B.MBI 
	AND A.MBM_DEPLOY_DT = B.MBM_DEPLOY_DT
	AND A.I = B.I+1 
;
 
--66929218  65797555  64650521   select count(*) from TMP_1y.kn_MBM_EPISODE_lag	     --  where mbi like '7GT2FY4RA93%'
 

/*______________[ Episode START DATES  ]___________________________________________________________________________________*/;
DROP TABLE TMP_1y.kn_MBM_EPISODE_vst_ep_2;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_vst_ep_2  AS  
SELECT A.MBI
	,A.COMPONENT
	,A.ID
	,A.START_DT
	,A.PREV_START_DT
	,Visit_dy_lag
	,Ep_flag
	,MIN(START_DT) OVER (PARTITION BY MBI,CMLTV_EPISODES ) EP_START_DT
	,CMLTV_EPISODES
	,A.I 
	,A.PREV_I
	,A.SERV_MONTH
	,A.HCE_QTR
	,A.HCTAPAIDMONTH
	,MIN(HCTAPAIDMONTH) OVER (PARTITION BY MBI,CMLTV_EPISODES ) EP_HCTAPAIDMONTH
	,A.MBM_DEPLOY_DT
	,A.MARKET_FNL
	,A.CLAIM_STATUS
	,A.MBMSERV
	,A.CATEGORY
	,A.ALLOWED
	,A.PAID
	,A.TADM_UTIL
	,A.VISITS
	,A.VSTS
	,A.ADJ_SRVC_UNITS
FROM (SELECT *  
 			,SUM(IF(PREV_START_DT IS NULL,1,Ep_flag)) OVER (PARTITION BY  MBI ORDER BY START_DT  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) CMLTV_EPISODES 
	  FROM TMP_1y.kn_MBM_EPISODE_LAG ) A
 ;

--66929218  65797555  64650521   select count(*) from TMP_1y.kn_MBM_EPISODE_vst_ep_2      -- where mbi like '7GT2FY4RA93%'

select * from TMP_1y.kn_MBM_EPISODE_vst_ep_2 limit 2;

/*______________[ Episode START DATES  ]___________________________________________________________________________________*/;
DROP TABLE TMP_1y.kn_MBM_EPISODE_SMRY;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_SMRY AS  
select  
	A.SERV_MONTH VISIT_MONTH
	,DATE_FORMAT(EP_START_DT,'yyyyMM') EP_START_MO 
	,A.HCTAPAIDMONTH
	,A.MBM_DEPLOY_DT
	,A.MARKET_FNL
	,A.CLAIM_STATUS
	,A.MBMSERV
	,A.CATEGORY
	,COUNT(DISTINCT MBI) MBR_COUNT
	,SUM(A.ALLOWED) ALLW 
	,SUM(A.PAID) PD 
	,SUM(A.VISITS) VISITS
	,SUM(Ep_flag) EPISODES
from TMP_1y.kn_MBM_EPISODE_vst_ep_2 a
GROUP BY 
	A.SERV_MONTH  
	,DATE_FORMAT(EP_START_DT,'yyyyMM') 
	,A.HCTAPAIDMONTH
	,A.MBM_DEPLOY_DT
	,A.MARKET_FNL	
	,A.CLAIM_STATUS
	,A.MBMSERV
	,A.CATEGORY
;
--1555388  1526174  1496541   select count(*) from TMP_1y.kn_MBM_EPISODE_SMRY
  
 

/*______________[ Episode START DATES  ]___________________________________________________________________________________*/
DROP TABLE TMP_1y.kn_MBM_EPISODE_RO_LAG;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_RO_LAG   AS  
SELECT A.MBI
	,A.COMPONENT
	,A.ID
	,A.START_DT
	,floor((DATEDIFF(HCTAPAIDMONTH,START_DT)+20)/30.5) VISIT_RUNOUT_MO  
	,round((DATEDIFF(HCTAPAIDMONTH,START_DT)+20)/1,0) VISIT_RUNOUT 
	,floor(DATEDIFF(START_DT,EP_START_DT)/30.5) VISIT_EP_LAG 
	,Visit_dy_lag
	,IF(PREV_START_DT IS NULL,1,Ep_flag) Ep_flag
	,EP_START_DT
	,CMLTV_EPISODES
	,A.I 
	,A.PREV_I
	,A.SERV_MONTH
	,A.HCE_QTR
	,A.HCTAPAIDMONTH
	,EP_HCTAPAIDMONTH
	,A.MBM_DEPLOY_DT
	,A.MARKET_FNL
	,A.CLAIM_STATUS
	,A.MBMSERV
	,A.CATEGORY
	,A.ALLOWED
	,A.PAID
	,A.TADM_UTIL
	,A.VISITS
	,A.VSTS
	,A.ADJ_SRVC_UNITS
FROM TMP_1y.kn_MBM_EPISODE_vst_ep_2 A
;

--66929218  65797555  64650521  select count(*) from TMP_1y.kn_MBM_EPISODE_RO_LAG --  where mbi like '7GT2FY4RA93%'

/*______________[ Episode START DATES  ]___________________________________________________________________________________*/
DROP TABLE TMP_1y.kn_MBM_EPISODE_RO_LAG2;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_RO_LAG2   AS  
SELECT 
	A.MBI
	,A.ID
	,EP_START_DT
	,CMLTV_EPISODES
	,START_DT
	,DATE_FORMAT(EP_START_DT,'yyyyMM') EP_START_MO 
	,DATE_FORMAT(EP_START_DT,'yyyy') ep_start_YEAR
	,MARKET_FNL
	,MBM_DEPLOY_DT
	,CATEGORY
	,CLAIM_STATUS
	,HCTAPAIDMONTH
	,MBMSERV VISIT_MBMSERV
	,VISIT_RUNOUT_MO
	,0 EP_RUNOUT_MO
	,DATE_FORMAT(START_DT,'yyyyMM') VISIT_MO
	,VISIT_EP_LAG 
	,Ep_flag EPISODES
	,VISITS
	,ALLOWED
	,0 MM 
FROM TMP_1y.kn_MBM_EPISODE_RO_LAG A
;
--66929218  65797555  64650521    select count(*) from  TMP_1y.kn_MBM_EPISODE_RO_LAG2


/*______________[ Episode START DATES  ]___________________________________________________________________________________*/
DROP TABLE TMP_1y.kn_MBM_EPISODE_AGG6_EP;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_AGG6_EP    AS  
SELECT 
	'EPISODES' DATA_TYPE
	,EP_START_MO 
	,CONCAT(ep_start_YEAR,'Q9') EP_START_QTR
	,MARKET_FNL
	,MBM_DEPLOY_DT
	,CATEGORY
	,CLAIM_STATUS
	,''VISIT_MBMSERV
	,0 VISIT_RUNOUT_MO
	,0 EP_RUNOUT_MO
	,0 VISIT_MO
	,0 VISIT_EP_LAG 
	,SUM(EPISODES) EPISODES 
	,0 VISITS 
	,0 ALLOWED
	,0 MM 
FROM (SELECT * FROM TMP_1y.kn_MBM_EPISODE_RO_LAG2  WHERE  EPISODES = 1 )A
GROUP BY 
	EP_START_MO
	,CONCAT(ep_start_YEAR,'Q9')
	,MARKET_FNL
	,MBM_DEPLOY_DT
	,CATEGORY
	,CLAIM_STATUS
;

--45630  44961 44193  43381   select count(*) from TMP_1y.kn_MBM_EPISODE_AGG6_EP 
 

/*______________[ Episode START DATES  ]___________________________________________________________________________________*/
DROP TABLE TMP_1y.kn_MBM_EPISODE_AGG6;  
CREATE TABLE TMP_1y.kn_MBM_EPISODE_AGG6 AS  
	SELECT 
	'VISITS' DATA_TYPE
	,EP_START_MO 
	,CONCAT(ep_start_YEAR,'Q9') EP_START_QTR
	,MARKET_FNL
	,MBM_DEPLOY_DT
	,CATEGORY
	,CLAIM_STATUS
	,VISIT_MBMSERV
	,VISIT_RUNOUT_MO
	,EP_RUNOUT_MO
	,VISIT_MO
	,VISIT_EP_LAG 
	,SUM(0) EPISODES 
	,SUM(VISITS) VISITS 
	,SUM(ALLOWED) ALLOWED
	,0 MM 
FROM TMP_1y.kn_MBM_EPISODE_RO_LAG2
GROUP BY 
	EP_START_MO 
	,CONCAT(ep_start_YEAR,'Q9') 
	,MARKET_FNL
	,MBM_DEPLOY_DT
	,CATEGORY
	,CLAIM_STATUS
	,VISIT_MBMSERV
	,VISIT_RUNOUT_MO
	,EP_RUNOUT_MO
	,VISIT_MO
	,VISIT_EP_LAG
 ;

select * from tmp_1m.kn_mbm_episode_agg6
where visit_mo >= '202101' and visit_mo <= '202104'

--2322113  2278610 2234635    select count(*) from TMP_1y.kn_MBM_EPISODE_AGG6

/*______________[ Episode START DATES  ]___________________________________________________________________________________*/
  
INSERT INTO TMP_1y.kn_MBM_EPISODE_AGG6   
SELECT * FROM TMP_1y.kn_MBM_EPISODE_AGG6_EP A;

 
alter table  TMP_1y.kn_MBM_EPISODE_AGG6 change data_type data_type varchar(20);
alter table  TMP_1y.kn_MBM_EPISODE_AGG6 change ep_start_mo ep_start_mo varchar(20);
alter table  TMP_1y.kn_MBM_EPISODE_AGG6 change ep_start_qtr ep_start_qtr varchar(20);
alter table  TMP_1y.kn_MBM_EPISODE_AGG6 change mbm_deploy_dt mbm_deploy_dt varchar(20);
alter table  TMP_1y.kn_MBM_EPISODE_AGG6 change claim_status claim_status varchar(20);
alter table  TMP_1y.kn_MBM_EPISODE_AGG6 change visit_mo visit_mo varchar(20);
alter table  TMP_1y.kn_MBM_EPISODE_AGG6 change category category varchar(20);

--2367743  2323571  2278828  2235149   select count(*) from TMP_1y.kn_MBM_EPISODE_AGG6  

select * from TMP_1y.kn_MBM_EPISODE_AGG6 limit 2;


select sum(allowed) 
from TMP_1y.kn_MBM_EPISODE_AGG6
where visit_mo = '202406'  --73432103.42  73635236.05  73514079.66  

select sum(allowed) 
from TMP_1y.cl_MBM_EPISODE_AGG6
where visit_mo between '202101' and '202108'
;
select sum(allowed) 
from TMP_1m.kn_MBM_EPISODE_AGG6
where visit_mo between '202101' and '202108' 
;

select sum(Allowed_Amt) 
from TMP_1m.kn_MBM_EPISODE_AGG6_sum1
where visit_mo between '202401' and '202408' -- 
;



/*___________________[ SUMARIZING DATA FOR EXCEL ]_________________________________________________*/


--___________________  Summarize for Excel  ______________________
drop table TMP_1y.kn_MBM_EPISODE_AGG6_sum1;
create table TMP_1y.kn_MBM_EPISODE_AGG6_sum1 stored as orc AS 	
select 
	data_type,
	ep_start_mo,
	substring(ep_start_mo, 0, 4) as ep_year,
	substring(ep_start_mo, 5,2) as ep_month,
	visit_mo,
	case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as Pilot_Nat,  --caroline: force Phase-I and Phase-II to Pilot
	category,
--	claim_status,
	visit_ep_lag ,
	visit_runout_mo,
	sum(episodes) as ep_cnt,
	sum(visits) as visit_cnt,
	sum(allowed) as Allowed_Amt,
	sum(mm) as MMs
from TMP_1y.kn_MBM_EPISODE_AGG6
where ep_start_mo > '201812' --and claim_status in ('APC-Paid','Paid')    --remove the paid condition on 2/21/2025
group by
	data_type,
	ep_start_mo,
	substring(ep_start_mo, 0, 4),
	substring(ep_start_mo, 5,2),
	visit_mo,
	case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end,
	category,
	claim_status,
	visit_ep_lag,
	visit_runout_mo
UNION 
select 
	data_type,
	ep_start_mo,
	substring(ep_start_mo, 0, 4) as ep_year,
	substring(ep_start_mo, 5,2) as ep_month,
	visit_mo,
	Pilot_Nat,  --caroline: force Phase-I and Phase-II to Pilot
	category,
--	claim_status,
	visit_ep_lag ,
	visit_runout_mo,
	ep_cnt,
	visit_cnt,
	Allowed_Amt,
	MMs
from TMP_1y.kn_MBM_MSHP_sum1
;

select count(*) from TMP_1y.kn_MBM_EPISODE_AGG6_sum1  --236851  231611  226322  220,968   Paid and Denied

select * from TMP_1y.kn_MBM_EPISODE_AGG6_sum1

--ALL ABOVE RAN AS OF 5/13/2025

