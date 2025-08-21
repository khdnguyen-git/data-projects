
-- Pulling OP claims for 2018, 2019, 2020 - Only need to run once, ran on 10/8/2024
drop table TMP_1y.cl_MBM_CLAIMS_2018_2020;
CREATE TABLE TMP_1y.cl_MBM_CLAIMS_2018_2020 AS 
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
--38195692   38203014             select count(*) from TMP_1y.cl_MBM_CLAIMS_2018_2020


DROP TABLE TMP_1y.cl_MBM_EPISODE_1b_2018_2020; 
 CREATE TABLE TMP_1y.cl_MBM_EPISODE_1b_2018_2020 AS 
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
		FROM  TMP_1y.cl_MBM_CLAIMS_2018_2020 a
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
--36737912  32319558    select count(*) from TMP_1y.cl_MBM_EPISODE_1b
*/