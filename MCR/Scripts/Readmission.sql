select * from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work


select 
count(*)
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where to_timestamp_ntz(uhg_received_date) >= '2024-07-01'
	and member_id is not null

-- 4815305


select 
	work_item_type
	, business_area
	, business_segment
	, import_source
	, count(distinct work_item_id)
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where member_id is not null
	and work_item_type ilike 'readmission%'
group by 1,2,3,4

select 
work_item_type
, business_area
, business_segment
, import_source
, count(distinct work_item_id)
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where member_id is not null
	and work_item_type ilike 'readmission%' and to_char(uhg_received_date, 'yyyy') = '2024'
	--and import_source = 'COSMOS'
group by 1,2,3,4
-- 250,876

-- Join with tre_membership
-- Sample to 2024
-- Gal_sbsc_mnr = member_id
-- Match vs not-match
-- 

select * from fichsrv.tre_membership

select 
    member_id
    , work_item




select 

select distinct expected_start

-- COSMOS import_source
-- uhg_received_date: 2024 

select *
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where member_id is not null
	and work_item_type ilike 'readmission%'
	
	busniess_area , Business_segment, import_soruce

--
--Readmissions - PTMM	8642
--Readmissions - PTMD	86533
--Readmissions	379367

select work_item_id from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where length(work_item_id) = 11 

use secondary role all;

select * from ving_prd_trend_db.hce_ops_fnl.hce_adr_avtar_like_24_25_f

-- case_id
-- purchaser_id
-- member_id
-- medicare_id: 11
-- subscriber_id


select medicare_id from ving_prd_trend_db.hce_ops_fnl.hce_adr_avtar_like_24_25_f
-- 
                                                                   

-- Readmission = admit

-- Pick an old quarter, sample
-- WOrk_item_id


select case_category_cd, count(*) from ving_prd_trend_db.hce_ops_fnl.hce_adr_avtar_like_24_25_f
group by 1


select * from ving_prd_trend_db.tadm_tre_cpy.GLXY_IP_ADMIT_F_202510



-- Make admit table, include sbscr_nbr, restrict to 2024q2q3
create or replace table tmp_1m.kn_mcr_radmts_base_2024q2q3 as
select 
	gal_mbi_hicn_fnl
	, ip.pd_dn_ol_admitid 
	, ip_status_code
	, Readmit_masteradmitid
	, indexadmitind
	, readmit_ind
	, sbscr_nbr
/*Reason Code Fileds*/
	, rsn.rsn_cd 
	, rsn.rsn_cd_desc
/*ADMIT TYPES FIELDS*/
	, Pd_dn_ol_Tadm_admit_type as TADM_ADMIT_TYPE
	--,TADM_MDC
	, admit_drg_cd 
	, allw_amt_fnl 
	, FNL_DRG_CD
	, CLM_ADMIT_TYPE
	, FNL_ADMIT_TYP
	, PROC_CD
	, PRIMARY_DIAG_CD
	, AHRQ_DIAG_GENL_CATGY_DESC
	, AHRQ_DIAG_DTL_CATGY_DESC
	, RVNU_CD
	/*DOL fields */
	, DOL.CLM_DVLP_RSN_CD
	, DOL.CLM_DVLP_REV
	, DOL.CLM_DVLP_INFO_RECV_DT
	, DOL.COS_CLOS_CLM_CD
	, DOL.LOAD_DT
	, DOL.UPDT_DT
	, year(CLM_DVLP_INFO_RECV_DT) as INFO_RECV_YR
	, year(UPDT_DT) as UPDT_YR
/*CLAIM-RELATED FIELDS*/
	, IP.SITE_CLM_AUD_NBR
	, IP.COMPONENT
	, IP.SUB_AUD_NBR
	, IP.DTL_LN_NBR
	, CLM_REC_CD
	, EVENTKEY	
	, to_char(Pd_dn_ol_admit_start_dt,'yyyyMM') as ADMIT_YR_MONTH
	, Pd_dn_ol_admit_start_dt ADMIT_START_DT
	, Pd_dn_ol_admit_end_dt ADMIT_END_DT
	, FST_SRVC_DT
	, ERLY_SRVC_QTR
	, ERLY_SRVC_DT
	, DSCHRG_STS_CD
	, CATGY_ROL_UP_2_DESC
	, BRAND_FNL
	, CLM_DNL_F
	, PROV_TIN
	, MPIN
	, SITE_CD
/*CLAIM ADJUDICATION FIELDS*/
	, BIL_RECV_DT
	, ADJD_QTR
	, ADJD_DT
	, CLM_PD_DT
	, CLM_LVL_RSN_CD_SYS_ID
	, SRVC_LVL_RSN_CD_SYS_ID
/*DEMOGRAPHIC FIELDS*/
	, PLAN_LEVEL_2_FNL
	, PRODUCT_LEVEL_3_FNL
	, REGION_FNL
	, MARKET_FNL
	, FIN_SUBMARKET
	, GLOBAL_CAP
	, GROUP_IND_FNL
	, TFM_INCLUDE_FLAG
	, MIGRATION_SOURCE
	, GROUPNUMBER
	, SEGMENT_NAME_FNL
	, CONTRACTPBP_FNL
	, CONTRACT_FNL
	, CASE WHEN PRODUCT_LEVEL_3_FNL<>'INSTITUTIONAL' AND GLOBAL_CAP='NA' AND TFM_INCLUDE_FLAG = 1 THEN 1 ELSE 0 
	END as MNR_RISK_IND
/*Review Reason Codes*/
	, clm_rev_rsn_1_cd 	
	, clm_rev_rsn_2_cd 
	, clm_rev_rsn_3_cd 
	, clm_rev_rsn_4_cd 
	, clm_rev_rsn_5_cd 
	, clm_rev_rsn_6_cd 
	, clm_rev_rsn_7_cd 
	, clm_rev_rsn_8_cd 
	, clm_rev_rsn_9_cd 
	, clm_rev_rsn_10_cd 
/*Keys*/
	, IP.cos_clm_head_sys_id 
	, IP.cos_clm_head_sys_id_orgnl 
	, IP.cos_clm_srvc_sys_id 
	, CASE WHEN rsn.rsn_cd in (274,279,381,459,504,695,894,1018,1139,1569,1575,1581) THEN 1 ELSE 0 END AS RSNCD_EVR_MCRDNL_IND
	, CASE WHEN ( rsn.rsn_cd in  ('1087','1098','1099','0380') OR DOL.CLM_DVLP_RSN_CD IN ('1087','1098','1099','0380') ) THEN 1 ELSE 0 END AS DOL_RSNCD_Merged_MCRTouchIND
	, CASE WHEN rsn.rsn_cd in ('1087','1098','1099','0380') THEN 1 ELSE 0 END AS RSNCD_MCRTouchIND
	, CASE WHEN DOL.CLM_DVLP_RSN_CD IN ('1087','1098','1099','0380') THEN 1 ELSE 0 END AS DOL_MCRTouchIND
	, case when rsn.rsn_cd_sys_id  is null then 0 else 1 end as RSNCD_MTCH_IND
	, case when DOL.cos_clm_head_sys_id is null then 0 else 1 end as DOL_MTCH_IND
	, DENSE_RANK() OVER (PARTITION BY pd_dn_ol_admitid,GAL_MBI_HICN_FNL ORDER BY adjd_dt desc ) as latest_clm_entry
	, proc_mod1_cd
from 
--	tadm_tre_cpy.GLXY_IP_ADMIT_F_202304_TEST_SREE ip 
-- tadm_tre_cpy.GLXY_IP_ADMIT_F_202311_sree ip	
   tadm_tre_cpy.GLXY_IP_ADMIT_F_202510 as ip
left outer join fichsrv.tadm_glxy_reason_code as rsn
	on ip.fnl_rsn_cd_sys_id = rsn.rsn_cd_sys_id 
left outer join HCE_OPS_FNL.COSMOS_DOL_202511 as dol
	on trim(ip.cos_clm_head_sys_id_orgnl)  = dol.cos_clm_head_sys_id 
where clm_admit_type ='ACUTE' and to_char(Pd_dn_ol_admit_start_dt,'yyyyMM') between '202404' and '202409'
--AND ip_status_code<>'OL'
;

select * from tadm_tre_cpy.GLXY_IP_ADMIT_F_202510 


select count(*) from tmp_1m.kn_mcr_radmts_base_2024q2q3;
-- 16,955,613


create or replace table tmp_1m.kn_mcr_radmts_rollup_inds_2024q2q3 as 
select 
	tadm_admit_type 
	,pd_dn_ol_admitid
	,GAL_MBI_HICN_FNL
	,PRODUCT_LEVEL_3_FNL
	,MARKET_FNL
	,GLOBAL_CAP
	,TFM_INCLUDE_FLAG
	,MIGRATION_SOURCE
	,brand_fnl
	,max(RSNCD_EVR_MCRDNL_IND) RSNCD_EVR_MCRDNL_IND
	,max(DOL_RSNCD_Merged_MCRTouchIND) DOL_RSNCD_Merged_MCRTouchIND
	,max(RSNCD_MCRTouchIND) RSNCD_MCRTouchIND
	,max(DOL_MCRTouchIND) DOL_MCRTouchIND
	,sum(allw_amt_fnl) admit_allw
from tmp_1m.kn_mcr_radmts_base_2024q2q3 as a
group by 
	tadm_admit_type 
	,pd_dn_ol_admitid
	,GAL_MBI_HICN_FNL
	,PRODUCT_LEVEL_3_FNL
	,MARKET_FNL
	,GLOBAL_CAP
	,TFM_INCLUDE_FLAG
	,MIGRATION_SOURCE
	,brand_fnl
	;


--Exclude Overlap Admits
create or replace table tmp_1m.kn_mnr_mcr_drg_rollup_inds_2024q2q3 as
select distinct 
 	 a.tadm_admit_type
	,a.pd_dn_ol_admitid
	,a.GAL_MBI_HICN_FNL
	, a.sbscr_nbr
	,a.ADMIT_YR_MONTH
--	,to_char(a.adjd_dt,'yyyyMM') adjd_yrmonth
	,substr(a.admit_yr_month,1,4) ADMIT_YR
	,a.admit_DRG_CD
	,a.IP_STATUS_Code
	,case when b.RSNCD_EVR_MCRDNL_IND=1 and b.DOL_RSNCD_Merged_MCRTouchIND=1 then 1 else 0 end as MCR_EVR_Denied_readmit
	,case when a.latest_clm_entry=1 and a.ip_status_code='DN' and b.RSNCD_EVR_MCRDNL_IND=1 and b.DOL_RSNCD_Merged_MCRTouchIND=1 then 1 else 0 end  still_MCR_DNL_IND
	,case when a.latest_clm_entry=1 and a.ip_status_code='DN' and b.RSNCD_EVR_MCRDNL_IND=1 then 1 else 0 end  still_MCR_DNL_W_NODOL_IND
	,case when  b.DOL_RSNCD_Merged_MCRTouchIND=1 then 1 else 0 end as MCR_touched
	,a.indexadmitind
	,a.readmit_ind
	,b.admit_allw
	,a.PRODUCT_LEVEL_3_FNL
	,a.MARKET_FNL
	,a.GLOBAL_CAP
	,a.TFM_INCLUDE_FLAG
	,a.MIGRATION_SOURCE
	,a.brand_fnl
from 
	tmp_1m.kn_mcr_radmts_base_2024q2q3 a 
inner join
	tmp_1m.kn_mcr_radmts_rollup_inds_2024q2q3 b
on a.pd_dn_ol_admitid = b.pd_dn_ol_admitid
and a.ip_status_code !='OL' and latest_clm_entry = 1 and a.mnr_risk_ind = 1
;

select count(*) from tmp_1m.kn_mnr_mcr_drg_rollup_inds_2024q2q3;
-- 571,331

--Add DRG Unit ost
create or replace table tmp_1m.kn_mnr_mcr_radmts_readmts_1_2024q2q3 as
select distinct 
 	 tadm_admit_type
	,a.pd_dn_ol_admitid
	,a.GAL_MBI_HICN_FNL
	, a.sbscr_nbr
	,a.admit_yr
	,a.admit_yr_month
	--,a.adjd_yrmonth
	,a.admit_DRG_CD
	,a.IP_STATUS_Code
	,a.MCR_EVR_Denied_readmit
	,a.MCR_touched
	,a.still_MCR_DNL_IND
	,a.still_MCR_DNL_W_NODOL_IND
	,a.indexadmitind
	,a.readmit_ind
	,a.admit_allw
    ,b.unit_cost
--	,case when b.admit_DRG_CD is null then c.medsurgicu_avg_cost else b.medsurgicu_avg_cost end medsurgicu_avg_cost
--	,case when b.admit_DRG_CD is null then c.snf_avg_cost else b.snf_avg_cost end snf_avg_cost
--	,case when b.admit_DRG_CD is null then c.rehab_avg_cost else b.rehab_avg_cost end rehab_avg_cost
--	,case when b.admit_DRG_CD is null then c.ltac_avg_cost else b.ltac_avg_cost end ltac_avg_cost
	,a.PRODUCT_LEVEL_3_FNL
	,a.MARKET_FNL
	,a.GLOBAL_CAP
	,a.TFM_INCLUDE_FLAG
	,a.MIGRATION_SOURCE
	,a.brand_fnl
from 
	tmp_1m.kn_mnr_mcr_drg_rollup_inds_2024q2q3 as a
left Outer Join 
	HCE_OPS_FNL.HCEOPS_DRG_Unit_cost_IP_2025Q3 b
on a.admit_drg_cd  = b.admit_drg_cd 
and a.admit_yr  = concat('20',b."YEAR" )
;

select count(*) from tmp_1m.kn_mnr_mcr_radmts_readmts_1_2024q2q3;
-- 571,331



select count(*) 
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where to_timestamp_ntz(uhg_received_date) >= '2024-07-01'
	  and member_id is not null
;

select 
		work_item_type
		, work_item_id
		, resolution_code
		, count(*)
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where to_timestamp_ntz(uhg_received_date) >= '2024-07-01'
		and member_id is not null
group by 1 ,2
order by 1 desc
;

use secondary role all;

-- MCR
create or replace table tmp_1m.kn_mcr_2024q2q3 as
select 
		member_id
		, claim_id
		, work_item_id
		, regexp_substr(work_item_id, '^[^-]+') as work_item_id_cleaned
		, work_item_type
		, resolution_code
		, mcr_disposition_code as mcr_decision
		, concat(substring(cast(resolved_at as string), 1, 4), substring(cast(resolved_at as string), 6, 2)) as mcr_month
		, substring(cast(resolved_at as string), 1, 4) as mcr_year
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work 
where to_timestamp_ntz(uhg_received_date) between '2024-01-01' and '2024-09-30'
		and member_id is not null
;




select * from hce_ops_fnl.hce_adr_avtar_like_24_25_f;
select * from tmp_1m.kn_mcr_2024q;
select * from tmp_1m.kn_mnr_mcr_radmts_readmts_1_2024q2q3;
select * from tadm_tre_cpy.glxy_ip_admit_f_202510;


select * from fichsrv.cosmos_ip

use secondary role all;
select * from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work 

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

select distinct resolution_code from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work 






