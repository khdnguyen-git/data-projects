/*============================================================================================================
 * OP Therapies VpE Calculation
 * 03/12: added _${paid_thru} suffix to ALL tables, category_1, ahrq fields, optum_tin_flag + tin_owner
 *        via LEFT JOIN to tmp_1y.cl_therapy_optum_tins_202602
 *        renamed mbm_deploy_dt -> national_pilot_flag
 *        final deliverable: tmp_1m.knd_mbm_visits_episodes_extract_${paid_thru}
 * 03/12: added paidthru suffix + ahrq
 * 02/12: changed hctapaidmonth to hcta_paid_dt because of format
 * 02/11: added visit_paid_month to recalculate 2024Q1Q2 VpE with similar runout to 2025Q1Q2
 * 02/10: added separate VpE analysis, where visits are counted at the episode level to break VpE into 10s tier and count visits/episodes that fall into these levels
 * 02/09: added rvnu_cd to NICE
 * 02/09: fixed substring(coalesce(bil_typ_cd,'0'), 0, 1) != '3' to substring(coalesce(bil_typ_cd,'0'), 1, 1) != '3'
 * 02/09: fixed home health filters
 * 02/09: removed service_code from visit aggregation
 * 02/05: removing claim_status case when (resulted in, same claim, same ID, 2 claim_status) in extraction,
 *    but adding it back after aggregation (not using clm_dnl_f field)
 * 02/05: changed M&R FFS to M&R FFS excl. DSNP (product_level_3 not in ('DUAL', 'INSTITUTIONAL')
 * 02/05: updated script to use lag() window instead of 2 int. tables
 * Visits definition (from _stable script): count(concat(eventkey, fst_srvc_dt))
 *  Eventkey: field in claims data, equiv. to mbi | fst_srvc_dt | srvc_prov_id 
 * Episode definition
 *	Partition by mbi-category (Office/Chiro/OP_Rehab), national_pilot_flag (National/Pilot)
 *  Order by fst_srvc_dt
 *  If the (current fst_srvc_dt - previous fst_srvc_dt) for this partition > 30 -> New Episode
 *  Or if (current fst_srvc_dt - previous fst_srvc_dt) is NULL -> New Episode
 *===========================================================================================================*/


@set paid_thru = 202602
show variables;


-- COSMOS claims
drop table if exists tmp_1m.knd_mbm_cosmos_claims_${paid_thru};
create table tmp_1m.knd_mbm_cosmos_claims_${paid_thru} as
select
	'COSMOS' as entity
	, a.component
	, a.eventkey as visit_id
	, a.service_code
	, a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
	, a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category_2
    , case when a.proc_cd in ('98940','98941','98942') then 'Chiro'
           when a.proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
           when a.proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
           else 'Other'
    end as category_1
    , a.prov_tin
    , case when b.tin is not null then 1 else 0 end as optum_tin_flag
    , b.tin_owner
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
	, a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.tfm_product_new_fnl
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.glxy_pr_f a
left join tmp_1y.cl_therapy_optum_tins_202602 b
    on a.prov_tin = b.tin
where a.brand_fnl in ('M&R', 'C&S')
	and a.global_cap = 'NA'
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.prov_prtcp_sts_cd = 'P'		
	and a.st_abbr_cd = a.market_fnl
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
	and a.ama_pl_of_srvc_cd != '12' 
	and (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and a.fst_srvc_year >= '2023'
union all
select
	'COSMOS' as entity
	, a.component
	, a.eventkey as visit_id
	, a.hce_service_code as service_code
	, a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
	, a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category_2
    , case when a.proc_cd in ('98940','98941','98942') then 'Chiro'
           when a.proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
           when a.proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
           else 'Other'
    end as category_1
    , a.prov_tin
    , case when b.tin is not null then 1 else 0 end as optum_tin_flag
    , b.tin_owner
 	, a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
	, a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.tfm_product_new_fnl
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.glxy_op_f a
left join tmp_1y.cl_therapy_optum_tins_202602 b
    on a.prov_tin = b.tin
where a.brand_fnl in ('M&R', 'C&S')
	and a.global_cap = 'NA'
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.prov_prtcp_sts_cd = 'P'
	and a.st_abbr_cd = a.market_fnl
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
	and a.ama_pl_of_srvc_cd != '12'   
	and (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and a.fst_srvc_year >= '2023'
;

select sum(allw_amt_fnl) from tmp_1m.knd_mbm_cosmos_claims_${paid_thru}
where fst_srvc_month = '202406'
;


-- CSP claims
drop table if exists tmp_1m.knd_mbm_csp_claims_${paid_thru};
create table tmp_1m.knd_mbm_csp_claims_${paid_thru} as
select
	'CSP' as entity
	, a.component
	, a.eventkey as visit_id
	, a.service_code
	, a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
	, a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category_2
    , case when a.proc_cd in ('98940','98941','98942') then 'Chiro'
           when a.proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
           when a.proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
           else 'Other'
    end as category_1
    , a.tin as prov_tin
    , case when b.tin is not null then 1 else 0 end as optum_tin_flag
    , b.tin_owner
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
	, a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.tfm_product_new_fnl
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.dcsp_pr_f a
left join tmp_1y.cl_therapy_optum_tins_202602 b
    on a.tin = b.tin
where a.brand_fnl = 'C&S'	
	and a.global_cap = 'NA'
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.prov_prtcp_sts_cd = 'P'		
	and a.st_abbr_cd = a.market_fnl
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
	and a.ama_pl_of_srvc_cd != '12'  
	and (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and a.fst_srvc_year >= '2023'
union all
select
	'CSP' as entity
	, a.component
	, a.eventkey as visit_id
	, a.service_code
	, a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
	, a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category_2
    , case when a.proc_cd in ('98940','98941','98942') then 'Chiro'
           when a.proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
           when a.proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
           else 'Other'
    end as category_1
    , a.tin as prov_tin
    , case when b.tin is not null then 1 else 0 end as optum_tin_flag
    , b.tin_owner
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
	, a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.tfm_product_new_fnl
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.dcsp_op_f a
left join tmp_1y.cl_therapy_optum_tins_202602 b
    on a.tin = b.tin
where a.brand_fnl = 'C&S'
	and a.global_cap = 'NA'
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.prov_prtcp_sts_cd = 'P'		
	and a.st_abbr_cd = a.market_fnl
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
	and a.ama_pl_of_srvc_cd != '12'
	and (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and a.fst_srvc_year >= '2023'
;

-- NICE claims
-- special_network doesn't exist in NCE; ericksonflag doesn't work
drop table if exists tmp_1m.knd_mbm_nice_claims_${paid_thru};
create table tmp_1m.knd_mbm_nice_claims_${paid_thru} as
select
	'NICE' as entity
	, a.component
	, a.eventkey as visit_id
	, a.service_code
	, a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
	, a.mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category_2
    , case when a.proc_cd in ('98940','98941','98942') then 'Chiro'
           when a.proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
           when a.proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
           else 'Other'
    end as category_1
    , a.tin as prov_tin
    , case when b.tin is not null then 1 else 0 end as optum_tin_flag
    , b.tin_owner
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
	, iff(a.clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , 'NA' as migration_source
    , a.tfm_product_fnl as tfm_product_new_fnl
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.calc_allw as allw_amt_fnl
	, a.calc_net_pd as net_pd_amt_fnl
from fichsrv.nce_pr_f a
left join tmp_1y.cl_therapy_optum_tins_202602 b
    on a.tin = b.tin
where a.brand_fnl = 'M&R'
	and a.plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and a.prov_prtcp_sts_cd = 'P'		
	and a.st_abbr_cd = a.market_fnl
	and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3' -- Home Health
	and a.claim_place_of_svc_cd != '12'   
	and (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and a.fst_srvc_year >= '2023'
union all
select
	'NICE' as entity
	, a.component
	, a.eventkey as visit_id
	, a.service_code
	, a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
	, a.mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category_2
    , case when a.proc_cd in ('98940','98941','98942') then 'Chiro'
           when a.proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
           when a.proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
           else 'Other'
    end as category_1
    , a.tin as prov_tin
    , case when b.tin is not null then 1 else 0 end as optum_tin_flag
    , b.tin_owner
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
	, iff(a.clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
	, a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , 'NA' as migration_source
    , a.tfm_product_fnl as tfm_product_new_fnl
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.allw_amt as allw_amt_fnl
	, a.net_pd_amt as net_pd_amt_fnl
from fichsrv.nce_op_f a
left join tmp_1y.cl_therapy_optum_tins_202602 b
    on a.tin = b.tin
where a.brand_fnl = 'M&R'
	and a.plan_level_2_fnl not in ('PFFS')			
	--and special_network not in ('ERICKSON')			
	and a.prov_prtcp_sts_cd = 'P'		
	and a.st_abbr_cd = a.market_fnl
	and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3' -- Home Health
	and a.claim_place_of_svc_cd != '12'
	and (a.proc_cd in
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
     	or 
	 	a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
	and a.fst_srvc_year >= '2023'
;

-- Stack COSMOS + CSP + NICE claims
-- Make flags for population
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_${paid_thru};
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims_${paid_thru} as
with cte_union as (
select
	entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
	, hcta_paid_dt
    , fst_srvc_year
	, mbi
	, proc_cd
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
    , primary_diag_cd
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, national_pilot_flag
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_claims_${paid_thru}
union all
select
	entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , hcta_paid_dt
    , fst_srvc_year
	, mbi
	, proc_cd
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
    , primary_diag_cd
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, national_pilot_flag
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_csp_claims_${paid_thru}
union all
select
	entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , hcta_paid_dt
    , fst_srvc_year
	, mbi
	, proc_cd
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
    , primary_diag_cd
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, national_pilot_flag
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_nice_claims_${paid_thru}
)
select
	*
	, case when OAH_flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when MnR_Dual_flag = 1 then 'M&R DSNP'
		   when MnR_ISNP_flag = 1 then 'M&R ISNP'
		   when MnR_FFS_flag  = 1 then 'M&R FFS (excl. DSNP)'
		   else 'N/A'
	end as population
from cte_union;

-- Aggregate to sum(allowed) and sum(paid) before VpE analysis
-- Adding claim_status
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_${paid_thru};
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_${paid_thru} as
with aggregated as (
select
	population
	, entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
	, fst_srvc_month
	, fst_srvc_qtr
	, hcta_paid_dt
	, fst_srvc_year
	, mbi
	, proc_cd
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, primary_diag_cd
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, tfm_product_new_fnl
	, product_level_3_fnl
	, national_pilot_flag
    , sum(allw_amt_fnl) as allw_amt_fnl
    , sum(net_pd_amt_fnl) as net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_${paid_thru}
group by
	population
	, entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
	, fst_srvc_month
	, fst_srvc_qtr
	, hcta_paid_dt
	, fst_srvc_year
	, mbi
	, proc_cd
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, primary_diag_cd
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, tfm_product_new_fnl
	, product_level_3_fnl
	, national_pilot_flag
)
select
	*
	, iff(sum(allw_amt_fnl) over (partition by visit_id, fst_srvc_dt, category_2)  > 0.01, 'Paid', 'Denied') as claim_status
from aggregated
;


-- Defining visits ranking structure
-- Grouping proc_cd into mbmserv_dtl (PT/OT/ST, )
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_${paid_thru};
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_${paid_thru} as
select 
    entity
    , concat(mbi, '-', category_2) as mbi_key
	, component
	, visit_id
    , case when proc_cd in ('98940','98941','98942') 
    		then 'Chiro'
           when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') 
			then 'PT-OT'
           when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') 
           	then 'ST'
           else 'Other' 
	end as mbmserv_dtl
	, fst_srvc_dt
    , fst_srvc_month
    , min(hcta_paid_dt) as min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_${paid_thru}
group by
    entity
    , concat(mbi, '-', category_2)
	, component
	, visit_id
    , case when proc_cd in ('98940','98941','98942') 
    		then 'Chiro'
           when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') 
			then 'PT-OT'
           when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') 
           	then 'ST'
           else 'Other' 
	end
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
;


-- Mark new episode
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_${paid_thru};
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_${paid_thru} as
select
	mbi_key
	, entity
	, component
	, visit_id
	, mbmserv_dtl
	, fst_srvc_dt
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , allowed
    , paid
 	, lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt) 
 	as prev_srvc_dt
    , datediff('day'
    		, lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt)
    		, fst_srvc_dt) 
    as visit_day_diff
    , iff(datediff('day'
    		, lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt)
    		, fst_srvc_dt) > 30, 1 , 0) 
    as ep_flag
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_${paid_thru}
;


-- Episodes
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_${paid_thru} as
with ep_numbering as 
(
select
	*
  	, sum(iff(prev_srvc_dt is null, 1, ep_flag)) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt rows between unbounded preceding and current row) 
  	as cmltv_episodes
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_${paid_thru}
)
select 
	mbi_key
	, fst_srvc_dt
	, prev_srvc_dt
	, visit_day_diff
	, iff(prev_srvc_dt is null, 1, ep_flag) as ep_flag 
	, cmltv_episodes
	, min(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag, cmltv_episodes) as ep_start_dt
	, min(min_hcta_paid_dt) over (partition by mbi_key, national_pilot_flag, cmltv_episodes) as ep_hcta_paid_dt
	, entity
	, mbmserv_dtl
	, visit_id
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , allowed
    , paid    
from ep_numbering
;

-- Episodes summary
create or replace table tmp_1m.knd_mbm_episodes_summary_${paid_thru} as
select 
	'EPISODES' as data_type
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') as ep_start_year
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
	, cast(null as varchar) as visit_month
	, cast(null as varchar) as visit_year
	, cast(null as varchar) as visit_paid_month
	, ep_hcta_paid_dt as ep_paid_month
	, entity
	, mbmserv_dtl
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
	, 0 as visit_ep_runout_month
	, 0 as visit_runout_month
	, sum(ep_flag) as n_episodes
	, 0 as n_visits
	, 0 as sum_allowed
	, 0 as sum_paid
	, 0 as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_${paid_thru}
where ep_flag = 1  -- Filter for only episode-starting visits
group by 
	to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy')
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
	, ep_hcta_paid_dt
	, entity
	, mbmserv_dtl
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
;

-- Visits summary
create or replace table tmp_1m.knd_mbm_visits_summary_${paid_thru} as
select
    'VISITS' as data_type
    , to_char(ep_start_dt, 'yyyyMM') as ep_start_month
    , to_char(ep_start_dt, 'yyyy') as ep_start_year
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
    , fst_srvc_month as visit_month
    , fst_srvc_year as visit_year
    , min_hcta_paid_dt as visit_paid_month
    , cast(null as varchar) as ep_paid_month
    , entity
    , mbmserv_dtl
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , tin_owner
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5) as visit_ep_runout_month
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5) as visit_runout_month
    , 0 as n_episodes
    , count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
    , sum(allowed) as sum_allowed
    , sum(paid) as sum_paid
    , count(distinct mbi_key) as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_${paid_thru}
group by
    to_char(ep_start_dt, 'yyyyMM')
    , to_char(ep_start_dt, 'yyyy')
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
    , fst_srvc_month
    , fst_srvc_year
    , min_hcta_paid_dt
    , entity
    , mbmserv_dtl
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , tin_owner
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5)
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5)
;

-- Stack VISITS and EPISODES
create or replace table tmp_1m.knd_mbm_visits_episodes_stacked_${paid_thru} as
select * from tmp_1m.knd_mbm_visits_summary_${paid_thru}
union all
select * from tmp_1m.knd_mbm_episodes_summary_${paid_thru}
;

-- Summary
create or replace table tmp_1m.knd_mbm_vpe_summary_${paid_thru} as
select
    data_type
    , ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , mbmserv_dtl
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , tin_owner
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
    , sum(n_episodes) as total_episodes
    , sum(n_visits) as total_visits
    , sum(sum_allowed) as allowed
    , sum(sum_paid) as paid
    , sum(mbr_count) as mbr_count
from tmp_1m.knd_mbm_visits_episodes_stacked_${paid_thru}
group by
    data_type
    , ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , mbmserv_dtl
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , tin_owner
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
;

-- Final deliverable: Remove 'NA' population before loading into Excel
create or replace table tmp_1m.knd_mbm_visits_episodes_extract_${paid_thru} as
select
	*
from tmp_1m.knd_mbm_vpe_summary_${paid_thru}
where population != 'NA'
;


-- VpE in episodes
create or replace table tmp_1m.knd_mbm_vpe_aggregated_${paid_thru} as
with vpe as (
select
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, ep_hcta_paid_dt
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt) as ep_start_qtr
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
	, sum(allowed) as allowed
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_${paid_thru}
group by
	mbi_key
	, cmltv_episodes
	, ep_start_dt
	, ep_hcta_paid_dt
	, to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy') || 'Q' || extract(quarter from ep_start_dt)
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
)
select * from vpe;


-- VpE in episodes with percentile categories
create or replace table tmp_1m.knd_mbm_vpe_aggregated_category_mnr_${paid_thru} as
with pct_mnr as (
    select
        *
        , percentile_cont(0.25) within group (order by n_visits)
            over (partition by national_pilot_flag, category_2) as p25
        , percentile_cont(0.50) within group (order by n_visits)
            over (partition by national_pilot_flag, category_2) as p50
        , percentile_cont(0.75) within group (order by n_visits)
            over (partition by national_pilot_flag, category_2) as p75
    from tmp_1m.knd_mbm_vpe_aggregated_${paid_thru}
    where population = 'M&R FFS (excl. DSNP)'
)
select
	*
	, case when n_visits between 0 and 6 then '1 - 6'
		   when n_visits between 7 and 12 then '7 - 12'
		   when n_visits between 13 and 24 then '13 - 24'
		   when n_visits between 25 and 35 then '25 - 35'
		   when n_visits between 36 and 45 then '36 - 45'
		   when n_visits >= 46 then '46+'
		   else ''
	end as vpe_cat1
    , case when n_visits between 1 and 10 then '1 - 10'
           when n_visits between 11 and 20 then '11 - 20'
           when n_visits between 21 and 30 then '21 - 30'
           when n_visits >= 31 then '31+'
           else ''
    end as vpe_cat2
	, case when n_visits > (p75 + 3 * (p75 - p25)) then 'Extreme Outlier'
		   when n_visits > (p75 + 1.5 * (p75 - p25)) then 'Mild Outlier'
		   when n_visits > p75 then 'Above Average'
		   when n_visits > p25 then 'Normal'
		   else 'Below Average'
	end as vpe_cat3
	, 1 as n_episodes
from pct_mnr
;


-- Episodes summary for stacking, to count only episodes
create or replace table tmp_1m.knd_mbm_vpe_with_runout_episodes_${paid_thru} as
select
	'EPISODES' as data_type
	, ep_start_month
	, cast(null as varchar) as visit_month
	, cast(null as varchar) as visit_paid_month
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat2 as vpe_buckets_10
	, vpe_cat3 as vpe_buckets_stat
	, 0 as visit_runout_month
	, 0 as visit_ep_runout_month
	, sum(n_episodes) as n_episodes
	, 0 as n_visits
	, 0 as allowed
	, 0 as mm
from tmp_1m.knd_mbm_vpe_aggregated_category_mnr_${paid_thru}
group by
	ep_start_month
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat2
	, vpe_cat3
;

-- Visit summary for stacking
create or replace table tmp_1m.knd_mbm_vpe_with_runout_visits_${paid_thru} as
with visits_dedup as (
select
	mbi_key
	, visit_id
	, fst_srvc_dt
	, fst_srvc_month
	, min(min_hcta_paid_dt) as min_hcta_paid_dt
	, ep_start_dt
	, national_pilot_flag
	, cmltv_episodes
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, sum(allowed) as allowed
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_${paid_thru}
where population = 'M&R FFS (excl. DSNP)'
group by
	mbi_key
	, visit_id
	, fst_srvc_dt
	, fst_srvc_month
	, ep_start_dt
	, national_pilot_flag
	, cmltv_episodes
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
)
select
	'VISITS' as data_type
	, to_char(a.ep_start_dt, 'yyyyMM') as ep_start_month
	, a.fst_srvc_month as visit_month
	, a.min_hcta_paid_dt as visit_paid_month
	, a.national_pilot_flag
	, a.category_2
	, a.category_1
	, a.prov_tin
	, a.optum_tin_flag
	, a.tin_owner
	, a.ahrq_diag_genl_catgy_desc
	, a.ahrq_diag_dtl_catgy_desc
	, a.market_fnl
	, a.population
	, b.vpe_cat2 as vpe_buckets_10
	, b.vpe_cat3 as vpe_buckets_stat
    , floor((datediff('day', a.fst_srvc_dt, a.min_hcta_paid_dt) + 20) / 30.5) as visit_runout_month
    , floor(datediff('day', a.ep_start_dt, a.fst_srvc_dt) / 30.5) as visit_ep_runout_month
	, 0 as n_episodes
	, count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
	, sum(a.allowed) as allowed
	, count(distinct a.mbi_key) as mm
from visits_dedup as a
join tmp_1m.knd_mbm_vpe_aggregated_category_mnr_${paid_thru} as b
	on a.mbi_key = b.mbi_key 
	and a.national_pilot_flag = b.national_pilot_flag
	and a.cmltv_episodes = b.cmltv_episodes
where a.population = 'M&R FFS (excl. DSNP)'
group by
	to_char(a.ep_start_dt, 'yyyyMM')
	, a.fst_srvc_month
	, a.min_hcta_paid_dt
	, a.national_pilot_flag
	, a.category_2
	, a.category_1
	, a.prov_tin
	, a.optum_tin_flag
	, a.tin_owner
	, a.ahrq_diag_genl_catgy_desc
	, a.ahrq_diag_dtl_catgy_desc
	, a.market_fnl
	, a.population
	, b.vpe_cat2
	, b.vpe_cat3
    , floor((datediff('day', a.fst_srvc_dt, a.min_hcta_paid_dt) + 20) / 30.5)
    , floor(datediff('day', a.ep_start_dt, a.fst_srvc_dt) / 30.5)
;


create or replace table tmp_1m.knd_mbm_vpe_with_runout_visits_episodes_stacked_${paid_thru} as
select * from tmp_1m.knd_mbm_vpe_with_runout_visits_${paid_thru}
union all
select * from tmp_1m.knd_mbm_vpe_with_runout_episodes_${paid_thru}
;


create or replace table tmp_1m.knd_mbm_vpe_with_runout_summary_${paid_thru} as
select
	ep_start_month
	, visit_month
	, visit_paid_month
	, visit_ep_runout_month
	, visit_runout_month
	, vpe_buckets_10
	, vpe_buckets_stat
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, sum(n_visits) as n_visits
	, sum(n_episodes) as n_episodes
	, sum(allowed) as allowed
	, sum(mm) as mm
from tmp_1m.knd_mbm_vpe_with_runout_visits_episodes_stacked_${paid_thru}
group by
	ep_start_month
	, visit_month
	, visit_paid_month
	, visit_ep_runout_month
	, visit_runout_month
	, vpe_buckets_10
	, vpe_buckets_stat
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
;


create or replace table tmp_1m.knd_mbm_vpe_category_mnr_summmary_${paid_thru} as
select	
	ep_start_qtr
	, ep_start_month
	, ep_hcta_paid_dt
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat1
	, vpe_cat2
	, vpe_cat3
	, sum(n_episodes) as n_episodes
	, sum(n_visits) as n_visits
	, sum(allowed) as allowed
from tmp_1m.knd_mbm_vpe_aggregated_category_mnr_${paid_thru}
group by
	ep_start_qtr
	, ep_start_month
	, ep_hcta_paid_dt
	, national_pilot_flag
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, tin_owner
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, population
	, vpe_cat1
	, vpe_cat2
	, vpe_cat3
;

--- VpE Code ends here ---


/*==============================================================================
 * Membership
 *==============================================================================*/
-- COSMOS
drop table if exists tmp_1m.knd_mbm_cosmos_mm_${paid_thru}; 
create table tmp_1m.knd_mbm_cosmos_mm_${paid_thru} as 
select 
	'COSMOS' as entity
	, '' as component
	, '' as service_code
	, fin_inc_month as fst_srvc_month
	, fin_inc_year as fst_srvc_year
	, global_cap
	, fin_market as market_fnl
	, fin_state as st_abbr_cd
	, fin_brand as brand_fnl
	, fin_g_i as group_ind_fnl
	, tfm_include_flag
	, migration_source
	, fin_tfm_product_new as tfm_product_new_fnl 
	, fin_product_level_3 as product_level_3_fnl
	, nce_tadm_dec_risk_type
	, fin_member_cnt
from fichsrv.tre_membership
where		
	sgr_source_name = 'COSMOS'
	and fin_brand in ('M&R', 'C&S')
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
;

-- CSP
drop table if exists tmp_1m.knd_mbm_csp_mm_${paid_thru};
create table tmp_1m.knd_mbm_csp_mm_${paid_thru} as 
select 
	'CSP' as entity
	, '' as component
	, '' as service_code
	, fin_inc_month as fst_srvc_month
	, fin_inc_year as fst_srvc_year
	, global_cap
	, fin_market as market_fnl
	, fin_state as st_abbr_cd
	, fin_brand as brand_fnl
	, fin_g_i as group_ind_fnl
	, tfm_include_flag
	, migration_source
	, fin_tfm_product_new as tfm_product_new_fnl 
	, fin_product_level_3 as product_level_3_fnl
	, nce_tadm_dec_risk_type
	, fin_member_cnt
from fichsrv.tre_membership
where		
	sgr_source_name = 'CSP'
	and fin_brand in ('M&R', 'C&S')
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
;

-- NICE
drop table if exists tmp_1m.knd_mbm_nice_mm_${paid_thru};
create table tmp_1m.knd_mbm_nice_mm_${paid_thru} as 
select 
	'NICE' as entity
	, '' as component
	, '' as service_code
	, fin_inc_month as fst_srvc_month
	, fin_inc_year as fst_srvc_year
	, global_cap
	, fin_market as market_fnl
	, fin_state as st_abbr_cd
	, fin_brand as brand_fnl
	, fin_g_i as group_ind_fnl
	, tfm_include_flag
	, migration_source
	, fin_tfm_product_new as tfm_product_new_fnl 
	, fin_product_level_3 as product_level_3_fnl
	, nce_tadm_dec_risk_type
	, fin_member_cnt
from fichsrv.tre_membership
where		
	sgr_source_name = 'NICE'
	and fin_brand in ('M&R', 'C&S')
	and nce_tadm_dec_risk_type = 'FFS'
	and fin_inc_month >= '202301'
;


drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_mm_${paid_thru}; 
create table tmp_1m.knd_mbm_cosmos_csp_nice_mm_${paid_thru} as 
with cte_union as (
select
	entity
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , fin_member_cnt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.knd_mbm_cosmos_mm_${paid_thru}
union all
select
	entity
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , fin_member_cnt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.knd_mbm_csp_mm_${paid_thru}
union all
select
	entity
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , fin_member_cnt
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
			) then 1
		else 0
	end as CnS_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_Dual_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
from tmp_1m.knd_mbm_nice_mm_${paid_thru}
)
select
	*
	, case when OAH_flag = 1 then 'OAH'
		   when CnS_Dual_flag = 1 then 'C&S DSNP'
		   when MnR_Dual_flag = 1 then 'M&R DSNP'
		   when MnR_ISNP_flag = 1 then 'M&R ISNP'
		   when MnR_FFS_flag  = 1 then 'M&R FFS (excl. DSNP)'
		   else 'N/A'
	end as population
from cte_union;


drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary_${paid_thru}; 
create table tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary_${paid_thru} as 
select 
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , sum(fin_member_cnt) as sum_mm
from tmp_1m.knd_mbm_cosmos_csp_nice_mm_${paid_thru}
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
;


/*==============================================================================
 * Union Claims and Membership
 *==============================================================================*/
drop table if exists tmp_1m.knd_mbm_cosmos_csp_nice_claims_mm_summary_${paid_thru};
create table tmp_1m.knd_mbm_cosmos_csp_nice_claims_mm_summary_${paid_thru} as
select
	'Claims' as data_type
	, entity
	, population
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , sum(allw_amt_fnl) as sum_allowed
    , sum(net_pd_amt_fnl) as sum_paid
    , 0 as sum_mm
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_${paid_thru}
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
union all
select
	'Membership' as data_type
	, entity
	, population
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , 0 as sum_allowed
    , 0 as sum_paid
    , sum(sum_mm) as sum_mm
from tmp_1m.knd_mbm_cosmos_csp_nice_mm_summary_${paid_thru}
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
;
