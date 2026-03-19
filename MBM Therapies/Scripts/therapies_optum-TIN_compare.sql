-- ============================================================
-- NO DENIED: Excludes denied claims
-- ============================================================

-- COSMOS claims
create or replace table tmp_1m.knd_mbm_cosmos_claims_no_denied_${paid_thru} as
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
    , a.clm_dnl_f
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.glxy_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
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
	and a.clm_dnl_f not in ('D', 'Y')
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.clm_dnl_f
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.glxy_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
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
	and a.clm_dnl_f not in ('D', 'Y')
;


-- CSP claims
create or replace table tmp_1m.knd_mbm_csp_claims_no_denied_${paid_thru} as
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.clm_dnl_f
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.dcsp_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on substring(a.tin, 1 , 9) = b.tin
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
	and a.clm_dnl_f not in ('D', 'Y')
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.clm_dnl_f
    , a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.dcsp_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on substring(a.tin, 1 , 9) = b.tin
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
	and a.clm_dnl_f not in ('D', 'Y')
;


-- NICE claims
create or replace table tmp_1m.knd_mbm_nice_claims_no_denied_${paid_thru} as
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
    , a.dnl_f as clm_dnl_f
	, a.calc_allw as allw_amt_fnl
	, a.calc_net_pd as net_pd_amt_fnl
from fichsrv.nce_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.tin = b.tin
where a.brand_fnl = 'M&R'
	and a.plan_level_2_fnl not in ('PFFS')
	and a.prov_prtcp_sts_cd = 'P'
	and a.st_abbr_cd = a.market_fnl
	and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
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
	and a.dnl_f not in ('D', 'Y')
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
    , a.dnl_f as clm_dnl_f
	, a.allw_amt as allw_amt_fnl
	, a.net_pd_amt as net_pd_amt_fnl
from fichsrv.nce_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.tin = b.tin
where a.brand_fnl = 'M&R'
	and a.plan_level_2_fnl not in ('PFFS')
	and a.prov_prtcp_sts_cd = 'P'
	and a.st_abbr_cd = a.market_fnl
	and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
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
	and a.dnl_f not in ('D', 'Y')
;


-- Stack COSMOS + CSP + NICE claims
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_no_denied_${paid_thru} as
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
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
 	, clm_dnl_f
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_claims_no_denied_${paid_thru}
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
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
 	, clm_dnl_f
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_csp_claims_no_denied_${paid_thru}
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
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
 	, clm_dnl_f
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_nice_claims_no_denied_${paid_thru}
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


-- Aggregate
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_no_denied_${paid_thru} as
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
	, primary_diag_cd
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, product_level_3_fnl
	, national_pilot_flag
	, clm_dnl_f
    , sum(allw_amt_fnl) as allw_amt_fnl
    , sum(net_pd_amt_fnl) as net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_no_denied_${paid_thru}
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
	, primary_diag_cd
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, product_level_3_fnl
	, national_pilot_flag
	, clm_dnl_f
)
select
	*
	, iff(sum(allw_amt_fnl) over (partition by visit_id, fst_srvc_dt, category_2)  > 0.01, 'Paid', 'Denied') as claim_status
from aggregated
;


-- VPE 1
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_no_denied_${paid_thru} as
select
    entity
    , concat(mbi, '-', category_2) as mbi_key
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , min(hcta_paid_dt) as min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_no_denied_${paid_thru}
group by
    entity
    , concat(mbi, '-', category_2)
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
;


-- VPE 2 - Mark new episode
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_no_denied_${paid_thru} as
select
	mbi_key
	, entity
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
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
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_no_denied_${paid_thru}
;


-- VPE 3 - Episodes
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_no_denied_${paid_thru} as
with ep_numbering as
(
select
	*
  	, sum(iff(prev_srvc_dt is null, 1, ep_flag)) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt rows between unbounded preceding and current row)
  	as cmltv_episodes
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_no_denied_${paid_thru}
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
	, visit_id
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , allowed
    , paid
from ep_numbering
;


-- Episodes summary
create or replace table tmp_1m.knd_mbm_episodes_summary_no_denied_${paid_thru} as
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
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
	, clm_dnl_f
	, 0 as visit_ep_runout_month
	, 0 as visit_runout_month
	, sum(ep_flag) as n_episodes
	, 0 as n_visits
	, 0 as sum_allowed
	, 0 as sum_paid
	, 0 as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_no_denied_${paid_thru}
where ep_flag = 1
group by
	to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy')
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
	, ep_hcta_paid_dt
	, entity
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
	, clm_dnl_f
;


-- Visits summary
create or replace table tmp_1m.knd_mbm_visits_summary_no_denied_${paid_thru} as
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
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5) as visit_ep_runout_month
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5) as visit_runout_month
    , 0 as n_episodes
    , count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
    , sum(allowed) as sum_allowed
    , sum(paid) as sum_paid
    , count(distinct mbi_key) as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_no_denied_${paid_thru}
group by
    to_char(ep_start_dt, 'yyyyMM')
    , to_char(ep_start_dt, 'yyyy')
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
    , fst_srvc_month
    , fst_srvc_year
    , min_hcta_paid_dt
    , entity
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5)
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5)
;


-- Stack VISITS and EPISODES
create or replace table tmp_1m.knd_mbm_visits_episodes_stacked_no_denied_${paid_thru} as
select * from tmp_1m.knd_mbm_visits_summary_no_denied_${paid_thru}
union all
select * from tmp_1m.knd_mbm_episodes_summary_no_denied_${paid_thru}
;


-- VPE Summary
create or replace table tmp_1m.knd_mbm_vpe_summary_no_denied_${paid_thru} as
select
    ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , visit_ep_runout_month
    , visit_runout_month
    , sum(n_episodes) as total_episodes
    , sum(n_visits) as total_visits
    , sum(sum_allowed) as allowed
    , sum(sum_paid) as paid
    , sum(mbr_count) as mbr_count
from tmp_1m.knd_mbm_visits_episodes_stacked_no_denied_${paid_thru}
where population != 'NA'
group by
    ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , visit_ep_runout_month
    , visit_runout_month
;


-- Extract
create or replace table tmp_1m.knd_mbm_visits_episodes_extract_no_denied_${paid_thru} as
select
	population
	, prov_tin
	, optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , category_1 as category
    , market_fnl
    , sum(mbr_count) as unique_member_count
    , sum(total_episodes) as episode_count
    , sum(total_visits) as visit_count
    , sum(allowed) as allowed
from tmp_1m.knd_mbm_vpe_summary_no_denied_${paid_thru}
where ep_start_month >= '202501'
group by
	population
	, prov_tin
	, optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , category_1
    , market_fnl
;


-- ============================================================
-- WITH DENIED: Same pipeline as no_denied but includes denied claims
-- (clm_dnl_f / dnl_f filter commented out)
-- ============================================================

-- COSMOS claims (with denied)
create or replace table tmp_1m.knd_mbm_cosmos_claims_with_denied_${paid_thru} as
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
    , a.clm_dnl_f
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.glxy_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
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
	-- and a.clm_dnl_f not in ('D', 'Y')  -- COMMENTED OUT: include denied claims
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.clm_dnl_f
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.glxy_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
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
	-- and a.clm_dnl_f not in ('D', 'Y')  -- COMMENTED OUT: include denied claims
;


-- CSP claims (with denied)
create or replace table tmp_1m.knd_mbm_csp_claims_with_denied_${paid_thru} as
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.clm_dnl_f
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.dcsp_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on substring(a.tin, 1 , 9) = b.tin
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
	-- and a.clm_dnl_f not in ('D', 'Y')  -- COMMENTED OUT: include denied claims
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
	, a.clm_dnl_f
    , a.allw_amt_fnl
	, a.net_pd_amt_fnl
from fichsrv.dcsp_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on substring(a.tin, 1 , 9) = b.tin
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
	-- and a.clm_dnl_f not in ('D', 'Y')  -- COMMENTED OUT: include denied claims
;


-- NICE claims (with denied)
create or replace table tmp_1m.knd_mbm_nice_claims_with_denied_${paid_thru} as
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
    , a.dnl_f as clm_dnl_f
	, a.calc_allw as allw_amt_fnl
	, a.calc_net_pd as net_pd_amt_fnl
from fichsrv.nce_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.tin = b.tin
where a.brand_fnl = 'M&R'
	and a.plan_level_2_fnl not in ('PFFS')
	and a.prov_prtcp_sts_cd = 'P'
	and a.st_abbr_cd = a.market_fnl
	and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
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
	-- and a.dnl_f not in ('D', 'Y')  -- COMMENTED OUT: include denied claims
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
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
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
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
    	else 'National'
    end as national_pilot_flag
    , a.dnl_f as clm_dnl_f
	, a.allw_amt as allw_amt_fnl
	, a.net_pd_amt as net_pd_amt_fnl
from fichsrv.nce_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.tin = b.tin
where a.brand_fnl = 'M&R'
	and a.plan_level_2_fnl not in ('PFFS')
	and a.prov_prtcp_sts_cd = 'P'
	and a.st_abbr_cd = a.market_fnl
	and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN'))
	and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
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
	-- and a.dnl_f not in ('D', 'Y')  -- COMMENTED OUT: include denied claims
;


-- Stack COSMOS + CSP + NICE claims (with denied)
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_with_denied_${paid_thru} as
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
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
 	, clm_dnl_f
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_claims_with_denied_${paid_thru}
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
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
 	, clm_dnl_f
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_csp_claims_with_denied_${paid_thru}
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
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
 	, clm_dnl_f
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.knd_mbm_nice_claims_with_denied_${paid_thru}
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


-- Aggregate (with denied)
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_with_denied_${paid_thru} as
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
	, primary_diag_cd
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, product_level_3_fnl
	, national_pilot_flag
	, clm_dnl_f
    , sum(allw_amt_fnl) as allw_amt_fnl
    , sum(net_pd_amt_fnl) as net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_with_denied_${paid_thru}
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
	, primary_diag_cd
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, product_level_3_fnl
	, national_pilot_flag
	, clm_dnl_f
)
select
	*
	, iff(sum(allw_amt_fnl) over (partition by visit_id, fst_srvc_dt, category_2)  > 0.01, 'Paid', 'Denied') as claim_status
from aggregated
;


-- VPE 1 (with denied)
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_with_denied_${paid_thru} as
select
    entity
    , concat(mbi, '-', category_2) as mbi_key
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , min(hcta_paid_dt) as min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_with_denied_${paid_thru}
group by
    entity
    , concat(mbi, '-', category_2)
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
;


-- VPE 2 - Mark new episode (with denied)
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_with_denied_${paid_thru} as
select
	mbi_key
	, entity
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
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
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_with_denied_${paid_thru}
;


-- VPE 3 - Episodes (with denied)
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_with_denied_${paid_thru} as
with ep_numbering as
(
select
	*
  	, sum(iff(prev_srvc_dt is null, 1, ep_flag)) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt rows between unbounded preceding and current row)
  	as cmltv_episodes
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_with_denied_${paid_thru}
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
	, visit_id
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , allowed
    , paid
from ep_numbering
;


-- Episodes summary (with denied)
create or replace table tmp_1m.knd_mbm_episodes_summary_with_denied_${paid_thru} as
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
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
	, clm_dnl_f
	, 0 as visit_ep_runout_month
	, 0 as visit_runout_month
	, sum(ep_flag) as n_episodes
	, 0 as n_visits
	, 0 as sum_allowed
	, 0 as sum_paid
	, 0 as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_with_denied_${paid_thru}
where ep_flag = 1
group by
	to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy')
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
	, ep_hcta_paid_dt
	, entity
	, category_2
	, category_1
	, prov_tin
	, optum_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
	, clm_dnl_f
;


-- Visits summary (with denied)
create or replace table tmp_1m.knd_mbm_visits_summary_with_denied_${paid_thru} as
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
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5) as visit_ep_runout_month
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5) as visit_runout_month
    , 0 as n_episodes
    , count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
    , sum(allowed) as sum_allowed
    , sum(paid) as sum_paid
    , count(distinct mbi_key) as mbr_count
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_with_denied_${paid_thru}
group by
    to_char(ep_start_dt, 'yyyyMM')
    , to_char(ep_start_dt, 'yyyy')
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
    , fst_srvc_month
    , fst_srvc_year
    , min_hcta_paid_dt
    , entity
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5)
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5)
;


-- Stack VISITS and EPISODES (with denied)
create or replace table tmp_1m.knd_mbm_visits_episodes_stacked_with_denied_${paid_thru} as
select * from tmp_1m.knd_mbm_visits_summary_with_denied_${paid_thru}
union all
select * from tmp_1m.knd_mbm_episodes_summary_with_denied_${paid_thru}
;


-- VPE Summary (with denied)
create or replace table tmp_1m.knd_mbm_vpe_summary_with_denied_${paid_thru} as
select
    ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , visit_ep_runout_month
    , visit_runout_month
    , sum(n_episodes) as total_episodes
    , sum(n_visits) as total_visits
    , sum(sum_allowed) as allowed
    , sum(sum_paid) as paid
    , sum(mbr_count) as mbr_count
from tmp_1m.knd_mbm_visits_episodes_stacked_with_denied_${paid_thru}
where population != 'NA'
group by
    ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , category_2
    , category_1
    , prov_tin
    , optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , clm_dnl_f
    , visit_ep_runout_month
    , visit_runout_month
;


-- Extract (with denied)
create or replace table tmp_1m.knd_mbm_visits_episodes_extract_with_denied_${paid_thru} as
select
	population
	, prov_tin
	, optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , category_1 as category
    , market_fnl
    , sum(mbr_count) as unique_member_count
    , sum(total_episodes) as episode_count
    , sum(total_visits) as visit_count
    , sum(allowed) as allowed
from tmp_1m.knd_mbm_vpe_summary_with_denied_${paid_thru}
where ep_start_month >= '202501'
group by
	population
	, prov_tin
	, optum_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , category_1
    , market_fnl
;


-- ============================================================
-- COMPARISON QUERIES: with_denied vs no_denied
-- By population, optum_tin_flag | ep_start_month >= '202501'
-- ============================================================

-- 1. Compare AGGREGATED tables
-- Note: aggregated doesn't have ep_start_month, so we filter on fst_srvc_month >= '202501'
select
    'aggregated' as table_name
    , a.population
    , a.optum_tin_flag
    , a.nd_allowed
    , b.wd_allowed
    , coalesce(b.wd_allowed, 0) - coalesce(a.nd_allowed, 0) as delta_allowed
    , a.nd_rows
    , b.wd_rows
    , coalesce(b.wd_rows, 0) - coalesce(a.nd_rows, 0) as delta_rows
from (
    select population, optum_tin_flag
        , sum(allw_amt_fnl) as nd_allowed
        , count(*) as nd_rows
    from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_no_denied_${paid_thru}
    where fst_srvc_month >= '202501'
    group by population, optum_tin_flag
) as a
full outer join (
    select population, optum_tin_flag
        , sum(allw_amt_fnl) as wd_allowed
        , count(*) as wd_rows
    from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_with_denied_${paid_thru}
    where fst_srvc_month >= '202501'
    group by population, optum_tin_flag
) as b
    on a.population = b.population
    and a.optum_tin_flag = b.optum_tin_flag
order by a.population, a.optum_tin_flag
;


-- 2. Compare VPE_3 tables
select
    'vpe_3' as table_name
    , coalesce(a.population, b.population) as population
    , coalesce(a.optum_tin_flag, b.optum_tin_flag) as optum_tin_flag
    , a.nd_allowed
    , b.wd_allowed
    , coalesce(b.wd_allowed, 0) - coalesce(a.nd_allowed, 0) as delta_allowed
    , a.nd_visits
    , b.wd_visits
    , coalesce(b.wd_visits, 0) - coalesce(a.nd_visits, 0) as delta_visits
    , a.nd_episodes
    , b.wd_episodes
    , coalesce(b.wd_episodes, 0) - coalesce(a.nd_episodes, 0) as delta_episodes
    , a.nd_members
    , b.wd_members
    , coalesce(b.wd_members, 0) - coalesce(a.nd_members, 0) as delta_members
from (
    select population, optum_tin_flag
        , sum(allowed) as nd_allowed
        , count(distinct concat(visit_id, fst_srvc_dt)) as nd_visits
        , sum(ep_flag) as nd_episodes
        , count(distinct mbi_key) as nd_members
    from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_no_denied_${paid_thru}
    where to_char(ep_start_dt, 'yyyyMM') >= '202501'
    group by population, optum_tin_flag
) as a
full outer join (
    select population, optum_tin_flag
        , sum(allowed) as wd_allowed
        , count(distinct concat(visit_id, fst_srvc_dt)) as wd_visits
        , sum(ep_flag) as wd_episodes
        , count(distinct mbi_key) as wd_members
    from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_with_denied_${paid_thru}
    where to_char(ep_start_dt, 'yyyyMM') >= '202501'
    group by population, optum_tin_flag
) as b
    on a.population = b.population
    and a.optum_tin_flag = b.optum_tin_flag
order by coalesce(a.population, b.population), coalesce(a.optum_tin_flag, b.optum_tin_flag)
;


-- 3. Compare VPE_SUMMARY tables
select
    'vpe_summary' as table_name
    , coalesce(a.population, b.population) as population
    , coalesce(a.optum_tin_flag, b.optum_tin_flag) as optum_tin_flag
    , a.nd_allowed
    , b.wd_allowed
    , coalesce(b.wd_allowed, 0) - coalesce(a.nd_allowed, 0) as delta_allowed
    , a.nd_visits
    , b.wd_visits
    , coalesce(b.wd_visits, 0) - coalesce(a.nd_visits, 0) as delta_visits
    , a.nd_episodes
    , b.wd_episodes
    , coalesce(b.wd_episodes, 0) - coalesce(a.nd_episodes, 0) as delta_episodes
    , a.nd_members
    , b.wd_members
    , coalesce(b.wd_members, 0) - coalesce(a.nd_members, 0) as delta_members
from (
    select population, optum_tin_flag
        , sum(allowed) as nd_allowed
        , sum(total_visits) as nd_visits
        , sum(total_episodes) as nd_episodes
        , sum(mbr_count) as nd_members
    from tmp_1m.knd_mbm_vpe_summary_no_denied_${paid_thru}
    where ep_start_month >= '202501'
    group by population, optum_tin_flag
) as a
full outer join (
    select population, optum_tin_flag
        , sum(allowed) as wd_allowed
        , sum(total_visits) as wd_visits
        , sum(total_episodes) as wd_episodes
        , sum(mbr_count) as wd_members
    from tmp_1m.knd_mbm_vpe_summary_with_denied_${paid_thru}
    where ep_start_month >= '202501'
    group by population, optum_tin_flag
) as b
    on a.population = b.population
    and a.optum_tin_flag = b.optum_tin_flag
order by coalesce(a.population, b.population), coalesce(a.optum_tin_flag, b.optum_tin_flag)
;


-- 4. Compare EXTRACT tables (already filtered to ep_start_month >= '202501' in table definition)
select
    'extract' as table_name
    , coalesce(a.population, b.population) as population
    , coalesce(a.optum_tin_flag, b.optum_tin_flag) as optum_tin_flag
    , a.nd_allowed
    , b.wd_allowed
    , coalesce(b.wd_allowed, 0) - coalesce(a.nd_allowed, 0) as delta_allowed
    , a.nd_visits
    , b.wd_visits
    , coalesce(b.wd_visits, 0) - coalesce(a.nd_visits, 0) as delta_visits
    , a.nd_episodes
    , b.wd_episodes
    , coalesce(b.wd_episodes, 0) - coalesce(a.nd_episodes, 0) as delta_episodes
    , a.nd_members
    , b.wd_members
    , coalesce(b.wd_members, 0) - coalesce(a.nd_members, 0) as delta_members
from (
    select population, optum_tin_flag
        , sum(allowed) as nd_allowed
        , sum(visit_count) as nd_visits
        , sum(episode_count) as nd_episodes
        , sum(unique_member_count) as nd_members
    from tmp_1m.knd_mbm_visits_episodes_extract_no_denied_${paid_thru}
    group by population, optum_tin_flag
) as a
full outer join (
    select population, optum_tin_flag
        , sum(allowed) as wd_allowed
        , sum(visit_count) as wd_visits
        , sum(episode_count) as wd_episodes
        , sum(unique_member_count) as wd_members
    from tmp_1m.knd_mbm_visits_episodes_extract_with_denied_${paid_thru}
    group by population, optum_tin_flag
) as b
    on a.population = b.population
    and a.optum_tin_flag = b.optum_tin_flag
order by coalesce(a.population, b.population), coalesce(a.optum_tin_flag, b.optum_tin_flag)
;
