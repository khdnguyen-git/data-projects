-- Remake MnR Readmission tables



-- Make admit table, include sbscr_nbr
create or replace table tmp_1m.kn_mcr_readmits_base_202512 as
select 
	gal_mbi_hicn_fnl
	, ip.pd_dn_ol_admitid 
	, ip_status_code
	, Readmit_masteradmitid as readmit_master_admitid
	, indexadmitind as indexadmit_ind
	, readmit_ind
	, sbscr_nbr as gal_sbscr_nbr
/*REASON CODE FILEDS*/
	, rsn.rsn_cd 
	, rsn.rsn_cd_desc
/*ADMIT TYPES FIELDS*/
	, pd_dn_ol_tadm_admit_type as tadm_admit_type
	--, tadm_mdc
	, admit_drg_cd 
	, allw_amt_fnl 
	, fnl_drg_cd
	, clm_admit_type
	, fnl_admit_typ
	, proc_cd
	, primary_diag_cd
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, rvnu_cd
/*DOL FIELDS*/
	, dol.clm_dvlp_rsn_cd
	, dol.clm_dvlp_rev
	, dol.clm_dvlp_info_recv_dt
	, dol.cos_clos_clm_cd
	, dol.load_dt
	, dol.updt_dt
	, year(clm_dvlp_info_recv_dt) as info_recv_yr
	, year(updt_dt) as updt_yr
/*CLAIM-RELATED FIELDS*/
	, ip.site_clm_aud_nbr
	, ip.component
	, ip.sub_aud_nbr
	, ip.dtl_ln_nbr
	, clm_rec_cd
	, eventkey	
	, to_char(pd_dn_ol_admit_start_dt, 'yyyyMM') as admit_yr_month
	, pd_dn_ol_admit_start_dt as admit_start_dt
	, pd_dn_ol_admit_end_dt as admit_end_dt
	, fst_srvc_dt
	, erly_srvc_qtr
	, erly_srvc_dt
	, dschrg_sts_cd
	, catgy_rol_up_2_desc
	, brand_fnl
	, clm_dnl_f
	, prov_tin
	, mpin
	, site_cd
/*CLAIM ADJUDICATION FIELDS*/
	, bil_recv_dt
	, adjd_qtr
	, adjd_dt
	, clm_pd_dt
	, clm_lvl_rsn_cd_sys_id
	, srvc_lvl_rsn_cd_sys_id
/*DEMOGRAPHIC FIELDS*/
	, plan_level_2_fnl
	, product_level_3_fnl
	, region_fnl
	, market_fnl
	, fin_submarket
	, global_cap
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, groupnumber
	, segment_name_fnl
	, contractpbp_fnl
	, contract_fnl
	, case when product_level_3_fnl != 'INSTITUTIONAL' and global_cap = 'NA' and tfm_include_flag = 1 then 1 
		else 0 
	end as mnr_risk_ind
/*REVIEW REASON CODES*/
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
	, ip.cos_clm_head_sys_id 
	, ip.cos_clm_head_sys_id_orgnl 
	, ip.cos_clm_srvc_sys_id 
	, case when rsn.rsn_cd in (274, 279, 381, 459, 504, 695, 894, 1018, 1139, 1569, 1575, 1581) then 1 
		else 0 
	end as rsncd_ever_mcr_dnl_ind
	, case when (rsn.rsn_cd in  ('1087', '1098', '1099', '0380') or dol.clm_dvlp_rsn_cd in ('1087', '1098', '1099', '0380')) then 1 
		else 0 
	end as dol_rsncd_merged_mcr_touch_ind
	, case when rsn.rsn_cd in ('1087', '1098', '1099', '0380') then 1 
		else 0 
	end as rsncd_mcr_touch_ind
	, case when dol.clm_dvlp_rsn_cd in ('1087', '1098', '1099', '0380') then 1 
		else 0 
	end as dol_mcr_touch_ind
	, case when rsn.rsn_cd_sys_id is null then 0 
		else 1 
	end as rsncd_match_ind
	, case when dol.cos_clm_head_sys_id is null then 0 
		else 1 
	end as dol_match_ind
	, dense_rank() over (partition by pd_dn_ol_admitid, gal_mbi_hicn_fnl order by adjd_dt desc) as latest_clm_entry
	, proc_mod1_cd
from 	
   tadm_tre_cpy.glxy_ip_admit_f_202512 as ip
left outer join fichsrv.tadm_glxy_reason_code as rsn
	on ip.fnl_rsn_cd_sys_id = rsn.rsn_cd_sys_id 
left outer join hce_ops_fnl.cosmos_dol_202511 as dol
	on trim(ip.cos_clm_head_sys_id_orgnl)  = dol.cos_clm_head_sys_id 
where clm_admit_type = 'ACUTE' and to_char(pd_dn_ol_admit_start_dt, 'yyyyMM')
--AND ip_status_code<>'OL'
;




create or replace table tmp_1m.kn_mcr_readmits_rollup_inds_202512 as 
select 
	tadm_admit_type 
	, pd_dn_ol_admitid
	, gal_mbi_hicn_fnl
	, gal_sbscr_nbr
	, product_level_3_fnl
	, market_fnl
	, global_cap
	, tfm_include_flag
	, migration_source
	, brand_fnl
	, rsn_cd
	, rsn_cd_dec
	, max(rsncd_ever_mcr_dnl_ind) as rsncd_ever_mcr_dnl_ind
	, max(dol_rsncd_merged_mcr_touch_ind) as dol_rsncd_merged_mcr_touch_ind
	, max(rsncd_mcr_touch_ind) as rsncd_mcr_touch_ind
	, max(dol_mcr_touch_ind) as dol_mcr_touch_ind
	, sum(allw_amt_fnl) admit_allw
from tmp_1m.kn_mcr_readmits_base as a
group by 
	tadm_admit_type 
	, pd_dn_ol_admitid
	, gal_mbi_hicn_fnl
	, gal_sbscr_nbr
	, product_level_3_fnl
	, market_fnl
	, global_cap
	, tfm_include_flag
	, migration_source
	, brand_fnl
	, rsn_cd
	, rsn_cd_dec
;

select * from tmp_1m.kn_mnr_mcr_readmits_1_202512
limit 100

-- Exclude Overlap Admits  + Add unit cost
create or replace table tmp_1m.kn_mnr_mcr_readmits_1_202512 as
select distinct 
 	 a.tadm_admit_type
 	, a.site_clm_aud_nbr
	, a.pd_dn_ol_admitid
	, a.gal_mbi_hicn_fnl
	, a.gal_sbscr_nbr
	, a.admit_yr_month
--	, to_char(a.adjd_dt,'yyyyMM') adjd_yrmonth
	, substr(a.admit_yr_month, 1, 4) as admit_yr
	, a.admit_drg_cd
	, a.ip_status_code
	, case when b.rsncd_ever_mcr_dnl_ind = 1 and b.dol_rsncd_merged_mcr_touch_ind = 1 then 1 
		else 0 
	end as mcr_ever_dnl_readmit
	, case when a.latest_clm_entry = 1 and a.ip_status_code= 'DN' and b.rsncd_ever_mcr_dnl_ind = 1 and b.dol_rsncd_merged_mcr_touch_ind = 1 then 1 
		else 0 
	end as still_mcr_dnl_ind
	, case when a.latest_clm_entry = 1 and a.ip_status_code= 'DN' and b.rsncd_ever_mcr_dnl_ind = 1 then 1 
		else 0 
	end as still_mcr_dnl_w_nodol_ind
	, case when b.dol_rsncd_merged_mcr_touch_ind = 1 then 1 
		else 0 
	end as mcr_touch_ind
	, a.indexadmitind
	, a.readmit_ind
	, b.admit_allw
	, a.product_level_3_fnl
	, a.market_fnl
	, a.global_cap
	, a.tfm_include_flag
	, a.migration_source
	, a.brand_fnl
	, c.unit_cost
from 
	tmp_1m.kn_mcr_readmits_base_202512 as a
inner join tmp_1m.kn_mcr_readmits_rollup_inds_202512 as b
	on a.pd_dn_ol_admitid = b.pd_dn_ol_admitid
	and a.ip_status_code != 'OL' and latest_clm_entry = 1 and a.mnr_risk_ind = 1
left outer join HCE_OPS_FNL.HCEOPS_DRG_Unit_cost_IP_2025Q3 as c
	on a.admit_drg_cd = c.admit_drg_cd
	on substr(a.admit_yr_month, 1, 4) = concat('20', c."YEAR")
;


select * from hce_ops_fnl.hceops_drg_unit


select count(*) from tmp_1m.kn_mnr_mcr_readmits_1_202512;
-- 4,826,307


create or replace table ving_prd_trend_db.tmp_1m.kn_mcr_2024_202512 as
select distinct
	a.work_item_id as mcr_work_item_id
	, regexp_substr(a.work_item_id, '^[^-]+') as mcr_work_item_id_cleaned -- anything before -
	, a.member_id as mcr_member_id
	, a.u_div as mcr_u_div
	, a.claim_id as mcr_claim_id
	, a.mcr_disposition_code as mcr_decision
	, to_char(uhg_received_date, 'yyyymm') as mcr_received_month
	, to_char(uhg_received_date, 'yyyy') as mcr_received_year
	, a.account as mcr_account
	, a.active as mcr_active
	, a.active_account_escalation as mcr_active_account_escalation
	, a.active_escalation as mcr_active_escalation
	, a.activity_due as mcr_activity_due
	, a.additional_assignee_list as mcr_additional_assignee_list
	, a.age_date as mcr_age_date
	, a.age_date_central as mcr_age_date_central
	, a.approval as mcr_approval
	, a.approval_history as mcr_approval_history
	, a.asset as mcr_asset
	, a.assigned_on as mcr_assigned_on
	, a.assigned_to as mcr_assigned_to
	, a.assigned_to_user_id as mcr_assigned_to_user_id
	, a.assignment_batch as mcr_assignment_batch
	, a.assignment_group as mcr_assignment_group
	, a.auto_close as mcr_auto_close
	, a.business_area as mcr_business_area
	, a.business_duration as mcr_business_duration
	, a.business_segment as mcr_business_segment
	, a.business_service as mcr_business_service
	, a.calendar_duration as mcr_calendar_duration
	, a.cancel_comments as mcr_cancel_comments
	, a.case_report as mcr_case_report
	, a.cases as mcr_cases
	, a.category as mcr_category
	, a.cause as mcr_cause
	, a.caused_by as mcr_caused_by
	, a.changes as mcr_changes
	, a.close_notes as mcr_close_notes
	, a.closed_at as mcr_closed_at
	, a.closed_by as mcr_closed_by
	, a.cmdb_ci as mcr_cmdb_ci
	, a.comments as mcr_comments
	, a.comments_and_work_notes as mcr_comments_and_work_notes
	, a.company as mcr_company
	, a.company_days_aged as mcr_company_days_aged
	, a.consumer as mcr_consumer
	, a.contact as mcr_contact
	, a.contact_local_time as mcr_contact_local_time
	, a.contact_time_zone as mcr_contact_time_zone
	, a.contact_type as mcr_contact_type
	, a.contract as mcr_contract
	, a.correlation_display as mcr_correlation_display
	, a.correlation_id as mcr_correlation_id
	, a.created_central as mcr_created_central
	, a.current_age as mcr_current_age
	, a.delivery_plan as mcr_delivery_plan
	, a.delivery_task as mcr_delivery_task
	, a.dept_days_aged as mcr_dept_days_aged
	, a.description as mcr_description
	, a.due_date as mcr_due_date
	, a.entitlement as mcr_entitlement
	, a.escalate as mcr_escalate
	, a.escalation as mcr_escalation
	, a.ewd_time_worked as mcr_ewd_time_worked
	, a.expected_start as mcr_expected_start
	, a.first_response_time as mcr_first_response_time
	, a.firstpass_rework_indicator as mcr_firstpass_rework_indicator
	, a.follow_the_sun as mcr_follow_the_sun
	, a.follow_up as mcr_follow_up
	, a.geostate as mcr_geostate
	, a.group_list as mcr_group_list
	, a.impact as mcr_impact
	, a.import_source as mcr_import_source
	, a.knowledge as mcr_knowledge
	, a.location as mcr_location
	, a.made_sla as mcr_made_sla
	, a.non_workable_to_workable as mcr_non_workable_to_workable
	, a.notes_to_comments as mcr_notes_to_comments
	, a.notify as mcr_notify
	, a.number as mcr_number
	, a.opened_at as mcr_opened_at
	, a.opened_by as mcr_opened_by
	, a.orders as mcr_orders
	, a.parent as mcr_parent
	, a.partner as mcr_partner
	, a.partner_contact as mcr_partner_contact
	, a.planning_queue as mcr_planning_queue
	, a.primary_skill as mcr_primary_skill
	, a.priority as mcr_priority
	, a.priority_classification as mcr_priority_classification
	, a.proactive as mcr_proactive
	, a.problem as mcr_problem
	, a.product as mcr_product
	, a.project as mcr_project
	, a.reassignment_count as mcr_reassignment_count
	, a.requestor_comments as mcr_requestor_comments
	, a.resolution_code as mcr_resolution_code
	, a.resolution_comments as mcr_resolution_comments
	, a.resolved_at as mcr_resolved_at
	, a.resolved_by as mcr_resolved_by
	, a.route_reason as mcr_route_reason
	, a.routing_date as mcr_routing_date
	, a.service_offering as mcr_service_offering
	, a.short_description as mcr_short_description
	, a.skill_id as mcr_skill_id
	, a.skills as mcr_skills
	, a.sla_due as mcr_sla_due
	, a.sn_app_cs_social_social_profile as mcr_sn_app_cs_social_social_profile
	, a.source_system as mcr_source_system
	, a.special_processing as mcr_special_processing
	, a.special_processing_rule as mcr_special_processing_rule
	, a.state as mcr_state
	, a.subcategory as mcr_subcategory
	, a.support_manager as mcr_support_manager
	, a.sync_driver as mcr_sync_driver
	, a.sys_class_name as mcr_sys_class_name
	, a.sys_created_by as mcr_sys_created_by
	, a.sys_created_on as mcr_sys_created_on
	, a.sys_domain as mcr_sys_domain
	, a.sys_domain_path as mcr_sys_domain_path
	, a.sys_mod_count as mcr_sys_mod_count
	, a.sys_tags as mcr_sys_tags
	, a.sys_updated_by as mcr_sys_updated_by
	, a.sys_updated_on as mcr_sys_updated_on
	, a.task_effective_number as mcr_task_effective_number
	, a.time_worked as mcr_time_worked
	, a.tw_min_dec as mcr_tw_min_dec
	, a.tw_sec_dec as mcr_tw_sec_dec
	, a.u_current_inhouse_age as mcr_u_current_inhouse_age
	, a.u_due_date_reporting as mcr_u_due_date_reporting
	, a.u_member as mcr_u_member
	, a.u_percent_complete as mcr_u_percent_complete
	, a.u_provider as mcr_u_provider
	, a.u_region as mcr_u_region
	, a.u_resolved_date_reporting as mcr_u_resolved_date_reporting
	, a.u_updated_date_reporting as mcr_u_updated_date_reporting
	, a.u_warehoused_on as mcr_u_warehoused_on
	, a.u_workitem_level as mcr_u_workitem_level
	, a.uhc_dept_received_date as mcr_uhc_dept_received_date
	, a.uhg_received_date as mcr_uhg_received_date
	, a.universal_request as mcr_universal_request
	, a.upon_approval as mcr_upon_approval
	, a.upon_reject as mcr_upon_reject
	, a.urgency as mcr_urgency
	, a.user_input as mcr_user_input
	, a.warehouse_issued as mcr_warehouse_issued
	, a.watch_list as mcr_watch_list
	, a.work_end as mcr_work_end
	, a.work_item_type as mcr_work_item_type
	, a.work_notes as mcr_work_notes
	, a.work_notes_list as mcr_work_notes_list
	, a.work_start as mcr_work_start
	, a.work_warehouse_issued as mcr_work_warehouse_issued
	, a.work_warehoused as mcr_work_warehoused
	, a.x_uhgen_ewr_work_rule_log as mcr_x_uhgen_ewr_work_rule_log
	, a.tpsm_description as mcr_tpsm_description
	, a.custom_identifier as mcr_custom_identifier
	, a.policy_number as mcr_policy_number
	, a.enterprisenow_task as mcr_enterprisenow_task
	, a.enterprisenow_issue_number as mcr_enterprisenow_issue_number
	, a.resolution_status as mcr_resolution_status
	, a.additional_comments as mcr_additional_comments
	, a.seq_no as mcr_seq_no
	, a.primary as mcr_primary
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work as a
where to_char(uhg_received_date, 'yyyy') >= '2024'
		and member_id is not null
;


create or replace table tmp_1m.kn_mcr_readmission_tre_2024_202512 as
with tre_sbscr as (
select distinct
	gal_sbscr_nbr
from fichsrv.tre_membership
)
, mcr_readmit_flag as (
select
	mcr_member_id
	, mcr_work_item_id_cleaned
    , max(case when mcr_work_item_type ilike '%readmission%' then 1 
    	else 0 
    end) as mcr_ever_readmit_ind
from tmp_1m.kn_mcr_2024_202512
where mcr_import_source = 'COSMOS' 
group by 1,2
)
, mcr_earliest as ( -- or latest status? Choosing earliest since some work_item_id flipped from Readmission to others
select
    mcr_work_item_type
    , mcr_business_area
    , mcr_business_segment
    , mcr_import_source
    , mcr_work_item_id_cleaned
    , mcr_member_id
    , mcr_received_month
    , mcr_received_year
from tmp_1m.kn_mcr_2024_202512
where mcr_import_source = 'COSMOS' 
qualify row_number() over (partition by mcr_member_id, mcr_work_item_id order by mcr_resolved_at) = 1
)
select
    case when c.mcr_ever_readmit_ind = 1 then 'Readmission'
        else a.mcr_work_item_type -- 79238772-
    end as mcr_work_item_type
    , a.mcr_business_area
    , a.mcr_business_segment
    , a.mcr_import_source
    , a.mcr_work_item_id_cleaned
    , a.mcr_member_id
    , a.mcr_received_month
    , a.mcr_received_year
    , c.mcr_ever_readmit_ind
    , b.gal_sbscr_nbr
from mcr_earliest as a
inner join tre_sbscr as b
	on a.mcr_member_id = substring(b.gal_sbscr_nbr, 3)
left join mcr_readmit_flag as c
	on a.mcr_work_item_id = c.mcr_work_item_id
;




create or replace table tmp_1m.kn_mcr_join_mnr_readmit_2024_202512 as
with mcr as ( 
select
	*
from tmp_1m.kn_mcr_2024_202512 
where mcr_work_item_type ilike '%readmission%'
)
,
mnr as (
select 
    b.gal_sbscr_nbr
    , b.gal_mbi_hicn_fnl
    , b.pd_dn_ol_admitid
    , b.ip_status_code
    , b.readmit_master_admitid
    , b.indexadmit_ind
    , max(b.readmit_ind) as readmit_ind
    , b.rsn_cd
    , b.rsn_cd_desc
    , b.allw_amt_fnl
    , b.clm_admit_type
    , b.fnl_admit_typ
    , b.proc_cd
    , b.clm_dvlp_rsn_cd
    , b.cos_clm_head_sys_id
    , substr(b.admit_yr_month, 1, 4) as admit_yr
    , b.admit_yr_month
    , b.admit_start_dt
    , b.admit_end_dt
    , b.fst_srvc_dt
    , b.bil_recv_dt
    , b.brand_fnl
    , b.prov_tin
    , b.mpin
    , b.product_level_3_fnl
    , b.market_fnl
    , b.rsncd_ever_mcr_dnl_ind
    , b.dol_rsncd_merged_mcr_touch_ind
    , b.rsncd_mcr_touch_ind
    , b.dol_mcr_touch_ind
    , b.rsncd_match_ind
    , b.dol_match_ind
from tmp_1m.kn_mcr_readmits_base_202512 as b
)
select distinct
	a.mcr_work_item_id_cleaned
    , a.mcr_member_id
    , a.mcr_u_div
    , a.mcr_uhg_received_date
    , a.mcr_received_month
    , a.mcr_received_year
    , a.mcr_decision
    , a.mcr_active
    , a.mcr_state
    , a.mcr_resolution_code
  	, a.mcr_routing_date
  	, a.mcr_escalation
	, a.mcr_work_item_type
	, a.mcr_business_area
    , a.mcr_business_segment
    , a.mcr_import_source
    , b.gal_sbscr_nbr
    , b.gal_mbi_hicn_fnl
    , b.pd_dn_ol_admitid
    , b.ip_status_code
    , b.readmit_master_admitid
    , b.indexadmit_ind
    , b.readmit_ind
    , b.rsn_cd
    , b.rsn_cd_desc
    , b.allw_amt_fnl
    , b.clm_admit_type
    , b.fnl_admit_typ
    , b.proc_cd
    , b.clm_dvlp_rsn_cd
    , b.cos_clm_head_sys_id
    , substr(b.admit_yr_month, 1, 4) as admit_yr
    , b.admit_yr_month
    , b.admit_start_dt
    , b.admit_end_dt
    , b.fst_srvc_dt
    , b.bil_recv_dt
    , b.brand_fnl
    , b.prov_tin
    , b.mpin
    , b.product_level_3_fnl
    , b.market_fnl
    , b.rsncd_ever_mcr_dnl_ind
    , b.dol_rsncd_merged_mcr_touch_ind
    , b.rsncd_mcr_touch_ind
    , b.dol_mcr_touch_ind
    , b.rsncd_match_ind
    , b.dol_match_ind
from mcr as a
join mnr as b
	on a.mcr_work_item_id_cleaned = regexp_replace(b.site_clm_aud_nbr, '^[A-Z]+0*', '')
	and a.mcr_member_id = substring(b.gal_sbscr_nbr, 3)
	--and to_char(a.mcr_uhg_received_date, 'yyyy-MM-DD') = to_char(b.bil_recv_dt, 'yyyy-MM-DD')
;

--select to_char(bil_recv_dt, 'yyyy-MM-DD') from tmp_1m.kn_mcr_readmits_base_202512
--limit 100;
--
--select to_char(mcr_uhg_received_date, 'yyyy-MM-DD') from tmp_1m.KN_MCR_2024_202512 
--limit 100

select count(*) from tmp_1m.kn_mcr_join_mnr_readmit_2024_202512;
-- 14,020,731 -- w/o date join
-- 13,980,479 -- with date join
-- 1,320,729 -- with distinct

select count(distinct gal_sbscr_nbr) from tmp_1m.kn_mcr_join_mnr_readmit_2024_202512
where readmit_ind = 0
-- 144,610

select count(gal_sbscr_nbr) from tmp_1m.kn_mcr_join_mnr_readmit_2024_202512
where readmit_ind = 0
and mcr_received_year = '2024' or admit_yr = '2024'


create or replace table tmp_1m.kn_mcr_readmit_mnr_not_readmit_2024_v2 as
select 
	*
from tmp_1m.kn_mcr_join_mnr_readmit_2024_202512
where readmit_ind = 0
and mcr_received_year = '2024' or admit_yr = '2024'
;

select gal_sbscr_nbr, admit_yr_month  from tmp_1m.kn_mcr_readmit_mnr_not_readmit_2024_v2
where 

/*
GAL_SBSCR_NBR	ADMIT_YR_MONTH
00966001240	202412
00990941236	202406
00912678169	202410
00963759263	202402
00989162304	202403
00919196498	202311
00982367604	202402
00996914411	202401
00990944325	202410
00949139354	202404
*/

select distinct *
from TMP_1M.kn_mcr_readmits_base_202512
where gal_sbscr_nbr in 
(
'00966001240'
,'00990941236'
,'00912678169'
,'00963759263'
,'00989162304'
,'00919196498'
,'00982367604'
,'00996914411'
,'00990944325'
,'00949139354'
)



00963759263
select sbscr_nbr, admits, admit_start_dt, admit_end_dt, readmit_ind from tadm_tre_cpy.glxy_ip_admit_f_202512
where sbscr_nbr = '00963759263'


select distinct
	gal_sbscr_nbr
	, admit_yr_month
	, admit_start_dt
	, admit_end_dt
	, site_clm_aud_nbr 
	, pd_dn_ol_admitid 
	, indexadmit_ind
	, readmit_master_admitid
	, readmit_ind
from tmp_1m.kn_mcr_readmits_base_202512
where gal_sbscr_nbr = '00963759263'
order by admit_yr_month, pd_dn_ol_admitid, indexadmit_ind desc


select distinct
	a.mcr_member_id
	, a.mcr_work_item_id_cleaned
	, b.site_clm_aud_nbr 
	, a.mcr_received_month 
	, to_char(b.bil_recv_dt, 'yyyyMM') as bil_recv_month
	, b.admit_yr_month
	, b.admit_start_dt
	, b.admit_end_dt
	, b.pd_dn_ol_admitid 
	, b.indexadmit_ind
	, b.readmit_master_admitid
	, a.mcr_work_item_type
	, b.readmit_ind
from tmp_1m.kn_mcr_2024_202512 as a 
full join tmp_1m.kn_mcr_readmits_base_202512 as b
on a.mcr_member_id = substring(b.gal_sbscr_nbr, 3)
	--and a.mcr_work_item_id_cleaned = regexp_replace(b.site_clm_aud_nbr, '^[A-Z]+0*', '')
where mcr_member_id = '963759263'
order by mcr_received_month, bil_recv_month, admit_yr_month, mcr_work_item_id_cleaned, site_clm_aud_nbr, pd_dn_ol_admitid, indexadmit_ind desc


select
	*
from tmp_1m.kn_mcr_2024_202512
where mcr_member_id = '963759263' and mcr_work_item_id_cleaned in ('88337657', '88346555', '88372789')
order by mcr_work_item_id_cleaned




select distinct
	m
	, mcr_work_item_id_cleaned
	, to_char(bil_recv_dt, 'yyyyMM') as bil_recv_month
	, admit_yr_month
	, admit_start_dt
	, admit_end_dt
	, pd_dn_ol_admitid 
	, indexadmit_ind
	, readmit_master_admitid
	, readmit_ind
from tmp_1m.kn_mcr_join_mnr_readmit_2024_202512
where gal_sbscr_nbr = '00963759263'
order by bil_recv_month, admit_yr_month, pd_dn_ol_admitid, indexadmit_ind desc


from 
where mcr_work_item_type ilike '%readmission%'


select distinct
	gal_sbscr_nbr
	, bil_recv_dt
	, admit_start_dt
	, admit_end_dt
	, datediff('day', admit_start_dt, bil_recv_dt) as admit_bil_duration
from tmp_1m.kn_mcr_readmits_base_202512
where gal_sbscr_nbr in
(
'00963759263'
)
group by 1,2,3,4
order by bil_recv_dt, admit_start_dt




select distinct *
from tmp_1m.kn_mcr_readmits_base_202512
where gal_sbscr_nbr in
(
'00963759263'
)
order by bil_recv_dt, admit_start_dt


select distinct *
from tadm_tre_cpy.glxy_ip_admit_f_202512
where site_clm_aud_nbr like '%88450370%'
order by bil_recv_dt, admit_start_dt


select * from tmp_1m.kn_mcr_2024_202512
--where mcr_member_id = '963759263'
where mcr_work_item_id_cleaned like '%88450370%'

-- Example cases
-- 1
-- where mcr_work_item_id_cleaned = '88450370'
-- where site_clm_aud_nbr = 'FLA0088450370'



select distinct readmit_ind
from tadm_tre_cpy.glxy_ip_admit_f_202512 
where site_clm_aud_nbr = 'FLA0088450370'



