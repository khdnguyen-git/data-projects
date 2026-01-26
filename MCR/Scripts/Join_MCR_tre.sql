use secondary role all;


select to_char(uhg_received_date, 'yyyymm') from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work




create or replace table ving_prd_trend_db.tmp_1m.kn_mcr_2024 as
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
where to_char(uhg_received_date, 'yyyy') = '2024'
		and member_id is not null
;

-- Checking readmission
select 
	mcr_work_item_type
	, mcr_business_area
	, mcr_business_segment
	, mcr_import_source
	, count(distinct mcr_work_item_id)
from tmp_1m.kn_mcr_2024 
where mcr_work_item_type ilike 'readmission%'
	--and import_source = 'COSMOS'
group by 1, 2, 3, 4;
-- 250,877 if import_source = 'COSMOS'

-- Checking tre_membership for keys
select * from fichsrv.tre_membership 


select member_id from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where member_id like '%00968771010%'

select
	gal_sbscr_nbr
	, fin_brand
	, sgr_source_name
from fichsrv.tre_membership
group by 1,2,3
having count(*) > 1


select
	gal_sbscr_nbr
	, fin_brand
	, sgr_source_name
from fichsrv.TRE_MEMBERSHIP 
where gal_sbscr_nbr = '00904227280'



-- Join tre_membership with MCR table
-- MCR is limited to members who had a Readmission status
create or replace table tmp_1m.kn_mcr_readmission_tre_2024 as
with tre_sbscr as (
select distinct
	gal_sbscr_nbr
from fichsrv.tre_membership
)
, mcr_readmit_flag as (
select
	mcr_member_id
	, mcr_work_item_id
    , max(case when mcr_work_item_type ilike '%readmission%' then 1 
    	else 0 
    end) as mcr_ever_readmit_ind
from tmp_1m.kn_mcr_2024
where mcr_import_source = 'COSMOS' 
group by 1,2
)
, mcr_earliest as ( -- or latest status? Choosing earliest since some work_item_id flipped from Readmission to others
select
    mcr_work_item_type
    , mcr_business_area
    , mcr_business_segment
    , mcr_import_source
    , mcr_work_item_id
    , mcr_work_item_id_cleaned
    , mcr_member_id
    , mcr_received_month
    , mcr_received_year
from tmp_1m.kn_mcr_2024
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
    , a.mcr_work_item_id
    , a.mcr_work_item_id_cleaned
    , a.mcr_member_id
    , a.mcr_received_month
    , a.mcr_received_year
    , c.mcr_ever_readmit_ind
    , b.gal_sbscr_nbr
    , case when b.gal_sbscr_nbr is null then 'N' else 'Y' end as tre_join_matched
from mcr_earliest as a
left join tre_sbscr as b
	on a.mcr_member_id = substring(b.gal_sbscr_nbr, 3)
left join mcr_readmit_flag as c
	on a.mcr_work_item_id = c.mcr_work_item_id
;

select 
	mcr_work_item_type
	, mcr_business_area
	, mcr_business_segment
	, mcr_import_source
	, count(distinct mcr_work_item_id)
from tmp_1m.kn_mcr_2024 as a 
where a.mcr_work_item_type = 'Readmissions'
	and a.mcr_import_source = 'COSMOS'
group by 1, 2, 3, 4
-- 250,877

select 
	mcr_work_item_type
	, mcr_business_area
	, mcr_business_segment
	, mcr_import_source
	, mcr_matched
	, count(distinct mcr_work_item_id)
from tmp_1m.kn_mcr_readmission_tre_2024
where mcr_ever_readmit_ind = 1
group by 1, 2, 3, 4, 5
-- 250,877


-- IDs in source but not in final
--select mcr_work_item_id
--from tmp_1m.kn_mcr_2024
--where mcr_work_item_type = 'Readmissions'
--	and mcr_import_source = 'COSMOS'
--except
--select mcr_work_item_id
--from tmp_1m.kn_mcr_readmission_tre_2024
--where mcr_readmit_ind = 1;
--
--
--select * from tmp_1m.kn_mcr_2024
--where mcr_work_item_id = '79238772-'



-- Making 2024 MnR readmit table
create or replace table tmp_1m.kn_mnr_mcrreadmits_1_202510 as
select
	a.tadm_admit_type
	, a.gal_mbi_hicn_fnl
	, b.gal_sbscr_nbr 
	, a.pd_dn_ol_admitid
	, a.admit_yr
	, a.admit_yr_month
	, a.unit_cost
	, a.admit_allw
	, a.ip_status_code
	, a.mcr_touched as mcr_readmitreviews_touched
	, a.mcr_evr_denied_readmit as mcr_readmitreviews_ever_denied
	, a.still_mcr_dnl_ind
	, a.still_mcr_dnl_w_nodol_ind
	, a.readmit_ind as mnr_readmit_ind
	, a.indexadmitind as index_ind
	, a.product_level_3_fnl
	, a.market_fnl
	, a.global_cap
	, a.tfm_include_flag
	, a.migration_source
	, a.brand_fnl
from hce_ops_stage.hceops_mnr_mcrreadmts_1_202510 as a
left join fichsrv.tre_membership as b
	on a.gal_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
where a.readmit_ind = 1 and a.admit_yr = 2024
;

select count(distinct pd_dn_ol_admitid) from tmp_1m.kn_mnr_mcrreadmits_1_202510;
-- 217,259


select * from tmp_1m.kn_mnr_mcrreadmits_1_202510



select distinct service_catg from hce_ops_stage.hceops_mnr_mcrreadmts_1_202510


selet 




select
	readmit_ind
	, count(distinct gal_sbscr_nbr)
from tmp_1m.kn_mnr_mcrreadmits_1_202510
group by 1
-- 1	16961

select admit_yr_month from tmp_1m.kn_mnr_mcrreadmits_1_202510

-- 202406 our readmission
create or replace table tmp_1m.kn_mnr_mcrreadmits_202406 as
select
	*
from tmp_1m.kn_mnr_mcrreadmits_1_202510 as a
where admit_yr_month = '202406'
;

select count(distinct gal_sbscr_nbr) from tmp_1m.kn_mnr_mcrreadmits_202406
-- 16,887

-- 202406 MCR readmission
create or replace table tmp_1m.kn_mcr_readmission_tre_202406 as
select 
	*
from tmp_1m.kn_mcr_readmission_tre_2024
where mcr_received_month = '202406' and mcr_readmit_ind = 1 and mcr_matched = 'Y'

select count(distinct gal_sbscr_nbr) from tmp_1m.kn_mcr_readmission_tre_202406
-- 17,476
; 



select gal_sbscr_nbr
from tmp_1m.kn_mcr_readmission_tre_202406
except
select gal_sbscr_nbr
from tmp_1m.kn_mnr_mcrreadmits_202406
;


select * from tmp_1m.kn_mcr_2024
where mcr_member_id like '%921093106%'

select * from tmp_1m.kn_mnr_mcrreadmits_1_202510
where gal_sbscr_nbr = '00921093106'



-- 202406 our readmission
create or replace table tmp_1m.kn_mnr_mcrreadmits_202406 as
select
	*
from tmp_1m.kn_mnr_mcrreadmits_1_202510 as a
where admit_yr_month = '202406'
;

select count(distinct gal_sbscr_nbr) from tmp_1m.kn_mnr_mcrreadmits_202406
-- 16,887

-- 202406 MCR readmission
create or replace table tmp_1m.kn_mcr_readmission_tre_202406 as
select 
	*
from tmp_1m.kn_mcr_readmission_tre_2024
where mcr_received_month = '202406' and mcr_readmit_ind = 1 and mcr_matched = 'Y'
;

select count(distinct gal_sbscr_nbr) from tmp_1m.kn_mcr_readmission_tre_202406
-- 17476
; 
              

select count(*) from tmp_1m.kn_mnr_join_mcr_readmission_tre_2024


-- Join our Readmission table with MCR
-- Limited MCR to those who were flagged as readmission
create or replace table tmp_1m.kn_mnr_join_mcr_readmission_tre_2024 as
with mnr_readmit_2024 as (
select
	*
from tmp_1m.kn_mnr_mcrreadmits_1_202510
where admit_yr_month between '202401' and '202412'
)
,
mcr_readmit_2024 as (
select
	*
from tmp_1m.kn_mcr_readmission_tre_2024
where mcr_received_month between '202401' and '202412'
	and mcr_ever_readmit_ind = 1
	and tre_join_matched = 'Y'
)
select distinct
	a.mcr_work_item_id
	, a.mcr_work_item_type
	, a.mcr_business_area
	, a.mcr_business_segment
	, a.mcr_import_source
	, a.mcr_member_id
	, a.gal_sbscr_nbr as mcr_gal_sbscr_nbr
	, a.mcr_received_month
	, a.mcr_received_year
	, cast(a.mcr_ever_readmit_ind as varchar) as mcr_ever_readmit_ind
	, a.tre_join_matched
	, b.tadm_admit_type
	, b.gal_mbi_hicn_fnl
	, b.gal_sbscr_nbr as mnr_gal_sbscr_nbr
	, case when b.gal_sbscr_nbr is null then 'N' else 'Y'
	end as readmit_join_matched
	, b.pd_dn_ol_admitid
	, b.admit_yr
	, b.admit_yr_month
	, b.unit_cost
	, b.admit_allw
	, b.ip_status_code
	, b.mcr_readmitreviews_touched
	, b.mcr_readmitreviews_ever_denied
	, b.still_mcr_dnl_ind
	, b.still_mcr_dnl_w_nodol_ind
	, case 
	    when b.mnr_readmit_ind is null then 'NA' 
	    else cast(b.mnr_readmit_ind as varchar) 
	end as mnr_readmit_ind
	, b.index_ind
	, b.product_level_3_fnl
	, b.market_fnl
	, b.global_cap
	, b.tfm_include_flag
	, b.migration_source
	, b.brand_fnl
from mcr_readmit_2024 as a
left join mnr_readmit_2024 as b
	on a.gal_sbscr_nbr = b.gal_sbscr_nbr
;

select * from tmp_1m.kn_mnr_join_mcr_readmission_tre_2024 limit 20




select gal_sbscr_nbr from tmp_1m.kn_mnr_mcrreadmits_1_202510;
select gal_sbscr_nbr from tmp_1m.kn_mcr_readmission_tre_2024;



select * from tmp_1m.kn_mcr_readmission_tre_2024



select count(*) from tmp_1m.kn_mnr_join_mcr_readmission_tre_2024;


select * from tmp_1m.


select * from tadm_tre_cpy.glxy_ip_admit_f_202510

select count(distinct gal_sbscr_nbr) from tmp_1m.kn_mnr_join_mcr_readmission_tre_2024;
-- 47,802

select * from tmp_1m.kn_mnr_join_mcr_readmission_tre_2024q1q2 where readmit_join_matched = 'N' 


-- ID
-- MCR_member_id
-- 994021076
-- 927767292


-- MNR_SBSCR_NBR
-- 00994021076
-- 00927767292

-- ID - no match
-- MCR_member_id
-- 935160814
-- 947344151


-- MNR_SBSCR_NBR
-- 00935160814
-- 00947344151

select
	*
from tmp_1m.kn_mcr_readmission_tre_2024
where mcr_member_id in ('994021076', '927767292', '935160814', '947344151')

select
	*
from tmp_1m.kn_mnr_mcrreadmits_1_202510
where gal_sbscr_nbr in ('00994021076', '00927767292', '00935160814', '00947344151')




select readmit_join_matched, mcr_ever_readmit_ind, mnr_readmit_ind, count(distinct mcr_gal_sbscr_nbr)
from tmp_1m.kn_mnr_join_mcr_readmission_tre_2024
group by 1,2,3
;


select * from tmp_1m.KN_MNR_MCRREADMITS_1_202510 where gal_sbscr_nbr = '00959564344'
;

select * from tadm_tre_cpy.GLXY_IP_ADMIT_F_202510
;


















                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     