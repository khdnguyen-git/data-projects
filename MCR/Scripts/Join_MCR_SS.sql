/*==============================================================================
 * Subsetting MCR data to 202409, and making variables for matching 
 *==============================================================================*/
use secondary role all;

drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025;
create or replace table ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as
select distinct
	a.work_item_id as mcr_work_item_id
	, regexp_substr(a.work_item_id, '^[^-]+') as mcr_work_item_id_cleaned -- anything before -
	, a.member_id as mcr_member_id
	, a.u_div as mcr_u_div
	, a.claim_id as mcr_claim_id
	, a.mcr_disposition_code as mcr_decision
	, concat(substring(cast(resolved_at as string), 1, 4), substring(cast(resolved_at as string), 6, 2)) as mcr_month -- can use other date here
	, substring(cast(resolved_at as string), 1, 4) as mcr_year
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
where to_timestamp_ntz(uhg_received_date) >= '2024-07-01'
	and member_id is not null
;




select max(routing_date) from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work

select routing_date from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where routing_date is not null
order by routing_date desc


use secondary role all;
select count(*) from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
;
select count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 -- 4358628 3583378
;



select * from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a
limit 200;


/*==============================================================================
 * Subsetting SS data and making matching variables
 *==============================================================================*/
drop table if exists ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025;
create or replace table ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as 
with cte_subset as (
select distinct
	b.site_clm_aud_nbr as mnr_site_clm_aud_nbr
	, regexp_replace(b.site_clm_aud_nbr, '^[A-Z]+0*', '') as mnr_clm_aud_nbr -- non-zero int
    , substring(b.sbscr_nbr, 3) as mnr_sbscr_nbr
    , b.site_cd as mnr_site_cd
    , b.mth as mnr_month
    , iff(b.mth between '202409' and '202504', 1, 0) as mnr_ss_compare_month
    , b.years as mnr_year
	, b.entity_source as mnr_entity_source
	, b.brand_fnl as mnr_brand_fnl
	, b.proc_cd as mnr_proc_cd
	, b.gal_mbi_hicn_fnl as mnr_gal_mbi_hicn_fnl
	, b.component as mnr_component
	, b.hce_service_code as mnr_hce_service_code
	, b.ahrq_diag_dtl_catgy_desc as mnr_ahrq_diag_dtl_catgy_desc
	, b.group_ind_fnl as mnr_group_ind_fnl
	, b.prov_tin as mnr_prov_tin
	, b.full_nm as mnr_full_nm
	, b.st_abbr_cd as mnr_st_abbr_cd
	, b.prov_prtcp_sts_cd as mnr_prov_prtcp_sts_cd
	, b.tfm_include_flag as mnr_tfm_include_flag
	, b.product_level_3_fnl as mnr_product_level_3_fnl
	, b.tfm_product_new_fnl as mnr_tfm_product_new_fnl
	, b.migration_source as mnr_migration_source
	, b.global_cap as mnr_global_cap
	, b.covered_unproven as mnr_covered_unproven
	, b.sbmt_chrg_amt as mnr_sbmt_chrg_amt
	, b.allw_amt_fnl as mnr_allw_amt_fnl
	, b.net_pd_amt_fnl as mnr_net_pd_amt_fnl
	, b.adj_srvc_unit_cnt as mnr_adj_srvc_unit_cnt
	, b.tadm_units as mnr_tadm_units
	, b.ekp as mnr_ekp
	, b.clm_pd_dt as mnr_clm_pd_dt
	, b.clm_dnl_f as mnr_clm_dnl_f
	, case 
	    when b.clm_dnl_f in ('P', 'N') then 'Paid'
	    when b.clm_dnl_f in ('Y', 'D') then 'Denied'
		else 'N/A'
	end as mnr_clm_dnl_status
	, b.primary_diag_cd as mnr_primary_diag_cd
	, b.location_type as mnr_location_type
	, b.locationtype as mnr_locationtype
	, b.market as mnr_market
	, b.fin_market as mnr_fin_market
	, b.fin_state as mnr_fin_state
	, case when brand_fnl = 'C&S' and years = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 0
			when brand_fnl != 'C&S' and years = '2024' and migration_source = 'OAH' and fin_market = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as mnr_OAH_Flag
		, case when (
				   (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX'))
				or (brand_fnl = 'C&S' and years = '2024' and migration_source = 'OAH' and fin_state = 'MD') 
				or (brand_fnl != 'C&S' and years = '2024' and migration_source = 'OAH' and fin_market = 'MD')
				) then 1
			else 0
		end as mnr_CnS_Dual_Flag
		, case when (brand_fnl  = 'C&S' and fin_state in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES')) then 'LCD'
			   when (brand_fnl  != 'C&S' and fin_market in ('AR', 'CO', 'DC', 'DE', 'FL', 'KY', 'LA', 'MD', 'MS', 'NJ', 'NM', 'OH', 'OK', 'PA', 'PR', 'TX', 'VI', 'ALL STATES')) then 'LCD'
			else 'Non-LCD'
		end as mnr_lcd_status
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
where b.mth >= '202409'
)
select 
	*
	, case when mnr_OAH_Flag = 1 then 'OAH'
		   when mnr_CnS_Dual_Flag = 1 then 'C&S DSNP'
		   when mnr_product_level_3_fnl = 'INSTITUTIONAL' and mnr_brand_fnl = 'M&R' then 'M&R ISNP'
		   when mnr_brand_fnl = 'C&S' and mnr_migration_source != 'OAH' and mnr_product_level_3_fnl = 'DUAL' and mnr_st_abbr_cd in ('OK','NC','NM','NV','OH','TX') then 'N/A C&S'
		   else 'M&R FFS'
	end as mnr_entity
from cte_subset
;


select count(*) from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025
-- 70,277

select mnr_clm_dnl_status, count(*) from tmp_1m.kn_ss_claims_202409_2025
group by mnr_clm_dnl_status

--MNR_CLM_DNL_STATUS	COUNT(*)
--N/A	2,653
--Paid	30,281
--Denied	37,343


-- Check count for data source
select 
    mnr_entity_source
  , count(*) as n_source
  , count(*) * 100 / (select count(*) from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025) as pct
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025
group by mnr_entity_source


--MNR_ENTITY_SOURCE	N_SOURCE	PCT
--COSMOS	52,918	75.299173
--NICE	419	0.596212
--CSP	16,940	24.104615



/*==============================================================================
 * Join COSMOS
 *==============================================================================*/
drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos;
create or replace table ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos as
select distinct
	a.*
	, b.*
    , case when b.mcr_work_item_id_cleaned is null then 'N' else 'Y' end as mcr_routed
    , case when a.mnr_month between '202409' and '202504' then 1
    	else 0
    end as mnr_ss_compare
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
	on 
	(
		a.mnr_sbscr_nbr = b.mcr_member_id
		and a.mnr_clm_aud_nbr = b.mcr_work_item_id_cleaned
		and a.mnr_site_cd = b.mcr_u_div
	)
where a.mnr_entity_source = 'COSMOS'
;

select count(*) from tmp_1m.kn_mcr_ss_join_cosmos
-- 68,170


select * from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_SBSCR_NBR = '937975211'


-- Dedup
create or replace table ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos_dedup as
with rn as (
select 
	*
	, row_number() over (partition by mnr_sbscr_nbr, mnr_site_cd 
						 order by mcr_resolved_at desc)
	as rn
)
select
	*
from rn
where rn = 1
;


select
	*
from tmp_1m.kn_mcr_ss_join_cosmos_dedup
where mnr_sbscr_nbr in (
'861588582'
,'868040245'
,'900716200'
)

select
	*
from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_sbscr_nbr in (
'861588582'
,'868040245'
,'900716200'
)

'900716200'


select
	mnr_sbscr_nbr as sbscr_nbr
	, mnr_site_cd as site_cd
	, mnr_clm_aud_nbr as clm_aud_nbr
	, mnr_proc_cd as proc_cd
	, mnr_prov_tin as prov_tin
	, mnr_month as fst_srvc_month
	, mnr_clm_dnl_status as clm_dnl_f
	, mnr_clm_pd_dt as clm_pd_dt
	, mnr_allw_amt_fnl as allw_amt_fnl
	, mnr_net_pd_amt_fnl as net_pd_amt_fnl 
	, mcr_member_id as member_id
	, mcr_work_item_id_cleaned as work_item_id
	, mcr_u_div as u_div
	, mcr_routed as MCR_routed
	, mcr_routing_date as MCR_routed_date
	, mcr_decision as MCR_decision
	, mcr_resolved_at as MCR_decision_date
from tmp_1m.kn_mcr_ss_join_cosmos
where sbscr_nbr in ('250541470', '256274362', '861588582', '868040245', '900716200')
order by
	sbscr_nbr
	, site_cd
	, clm_aud_nbr
	, fst_srvc_month
	, mcr_routed_date
	, mcr_decision_date desc








select
	mnr_sbscr_nbr
	, mnr_site_cd
	, mnr_clm_aud_nbr
	, mnr_month
	, mcr_member_id
	, mcr_work_item_id_cleaned
	, mcr_u_div
	, mcr_routed
	, mcr_decision
	, mnr_clm_dnl_status
	, mnr_clm_pd_dt
	, mcr_routing_date
	, mcr_resolved_at
from tmp_1m.kn_mcr_ss_join_cosmos
order by 
	mnr_sbscr_nbr
	, mcr_work_item_id_cleaned
	, mcr_uhg_received_date
	, mcr_uhc_dept_received_date 
	, mcr_routing_date
	, mcr_resolved_at
	, mnr_clm_pd_dt 
;

select
	mnr_sbscr_nbr
	, mnr_site_cd
	, mnr_clm_aud_nbr
	, mnr_month
	, mcr_member_id
	, mcr_work_item_id_cleaned
	, mcr_u_div
	, mcr_routed
	, mcr_decision
	, mnr_clm_dnl_status
	, mnr_clm_pd_dt
	, mcr_routing_date
	, mcr_resolved_at
from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_sbscr_nbr in (
'861588582'
,'868040245'
,'900716200'
)
order by 
	mnr_sbscr_nbr
	, mcr_work_item_id_cleaned
	, mcr_uhg_received_date
	, mcr_uhc_dept_received_date 
	, mcr_routing_date
	, mcr_resolved_at
	, mnr_clm_pd_dt 
	





select mnr_site_clm_aud_nbr, mnr_clm_aud_nbr, mnr_site_cd, mnr_sbscr_nbr 
from tmp_1m.kn_ss_claims_202409_2025
where mnr_entity_source = 'CSP'
and mnr_clm_aud_nbr = '24V213290000'



select substring(mnr_site_clm_aud_nbr, 3, 9)
from tmp_1m.kn_ss_claims_202409_2025
where mnr_entity_source = 'NICE'



select 
	mcr_knowledge
from tmp_1m.kn_mcr_ss_join_cosmos
group by
	mcr_knowledge
select count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos
-- 25,201

select mnr_entity, mcr_routed, count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos group by 1,2;

-- Export send to MCR team for verification
drop table if exists tmp_1m.kn_mcr_ss_join_cosmos_send;
create or replace table tmp_1m.kn_mcr_ss_join_cosmos_send as
with cte_window_pd_dt as (
select distinct
    mnr_clm_aud_nbr as clm_aud_nbr
    , mnr_site_cd as site_cd
	, mnr_site_clm_aud_nbr as site_clm_aud_nbr
    , mnr_sbscr_nbr as sbscr_nbr
    , concat_ws('_', mnr_clm_aud_nbr, mnr_site_cd, mnr_sbscr_nbr) as claimkey
    , mnr_entity as entity
	, mnr_brand_fnl as brand_fnl
	, mnr_product_level_3_fnl as product_level_3_fnl
    , mnr_market as market
    , mnr_gal_mbi_hicn_fnl as gal_mbi_hicn_fnl
    , mnr_component as component
    , mnr_lcd_status as lcd_status
    , mnr_locationtype as locationtype
    , mnr_migration_source as migration_source
    , mnr_proc_cd as proc_cd
    , mnr_prov_prtcp_sts_cd as prov_prtcp_sts_cd
    , mnr_allw_amt_fnl as allw_amt_fnl
    , mnr_net_pd_amt_fnl as net_pd_amt_fnl
    , mnr_sbmt_chrg_amt as sbmt_chrg_amt
    , mnr_month as fst_srvc_month
    , max(mnr_clm_pd_dt) over (partition by mnr_clm_aud_nbr) as max_clm_pd_dt1
from tmp_1m.kn_mcr_ss_join_cosmos
where fst_srvc_month <= '202508'
and entity = 'M&R FFS'
and component = 'PR'
)
select
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, claimkey
	, entity
	, brand_fnl 
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl 
	, component
	, lcd_status 
	, locationtype
	, migration_source 
	, proc_cd 
	, prov_prtcp_sts_cd 
	, fst_srvc_month
	, max(max_clm_pd_dt1) as max_clm_pd_dt
	, sum(allw_amt_fnl) as sum_allowed
	, sum(net_pd_amt_fnl) as sum_paid
	, sum(sbmt_chrg_amt) as sum_billed
	, count(distinct concat(sbscr_nbr, site_clm_aud_nbr, fst_srvc_month)) as n_distinct
from cte_window_pd_dt
group by
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, claimkey
	, entity
	, brand_fnl 
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl 
	, component
	, lcd_status 
	, locationtype
	, brand_fnl 
	, migration_source 
	, proc_cd 
	, prov_prtcp_sts_cd 
	, fst_srvc_month
;



create or replace table tmp_1m.kn_mcr_ss_join_cosmos_send_with_routed as
with cte_window_pd_dt as (
select distinct
    mnr_clm_aud_nbr as clm_aud_nbr
    , mnr_site_cd as site_cd
	, mnr_site_clm_aud_nbr as site_clm_aud_nbr
    , mnr_sbscr_nbr as sbscr_nbr
    , concat_ws('_', mnr_clm_aud_nbr, mnr_site_cd, mnr_sbscr_nbr) as claimkey
    , mnr_entity as entity
	, mnr_brand_fnl as brand_fnl
	, mnr_product_level_3_fnl as product_level_3_fnl
    , mnr_market as market
    , mnr_gal_mbi_hicn_fnl as gal_mbi_hicn_fnl
    , mnr_component as component
    , mnr_lcd_status as lcd_status
    , mnr_locationtype as locationtype
    , mnr_migration_source as migration_source
    , mnr_proc_cd as proc_cd
    , mnr_prov_prtcp_sts_cd as prov_prtcp_sts_cd
    , mnr_allw_amt_fnl as allw_amt_fnl
    , mnr_net_pd_amt_fnl as net_pd_amt_fnl
    , mnr_sbmt_chrg_amt as sbmt_chrg_amt
    , mnr_month as fst_srvc_month
    , max(mnr_clm_pd_dt) over (partition by mnr_clm_aud_nbr) as max_clm_pd_dt1
    , mcr_routed
    , mcr_decision
from tmp_1m.kn_mcr_ss_join_cosmos
where fst_srvc_month <= '202508'
and entity = 'M&R FFS'
and component = 'PR'
)
select
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, claimkey
	, entity
	, brand_fnl 
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl 
	, component
	, lcd_status 
	, locationtype
	, migration_source 
	, proc_cd 
	, prov_prtcp_sts_cd 
	, fst_srvc_month

	, max(max_clm_pd_dt1) as max_clm_pd_dt
	, sum(allw_amt_fnl) as sum_allowed
	, sum(net_pd_amt_fnl) as sum_paid
	, sum(sbmt_chrg_amt) as sum_billed
	, count(distinct concat(sbscr_nbr, site_clm_aud_nbr, fst_srvc_month)) as n_distinct
	, mcr_routed
    , mcr_decision
from cte_window_pd_dt
group by
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, claimkey
	, entity
	, brand_fnl 
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl 
	, component
	, lcd_status 
	, locationtype
	, brand_fnl 
	, migration_source 
	, proc_cd 
	, prov_prtcp_sts_cd 
	, fst_srvc_month
	, mcr_routed
    , mcr_decision
;



select count(*) from tmp_1m.kn_mcr_ss_join_cosmos_send
;

select count(*) from tmp_1m.kn_mcr_ss_join_cosmos_send 
where fst_srvc_month <= '202508'
and entity = 'M&R FFS'
and component = 'PR'

select distinct migration_source from tmp_1m.kn_mcr_ss_join_cosmos_send 
where fst_srvc_month <= '202508'
and entity = 'M&R FFS'
and component = 'PR'


select count(distinct mnr_sbscr_nbr) from tmp_1m.KN_MCR_SS_JOIN_COSMOS_EXPORT_SEND 



	
select
	*
from tmp_1m.kn_mcr_202409_2025
where mcr_work_item_id_cleaned = '42841830'
order by
	mcr_resolved_at

	
	

-- Export for analysis
drop table if exists tmp_1m.kn_mcr_ss_join_cosmos_export;
create or replace table tmp_1m.kn_mcr_ss_join_cosmos_export as
with cte_window_pd_dt as (
select
	mnr_clm_aud_nbr
	, mnr_product_level_3_fnl
	, mnr_market
	, mnr_gal_mbi_hicn_fnl 
	, mnr_sbscr_nbr
	, mnr_month
	, mnr_entity
	, mnr_component
	, mnr_lcd_status 
	, mnr_locationtype
	, mnr_brand_fnl 
	, mnr_migration_source 
	, mnr_proc_cd 
	, mnr_prov_tin
	, mnr_prov_prtcp_sts_cd 
	, mnr_site_cd 
	, mnr_site_clm_aud_nbr 
	, mnr_allw_amt_fnl
	, mnr_net_pd_amt_fnl
	, mnr_sbmt_chrg_amt
	, mnr_clm_dnl_status
	, mcr_routed
	, mcr_decision
	, concat(substring(mcr_u_resolved_date_reporting, 1, 4), substring(mcr_u_resolved_date_reporting, 6, 2)) as mcr_decision_date
	, max(mnr_clm_pd_dt) over (partition by mnr_clm_aud_nbr) as mnr_max_clm_pd_dt1
from tmp_1m.kn_mcr_ss_join_cosmos
)
select
	mnr_clm_aud_nbr
	, mnr_product_level_3_fnl
	, mnr_market
	, mnr_gal_mbi_hicn_fnl 
	, mnr_sbscr_nbr
	, mnr_month
	, mnr_entity
	, mnr_component
	, mnr_lcd_status 
	, mnr_locationtype
	, mnr_brand_fnl 
	, mnr_migration_source 
	, mnr_proc_cd 
	, mnr_prov_tin
	, mnr_prov_prtcp_sts_cd 
	, mnr_site_cd 
	, mnr_site_clm_aud_nbr 
	, mcr_routed
	, iff(mcr_routed = 'Y', 'Routed', 'Not Routed') as mcr_routed_status
	, mcr_decision
	, mcr_decision_date
	, mnr_clm_dnl_status
	, sum(mnr_allw_amt_fnl) as mnr_sum_allowed
	, sum(mnr_net_pd_amt_fnl) as mnr_sum_paid
	, sum(mnr_sbmt_chrg_amt) as mnr_sum_billed
	, max(mnr_max_clm_pd_dt1) as mnr_max_clm_pd_dt
	, count(distinct concat(mnr_sbscr_nbr, mnr_site_clm_aud_nbr, mnr_month)) as n_distinct
	, count(distinct concat(mnr_gal_mbi_hicn_fl)
from cte_window_pd_dt
group by
	mnr_clm_aud_nbr
	, mnr_product_level_3_fnl
	, mnr_market
	, mnr_gal_mbi_hicn_fnl 
	, mnr_sbscr_nbr
	, mnr_month
	, mnr_entity
	, mnr_component
	, mnr_lcd_status 
	, mnr_locationtype
	, mnr_brand_fnl 
	, mnr_migration_source 
	, mnr_proc_cd 
	, mnr_prov_prtcp_sts_cd 
	, mnr_site_cd 
	, mnr_site_clm_aud_nbr 
	, mcr_routed
	, iff(mcr_routed = 'Y', 'Routed', 'Not Routed')
	, mcr_decision
	, mcr_decision_date
	, mnr_clm_dnl_status
;

select * from tmp_1m.kn_mcr_ss_join_cosmos_export
limit 300;

select mcr_decision_date from tmp_1m.kn_mcr_ss_join_cosmos_export
limit 300

select count(*) from tmp_1m.kn_mcr_ss_join_cosmos_export
-- 52,203



select
	mnr_sbscr_nbr 
	, mnr_clm_aud_nbr
	, mnr_site_clm_aud_nbr
	, mnr_month
	, mnr_site_cd
	, mcr_member_id
	, mcr_work_item_id_cleaned
	, mcr_claim_id
	, mcr_u_div
	, mcr_assigned_on
	, mcr_uhg_received_date
	, mcr_uhc_dept_received_date 
	, mcr_routed
	, mcr_routing_date 
	, mcr_decision
	, mcr_resolved_at
	, mcr_u_resolved_date_reporting
from tmp_1m.kn_mcr_ss_join_cosmos
order by 
	mnr_sbscr_nbr
	, mcr_work_item_id_cleaned
	, mcr_uhg_received_date
	, mcr_uhc_dept_received_date 
	, mcr_routing_date
	, mcr_resolved_at 
	, mcr_u_resolved_date_reporting

max(mcr_decision) over (partition by mnr_sbscr_nbr, mcr_work_id_cleaned order by mcr_routing_date)









-- Check dates and routed
-- 250541470


select 
	*
from tmp_1m.kn_mcr_202409_2025
where mcr_routing_date is null
order by
	mcr_work_item_id_cleaned
	, mcr_uhg_received_date
	, mcr_uhc_dept_received_date 
	, mcr_routing_date
	, mcr_resolved_at 
	, mcr_u_resolved_date_reporting






/*==============================================================================
 * Join CSP
 * latest decision partition by sbs, site_cd order by 
 *==============================================================================*/
drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_csp;
create or replace table ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_csp as
with joining as (
select 
	a.*
	, b.*
    , case when b.mcr_work_item_id_cleaned is null then 'Not Routed' else 'Routed' end as mcr_routed_status
    , case when a.mnr_month between '202409' and '202504' then 1
    	else 0
    end as mnr_ss_compare
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
	on a.mnr_clm_aud_nbr = b.mcr_work_item_id_cleaned
where a.mnr_entity_source = 'CSP'
)
select
	*
	, max(mcr_decision) over (partition by mnr_sbscr_nbr, )
from joining



;


select mcr_u_div, count(*) from (
select
	a.*
	, b.*
    , case when b.mcr_work_item_id_cleaned is null then 'Not Routed' else 'Routed' end as mcr_routed_status
    , case when a.mnr_month between '202409' and '202504' then 1
    	else 0
    end as mnr_ss_compare
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
	on a.mnr_clm_aud_nbr = b.mcr_work_item_id_cleaned
where a.mnr_entity_source = 'CSP'
) as sub
group by mcr_u_div







/*==============================================================================
 * Join NICE
 *==============================================================================*/
drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos;
create table ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos as
select 
	a.*
	, b.*
    , case when b.mcr_work_item_id is null then 'N' else 'Y' end as mcr_routed
    , case when a.mnr_month between '202409' and '202504' then 1
    	else 0
    end as mnr_ss_compare
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
where a.mnr_entity_source = 'NICE'
;

select count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos
-- 25,201



select 

select mnr_entity, mcr_routed, count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_cosmos group by 1,2;



-- on a.mnr_clm_aud_nbr = b.mcr_work_item_id 
-- 393
-- on substring(b.mcr_member_id, 3) = a.mnr_sbscr_nbr
-- 206,395
--	on (
--		a.mnr_sbscr_nbr = substring(b.mcr_member_id, 3) 
--	and a.mnr_clm_aud_nbr = b.mcr_work_item_id_cleaned
-- )
-- 393





select distinct mcr_business_area, mcr_business_segment from tmp_1m.kn_mcr_202409_2025

select 
	notif_yrmonth
	, case when los < 0 then '< 0'
		   when los = 1 then '1'
	else '> 1'
	end as los_categories
	, count(distinct case_id) 
from hce_ops_fnl.hce_adr_avtar_like_24_25_f as a
where a.svc_setting = 'Inpatient'
  and a.plc_of_svc_cd = '21 - Acute Hospital'
  and a.admit_cat_cd in ('17 - Medical', '30 - Surgical')
  or a.transplant_flag = 'Y'
group by
	notif_yrmonth
	, case when los < 0 then '< 0'
		   when los = 1 then '1'
	else '> 1'
	end
;

where 	
		fin_brand in ('M&R','C&S')
       and ((IP_type in ('Medical','Surgical','Transplant') and DATE_FORMAT(admit_dt_act, 'MM/dd/yyyy') is not null) or IP_type in ('LTAC','SNF','AIR'))


-- 