
/*==============================================================================
 * Make a subset of MCR data
 *==============================================================================*/
use secondary role all;

drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025;
create table ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as
select distinct
	a.work_item_id as mcr_work_item_id_1
	, regexp_substr(a.work_item_id, '^[^-]+') as mcr_work_item_id_2 -- anything before -
	, a.member_id as mcr_member_id
	, a.u_div as mcr_u_div
	--, substring(a.work_item_id, 1, 2) as mcr_u_div_2 -- KEN09012091
	, a.claim_id as mcr_claim_id_1
	, left(a.claim_id, 11) as mcr_claim_id_2
	, a.mcr_disposition_code as mcr_decision
	, concat(substring(cast(resolved_at as string), 1, 4), substring(cast(resolved_at as string), 6, 2)) as mcr_month
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
where to_timestamp_ntz(uhg_received_date) >= '2024-09-01'
	and member_id is not null
	and work_item_id is not null
;

use secondary role all;
select count(*) from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
;
select count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 -- 3583378
;


/*==============================================================================
 * 
 * 
 *==============================================================================*/

drop table if exists ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025;
create table ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as
select distinct
	b.site_clm_aud_nbr as mnr_site_clm_aud_nbr_1
    , substring(b.site_clm_aud_nbr, 6) as mnr_site_clm_aud_nbr_2
	, left(b.site_clm_aud_nbr, 11) as mnr_site_clm_aud_nbr_3
    , b.sbscr_nbr as mnr_sbscr_nbr_1
    , substring(b.sbscr_nbr, 3) as mnr_sbscr_nbr_2
    , b.site_cd as mnr_site_cd_1
    , substring(b.site_clm_aud_nbr, 1, 2) as mnr_site_cd_2
   	, substring(b.site_clm_aud_nbr, 1, 3) as mnr_site_cd_3
    , b.mth as mnr_month
    , iff(b.mth >= '202409', 1, 0) as mnr_ss_month
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
;

select * from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a limit 100;

select mnr_entity_source, count(*) from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 
group by mnr_entity_source

-- COSMOS 22109
-- CSP	5721
-- NICE	385

/*==============================================================================
 * Create an intermediate join table for Python fuzzymatch
 * Find possible patterns
 *==============================================================================*/
drop table if exists tmp_1m.kn_mcr_ss_join_mmid;
create table tmp_1m.kn_mcr_ss_join_mmid as
with mcr_1 as (
select
	a.work_item_id
	, regexp_substr(a.work_item_id, '^[^-]+') as work_item_id_2 -- anything before -
	, regexp_substr(a.work_item_id, '[0-9]+') as work_item_id_3 -- first number sequence
	, a.member_id
	, a.u_div
	, substring(work_item_id, 1, 2) as u_div_2
	, substring(work_item_id, 1, 3) as u_div_3
from ving_prd_trend_db.tmp_1m.kn_mcr_2025 as a
),
ss_1 as (
select 
	b.site_clm_aud_nbr
	, substring(b.site_clm_aud_nbr, 6) as clm_aud_nbr_2
	, regexp_substr(b.site_clm_aud_nbr, '[0-9]+') as clm_aud_nbr_3
	, regexp_substr(b.site_clm_aud_nbr, '^.{4}(\\d+.*)', 1, 1, 'e', 1) as clm_aud_nbr_4
	, b.sbscr_nbr
	, substring(b.sbscr_nbr, 3) as sbscr_nbr_3
	, b.site_cd
	, substring(b.site_clm_aud_nbr, 1, 2) as site_cd_2
	, substring(b.site_clm_aud_nbr, 1, 3) as site_cd_3
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as b
)
select
	a.*
	, b.*
from mcr_1 as a
join ss_1 as b
on (a.member_id = b.sbscr_nbr
	 or a.member_id = sbscr_nbr_35
	)
;


/*==============================================================================
 * Python codes to performance fuzzy match on tmp_1m.kn_mcr_ss_join_mmid
 * Using Polars and rapidfuzz
 *==============================================================================*/

/*==============================================================================
-- Import packages + dataset
import polars as pl
from rapidfuzz import fuzz, process

df = (pl
      .read_csv(r"C:\Users\knguy139\Documents\Projects\Data\Output\KN_MCR_SS_JOIN_MMID_202509221642.csv")
      .select(["member_id", "work_item_id", "site_clm_aud_nbr"])
)

-- Minor cleanng to remove whitespaces, special characters to improve match performance
df_cleaned = (
    df
    .with_columns([
        pl.col("work_item_id")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .str.replace_all(r'[^A-Z0-9]', '')
            .alias('work_item_id_cleaned'), 
        pl.col("site_clm_aud_nbr")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .str.replace_all(r'[^A-Z0-9]', '')
            .alias('site_clm_aud_nbr_cleaned')
        ]
    )
)
df_cleaned

-- Apply lambda function fuzz.partial_ratio on these 2 columns
-- Using fuzz.partial_ratio instead of fuzz.ratio as data were already grouped by member_id
df_scored = (
    df_cleaned
    .with_columns([
        pl.struct(['work_item_id_cleaned', 'site_clm_aud_nbr_cleaned'])
        .map_elements(lambda x: int(fuzz.partial_ratio(str(x['work_item_id_cleaned']),str(x['site_clm_aud_nbr_cleaned']))), return_dtype = pl.Int64)
        .alias('match_score')
    ])
    .with_columns([
        pl
        .when(pl.col('match_score') >= 95).then(pl.lit(6))
        .when(pl.col('match_score') >= 90).then(pl.lit(5))
        .when(pl.col('match_score') >= 85).then(pl.lit(4))
        .when(pl.col('match_score') >= 80).then(pl.lit(3))
        .when(pl.col('match_score') >= 75).then(pl.lit(2))
        .when(pl.col('match_score') >= 70).then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias('match_grade')
    ])
    .sort(['match_grade', 'member_id'], descending = [True, False])
)
df_scored

-- Export to CSV
df_scored.write_csv(r"C:\Users\knguy139\Documents\Projects\Data\Output\df_scored.csv")
 *==============================================================================*/

/*=================================================================================================================
 * Join MCR 2025 with SS
 * On member_id/sbscr_nbr + variations, and work_item_id/site_clm_aud_nbr + variations
 * Separate join work_item_id/site_clm_aud_nbr + variations into 2 queries then union all due to performance reason
 *=================================================================================================================*/


use secondary role all;
drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_ss_join;
create table ving_prd_trend_db.tmp_1m.kn_mcr_ss_join as
-- Just mcr_work_item_id = mnr_site_clm_aud_nbr
-- Get C&S DSNP, OAH, N/A C&S
with join_1 as (
select 
	a.*
	, b.*
    , case when b.mcr_work_item_id_1 is null then 'N' else 'Y' end as mcr_routed
    , '1' as join_logic
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
	on (
		(a.mnr_sbscr_nbr_1 = b.mcr_member_id or a.mnr_sbscr_nbr_2 = b.mcr_member_id)
	and a.mnr_site_clm_aud_nbr_1 = b.mcr_work_item_id_1
	)
)
,
-- Anything before - in mcr_work_item_id = skip first 5 chars mnr_site_clm_aud_nbr
-- Get mostly M&R FFS and M&R ISNP, some OAH, some N/A C&S
join_2 as (
select 
	a.*
	, b.*
    , case when b.mcr_work_item_id_2 is null then 'N' else 'Y' end as mcr_routed
    , '2' as join_logic
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
	on (
		(a.mnr_sbscr_nbr_1 = b.mcr_member_id
		or a.mnr_sbscr_nbr_2 = b.mcr_member_id)
	and a.mnr_site_clm_aud_nbr_2 = b.mcr_work_item_id_2
	)
)
,
-- Just mcr_claim_id = mnr_site_clm_aud_nbr
-- Get C&S DSNP, OAH, N/A C&S
join_3 as (
select 
	a.*
	, b.*
    , case when b.mcr_claim_id_1 is null then 'N' else 'Y' end as mcr_routed
    , '3' as join_logic
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
	on (
		(a.mnr_sbscr_nbr_1 = b.mcr_member_id
		or a.mnr_sbscr_nbr_2 = b.mcr_member_id)
	and a.mnr_site_clm_aud_nbr_1 = b.mcr_claim_id_1
	)
)
,
-- First 11 chars mnr_site_clm_aud_nbr = First 11 chars mcr_claim_id
-- Get C&S DSNP, OAH, N/A C&S
join_4 as (
select 
	a.*
	, b.*
    , case when b.mcr_claim_id_2 is null then 'N' else 'Y' end as mcr_routed
    , '4' as join_logic
from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
	on (
		(a.mnr_sbscr_nbr_1 = b.mcr_member_id
		or a.mnr_sbscr_nbr_2 = b.mcr_member_id)
	and a.mnr_site_clm_aud_nbr_3 = b.mcr_claim_id_2
	)
)
,
union_all as (
select
	*
from join_1
union all
select
	*
from join_2
union all
select
	*
from join_3
union all
select
	*
from join_4
)
, 
-- windowed by each mnr_site_clm_aud_nbr_1, return 1 if any mcr_routed = 'Y'
routed_dedup as (
select 
	*
    , max(case when mcr_routed = 'Y' then 1 else 0 end) over (
        partition by mnr_site_clm_aud_nbr_1
    ) as routed_flag
from union_all
)
, 
-- change 1/0 back to 'Y', and return only 1 row per mnr_site_clm_aud_nbr_1
routed_final as (
select 
	*
    , case when routed_flag = 1 then 'Y' else 'N' end as mcr_routed_final
    , row_number() over (
        partition by mnr_site_clm_aud_nbr_1
        order by case when mcr_routed = 'Y' then 1 else 2 end
    ) as rn
from routed_dedup
)
select 
	*
	, case when mnr_OAH_Flag = 1 then 'OAH'
		   when mnr_CnS_Dual_Flag = 1 then 'C&S DSNP'
		   when mnr_product_level_3_fnl = 'INSTITUTIONAL' and mnr_brand_fnl = 'M&R' then 'M&R ISNP'
		   when mnr_brand_fnl = 'C&S' and mnr_migration_source != 'OAH' and mnr_product_level_3_fnl = 'DUAL' and mnr_st_abbr_cd in ('OK','NC','NM','NV','OH','TX') then 'N/A C&S'
		   else 'M&R FFS'
	end as mnr_entity
from routed_final
where rn = 1
;
--
---- Wouldn't match with M&R FFS
--drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_4;
--create table ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_4 as
--select 
--	a.*
--	, b.*
--    , case when b.mcr_claim_id_2 is null then 'N' else 'Y' end as mcr_routed
--	, case when mnr_OAH_Flag = 1 then 'OAH'
--		   when mnr_CnS_Dual_Flag = 1 then 'C&S DSNP'
--		   when mnr_product_level_3_fnl = 'INSTITUTIONAL' and mnr_brand_fnl = 'M&R' then 'M&R ISNP'
--		   when mnr_brand_fnl = 'C&S' and mnr_migration_source != 'OAH' and mnr_product_level_3_fnl = 'DUAL' and mnr_st_abbr_cd in ('OK','NC','NM','NV','OH','TX') then 'N/A C&S'
--		   else 'M&R FFS'
--	end as mnr_entity
--from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025 as a
--left join ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025 as b
--	on (
--		(a.mnr_sbscr_nbr_1 = b.mcr_member_id
--		or a.mnr_sbscr_nbr_2 = b.mcr_member_id)
--	and a.mnr_site_clm_aud_nbr_3 = b.mcr_claim_id_2
--	)
--;


select 
	*
from 
where entity_source = 'COSMOS'


-- COSMOS
-- 18317
select count(*) from (
select distinct
	a.entity_source
	, substring(a.site_clm_aud_nbr, 6) as clm_aud_nbr
	, sbscr_nbr as sbscr_nbr_1
	, substring(sbscr_nbr, 3) as sbscr_nbr_2
	, site_cd
	, work_item_id as work_item_id_1
	, regexp_substr(work_item_id, '^[^-]+') as work_item_id_2
	, member_id
	, u_div
	, case when member_id is null or work_item_id is null or u_div is null then 0
	else 1
	end as mcr_routed
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as a
left join cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work as b
	on substring(sbscr_nbr, 3) = member_id
	and substring(site_clm_aud_nbr, 6) = regexp_substr(work_item_id, '^[^-]+')
	and site_cd = u_div
where a.entity_source = 'COSMOS'
and a.mth >= '202409'
)

-- NICE
-- 18317
select count(*) from (
select distinct
	a.entity_source
	, site_clm_aud_nbr
	, substring(a.site_clm_aud_nbr, 6) as clm_aud_nbr
	, sbscr_nbr as sbscr_nbr_1
	, substring(sbscr_nbr, 3) as sbscr_nbr_2
	, site_cd
	, work_item_id as work_item_id_1
	, regexp_substr(work_item_id, '^[^-]+') as work_item_id_2
	, member_id
	, u_div
	, case when member_id is null or work_item_id is null or u_div is null then 0
	else 1
	end as mcr_routed
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as a
inner join cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work as b
	on sbscr_nbr = member_id
	or substring(sbscr_nbr, 3) = member_id
where a.entity_source = 'NICE'
and a.mth >= '202409'
)














select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_site_clm_aud_nbr_1 = 'EVC0025086057'

select count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join;
-- 127735
select count(*) from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025;
-- 152530

select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_site_clm_aud_nbr_1 = 'BHM0067294711'



select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_site_clm_aud_nbr_1 = 'STL0024569454'


select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_site_clm_aud_nbr_1 = 'BHM0067090265'



select * from ving_prd_trend_db.tmp_1m.kn_ss_claims_202409_2025
where mnr_site_clm_aud_nbr_1 = 'EVC0025086057'


select * from ving_prd_trend_db.tmp_1m.kn_mcr_202409_2025
where mcr_work_item_id_1 = 'EVC0025086057'


select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_sbscr_nbr_1 = '00116394781'

-- Problems
select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_sbscr_nbr_1 = '00116394781' or mnr_sbscr_nbr_2 = '00116394781'
or mnr_sbscr_nbr_1 = '116394781' or mnr_sbscr_nbr_2 = '116394781'


select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join 
where mnr_site_clm_aud_nbr_1 = '24S016131402'

select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join 
where mcr_work_item_id_1 = '24S0161314' or mcr_work_item_id_1 = '24S0161314' or mcr_claim_id = '24S0161314'



select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_site_clm_aud_nbr_1 = '24GMN6871401'

select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join 
where mcr_work_item_id_1 = '24GMN68714' or mcr_work_item_id_1 = '24GMN68714' or mcr_claim_id = '24GMN68714'







select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mcr_work_item_id_1 = '24S016131402' or mcr_work_item_id_2 = '24S016131402'



select mcr_routed_final, count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
group by mcr_routed_final;

--MCR_ROUTED_FINAL	COUNT(*)
--N	123,898
--Y	3,837






use secondary role all;
select work_item_id, member_id, u_div, claim_id from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work


select count(distinct sbscr_nbr)  from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a
where mth >= '202501';
-- 5,668

select count(distinct sbscr_nbr || site_clm_aud_nbr)  from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a
where mth >= '202501';
-- 12,705

select count(distinct mnr_sbscr_nbr) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_month >= '202501';
-- 1,352


select count(distinct mnr_sbscr_nbr|| mnr_site_clm_aud_nbr) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
where mnr_month >= '202501';
-- 1,986



select distinct mcr_problem from tmp_1m.kn_mcr_ss_join 

select 
	*
from tmp_1m.kn_mcr_ss_join 
where mnr_site_clm_aud_nbr = 'KLC0056746474'
;
-- has routing date and MCR Review Complete as MCR_Resolution_code


select 
	*
from tmp_1m.kn_mcr_ss_join 
where mnr_sbscr_nbr = '00955219623'
;

select 
	*
from tmp_1m.kn_mcr_ss_join 
where mcr_member_id = '00955219623'
;



-- Routed = joined
-- Decision - if mcr_decision is null -> N
-- Date: mcr_u_resolved_dae_reporting
-- MCR_state
-- MCR_resolution_code

select 
	mnr_brand_fnl
	, mnr_site_clm_aud_nbr
	, mnr_site_clm_aud_nbr_2
	, mnr_
	, mnr_sbscr_nbr
	, mnr_clm_pd_dt 
	, mcr_urgency
	, mcr_priority
	, mcr_escalate
	, mcr_escalation
	, mcr_business_area
	, mcr_approval
	, mcr_decision
	, mcr_month
	, mcr_state
	, mcr_resolution_code
	, mcr_proactive
	, mcr_routing_date
 	, 
 	
select * from fichsrv.cosmos_op limit 200;

select * from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a limit 200;

select * from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join



drop table if exists ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_export;
create table ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_export as
select 
	*
	, case when mnr_OAH_Flag = 1 then 'OAH'
		   when mnr_CnS_Dual_Flag = 1 then 'C&S DSNP'
		   when mnr_product_level_3_fnl = 'INSTITUTIONAL' and mnr_brand_fnl = 'M&R' then 'M&R ISNP'
		   when mnr_brand_fnl = 'C&S' and mnr_migration_source != 'OAH' and mnr_product_level_3_fnl = 'DUAL' and mnr_st_abbr_cd in ('OK','NC','NM','NV','OH','TX') then 'N/A C&S'
		   else 'M&R FFS'
	end as mnr_entity
from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join
;

select count(*) from ving_prd_trend_db.tmp_1m.kn_mcr_ss_join_export


use secondary role all;
select work_item_id, u_div, member_id, claim_id, u_resolved_date_reporting
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where work_item_id ilike '%25D7839189%'

-- 25D783918900
select mnr_site_clm_aud_nbr_1, mcr_work_item_id_1, mnr_sbscr_nbr_1, mcr_member_id, mnr_month, mcr_routed_final
from ving_prd_trend_db.tmp_1m.KN_MCR_SS_JOIN 
where mnr_site_clm_aud_nbr_1 ilike '%25D7839189%'





/*==============================================================================
 * Weird cases
 * site_clm_aud_nbr is null, but MCR_routed = 'Y'
 * left(site_clm_aud_nbr, 11) = left(claim_id, 11)
 *==============================================================================*/


select work_item_id from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work where work_item_id like '%24S0140493%'

select * from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work where claim_id like '%24S0140493%'


-- 24S0140493
select mnr_site_clm_aud_nbr_1, mcr_work_item_id_1, mnr_sbscr_nbr_1, mcr_member_id, mnr_month, mcr_routed_final
from ving_prd_trend_db.tmp_1m.KN_MCR_SS_JOIN 
where mnr_site_clm_aud_nbr_1 ilike '%24S0140493%' 

select work_item_id, u_div, member_id, claim_id, u_resolved_date_reporting
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where work_item_id ilike '%24S0140493%'

select *
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where member_id ilike '125614924'
-- 24S014049300 is MCR claim_id. Our clm_aud_nbr is 24S014049301, and site_clm_aud_nbr is NULL


-- 24Q4792898 claimkey, 118885164 member_id
select work_item_id, u_div, member_id, claim_id, u_resolved_date_reporting
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where work_item_id ilike '%24Q4792898%'

select work_item_id, u_div, member_id, claim_id, u_resolved_date_reporting
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where member_id ilike '118885164'

select *
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where member_id ilike '118885164'
-- 24Q479289800 is MCR claim_id. Our clm_aud_nbr is 24Q479289801, and site_clm_aud_nbr is NULL

select mnr_site_clm_aud_nbr_1, mcr_work_item_id_1, mcr_claim_id, mnr_sbscr_nbr_1, mcr_member_id, mnr_month, mcr_routed_final
from ving_prd_trend_db.tmp_1m.KN_MCR_SS_JOIN 
where mcr_member_id ilike '118885164'
-- our clm_aud_nbr is 24Q479289801, mcr claim_id is 24Q479289800, mcr_work_item is SF20241019478692404