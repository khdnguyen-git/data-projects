SELECT
work_item_id AS clm_aud_nbr
,U_DIV AS site_cd
,mcr_disposition_code as mcr_decision
,urgency
,uhg_received_date
,routing_date
,route_reason
,uhc_dept_received_date
,u_resolved_date_reporting
,u_due_date_reporting
,resolved_by
,resolved_at
,resolution_code
,resolution_comments
,sys_class_name
,state
,source_system
,skills
,business_segment
,business_area
,*
FROM CDW_PRD_CALL_DB.CDW_EWDO_MCR_BASE_VIEW_SC.OWR_X_UHGEN_EWR_WORK
WHERE work_item_id in ('94166766-0','94166768-0')

select escalate, escalation from CDW_PRD_CALL_DB.CDW_EWDO_MCR_BASE_VIEW_SC.OWR_X_UHGEN_EWR_WORK
group by escalate, escalation


join above with 

select * from ving_prd_trend_db.tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A;
select * from CDW_PRD_CALL_DB.CDW_EWDO_MCR_BASE_VIEW_SC.OWR_X_UHGEN_EWR_WORK;

drop table if exists ving_prd_trend_db.tmp_1m.test;
create table ving_prd_trend_db.tmp_1m.test as
select
	substr(b.clm_aud_nbr
from CDW_PRD_CALL_DB.CDW_EWDO_MCR_BASE_VIEW_SC.OWR_X_UHGEN_EWR_WORK

select work_item_id from CDW_PRD_CALL_DB.CDW_EWDO_MCR_BASE_VIEW_SC.OWR_X_UHGEN_EWR_WORK;
select sbscr_nbr from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a;

select work_item_id from CDW_PRD_CALL_DB.CDW_EWDO_MCR_BASE_VIEW_SC.OWR_X_UHGEN_EWR_WORK
where work_item_id = '963872594-0'

drop table if exists ving_prd_trend_db.tmp_1m.kn_skinsub_mcr_jointest;
create table ving_prd_trend_db.tmp_1m.kn_skinsub_mcr_jointest as
with mcr_id as (
select
	coalesce(regexp_substr(work_item_id, '^[0-9]+'), 
			 regexp_substr(work_item_id, '[0-9]{11}$') 
			) 
	as mcr_clm_aud_nbr
	, u_div as site_cd
	, member_id
	, cast(year(closed_at) as varchar()) as mcr_year
	, concat(substr(cast(closed_at as varchar()), 1, 4), substr(cast(closed_at as varchar()), 6, 2)) as mcr_yrmonth
	, urgency
	, mcr_disposition_code as mcr_decision
	, uhg_received_date
	, routing_date
	, route_reason
	, uhc_dept_received_date
	, u_resolved_date_reporting
	, u_due_date_reporting
	, resolved_by
	, resolved_at
	, resolution_code
	, resolution_comments
	, sys_class_name
	, state
	, source_system
	, skills
	, business_segment
	, business_area
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
)
select
	a.*
	, b.*
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as a
left join cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work as b
	on a.sbscr_nbr = b.member_id
	and a.st_abbr_cd = b.site_code
	and a.clm_
;

select site_clm_aud_nbr from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a


site_clm_aud_nbr = work_item_id

select work_item_id, member_id, u_div from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work;

select site_clm_aud_nbr, sbscr_nbr, st_abbr_cd from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a;


select * from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work


select 
	work_item_id
	, u_div
	, member_id
	, concat(substr(cast(closed_at as varchar()), 1, 4), substr(cast(closed_at as varchar()), 6, 2)) as mcr_yrmonth
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
where concat(substr(cast(closed_at as varchar()), 1, 4), substr(cast(closed_at as varchar()), 6, 2)) >= '202501'
	
select 
	a.mth
	, a.sbscr_nbr
	, b.work_item_id
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as a
left join cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work as b
	on b.work_item_id ilike '%' || a.sbscr_nbr || '%'
limit 100;
	


select 
	a.mth
	, a.sbscr_nbr
	, b.work_item_id
from ving_prd_trend_db.tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a as a
left join cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work as b
	on position(sbscr_nbr in b.work_item_id) > 0
;



using sbscr_nbr from skinsub claims to memberID in MCR view
and site


USE SECONDARY ROLES ALL;