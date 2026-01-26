
/*============================================================================================
 * NOTES
 * The raw tables were received in Excel format, and imported into the VING_PRD_TREND_DB.TMP_1M schema
 * Raw table name: tmp_1m.OPR_MR_AUTH_202409_20251031_v2
 *============================================================================================*/

select count(*) from tmp_1m.opr_mr_auth_202409_20251031_v2
-- 2,571,817

/*============================================================================================
 * Format variables
 *============================================================================================*/
create or replace table tmp_1m.opr_mr_auth_202409_20251031_v2_formatted as
select distinct
	authnumber as auth_id
	, cast(to_date(datereceived, 'mm/dd/yyyy hh24:mi') as date) as date_received_dt
	, to_char(to_date(datereceived, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as date_received_mth
	, cast(to_date(datereviewed, 'mm/dd/yyyy hh24:mi') as date) as date_reviewed_dt
	, to_char(to_date(datereviewed, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as date_reviewed_mth
	, cast(to_date(reqstart, 'mm/dd/yyyy hh24:mi') as date) as req_start_dt
	, to_char(to_date(reqstart, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as req_start_mth
	, cast(to_date(reqend, 'mm/dd/yyyy hh24:mi') as date) as req_end_dt
	, to_char(to_date(reqend, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as req_end_mth
	, cast(to_date(authstart, 'mm/dd/yyyy hh24:mi') as date) as auth_start_dt
	, to_char(to_date(authstart, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as auth_start_mth
	, cast(to_date(authend, 'mm/dd/yyyy hh24:mi') as date) as auth_end_dt
	, to_char(to_date(authend, 'mm/dd/yyyy hh24:mi'), 'yyyymm') as auth_end_mth
	, tinnumber as prov_tin
	, lpad(tinnumber, 10, '0') as prov_tin_10digit
	, providerspecialty as prov_specialty
	, regexp_replace(healthplanpatientid, '00$', '') as patient_id
	, patientstate as state
	, split_part(groupnumber, '-', 1) as group_number
	, split_part(groupnumber, '-', 2) as site_cd
	, visitreq as visit_req
	, iff(visitreq >= 18, '18+', to_char(visitreq)) as visit_req_cat
	, visitauth as visit_auth
	, iff(visitauth >= 18, '18+', to_char(visitauth)) as visit_auth_cat
	, reviewdecision as review_decision
	, auto_approval as auto_approval_flag
	, upper(diagcode) as diagcode
	, diagcodedesc as diagcode_desc
from tmp_1m.opr_mr_auth_202409_20251031_v2
;

select count(*) from tmp_1m.opr_mr_auth_202409_20251031_v2_formatted
-- 2,571,817