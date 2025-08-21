-- Context
-- THIS IS NOTIFICATION, NOT CLAIMS 

-- Total # of cases received in CCR for HCPC Code A0428 or A0425?
-- How many of those cases were Admin Approved?
-- How many of those cases were Clinically Approved ?
-- How many cases were denied?

-- Location
-- O:\National\Clinical\Prior Authorization Evaluation\2025 Prior Auth Programs\Data Requests\M&R Non-Emergent Ground Transport National Expansion.csv

-- Where
	-- M&R FFS COSMOS OP PR
	-- 01/01/2023 -> curent nofif dates
	-- Proc: 'A0425', 'A0428'

-- Select
	-- fst_srvc_dt, fst_srvc_mon, fst_srvc_year, 
-- proc_cd
-- Modifiers
-- State
-- Pre-d / PA indicator
-- Decision (approved / denied)
-- Admin vs Clinically approved? 
	-- Might need Avtar data for high-level
-- Other fields based on SS pre-d work 



select prim_diag_cd from hce_proj_bd.hce_adr_avtar_like_24_25_f;



drop table tmp_1m.kn_transport_dialysis_check_2023;
create table tmp_1m.kn_transport_dialysis_check_2023 as
select
    a.case_id 
    , a.member_id 
    , a.proc_cd 
    , replace(a.prim_diag_cd, '.', '') as prim_diag_cd
    , a.prim_srvc_cat
	, a.prim_proc_ind
    , b.diag_full_desc
    , a.svc_start_dt 
    , a.svc_end_dt 
    , a.svc_freq 
    , a.svc_freq_typ_cd 
    , a.proc_unit_cnt 
    , a.plc_of_svc_cd 
    , a.plc_of_svc_drv_cd 
    , a.svc_cat_cd 
    , a.svc_cat_dtl_cd 
    , a.svc_setting 
    , a.case_category_cd 
    , a.auth_typ_cd 
    , a.admit_cat_cd 
    , a.transplant_flag 
    , a.appeal_ind 
    , a.mcr_ovtrn_ind 
    , a.mcr_uphelp_ind
    , case
		when 
		a.prim_diag_cd in ( 'N18.6', 'Z99.2', 'Z94.0', 'T86.10', 'I12.0', 'E11.22', 'N18.5', 'N18.4', 'N18.3', 'N18.2', 'N18.1', 'N18.9', 'D63.1') 
		or 
		    a.prim_diag_cd like 'Z49.%' or 
		    a.prim_diag_cd like 'T81.502%' or 
		    a.prim_diag_cd like 'T85.71%' or 
		    a.prim_diag_cd like 'Y62.2%' or 
		    a.prim_diag_cd like 'Y84.1%' 
		    then 1
		else 
		    0
		end as diag_cd_flag_1
from hce_proj_bd.hce_adr_avtar_like_2023_f as a
left join fichsrv.tadm_glxy_diagnosis_code as b
    on  b.diag_cd = replace(a.prim_diag_cd, '.', '')
where a.proc_cd in ('A0428', 'A0425') 
	and a.business_segment not in ('EnI','ERR','null')
	and a.medicare_id is not null
	and a.proc_cd in ('A0425','A0428')
	and a.notif_yrmonth between '202301' and '202312';
;


drop table tmp_1m.kn_transport_dialysis_flag_2023; as 
select * from tmp_1m.kn_transport_dialysis_check_2023

;



drop table tmp_1m.kn_NonEmergentTransport_dialysis;
create table tmp_1m.kn_NonEmergentTransport_dialysis as 
with dialysis_check_2023 as (
select
    a.*
    , replace(a.prim_diag_cd, '.', '') as prim_diag_cd_clean
    , case
		when 
		a.prim_diag_cd in ('N18.6', 'Z99.2', 'Z94.0', 'T86.10', 'I12.0', 'E11.22', 'N18.5', 'N18.4', 'N18.3', 'N18.2', 'N18.1', 'N18.9', 'D63.1') 
		or 
		    a.prim_diag_cd like 'Z49.%' or 
		    a.prim_diag_cd like 'T81.502%' or 
		    a.prim_diag_cd like 'T85.71%' or 
		    a.prim_diag_cd like 'Y62.2%' or 
		    a.prim_diag_cd like 'Y84.1%' 
		    then 1
		else 
		    0
		end as diag_cd_flag_1
from hce_proj_bd.hce_adr_avtar_like_2023_f as a
left join fichsrv.tadm_glxy_diagnosis_code as b
    on b.diag_cd = replace(a.prim_diag_cd, '.', '')
where a.proc_cd in ('A0428', 'A0425') 
	and a.business_segment not in ('EnI','ERR','null')
	and a.medicare_id is not null
	and a.proc_cd in ('A0425','A0428')
	and a.notif_yrmonth between '202301' and '202312'
),
dialysis_check_2024_2025 as (
select
    a.*
    , replace(a.prim_diag_cd, '.', '') as prim_diag_cd_clean
    , case
		when 
		a.prim_diag_cd in ('N18.6', 'Z99.2', 'Z94.0', 'T86.10', 'I12.0', 'E11.22', 'N18.5', 'N18.4', 'N18.3', 'N18.2', 'N18.1', 'N18.9', 'D63.1') 
		or 
		    a.prim_diag_cd like 'Z49.%' or 
		    a.prim_diag_cd like 'T81.502%' or 
		    a.prim_diag_cd like 'T85.71%' or 
		    a.prim_diag_cd like 'Y62.2%' or 
		    a.prim_diag_cd like 'Y84.1%' 
		    then 1
		else 
		    0
		end as diag_cd_flag_1
from hce_proj_bd.hce_adr_avtar_like_24_25_f as a
left join fichsrv.tadm_glxy_diagnosis_code as b
    on b.diag_cd = replace(a.prim_diag_cd, '.', '')
where a.proc_cd in ('A0428', 'A0425') 
	and a.business_segment not in ('EnI','ERR','null')
	and a.medicare_id is not null
	and a.proc_cd in ('A0425','A0428')
	and a.notif_yrmonth >= '202401'
)
select * from dialysis_check_2023
union all
select * from dialysis_check_2024_2025
;