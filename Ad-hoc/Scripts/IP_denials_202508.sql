/*==============================================================================
 * 2024, 2025 M&R FFS Acute Inpatient Denied
 * Paid through: 202508
 *==============================================================================*/
drop table if exists tmp_1m.kn_acute_cosmos_ip;
create table tmp_1m.kn_acute_cosmos_ip as
select distinct
	hicn as mbi
	, admit_start_dt as admit_start_date
	, admit_end_dt as admit_end_date
from fichsrv.cosmos_ip_w_dnls_clm
where
	year(admit_start_dt) >= '2024'
	and clm_admit_type = 'ACUTE'
	and brand_fnl = 'M&R'
	and product_level_3_fnl != 'INSTITUTIONAL'
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and admit_ip_status_code = 'DN'
;

select count(*) from tmp_1m.kn_acute_cosmos_ip; 
-- 237,126

/*==============================================================================
 * 2024, 2025 M&R FFS Acute Inpatient Paid and Denied
 * Broken into quarters
 * Paid through: 202508
 *==============================================================================*/

drop table if exists tmp_1m.kn_acute_cosmos_ip_paid;
create table tmp_1m.kn_acute_cosmos_ip_paid as
select distinct
	hicn as mbi
	, admit_start_dt as admit_start_date
	, admit_end_dt as admit_end_date
	, admit_qtr as admit_quarter
	, 'Paid' as claim_status
from fichsrv.cosmos_ip_w_dnls_clm
where
	year(admit_start_dt) >= '2024'
	and clm_admit_type = 'ACUTE'
	and brand_fnl = 'M&R'
	and product_level_3_fnl != 'INSTITUTIONAL'
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and admit_ip_status_code = 'PD'
;

drop table if exists tmp_1m.kn_acute_cosmos_ip_denied;
create table tmp_1m.kn_acute_cosmos_ip_denied as
select distinct
	hicn as mbi
	, admit_start_dt as admit_start_date
	, admit_end_dt as admit_end_date
	, admit_qtr as admit_quarter
	, 'Denied' as claim_status
from fichsrv.cosmos_ip_w_dnls_clm
where
	year(admit_start_dt) >= '2024'
	and clm_admit_type = 'ACUTE'
	and brand_fnl = 'M&R'
	and product_level_3_fnl != 'INSTITUTIONAL'
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and admit_ip_status_code = 'DN'
;

select count(*) from tmp_1m.kn_acute_cosmos_ip_paid; 
-- 1,564,817
select count(*) from tmp_1m.kn_acute_cosmos_ip_denied; 
-- 237,126


drop table if exists tmp_1m.kn_acute_cosmos_ip_2024;
create table tmp_1m.kn_acute_cosmos_ip_2024 as
select distinct
	hicn as mbi
	, admit_start_dt as admit_start_date
	, admit_end_dt as admit_end_date
	, admit_qtr as admit_quarter
	, case when admit_ip_status_code = 'DN' then 'Denied'
		when admit_ip_status_code = 'PD' then 'Paid'
	end as claim_status
from fichsrv.cosmos_ip_w_dnls_clm
where
	year(admit_start_dt) = '2024'
	and clm_admit_type = 'ACUTE'
	and brand_fnl = 'M&R'
	and product_level_3_fnl != 'INSTITUTIONAL'
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and admit_ip_status_code in ('DN', 'PD')
;

drop table if exists tmp_1m.kn_acute_cosmos_ip_2025;
create table tmp_1m.kn_acute_cosmos_ip_2025 as
select distinct
	hicn as mbi
	, admit_start_dt as admit_start_date
	, admit_end_dt as admit_end_date
	, admit_qtr as admit_quarter
	, case when admit_ip_status_code = 'DN' then 'Denied'
		when admit_ip_status_code = 'PD' then 'Paid'
	end as claim_status
from fichsrv.cosmos_ip_w_dnls_clm
where
	year(admit_start_dt) = '2025'
	and clm_admit_type = 'ACUTE'
	and brand_fnl = 'M&R'
	and product_level_3_fnl != 'INSTITUTIONAL'
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and admit_ip_status_code in ('DN', 'PD')
;

select count(*) from tmp_1m.kn_acute_cosmos_ip_2024;
-- 1,215,529

select count(*) from tmp_1m.kn_acute_cosmos_ip_2025;
-- 586,414


drop table if exists tmp_1m.kn_acute_cosmos_ip_v2;
create table tmp_1m.kn_acute_cosmos_ip_v2 as
select
	hicn as mbi
	, admit_start_dt as admit_start_date
	, admit_end_dt as admit_end_date
	, admit_qtr as admit_quarter
	, case when admit_ip_status_code = 'DN' then 'Denied'
		when admit_ip_status_code = 'PD' then 'Paid'
	end as claim_status
    , count(distinct concat(hicn, admitid)) as n_claims
from fichsrv.cosmos_ip_w_dnls_clm
where
	year(admit_start_dt) >= '2024'
	and clm_admit_type = 'ACUTE'
	and brand_fnl = 'M&R'
	and product_level_3_fnl != 'INSTITUTIONAL'
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and admit_ip_status_code in ('DN', 'PD')
group by
	hicn
	, admit_start_dt 
	, admit_end_dt 
	, admit_qtr 
	, case when admit_ip_status_code = 'DN' then 'Denied'
		when admit_ip_status_code = 'PD' then 'Paid'
	end 
;

drop table if exists tmp_1m.kn_acute_cosmos_ip_v2;
create table tmp_1m.kn_acute_cosmos_ip_v2 as
select distinct
	hicn as mbi
	, admit_start_dt as admit_start_date
	, admit_end_dt as admit_end_date
	, admit_qtr as admit_quarter
	, case when admit_ip_status_code = 'DN' then 'Denied'
		when admit_ip_status_code = 'PD' then 'Paid'
	end as claim_status
from fichsrv.cosmos_ip_w_dnls_clm
where
	year(admit_start_dt) >= '2024'
	and clm_admit_type = 'ACUTE'
	and brand_fnl = 'M&R'
	and product_level_3_fnl != 'INSTITUTIONAL'
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and admit_ip_status_code in ('DN', 'PD')
;

select count(*) from tmp_1m.kn_acute_cosmos_ip_v2

select * from fichsrv.cosmos_ip_w_dnls_clm;


describe fichsrv.cosmos_ip_w_dnls_clm

select admit_ip_status_code from fichsrv.cosmos_ip_w_dnls_clm
group by admit_ip_status_code