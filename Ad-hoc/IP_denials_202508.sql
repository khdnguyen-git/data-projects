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
