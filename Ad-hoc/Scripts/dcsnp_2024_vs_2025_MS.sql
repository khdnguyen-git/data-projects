
create table tmp_1m.kn_dcsnp_unique_mbi_existing_202412 as
select distinct		
	fin_mbi_hicn_fnl as mbi
from fichsrv.tre_membership
where 1=1
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA'
	and fin_inc_month = '202412'
	and fin_tfm_product_new = 'DUAL_CHRONIC'
;

create table tmp_1m.kn_dcsnp_existing_case_count as
select 
	notif_yrmonth
	, count(distinct case_id) as case_count
from tmp_1m.kn_dcsnp_unique_mbi_existing_202412 as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
	on a.mbi = b.fin_mbi_hicn_fnl
where 1=1
	and notif_yrmonth >= '202401'
group by 
	notif_yrmonth
;

create table tmp_1m.kn_dcsnp_existing_mm as
select 
	fin_inc_month
	, count(distinct mbi) as mm
from tmp_1m.kn_dcsnp_unique_mbi_existing_202412 as a
left join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl
where 1=1
	and fin_inc_month >= '202401'
group by 
	fin_inc_month
;

drop table tmp_1m.kn_dcsnp_new_case_count;
create table tmp_1m.kn_dcsnp_new_case_count as
select 
	notif_yrmonth
	, count(distinct case_id) as case_count
from hce_proj_bd.hce_adr_avtar_like_24_25_f as a 
left join tmp_1m.kn_dcsnp_unique_mbi_existing_202412 as b
	on b.mbi = a.fin_mbi_hicn_fnl
where 1=1
	and b.mbi is null
	and notif_yrmonth >= '202401'
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA'
	and fin_tfm_product_new = 'DUAL_CHRONIC'
group by 
	notif_yrmonth
;

drop table tmp_1m.kn_dcsnp_new_mm;
create table tmp_1m.kn_dcsnp_new_mm as
select 
	fin_inc_month
	, count(distinct a.fin_mbi_hicn_fnl) as mm
from fichsrv.tre_membership as a 
left join tmp_1m.kn_dcsnp_unique_mbi_existing_202412 as b
	on b.mbi = a.fin_mbi_hicn_fnl
where 1=1
	and b.mbi is null
	and fin_inc_month >= '202401'
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA'
	and fin_tfm_product_new = 'DUAL_CHRONIC'
group by 
	fin_inc_month
;

drop table tmp_1m.kn_dcsnp_mm_combined;
create table tmp_1m.kn_dcsnp_mm_combined as
select
	'New Member' as member_cat
	, fin_inc_month
	, mm
from tmp_1m.kn_dcsnp_new_mm
union all
select
	'Existing Member' as member_cat
	, fin_inc_month
	, mm
from tmp_1m.kn_dcsnp_existing_mm
;


drop table tmp_1m.kn_dcsnp_case_count_combined;
create table tmp_1m.kn_dcsnp_case_count_combined as
select
	'New Member' as member_cat
	, notif_yrmonth
	, case_count
from tmp_1m.kn_dcsnp_new_case_count
union all
select
	'Existing Member' as member_cat
	, notif_yrmonth
	, case_count
from tmp_1m.kn_dcsnp_existing_case_count
;

-- Test join
drop table tmp_1m.kn_dcsnp_mm_case_count;
create table tmp_1m.kn_dcsnp_mm_case_count as 
select
	a.member_cat
	, a.fin_inc_month
	, a.mm
	, b.case_count
from tmp_1m.kn_dcsnp_mm_combined as a
join tmp_1m.kn_dcsnp_case_count_combined as b
	on a.member_cat = b.member_cat 
	and a.fin_inc_month = b.notif_yrmonth
;

-- Test union
drop table tmp_1m.kn_dcsnp_mm_case_count_v2;
create table tmp_1m.kn_dcsnp_mm_case_count_v2 as
select 
	'Membership' as data_type
	, member_cat
	, fin_inc_month as month
	, mm
	, 0 as case_count
from tmp_1m.kn_dcsnp_mm_combined
union all 
select 
	'Auth' as data_type
	, member_cat
	, notif_yrmonth as month
	, 0 as mm
	, case_count
from tmp_1m.kn_dcsnp_case_count_combined
;


select * from tmp_1m.kn_dcsnp_mm_case;
select * from tmp_1m.kn_dcsnp_mm_case_v2;