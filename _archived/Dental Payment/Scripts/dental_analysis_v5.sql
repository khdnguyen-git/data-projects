-- Sampling;
create table tmp_1m.kn_dental_serv_proc_list as
	with count as (
	select 
		service_category
		, proc_cd
		, count(*)
	from tmp_1m.kn_dental_claims_mbr_unit_cost
	group by
		service_category
		, proc_cd
	having count(*) >= 10
)
select distinct service_category, proc_cd
from count;

create table tmp_1m.kn_dental_serv_list as
select 
	distinct service_category
from tmp_1m.kn_dental_serv_proc_list;

create table tmp_1m.kn_dental_proc_list as
select 
	distinct proc_cd
from tmp_1m.kn_dental_serv_proc_list;

select * from tmp_1m.kn_dental_proc_list;


drop table if exists tmp_1m.kn_dental_claims_mbr_v2;
create table tmp_1m.kn_dental_claims_mbr_v2 as
select 
	data_type
	, fin_inc_year
	, fin_inc_month
	, fin_inc_quarter
	, fin_market
	, fin_product_level_3
	, fin_g_i
	, fin_brand
	, fin_source_name
	, tfm_include_flag
	, fin_tfm_product_new
	, global_cap
	, temp_provtin
	, temp_provparstatus
	, proc_cd
	, service_category
	, sum(allowed) as total_allowed
	, sum(unit_count) as total_unit_count
from tmp_1y.kn_dental_claims_mbr_v1
where util_check = 1
group by
	data_type
	, fin_inc_year
	, fin_inc_month
	, fin_inc_quarter
	, fin_market
	, fin_product_level_3
	, fin_g_i
	, fin_brand
	, fin_source_name
	, tfm_include_flag
	, fin_tfm_product_new
	, global_cap
	, temp_provtin
	, temp_provparstatus
	, proc_cd
	, service_category
;

select count(*) from tmp_1m.kn_dental_claims_mbr_v2
where temp_provparstatus = 'I'

drop table if exists tmp_1m.kn_pivot_test_1;
create table tmp_1m.kn_pivot_test_1 as
select 
	temp_provtin
	, temp_provparstatus
	, service_category
	, fin_inc_quarter
	, sum(total_allowed) as allowed
	, sum(total_unit_count) as unit_count
	, sum(sum(total_allowed)) / sum(nullif(sum(total_unit_count), 0)) as cost_sql_1
from tmp_1m.kn_dental_claims_mbr_v2
group by 
	temp_provtin
	, temp_provparstatus
	, service_category
	, fin_inc_quarter
limit 1000
;

drop table if exists tmp_1m.kn_pivot_test_2;
create table tmp_1m.kn_pivot_test_2 as
with pivot_test as (
select 
	temp_provtin
	, temp_provparstatus
	, service_category
	, fin_inc_quarter
	, sum(total_allowed) as allowed
	, sum(total_unit_count) as unit_count
from tmp_1m.kn_dental_claims_mbr_v2
group by 
	temp_provtin
	, temp_provparstatus
	, service_category
	, fin_inc_quarter
limit 1000
)
select 
	temp_provtin
	, temp_provparstatus
	, service_category
	, fin_inc_quarter
	, allowed
	, unit_count
	, sum(allowed) / sum(unit_count) as cost_sql_2
from pivot_test
group by 
	temp_provtin
	, temp_provparstatus
	, service_category
	, fin_inc_quarter
	, allowed
	, unit_count





drop table if exists tmp_1m.kn_dental_claims_mbr_v3;
create table tmp_1m.kn_dental_claims_mbr_v3 as
select distinct
	a.fin_inc_year
	, a.fin_inc_quarter
	, a.fin_market
	, b.temp_provtin
	, a.temp_provparstatus
	, c.service_category
	, d.proc_cd
	, a.total_allowed
	, a.total_unit_count
from tmp_1m.kn_dental_claims_mbr_v2 as a
join tmp_1m.kn_dental_top200_prov as b
	on a.temp_provtin = b.temp_provtin
join tmp_1m.kn_dental_serv_list as c
	on a.service_category = c.service_category
join tmp_1m.kn_dental_proc_list as d
	on a.proc_cd = d.proc_cd
;

select max(fin_inc_month) from tmp_1m.kn_dental_claims_mbr_v2



select count(*) from tmp_1m.kn_dental_claims_mbr_v3;
-- 334,627;



select count(*) from tmp_1m.kn_dental_export_1;





	
