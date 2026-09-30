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

create table tmp_1m.kn_dental_prov_check as 
select
	temp_provtin 
	, sum(totaL_allowed) as sum_allowed
	, sum(total_unit_count) as sum_count
from tmp_1m.kn_dental_claims_mbr_v2
group by
	temp_provtin
order by
	sum_allowed desc
limit 1000;

create table tmp_1m.kn_dental_top500_prov as
select distinct 
	temp_provtin
	, sum_allowed
from tmp_1m.kn_dental_prov_check
order by 
	sum_allowed desc
limit 500
;

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
join tmp_1m.kn_dental_top500_prov as b
	on a.temp_provtin = b.temp_provtin
join tmp_1m.kn_dental_serv_list as c
	on a.service_category = c.service_category
join tmp_1m.kn_dental_proc_list as d
	on a.proc_cd = d.proc_cd
;

select count(*) from tmp_1m.kn_dental_claims_mbr_v3;


