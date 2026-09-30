drop table tmp_1m.kn_dental_claims_mbr_v3;
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
	, a.total_allowed / a.total_unit_count as unit_cost
from tmp_1m.kn_dental_claims_mbr_v2 as a
join tmp_1m.kn_dental_top500_prov as b
	on a.temp_provtin = b.temp_provtin
join tmp_1m.kn_dental_serv_list as c
	on a.service_category = c.service_category
join tmp_1m.kn_dental_proc_list as d
	on a.proc_cd = d.proc_cd
;

select * from tmp_1m.kn_dental_claims_mbr_v2;

select count(*) from tmp_1m.kn_dental_claims_mbr_v3;

drop table tmp_1m.kn_dental_proc_risk;
create table tmp_1m.kn_dental_proc_risk as
with groups as (
select 
	temp_provtin
	, proc_cd
	, sum(total_allowed) as total_allowed
	, sum(total_unit_count) as total_unit_count
	, sum(total_allowed) / sum(total_unit_count) as total_unit_cost
from tmp_1m.kn_dental_claims_mbr_v3
group by
	temp_provtin
	, proc_cd
),
proc_thresholds as (
select
	proc_cd
	, avg(total_unit_cost) as mean
	, percentile_approx(total_unit_cost, 0.5) as median
	, percentile_approx(total_unit_cost, 0.70) as p70
	, percentile_approx(total_unit_cost, 0.95) as p95
from groups
group by 
	proc_cd
)
select 
	a.temp_provtin
	, a.proc_cd
	, a.total_unit_cost
	, b.mean
	, b.median
	, b.p70
	, b.p95
	, case 
		when total_unit_cost > p95 then 'high'
		when total_unit_cost < b.p95 and total_unit_cost > b.p70 then 'medium'
		when total_unit_cost < b.p70 and total_unit_cost > b.median then 'low'
		else 'normal'
	end as risk
from groups as a
join proc_thresholds as b
on a.proc_cd = b.proc_cd;
	
drop table tmp_1m.kn_dental_claims_mbr_v4;
create table tmp_1m.kn_dental_claims_mbr_v4 as
select
	a.fin_inc_year
	, a.fin_inc_quarter
	, a.fin_market
	, a.temp_provtin
	, a.temp_provparstatus
	, a.service_category
	, a.proc_cd
	, a.total_allowed
	, a.total_unit_count
	, b.mean
	, b.median
	, b.p70
	, b.p95
	, b.total_unit_cost
	, b.risk
from tmp_1m.kn_dental_claims_mbr_v3 as a
join tmp_1m.kn_dental_proc_risk as b
on a.temp_provtin = b.temp_provtin
	and a.proc_cd = b.proc_cd
	and a.unit_cost = b.total_unit_cost
;

select count(*) from (
select * from tmp_1m.kn_dental_claims_mbr_v4
where risk = 'high') as sub

-- 329
select count(distinct temp_provtin) from (
select * from tmp_1m.kn_dental_claims_mbr_v4
where risk = 'high') as sub

select count(*) from tmp_1m.kn_dental_claims_mbr_v4
where median > mean;

select count(*) from tmp_1m.kn_dental_claims_mbr_v4
where median < mean;

select count(*) from tmp_1m.kn_dental_claims_mbr_v4
where median > mean;

select count(*) from tmp_1m.kn_dental_claims_mbr_v4;






