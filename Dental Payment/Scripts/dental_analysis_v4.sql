drop table if exists tmp_1m.kn_dental_claims_mbr_v5;
create table tmp_1m.kn_dental_claims_mbr_v5 as 
with cost_stat as (
select
	avg(unit_cost) as mean_unit_cost
	, percentile_approx(unit_cost, 0.5) as median_unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost
)
select 
	a.fin_inc_year
	, a.fin_inc_quarter
	, a.fin_market
	, b.temp_provtin
	, a.temp_provparstatus
	, a.proc_cd
	, a.service_category
	, a.total_allowed
	, a.total_unit_count
	, a.unit_cost
	, s.mean_unit_cost
	, s.median_unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost as a
join tmp_1m.kn_dental_top200_prov as b on a.temp_provtin = b.temp_provtin
cross join cost_stat as s
;

select count(*) from tmp_1m.kn_dental_claims_mbr_v5;

s





-- Mean + 2sd flag on unit_cost
drop table if exists tmp_1m.kn_dental_flag_proc_prov;
create table tmp_1m.kn_dental_flag_proc_prov as
with proc_stats as (
    select 
        proc_cd,
        avg(unit_cost) as mean_cost_proc,
        stddev(unit_cost) as sd_cost_proc
    from tmp_1m.kn_dental_claims_mbr_unit_cost
    group by proc_cd
),
prov_stats as (
    select 
        temp_provtin,
        avg(unit_cost) as mean_cost_prov,
        stddev(unit_cost) as sd_cost_prov
    from tmp_1m.kn_dental_claims_mbr_unit_cost
    group by temp_provtin
),
claims_with_stats as (
    select 
        a.*,
        b.mean_cost_proc,
        b.sd_cost_proc,
        c.mean_cost_prov,
        c.sd_cost_prov
    from tmp_1m.kn_dental_claims_mbr_unit_cost as a
    left join proc_stats as b on a.proc_cd = b.proc_cd
    left join prov_stats as c on a.temp_provtin = c.temp_provtin
)
select 
    *,
    case
        when unit_cost > (mean_cost_proc + 2 * sd_cost_proc) then '>2sd'
        when unit_cost > (mean_cost_proc + 1 * sd_cost_proc) then '1sd - 2sd'
        when unit_cost > mean_cost_proc then 'm - 1sd'
        else null
    end as proc_flag,
    case
        when unit_cost > (mean_cost_prov + 2 * sd_cost_prov) then '>2sd'
        when unit_cost > (mean_cost_prov + 1 * sd_cost_prov) then '1sd - 2sd'
        when unit_cost > mean_cost_prov then 'm - 1sd'
        else null
    end as prov_flag
from claims_with_stats;

describe tmp_1m.kn_dental_claims_mbr_unit_cost


-- Mean + 2sd flag on unit_cost but with serv
drop table if exists tmp_1m.kn_dental_flag_serv_prov;
create table tmp_1m.kn_dental_flag_serv_prov as
with serv_stats as (
    select 
        service_category,
        avg(unit_cost) as mean_cost_serv,
        stddev(unit_cost) as sd_cost_serv
    from tmp_1m.kn_dental_claims_mbr_unit_cost
    group by service_category
),
prov_stats as (
    select 
        temp_provtin,
        avg(unit_cost) as mean_cost_prov,
        stddev(unit_cost) as sd_cost_prov
    from tmp_1m.kn_dental_claims_mbr_unit_cost
    group by temp_provtin
),
claims_with_stats as (
    select 
        a.*,
        b.mean_cost_serv,
        b.sd_cost_serv,
        c.mean_cost_prov,
        c.sd_cost_prov
    from tmp_1m.kn_dental_claims_mbr_unit_cost as a
    left join serv_stats as b on a.service_category = b.service_category
    left join prov_stats as c on a.temp_provtin = c.temp_provtin
)
select 
    *,
    case
        when unit_cost > (mean_cost_serv + 2 * sd_cost_serv) then '>2sd'
        when unit_cost > (mean_cost_serv + 1 * sd_cost_serv) then '1sd - 2sd'
        when unit_cost > mean_cost_serv then 'm - 1sd'
        else null
    end as serv_flag,
    case
        when unit_cost > (mean_cost_prov + 2 * sd_cost_prov) then '>2sd'
        when unit_cost > (mean_cost_prov + 1 * sd_cost_prov) then '1sd - 2sd'
        when unit_cost > mean_cost_prov then 'm - 1sd'
        else null
    end as prov_flag
from claims_with_stats;



-- proc_cd's unit_cost within provider
drop table if exists tmp_1m.kn_dental_flag_variability_proc;
create table tmp_1m.kn_dental_flag_variability_proc as
select 
	proc_cd
	, temp_provtin
	, count(*) as n
	, avg(unit_cost) as avg_unit_cost
	, stddev(unit_cost) as sd_unit_cost
	, stddev(unit_cost) / avg(unit_cost) as cv_unit_cost
	, (max(unit_cost) - min(unit_cost)) / avg(unit_cost) as range_over_avg
from tmp_1m.kn_dental_claims_mbr_unit_cost 
group by 
	proc_cd
	, temp_provtin
having count(*) >= 5
;
select * from tmp_1m.kn_dental_flag_variability


-- service_category 
drop table if exists tmp_1m.kn_dental_flag_variability_serv;
create table tmp_1m.kn_dental_flag_variability_serv as
select 
	service_category
	, temp_provtin
	, temp_provparstatus
	, proc_cd
	, fin_inc_year
	, fin_inc_quarter
	, count(*) as n
	, avg(unit_cost) as avg_unit_cost
	, stddev(unit_cost) as sd_unit_cost
	, stddev(unit_cost) / avg(unit_cost) as cv_unit_cost
	, (max(unit_cost) - min(unit_cost)) / avg(unit_cost) as range_over_avg
from tmp_1m.kn_dental_claims_mbr_unit_cost 
group by 
	service_category
	, temp_provtin
	, temp_provparstatus
	, proc_cd
	, fin_inc_year
	, fin_inc_quarter
having count(*) >= 5
;

select count(*) from tmp_1m.kn_dental_flag_variability_serv;

select * from tmp_1m.kn_dental_flag_variability_serv;

describe tmp_1m.kn_dental_claims_mbr_unit_cost;

select count(*) from tmp_1m.kn_dental_claims_mbr_unit_cost
where unit_cost = 0.0

select * from tmp_1m.kn_dental_claims_mbr_unit_cost
where unit_cost < 1.0
;

drop table if exists tmp_1m.kn_dental_serv_export_1;
create table tmp_1m.kn_dental_serv_export_1 as 
select
	a.service_category
	, b.temp_provtin
	, a.temp_provparstatus
	, a.proc_cd
	, a.fin_inc_year
	, a.fin_inc_quarter
	, a.n
	, a.avg_unit_cost
	, a.sd_unit_cost
	, a.cv_unit_cost
	, a.range_over_avg
from tmp_1m.kn_dental_flag_variability_serv as a 
join tmp_1m.kn_dental_top200_prov as b 
	on a.temp_provtin = b.temp_provtin
;

-- QoQ;
drop table if exists tmp_1m.kn_dental_unit_cost_diff;
create table tmp_1m.kn_dental_unit_cost_diff as
with lag_unit_cost as (
select
	temp_provtin
	, service_category
	, temp_provparstatus
	, proc_cd
	, fin_inc_year
	, fin_inc_quarter
	, total_unit_count
	, avg(unit_cost) as avg_unit_cost
	, stddev(unit_cost) as sd_unit_cost
	, stddev(unit_cost) / avg(unit_cost) as cv_unit_cost
	, (max(unit_cost) - min(unit_cost)) / avg(unit_cost) as range_over_avg
	, lag(avg(unit_cost)) over (
		partition by 
			temp_provtin
			, service_category
			, temp_provparstatus
			, proc_cd
		order by 
			fin_inc_year
			, fin_inc_quarter
	) as prev_quarter_unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost
group by
	temp_provtin
	, service_category
	, temp_provparstatus
	, proc_cd
	, fin_inc_year
	, fin_inc_quarter
	, total_unit_count
)
select
	temp_provtin
	, service_category
	, temp_provparstatus
	, proc_cd
	, fin_inc_year
	, fin_inc_quarter
	, total_unit_count
	, avg_unit_cost
	, prev_quarter_unit_cost
	, case 
		when prev_quarter_unit_cost is not null
		then avg_unit_cost - prev_quarter_unit_cost 
		else null
	end as qoq_avg_unit_cost
	, case
		when prev_quarter_unit_cost is not null and prev_quarter_unit_cost != 0
		then (avg_unit_cost - prev_quarter_unit_cost) / prev_quarter_unit_cost
		else null
	end as rel_qoq_avg_unit_cost
	, sd_unit_cost
	, cv_unit_cost
	, range_over_avg
from lag_unit_cost
;

select * from tmp_1m.kn_dental_unit_cost_diff;


drop table if exists tmp_1m.kn_dental_serv_export_2;
create table tmp_1m.kn_dental_serv_export_2 as 
select 
	b.temp_provtin
	, a.service_category
	, a.temp_provparstatus
	, a.proc_cd
	, a.fin_inc_year
	, a.fin_inc_quarter
	, a.total_unit_count
	, a.avg_unit_cost
	, a.qoq_avg_unit_cost
	, a.rel_qoq_avg_unit_cost
	, a.yoy_avg_unit_cost
	, a.rel_yoy_avg_unit_cost
from tmp_1m.kn_dental_unit_cost_diff as a
join tmp_1m.kn_dental_top200_prov as b 
	on a.temp_provtin = b.temp_provtin
;

select count(*) from  tmp_1m.kn_dental_serv_export_2 
select count(*) from  tmp_1m.kn_dental_serv_export_1


drop table if exists tmp_1m.kn_dental_serv_export_3;
create table tmp_1m.kn_dental_serv_export_3 as 
select
	b.temp_provtin
	, a.service_category
	, a.temp_provparstatus
	, a.proc_cd
	, a.fin_inc_year
	, a.fin_inc_quarter
	, a.total_unit_count
	, a.avg_unit_cost
	, a.qoq_avg_unit_cost
	, a.rel_qoq_avg_unit_cost
	, a.sd_unit_cost
	, a.cv_unit_cost
	, a.range_over_avg
from tmp_1m.kn_dental_unit_cost_diff as a
join tmp_1m.kn_dental_top200_prov as b 
	on a.temp_provtin = b.temp_provtin
;





















	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	