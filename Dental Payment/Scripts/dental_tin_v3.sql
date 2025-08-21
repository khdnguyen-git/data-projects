select count(*) from tmp_1y.HCE_DENTAL_CLMS_FNL
where hce_tre_


describe tmp_1y.HCE_DENTAL_CLMS_FNL;

select 
        paid_date_dt
        , case 
                when substring(paid_date_dt, 6, 2) in ('01', '02', '03') then concat(substring(paid_date_dt, 1, 4), 'Q1')
                when substring(paid_date_dt, 6, 2) in ('04', '05', '06') then concat(substring(paid_date_dt, 1, 4), 'Q2')
                when substring(paid_date_dt, 6, 2) in ('07', '08', '09') then concat(substring(paid_date_dt, 1, 4), 'Q3')
                when substring(paid_date_dt, 6, 2) in ('10', '11', '12') then concat(substring(paid_date_dt, 1, 4), 'Q4')
        end as paid_quarter
        , case 
                when substring(serv_date_dt, 6, 2) in ('01', '02', '03') then concat(substring(serv_date_dt, 1, 4), 'Q1')
                when substring(serv_date_dt, 6, 2) in ('04', '05', '06') then concat(substring(serv_date_dt, 1, 4), 'Q2')
                when substring(serv_date_dt, 6, 2) in ('07', '08', '09') then concat(substring(serv_date_dt, 1, 4), 'Q3')
                when substring(serv_date_dt, 6, 2) in ('10', '11', '12') then concat(substring(serv_date_dt, 1, 4), 'Q4')
        end as serv_quarter
        , fin_inc_month
        , case 
                when substring(fin_inc_month, 5, 2) in ('01', '02', '03') then concat(substring(fin_inc_month, 1, 4), 'Q1')
                when substring(fin_inc_month, 5, 2) in ('04', '05', '06') then concat(substring(fin_inc_month, 1, 4), 'Q2')
                when substring(fin_inc_month, 5, 2) in ('07', '08', '09') then concat(substring(fin_inc_month, 1, 4), 'Q3')
                when substring(fin_inc_month, 5, 2) in ('10', '11', '12') then concat(substring(fin_inc_month, 1, 4), 'Q4')
        end as fin_inc_quarter
       from tmp_1y.HCE_DENTAL_CLMS_FNL where dental_clm_source = 'FACETS'
limit 300;

select
	hce_tre_mbr_id
	, fin_mbi_hicn_fnl
	, member_id
	, temp_productid
from tmp_1y.HCE_DENTAL_CLMS_FNL
limit 100

	
-- Making claims table
create table tmp_1m.kn_dental_claims_unit
stored as orc as
select
	'Claims' as data_type
	, fin_inc_year
	, fin_inc_month
        , case 
                when substring(fin_inc_month, 5, 2) in ('01', '02', '03') then concat(substring(fin_inc_month, 1, 4), 'Q1')
                when substring(fin_inc_month, 5, 2) in ('04', '05', '06') then concat(substring(fin_inc_month, 1, 4), 'Q2')
                when substring(fin_inc_month, 5, 2) in ('07', '08', '09') then concat(substring(fin_inc_month, 1, 4), 'Q3')
                when substring(fin_inc_month, 5, 2) in ('10', '11', '12') then concat(substring(fin_inc_month, 1, 4), 'Q4')
        end as fin_inc_quarter
        , date_format(paid_date_dt,'yyyy') as paid_year
        , date_format(paid_date_dt,'yyyyMM') as paid_month
        , case 
                when substring(paid_date_dt, 6, 2) in ('01', '02', '03') then concat(substring(paid_date_dt, 1, 4), 'Q1')
                when substring(paid_date_dt, 6, 2) in ('04', '05', '06') then concat(substring(paid_date_dt, 1, 4), 'Q2')
                when substring(paid_date_dt, 6, 2) in ('07', '08', '09') then concat(substring(paid_date_dt, 1, 4), 'Q3')
                when substring(paid_date_dt, 6, 2) in ('10', '11', '12') then concat(substring(paid_date_dt, 1, 4), 'Q4')
        end as paid_quarter
	, fin_market
	, temp_provparstatus
	, temp_provtin
	, proc_cd
	, service_category
        , serv_date_dt
        , case 
                when substring(serv_date_dt, 6, 2) in ('01', '02', '03') then concat(substring(serv_date_dt, 1, 4), 'Q1')
                when substring(serv_date_dt, 6, 2) in ('04', '05', '06') then concat(substring(serv_date_dt, 1, 4), 'Q2')
                when substring(serv_date_dt, 6, 2) in ('07', '08', '09') then concat(substring(serv_date_dt, 1, 4), 'Q3')
                when substring(serv_date_dt, 6, 2) in ('10', '11', '12') then concat(substring(serv_date_dt, 1, 4), 'Q4')
        end as serv_quarter
	, sum(allowed_amt) as allowed
	, sum(paid_amt) as paid
	, sum(paid_amt_completed) as completed_paid
	, count(distinct concat(fin_mbi_hicn_fnl, '|', serv_date_dt, '|', proc_cd)) as unit_count
	, 0 as mm
	, fin_product_level_3
	, global_cap
	, tfm_include_flag
	, dental_clm_source
	, fin_tfm_product_new
	, fin_brand
	, fin_source_name
	, fin_g_i
from tmp_1y.HCE_DENTAL_CLMS_FNL 
where dental_clm_source = 'FACETS'
	and fin_inc_month between '202301' and '202412'
group by 
        fin_inc_year
        , fin_inc_month
        , case 
                when substring(fin_inc_month, 5, 2) in ('01', '02', '03') then concat(substring(fin_inc_month, 1, 4), 'Q1')
                when substring(fin_inc_month, 5, 2) in ('04', '05', '06') then concat(substring(fin_inc_month, 1, 4), 'Q2')
                when substring(fin_inc_month, 5, 2) in ('07', '08', '09') then concat(substring(fin_inc_month, 1, 4), 'Q3')
                when substring(fin_inc_month, 5, 2) in ('10', '11', '12') then concat(substring(fin_inc_month, 1, 4), 'Q4')
        end
        , date_format(paid_date_dt,'yyyy')
        , date_format(paid_date_dt,'yyyyMM')
        , case 
                when substring(paid_date_dt, 6, 2) in ('01', '02', '03') then concat(substring(paid_date_dt, 1, 4), 'Q1')
                when substring(paid_date_dt, 6, 2) in ('04', '05', '06') then concat(substring(paid_date_dt, 1, 4), 'Q2')
                when substring(paid_date_dt, 6, 2) in ('07', '08', '09') then concat(substring(paid_date_dt, 1, 4), 'Q3')
                when substring(paid_date_dt, 6, 2) in ('10', '11', '12') then concat(substring(paid_date_dt, 1, 4), 'Q4')
                end
        , fin_market
        , temp_provparstatus
        , temp_provtin
        , proc_cd
        , service_category
        , serv_date_dt
        , case 
        when substring(serv_date_dt, 6, 2) in ('01', '02', '03') then concat(substring(serv_date_dt, 1, 4), 'Q1')
                when substring(serv_date_dt, 6, 2) in ('04', '05', '06') then concat(substring(serv_date_dt, 1, 4), 'Q2')
                when substring(serv_date_dt, 6, 2) in ('07', '08', '09') then concat(substring(serv_date_dt, 1, 4), 'Q3')
                when substring(serv_date_dt, 6, 2) in ('10', '11', '12') then concat(substring(serv_date_dt, 1, 4), 'Q4')
                end
        , fin_product_level_3
        , global_cap
        , tfm_include_flag
        , dental_clm_source
        , fin_tfm_product_new
        , fin_brand
        , fin_source_name
        , fin_g_i
;


drop table tmp_1m.kn_dental_mbr;
create table tmp_1m.kn_dental_mbr 
stored as orc as
select
    'Membership' as data_type
    , fin_inc_year
    , fin_inc_month
    , case 
        when substring(fin_inc_month, 5, 2) in ('01', '02', '03') then concat(substring(fin_inc_month, 1, 4), 'Q1')
        when substring(fin_inc_month, 5, 2) in ('04', '05', '06') then concat(substring(fin_inc_month, 1, 4), 'Q2')
        when substring(fin_inc_month, 5, 2) in ('07', '08', '09') then concat(substring(fin_inc_month, 1, 4), 'Q3')
        when substring(fin_inc_month, 5, 2) in ('10', '11', '12') then concat(substring(fin_inc_month, 1, 4), 'Q4')
    end as fin_inc_quarter
    , '' as paid_year
    , '' as paid_month
    , '' as paid_quarter
    , fin_market
    , '' as temp_provparstatus
    , '' as temp_provtin
    , '' as proc_cd
    , '' as service_category
    , '' as serv_date_dt
    , '' as serv_quarter
    , cast(0 as decimal(38, 4)) as allowed
    , cast(0 as decimal(38, 4)) as paid
    , cast(0 as decimal(38, 4)) as completed_paid
    , cast(0 as decimal(38, 4)) as unit_count
    , sum(member_month) as mm
    , fin_product_level_3
    , global_cap
    , tfm_include_flag
    , dental_source as dental_clm_source
    , fin_tfm_product_new
    , fin_brand
    , fin_source_name
    , fin_g_i
from tmp_1y.hce_dental_facets_skygen_membership_2
where dental_source in ('FACETS')
    and fin_inc_month between '202301' and '202412'
group by
    fin_inc_year
    , fin_inc_month
    , case 
        when substring(fin_inc_month, 5, 2) in ('01', '02', '03') then concat(substring(fin_inc_month, 1, 4), 'Q1')
        when substring(fin_inc_month, 5, 2) in ('04', '05', '06') then concat(substring(fin_inc_month, 1, 4), 'Q2')
        when substring(fin_inc_month, 5, 2) in ('07', '08', '09') then concat(substring(fin_inc_month, 1, 4), 'Q3')
        when substring(fin_inc_month, 5, 2) in ('10', '11', '12') then concat(substring(fin_inc_month, 1, 4), 'Q4')
    end
    , fin_market
    , fin_product_level_3
    , global_cap
    , tfm_include_flag
    , dental_source
    , fin_tfm_product_new
    , fin_brand
    , fin_source_name
    , fin_g_i
;

select * from tmp_1m.kn_dental_claims_unit limit 2;

create table tmp_1m.kn_dental_mbr as 
select * from tmp_1y.kn_dental_mbr;

drop table if exists tmp_1m.kn_dental_claims_mbr_full;
create table tmp_1m.kn_dental_claims_mbr_full stored as orc as
select * from tmp_1m.kn_dental_claims_unit
union all 
select * from tmp_1m.kn_dental_mbr
group by
	data_type
    , fin_inc_year
    , fin_inc_month
    , fin_inc_quarter
    , paid_year
    , paid_month
    , paid_quarter
    , fin_market
    , temp_provparstatus
    , temp_provtin
    , proc_cd
    , service_category
    , serv_date_dt
    , serv_quarter
    , allowed
    , paid
    , paid_year
    , completed_paid
    , unit_count
    , mm
    , fin_product_level_3
    , global_cap
    , tfm_include_flag
    , dental_clm_source
    , fin_tfm_product_new
    , fin_brand
    , fin_source_name
    , fin_g_i
	
;


select count(*) from tmp_1y.kn_dental_claims_mbr_full;

drop table if exists tmp_1m.kn_dental_claims_mbr_full;
create table tmp_1m.kn_dental_claims_mbr_full stored as orc as
select * from tmp_1m.kn_dental_claims_unit
union 
select * from tmp_1m.kn_dental_mbr
;

select count(*) from tmp_1m.kn_dental_claims_mbr_full;

drop table if exists tmp_1m.kn_dental_claims_mbr_v1;
create table tmp_1m.kn_dental_claims_mbr_v1 as
select
    data_type
    , fin_inc_year
    , fin_inc_month
    , fin_inc_quarter
    , paid_year
    , paid_month
    , paid_quarter
    , fin_market
	, temp_provparstatus
    , temp_provtin
    , proc_cd
    , case 
        when service_category is null then "Unknown"
        else service_category
    end as service_category
    , serv_date_dt
    , serv_quarter
    , allowed
    , case 
        when allowed > 0 then 1
        else 0
    end as util_check
    , paid
    , completed_paid
    , unit_count
    , mm
    , fin_product_level_3
    , global_cap
    , tfm_include_flag
    , dental_clm_source
    , fin_tfm_product_new
    , fin_brand
    , fin_source_name
    , fin_g_i
from tmp_1m.kn_dental_claims_mbr_full
;

select count(*) from tmp_1m.kn_dental_claims_mbr_full
;

select count(*) from tmp_1m.kn_dental_claims_mbr_v1;

--create table tmp_1m.kn_dental_claims_mbr_v2 as
--select 
--	*
--	, case when allowed > 0 then 1 
--		else 0 
--	end as util_check
--from tmp_1y.kn_dental_claims_mbr;



describe tmp_1m.kn_dental_claims_mbr_v1;

describe tmp_1m.kn_dental_claims_mbr_v1


-- Adding unit cost and re-aggregate
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


-- select * from tmp_1m.kn_dental_claims_mbr_unit_cost;

select count(*) from tmp_1m.kn_dental_claims_mbr_v2; 
-- 15,513,231 20250605




-- Find abnormality
create table tmp_1m.kn_dental_r_2024 as
select 
proc_cd
	, service_category
	, total_allowed
	, total_unit_count
	, unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost 
where fin_inc_year = '2024'
;

create table tmp_1m.kn_dental_r_2023 as
select 
	temp_provtin
	, proc_cd
	, service_category
	, total_allowed
	, total_unit_count
	, unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost 
where fin_inc_year = '2024'
;


select count(*) from tmp_1m.kn_dental_top200_prov;
select count(*) from tmp_1m.kn_dental_top_proc;

drop table tmp_1m.kn_dental_claims_mbr_v4;
create table tmp_1m.kn_dental_claims_mbr_v4 as 
with cost_stat as (
select
	avg(unit_cost) as mean_unit_cost
	, percentile_approx(unit_cost, 0.5) as median_unit_cost
	, percentile_approx(unit_cost, 0.70) as p70_unit_cost
	, percentile_approx(unit_cost, 0.95) as p95_unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost
)
select 
	a.fin_inc_year
	, a.fin_inc_quarter
	, a.fin_market
	, b.temp_provtin
	, a.temp_provparstatus
	, c.proc_cd
	, a.service_category
	, a.total_allowed
	, a.total_unit_count
	, a.unit_cost
	, s.mean_unit_cost
	, s.median_unit_cost
	, s.p70_unit_cost
	, s.p95_unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost as a
join tmp_1m.kn_dental_top200_prov as b on a.temp_provtin = b.temp_provtin
join tmp_1m.kn_dental_top_proc as c on a.proc_cd = c.proc_cd
cross join cost_stat as s

select count (distinct temp_provtin) from tmp_1m.kn_dental_top200_prov

select count(distinct temp_provtin) from tmp_1m.kn_dental_top200_prov ;
select count(*) from tmp_1m.kn_dental_top200_prov ;

select count(*) from tmp_1. ;
select count(*) from tmp_1. ;

select count(*) from tmp_1m.kn_dental_claims_mbr_v4

drop table if exists tmp_1m.kn_dental_claims_mbr_v5;
create table tmp_1m.kn_dental_claims_mbr_v5 as 
with cost_stat as (
select
	avg(unit_cost) as mean_unit_cost
	, percentile_approx(unit_cost, 0.5) as median_unit_cost
	, percentile_approx(unit_cost, 0.70) as p70_unit_cost
	, percentile_approx(unit_cost, 0.95) as p95_unit_cost
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
	, s.p70_unit_cost
	, s.p95_unit_cost
from tmp_1m.kn_dental_claims_mbr_unit_cost as a
join tmp_1m.kn_dental_top200_prov as b on a.temp_provtin = b.temp_provtin
cross join cost_stat as s
;

select count(*) from tmp_1m.kn_dental_claims_mbr_v5;


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



select * from tmp_1m.kn_dental_flag_proc_prov;
	

	


drop table tmp_1m.dental_claims_mbr_v3_wide;
create table tmp_1m.dental_claims_mbr_v3_wide as
select
    fin_inc_year
    , fin_market
    , temp_provtin
    , temp_provparstatus
    , proc_cd
    , service_category
	, max(case when fin_inc_quarter = '2023Q1' then unit_cost else 0.0 end) as 2023Q1
	, max(case when fin_inc_quarter = '2023Q2' then unit_cost else 0.0 end) as 2023Q2
	, max(case when fin_inc_quarter = '2023Q3' then unit_cost else 0.0 end) as 2023Q3
	, max(case when fin_inc_quarter = '2023Q4' then unit_cost else 0.0 end) as 2023Q4
	, max(case when fin_inc_quarter = '2024Q1' then unit_cost else 0.0 end) as 2024Q1
	, max(case when fin_inc_quarter = '2024Q2' then unit_cost else 0.0 end) as 2024Q2
	, max(case when fin_inc_quarter = '2024Q3' then unit_cost else 0.0 end) as 2024Q3
	, max(case when fin_inc_quarter = '2024Q4' then unit_cost else 0.0 end) as 2024Q4
from tmp_1m.kn_dental_claims_mbr_unit_cost
group by
    data_type
    , fin_inc_year
    , fin_inc_month
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

select count(*) from tmp_1m.dental_claims_mbr_v3_wide;

-- Smaller version of v3_wide
create table tmp_1m.dental_claims_mbr_v4_wide as
select 
	temp_provtin
    , temp_provparstatus
    , proc_cd
    , service_category
	, max(case when fin_inc_quarter = '2023Q1' then unit_cost else 0.0 end) as 2023Q1
	, max(case when fin_inc_quarter = '2023Q2' then unit_cost else 0.0 end) as 2023Q2
	, max(case when fin_inc_quarter = '2023Q3' then unit_cost else 0.0 end) as 2023Q3
	, max(case when fin_inc_quarter = '2023Q4' then unit_cost else 0.0 end) as 2023Q4
	, max(case when fin_inc_quarter = '2024Q1' then unit_cost else 0.0 end) as 2024Q1
	, max(case when fin_inc_quarter = '2024Q2' then unit_cost else 0.0 end) as 2024Q2
	, max(case when fin_inc_quarter = '2024Q3' then unit_cost else 0.0 end) as 2024Q3
	, max(case when fin_inc_quarter = '2024Q4' then unit_cost else 0.0 end) as 2024Q4
from tmp_1m.kn_dental_claims_mbr_unit_cost
group by
	temp_provtin
    , temp_provparstatus
    , proc_cd
    , service_category
;


select count(*) from tmp_1m.dental_claims_mbr_v4_wide 
	



