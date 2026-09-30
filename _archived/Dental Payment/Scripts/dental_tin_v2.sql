
pick 3 random markets, sum completed_paid, prov in market, pick 20 providers, then proc_cd to compare completed_paid between providers. Examine, then restart with all markets


select data_type
	, fin_inc_year
	, fin_inc_month
, case when fin_inc_year is not null then concat(fin_inc_year, case 
        when substring(fin_inc_month, -2) in ('01', '02', '03') then 'q1'
        when substring(fin_inc_month, -2) in ('04', '05', '06') then 'q2'
        when substring(fin_inc_month, -2) in ('07', '08', '09') then 'q3'
        when substring(fin_inc_month, -2) in ('10', '11', '12') then 'q4'
        else null
    end
	)
	end as fin_inc_quarter
from tmp_1y.HCE_DENTAL_FACETS_CLM_MBR_ROLLUP
limit 100;

drop table tmp_1m.kn_dental_pmt;
create table tmp_1m.kn_dental_pmt as
select 
	*
	, case when substring(fin_inc_month, -2) in ('01', '02', '03') then concat(fin_inc_year, 'Q1')
		when substring(fin_inc_month, -2) in ('04', '05', '06') then concat(fin_inc_year, 'Q2')
		when substring(fin_inc_month, -2) in ('07', '08', '09') then concat(fin_inc_year, 'Q3')
		when substring(fin_inc_month, -2) in ('10', '11', '12') then concat(fin_inc_year, 'Q4')
	end as fin_inc_quarter
	, case when substring(paid_month, -2) in ('01', '02', '03') then concat(paid_year, 'Q1')
		when substring(paid_month, -2) in ('04', '05', '06') then concat(paid_year, 'Q2')
		when substring(paid_month, -2) in ('07', '08', '09') then concat(paid_year, 'Q3')
		when substring(paid_month, -2) in ('10', '11', '12') then concat(paid_year, 'Q4')
	end as paid_quarter
from tmp_1y.HCE_DENTAL_FACETS_CLM_MBR_ROLLUP
;

drop table tmp_1m.kn_dental_market_top5paid;
create table tmp_1m.kn_dental_market_top5paid as
select  
	fin_market
	, sum(paid) as total_market_paid
from tmp_1m.kn_dental_aggregated
group by fin_market
order by total_paid_market desc
;

drop table tmp_1m.kn_dental_market_top5mm;
create table tmp_1m.kn_dental_market_top5mm as
select  
	fin_market
	, sum(Mbrs) as total_mm_market
from tmp_1m.kn_dental_aggregated
group by fin_market
order by total_mm_market desc
;



select * from tmp_1m.kn_dental_market_top5paid
-- TX, FL, WI, AZ, CO, NC, CA-S, MO, WA, IN
select * from tmp_1m.kn_dental_market_top5mm
-- TX, FL, CA-S, WI, NC, NY, AZ, CO, MO, IN

create table tmp_1m.kn_dental_prov_top100paid_market as
select 
	temp_provtin
	, sum(completed_paid) as total_prov_paid
from tmp_1m.kn_dental_aggregated
where fin_market in ('TX')
group by temp_provtin
order by total_prov_paid desc
limit 100
;


-- Proportion of paid / prov

create table tmp_1m.kn_dental_prov_top100paid_all as
select 
	temp_provtin
	, fin_market
	, sum(completed_paid) as total_prov_paid
from tmp_1m.kn_dental_aggregated
group by temp_provtin
order by total_prov_paid desc
limit 100
;


select 
	*
from tmp_1m.kn_dental_aggregated
where fin_market = 'TX'

drop table tmp_1m.kn_dental_aggregated_sample;
create table tmp_1m.kn_dental_aggregated_sample as

select
	count(*)
from tmp_1m.kn_dental_aggregated
where fin_market in ('WI', 'IN')


;
== every date,


select * from tmp_1m.kn_dental_aggregated_sample

select count(*) from tmp_1m.kn_dental_aggregated_sample


-- 100,354
select
	'1' as source
	, count(distinct temp_provtin) as n_prov
from tmp_1m.kn_dental_aggregated
union all
-- 839
select
	'2' as source
	, count(distinct proc_cd) as n_proc
from tmp_1m.kn_dental_aggregated

('TX', 'FL', 'WI')

create table prov as
select 
	temp_provtin
	, sum(paid) as paid
from tmp_1m.kn_dental_aggregated
where fin_market = 'TX'
group by temp_provtin

-- Top 100 Prov for TX, FL, WI
create table tmp_1m.kn_dental_provtin_top100paid_3states as
with 
	top100paid_TX as (
		select 
			temp_provtin
			, sum(paid) as total_prov_paid
		from tmp_1m.kn_dental_aggregated
		where fin_market = 'TX'
		group by temp_provtin
		order by total_prov_paid desc
		limit 100
	)
,
	top100paid_FL as (
		select 
			temp_provtin
			, sum(paid) as total_prov_paid
		from tmp_1m.kn_dental_aggregated
		where fin_market = 'FL'
		group by temp_provtin
		order by total_prov_paid desc
		limit 100
	)
,
	top100paid_WI as (
		select 
			temp_provtin
			, sum(paid) as total_prov_paid
		from tmp_1m.kn_dental_aggregated
		where fin_market = 'WI'
		group by temp_provtin
		order by total_prov_paid desc
		limit 100
	)
select distinct 
	temp_provtin
from top100paid_TX
union all
select distinct 
	temp_provtin
from top100paid_FL
union all
select distinct 
	temp_provtin
from top100paid_WI
;

select * from tmp_1m.kn_dental_provtin_top100paid_3states

drop table tmp_1m.kn_dental_top100_paid_3states;
create table tmp_1m.kn_dental_top100_paid_3states as
select 
	a.temp_provtin
	, b.fin_market
	, b.temp_provparstatus
	, b.proc_cd
	, b.service_category
	, b.paid_year
	, b.paid_month
	, b.paid_quarter
	, sum(b.paid) as paid
	, sum(b.completed_paid) as completed_paid
	, sum(b.allowed) as allowed
	, coalesce(sum(b.Mbrs),0) as mm
	, b.fin_inc_year
	, b.fin_inc_month
	, b.fin_inc_quarter	
	, b.data_type
	, b.fin_brand
	, b.fin_source_name
	, b.fin_g_i
	, b.global_cap
	, b.fin_tfm_product_new
	, b.fin_product_level_3
	, b.tfm_include_flag
from tmp_1m.kn_dental_provtin_top100paid_3states as a
left join tmp_1m.kn_dental_aggregated as b
	on a.temp_provtin = b.temp_provtin
group by
	a.temp_provtin
	, b.fin_market
	, b.temp_provparstatus
	, b.proc_cd
	, b.service_category
	, b.paid_year
	, b.paid_month
	, b.paid_quarter
	, b.fin_inc_year
	, b.fin_inc_month
	, b.fin_inc_quarter	
	, b.data_type
	, b.fin_brand
	, b.fin_source_name
	, b.fin_g_i
	, b.global_cap
	, b.fin_tfm_product_new
	, b.fin_product_level_3
	, b.tfm_include_flag
;

select count(*) from tmp_1m.kn_dental_top100_paid_3states;

select * from tmp_1m.kn_dental_top100_paid_3states;


--
--top provi, this avg vs norm avg
--1 code that has way higher
--unit cost analysis
--which prov paid on avg more?
--which proc code driving diff
--top 10 provider
--
--proc code - on avg, split par and non-par
--
--
--
--top 10 provider, billing vs reimbursement
--
--allowed - we paid + member paid
--
--TIN -> PROC CODE AVG -> 10


create table tmp_1m.kn_dental_top100TIN_top10_proccd_3states as
select
	*
	, 
	case 
	  when proc_cd in ('D2740','D1110','D4910','D4341','D0210','D4342','D1206','D2950','D0150','D7210') then 1
	  else 0
	end as proc_cd_top10
	, case 
  		when paid > 0 then 1
  		else 0
	end as paid_utils
from tmp_1m.kn_dental_top100_paid_3states
where fin_market in ("TX", "WI", "FL")

select * from tmp_1m.kn_dental_top100TIN_top10_proccd_3states;


describe tn

select * from tmp_1y.HCE_DENTAL_FACETS_CLM_MBR_ROLLUP


create table tmp_1m.kn_dental_3states_v2 as
select * from tmp_1m.kn_dental_top100_paid_3states_v2
where fin_market in ('TX')


-- Top 10;

-- Top 100 Prov for TX, FL, WI
drop table tmp_1m.kn_dental_provtin_top100paid_3states_v2;
create table tmp_1m.kn_dental_provtin_top100paid_3states_v2 as
with top100paid_tx as (
    select temp_provtin
    from (
        select 
                temp_provtin
                , sum(paid) as total_paid
        from tmp_1y.kn_dental_claims_mbr
        where fin_market = 'TX'
        group by temp_provtin
        order by total_paid desc
        limit 100
    ) t
),
top100paid_fl as (
    select temp_provtin
    from (
        select 
                temp_provtin
                , sum(paid) as total_paid
        from tmp_1y.kn_dental_claims_mbr
        where fin_market = 'FL'
        group by temp_provtin
        order by total_paid desc
        limit 100
    ) 
),
top100paid_wi as (
    select temp_provtin
    from (
        select 
                temp_provtin
                , sum(paid) as total_paid
        from tmp_1y.kn_dental_claims_mbr
        where fin_market = 'WI'
        group by temp_provtin
        order by total_paid desc
        limit 100
    ) wi
)
select distinct temp_provtin
from (
    select temp_provtin from top100paid_tx
    union all
    select temp_provtin from top100paid_fl
    union all
    select temp_provtin from top100paid_wi
) ;



drop table tmp_1m.kn_dental_top100_paid_3states_v2;
create table tmp_1m.kn_dental_top100_paid_3states_v2 as
select
        b.*
from tmp_1m.kn_dental_provtin_top100paid_3states_v2 as a
join tmp_1y.kn_dental_claims_mbr as b
    on a.temp_provtin = b.temp_provtin

select 
    count(*) 
from tmp_1m.kn_dental_top100_paid_3states_v2 
;

select 
    * 
from tmp_1m.kn_dental_top100_paid_3states 
;

select count(*) from tmp_1m.kn_dental_top100_paid_2states
where fin_market in ('TX','FL')
limit 500000;

create table tmp_1m.kn_dental_top100_paid_tx as
select * from tmp_1m.kn_dental_top100_paid_3states_v2
where fin_market = 'TX'


select count(*) from tmp_1m.kn_dental_top100_paid_tx

create table default.kn_dental_export as
select * from tmp_1m.kn_dental_top100_paid_3states_v2 
limit 750000;


create table tmp_1m.kn_dental_export as select * from default.kn_dental_export;
