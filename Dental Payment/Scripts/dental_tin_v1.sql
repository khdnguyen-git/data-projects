select * from tmp_1y.HCE_DENTAL_FACETS_CLM_MBR_ROLLUP

describe tmp_1y.HCE_DENTAL_FACETS_CLM_MBR_ROLLUP


select right(fin_inc_month) from tmp_1y.HCE_DENTAL_FACETS_CLM_MBR_ROLLUP
group by fin_inc_month

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

select count(*) from tmp_1m.kn_dental_pmt


describe tmp_1m.kn_dental_pmt

select * from tmp_1m.kn_dental_pmt

describe 

select data_type, count(*) from tmp_1m.kn_dental_aggregated
group by data_type;

group by fin_market, temp_provtin

tin: 

drop table tmp_1m.kn_dental_aggregated;
create table tmp_1m.kn_dental_aggregated as 
select
	data_type
	, fin_brand
	, fin_source_name
	, fin_g_i
	, global_cap
	, fin_tfm_product_new
	, fin_product_level_3
	, tfm_include_flag
	, fin_market
	, fin_inc_year
	, fin_inc_month
	, fin_inc_quarter
	, paid_year
	, paid_month
	, paid_quarter
	, temp_provparstatus
	, temp_provtin
	, proc_cd
	, service_category
	, sum(allowed_amt_sum) as allowed
	, sum(paid_amt_sum) as paid
	, sum(paid_amt_completed_sum) as completed_paid
	, sum(mm) as Mbrs
from tmp_1m.kn_dental_pmt
where fin_inc_month <= '202412'
group by
	data_type
	, fin_brand
	, fin_source_name
	, fin_g_i
	, global_cap
	, fin_tfm_product_new
	, fin_product_level_3
	, tfm_include_flag
	, fin_market
	, fin_inc_year
	, fin_inc_month
	, fin_inc_quarter
	, paid_year
	, paid_month
	, paid_quarter
	, temp_provparstatus
	, temp_provtin
	, proc_cd
	, service_category
;

create table tmp_1m.kn_dental_top50mm_proccd as
select 
	*
from tmp_1m.kn_dental_aggregated
order by Mbrs desc
limit 50;

create table tmp_1m.kn_dental_top50paid_proccd as
select 
	*
from tmp_1m.kn_dental_aggregated
order by completed_paid desc
limit 50;

select * from tmp_1m.kn_dental_top50paid_proccd

	
select count(*) from tmp_1m.kn_dental_aggregated
where mbrs is null -- 9,074

select count(*) from tmp_1m.kn_dental_aggregated
where mbrs is null  -- 22883848
	
select count(*) from tmp_1m.kn_dental_aggregated
where data_type = 'Membership' -- 9074
	
select count(*) from tmp_1m.kn_dental_aggregated
where data_type = 'Claims' -- 22883848
	
create table tmp_1m.kn_dental_market_top5paid as
select distinct 
	fin_market
	, completed_paid
from tmp_1m.kn_dental_aggregated
order by completed_paid desc
limit 5;

create table tmp_1m.kn_dental_market_top5mm as
select distinct 
	fin_market
	, Mbrs
from tmp_1m.kn_dental_aggregated
order by Mbrs desc
limit 5;
	


	
	
drop table testc;
create table test_v1 as 
select
	proc_cd
	, sum(coalesce(allowed_amt_sum, 0)) as allowed
	, sum(coalesce(paid_amt_sum, 0)) as paid
	, sum(coalesce(paid_amt_completed_sum, 0)) as completed_paid
	, sum(coalesce(mm, 0)) as Mbrs
from tmp_1m.kn_dental_pmt
where data_type = 'Claims' and paid_month in ('202410', '202411', '202412')
group by
	proc_cd

;

create table test_v2 as 
select
	fin_market
	, temp_provparstatus
	, temp_provtin
	, proc_cd
	, sum(coalesce(allowed_amt_sum, 0)) as allowed
	, sum(coalesce(paid_amt_sum, 0)) as paid
	, sum(coalesce(paid_amt_completed_sum, 0)) as completed_paid
	, sum(coalesce(mm, 0)) as Mbrs
from tmp_1m.kn_dental_pmt
where data_type = 'Claims' and paid_month in ('202410', '202411', '202412')
group by
	fin_market
	, temp_provparstatus
	, temp_provtin
	, proc_cd	
;
	

with 
	v1 as (
	select proc_cd from test_v1
	order by Mbrs desc
	limit 50
)
	,
	v2 as ( 
	select proc_cd from test_v2
	order by Mbrs desc
	limit 50
)
select 
	a.proc_cd as aproc
	, b.proc_cd as bproc
from v1 as a
full join v2 as b
on a.proc_cd = b.proc_cd
where a.proc_cd is null or b.proc_cd is null

drop table mmsort;
create table mmsort as
select 
	*
from testp
order by Mbrs desc
limit 50;

select * from mmsort;


	
	
drop table tmp_1m.kn_dental_proccd_by_mm; 
create table tmp_1m.kn_dental_proccd_by_mm as
select 
	proc_cd
	, service_category
	, temp_provparstatus
	, temp_provtin
	, paid_year
	, paid_month
	, paid_quarter
	, completed_paid
	, proc_by_paid
	, Mbrs
	, proc_by_mm
from testp
order by
	proc_by_mm
;
select * from tmp_1m.kn_dental_proccd_by_mm

drop table tmp_1m.kn_dental_proccd_by_paid; 
create table tmp_1m.kn_dental_proccd_by_paid as
select 
	proc_cd
	, service_category
	, temp_provparstatus
	, temp_provtin
	, paid_year
	, paid_month
	, paid_quarter
	, completed_paid
	, proc_by_paid
	, Mbrs
	, proc_by_mm
from testp
order by
	proc_by_paid
;

select * from tmp_1m.kn_dental_proccd_by_paid
where proc_by_paid <= 50

select * from tmp_1m.kn_dental_proccd_by_mm
where proc_by_mm <= 50
	
	
select * from tmp_1m.kn_dental_pmt
-- 22892922
select * from tmp_1m.kn_dental_aggregated
-- 15531924



select dental_clm_source from tmp_1m.kn_dental_quarter_aggregated group by dental_clm_source -- all Facets

select 
fin_source_name
	, count(*)
from tmp_1m.kn_dental_quarter_aggregated
group by 
fin_source_name