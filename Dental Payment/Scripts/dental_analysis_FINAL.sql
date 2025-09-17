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

select * from tmp_1m.kn_dental_claims;
select * from tmp_1m.kn_dental_mbr;


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






