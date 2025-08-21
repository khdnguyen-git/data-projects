-- tmp_1m.kn_prtr_mbi_2024_2025;
create table tmp_1m.kn_prtr_cohort as 
select 
	fin_mbi_hicn_fnl
	, fin_inc_year
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_tfm_product_new as new_tfm_product
	, fin_g_i
	, mm
	, hce_cohort
	, 2025_anchormonth_tfm_include_flag
	, 2025_anchormonth_fin_product_level_3 as new_product_level_3
	, 202412_global_cap
	, 202412_fin_product_level_3 as old_product_level_3
	, 202412_fin_mbi_hicn_fnl
from tmp_1y.2024_2025_HCE_COHORT_6 
where hce_cohort = 'Product_Transition'

create table tmp_1m.kn_prtr_mbi_2025ytd as
with 
prtr_mbi as (
	select distinct 
		fin_mbi_hicn_fnl as mbi
	from tmp_1m.kn_prtr_cohort
),
unique_mbi_2024_2025 as (
	select distinct		
		fin_mbi_hicn_fnl as mbi	
	from fichsrv.tre_membership
	where 1=1
		and sgr_source_name = 'COSMOS'
		and fin_brand = 'M&R'
		and migration_source not in ('OAH', 'CSP')
		and fin_product_level_3 not in ('INSTITUTIONAL')
		and global_cap = 'NA' 
		and fin_inc_year in ('2025')
)
select distinct
	a.mbi
from unique_mbi_2024_2025 as a
inner join prtr_mbi as b
	on a.mbi = b.mbi

	

-- 186304 if in 2024, 2025
-- 153733 if just 2025 and product_transition
-- 

select 1 as source, count(distinct mbi) as n from tmp_1m.cl_unique_mbi_2025YTD_wCohort where hce_cohort = 'Product_Transition'
union all
select 2 as source, count(distinct mbi) as n from tmp_1m.kn_prtr_mbi_2025ytd
order by source



-- Membership 
-- drop table tmp_1m.kn_prtr_mbi_members_25_24;
create table tmp_1m.kn_prtr_mbi_members_25_24 as
select
	a.mbi
	, b.fin_brand
	, b.fin_tfm_product_new
	, b.fin_g_i
	, b.fin_product_level_3
	, case when b.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then b.fin_tfm_product_new
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'DUAL' then 'DUAL'
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	  	  else 'OTHER' end as product
	, b.fin_inc_year
	, b.fin_inc_month
	, row_number() over(partition by a.mbi order by b.fin_inc_month desc) as rn
from tmp_1m.kn_prtr_mbi_2025ytd as a
join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl
where fin_inc_month >= '202401';


-- select count(distinct mbi) from tmp_1m.kn_prtr_mbi_members_25_24
-- 153733

-- Claims;
-- drop table tmp_1m.kn_prtr_mbi_claims_25_24
create table tmp_1m.kn_prtr_mbi_claims_25_24 as
with 
product_claims_op_2025ytd as (
	select
		a.mbi
		, b.brand_fnl
		, b.tfm_product_new_fnl
		, b.group_ind_fnl
		, b.product_level_3_fnl
		, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
		      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
		      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
		      else 'OTHER' end as product
		, b.fst_srvc_year
		, b.fst_srvc_month
		, b.component
		, b.allw_amt_fnl
		, b.net_pd_amt_fnl
		, row_number() over(partition by a.mbi order by b.fst_srvc_month desc) as rn
	from tmp_1m.kn_prtr_mbi_2025ytd as a
	join tadm_tre_cpy.glxy_op_f_202503 as b
		on a.mbi = b.gal_mbi_hicn_fnl
	where fst_srvc_month >= '202401'
),
product_claims_pr_2025ytd as (
	select
		a.mbi
		, b.brand_fnl
		, b.tfm_product_new_fnl
		, b.group_ind_fnl
		, b.product_level_3_fnl
		, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
		      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
		      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
		      else 'OTHER' end as product
		, b.fst_srvc_year
		, b.fst_srvc_month
		, b.component
		, b.allw_amt_fnl
		, b.net_pd_amt_fnl
		, row_number() over(partition by a.mbi order by b.fst_srvc_month desc) as rn
	from tmp_1m.kn_prtr_mbi_2025ytd as a
	join tadm_tre_cpy.glxy_pr_f_202503 as b
		on a.mbi = b.gal_mbi_hicn_fnl
	where fst_srvc_month >= '202401'
)
select 
	mbi
	, brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, product
	, fst_srvc_year
	, fst_srvc_month
	, component
	, allw_amt_fnl 
	, net_pd_amt_fnl 
	, rn
from product_claims_op_2025ytd
union
select 
	mbi
	, brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, product
	, fst_srvc_year
	, fst_srvc_month
	, component
	, allw_amt_fnl 
	, net_pd_amt_fnl 
	, rn
from product_claims_pr_2025ytd

-- 139518 count
-- 19487360.02, 988778461.88
select 1 as source, sum(allw_amt_fnl) as sum from tmp_1m.kn_prtr_mbi_claims_25_24 group by brand_fnl
union all
select 2 as source, sum(allowed) as sum from tmp_1m.cl_25_24_claims where hce_cohort = 'Product_Transition' group by brand_fnl
-- 19487360.02, 988778461.88


-- drop table tmp_1m.kn_prtr_mbi_claims_members_25_24;
create table tmp_1m.kn_prtr_mbi_claims_members_25_24 as
select
	mbi
	, 'Claims' as category
	, brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, product
	, fst_srvc_year
	, fst_srvc_month
	, component
	, allw_amt_fnl 
	, net_pd_amt_fnl 
from tmp_1m.kn_prtr_mbi_claims_25_24
union all
select 
	mbi
	, 'Membership' as category
	, fin_brand
	, fin_tfm_product_new
	, fin_g_i
	, fin_product_level_3
	, product
	, fin_inc_year
	, fin_inc_month
	, '' as component
	, 0 as allw_amt_fnl
	, 0 as net_pd_amt_fnl
from tmp_1m.kn_prtr_mbi_members_25_24
;

-- 153733
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_prtr_mbi_claims_members_25_24
where fst_srvc_month >= '202412'
union all
select 2 as source, count(distinct mbi) as n from tmp_1m.cl_unique_mbi_2025YTD_wCohort 
where hce_cohort = 'Product_Transition'
-- 153733



drop table clean_product;

create table clean_product as 
with
mbi_product as (
	select distinct
		mbi
		, fst_srvc_month
		, fst_srvc_year
		, product
	from tmp_1m.kn_prtr_mbi_claims_members_25_24
	where product is not null
),
old_product as (
	select 
		mbi
		, product as old_product
		, fst_srvc_month as old_product_month
		, row_number() over (partition by mbi order by fst_srvc_month desc) as rn
	from mbi_product
	where fst_srvc_year = '2024'
),
new_product as (
	select 
		mbi
		, product as new_product
		, fst_srvc_month as new_product_month
		, row_number() over (partition by mbi order by fst_srvc_month asc) as rn
	from mbi_product
	where fst_srvc_year = '2025'
)
select distinct
	a.mbi
	, case 
		when o.old_product is NULL then 'MISSING' 
			else old_product 
	  end as old_product
	, o.old_product_month
	, case 
		when n.new_product is NULL then 'MISSING' 
			else n.new_product 
	  end as new_product
	, n.new_product_month
from mbi_product as a
left join (select * from old_product where rn = 1) as o
	on a.mbi = o.mbi
left join (select * from new_product where rn = 1) as n
	on a.mbi = n.mbi
	

-- 153733
select count(distinct mbi) as n from tmp_1m.cl_unique_mbi_2025YTD_wCohort where hce_cohort = 'Product_Transition'
union all
select count(distinct mbi) as n from tmp_1m.kn_prtr_mbi_claims_members_25_24
order by source
-- 153733

select 1 as source, count(distinct mbi ) as n from clean_product
union all
select 2 as source, count(mbi) as n from clean_product
union all
select 3 as source, count(distinct mbi) as n from tmp_1m.cl_unique_mbi_2025YTD_wCohort where hce_cohort = 'Product_Transition'
union all
select 4 as source, count(mbi) as n from tmp_1m.cl_unique_mbi_2025YTD_wCohort where hce_cohort = 'Product_Transition'
order by source

drop table countcheck;
create table countcheck as
select mbi, old_product, old_product_month, new_product_month, new_product
	, count(*) as n from clean_product
group by mbi, old_product, old_product_month, new_product_month, new_product
having count(*) > 1

select * from countcheck

select * from clean_product where mbi in ('1A05T72AG83', '1A04MU5EX67')

select * from tmp_1m.kn_prtr_mbi_claims_members_25_24 where mbi in ('1A05T72AG83')


select * from tmp_1y.2024_2025_HCE_COHORT_6 where fin_mbi_hicn_fnl = '1A05T72AG83'


select * from fichsrv.tre_membership where fin_mbi_hicn_fnl = '1A05T72AG83' 

select 
	gal_mbi_hicn_fnl
	, product_level_3_fnl
	, tfm_product_new_fnl 
	, fst_srvc_year
from tadm_tre_cpy.glxy_op_f_202503 
where gal_mbi_hicn_fnl = '1A05T72AG83'

select 
	gal_mbi_hicn_fnl
	, product_level_3_fnl
	, tfm_product_new_fnl 
	, fst_srvc_year
from tadm_tre_cpy.glxy_pr_f_202503 
where gal_mbi_hicn_fnl = '1A05T72AG83'

drop table testmissing; 


-- 21,854
create table tmp_1m.kn_mbi_no2024product_mm as
select 
	a.fin_mbi_hicn_fnl as p_mbi
	, b.fin_mbi_hicn_fnl as m_mbi
	, b.fin_tfm_product
	, b.fin_product_level_3 
	, b.fin_inc_year
from tmp_1y.2024_2025_HCE_COHORT_6 as a 
left join fichsrv.tre_membership as b
on a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl and b.fin_inc_year = '2024'
where a.hce_cohort = 'Product_Transition' and b.fin_mbi_hicn_fnl is null;


-- 
create table updated as
select 
	a.fin_mbi_hicn_fnl as p_mbi
	, b.fin_mbi_hicn_fnl as m_mbi
	, b.fin_tfm_product
	, b.fin_product_level_3 
	, b.fin_inc_year
from tmp_1y.2024_2025_HCE_COHORT_6 as a 
left join fichsrv.tre_membership as b
on a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl and b.fin_inc_year = '2024'
where a.hce_cohort = 'Product_Transition' and b.fin_mbi_hicn_fnl is null;

select * from updated;



create table mbi_no2024product_mm as;

select * from fichsrv.tre_membership where fin_inc_year > '202401' and fin_mbi_hicn_fnl in ('5GU8X56HR37'
,'8KK7PT1RP48'
,'5MM1NH1VF12'
)

-- 21,346
create table kn_mbi_no2024product_mm as
select 
	a.fin_mbi_hicn_fnl as p_mbi
	, b.fin_mbi_hicn_fnl as m_mbi
	, a.fin_tfm_product
	, a.fin_product_level_3 
	, a.fin_inc_year
	, a.fin_inc_month
	, case when a.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then a.fin_tfm_product_new
	      when a.fin_tfm_product_new = 'DUAL_CHRONIC' and a.2025_anchormonth_fin_product_level_3 = 'DUAL' then 'DUAL'
	      when a.fin_tfm_product_new = 'DUAL_CHRONIC' and a.2025_anchormonth_fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	  	  else 'OTHER' end as product 
from tmp_1y.2024_2025_HCE_COHORT_6 as a 
left join fichsrv.tre_membership as b
on a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl and b.fin_inc_year = '2024'
where a.hce_cohort = 'Product_Transition' and b.fin_mbi_hicn_fnl is null
and 1=1
	and b.sgr_source_name = 'COSMOS'
	and b.fin_brand = 'M&R'
	and b.migration_source not in ('OAH', 'CSP')
	and b.fin_product_level_3 not in ('INSTITUTIONAL')
	and b.global_cap = 'NA' 
	and a.fin_inc_year = '2025'
;
drop table kn_test_mbi;
create table kn_test_mbi as
with 
sree_cohort as (
select 
	fin_mbi_hicn_fnl as mbi
	, fin_tfm_product_new
	, 2025_anchormonth_fin_product_level_3 
	, fin_inc_year
	, fin_inc_month
	, case when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and 2025_anchormonth_fin_product_level_3 = 'DUAL' then 'DUAL'
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and 2025_anchormonth_fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	  	  else 'OTHER' end as product 
	, *
from tmp_1y.2024_2025_HCE_COHORT_6
where hce_cohort = 'Product_Transition'
),
tre as (
select distinct
	fin_mbi_hicn_fnl as mbi
	, tadm_cohort
	, fin_inc_month as month_tre
	, fin_inc_year as year_tre
from fichsrv.tre_membership
where 1=1
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA' 
	and fin_inc_month >= '202401'
)
select 
	a.*
	, b.*
from tre as a
	left join sree_cohort as b
using (mbi)
;

describe fichsrv.tre_membership

select * from kn_test_mbi;

m 


describe tmp_1y.2024_2025_HCE_COHORT_6



create table kn_mbi_no2024product_count
select 
	fin_tfm_product
	, fin_product_level_3
	, fin_inc_year
	, fin_inc_month
	, product
	, count (distinct fin_mbi_hicn_fnl) as mms
from kn_mbi_no2024product_mm
group by

select distinct 
;
describe formatted tadm_tre_cpy.gl_rstd_gpsgalnce_f_202504

describe formatted fichsrv.tre_membership

-- 324,092
select count(*) from tmp_1y.2024_2025_HCE_COHORT_6
where hce_cohort = 'Product_Transition' and 

describe tmp_1y.2024_2025_HCE_COHORT_6








select * from mbi_no2024product_mm;
create table kn_mbi_no2024product_claims as
select 
	a.fin_mbi_hicn_fnl as p_mbi
	, b.gal_mbi_hicn_fnl as g_mbi
	, b.tfm_product_new_fnl
	, b.product_level_3_fnl 
	, b.fst_srvc_year
from tmp_1y.2024_2025_HCE_COHORT_6_202503 as a 
left join tadm_tre_cpy.glxy_op_f_202504 as b
on a.fin_mbi_hicn_fnl = b.gal_mbi_hicn_fnl and b.fst_srvc_year = '2024'
where a.hce_cohort = 'Product_Transition' and b.gal_mbi_hicn_fnl is null
union
select 
	a.fin_mbi_hicn_fnl as p_mbi
	, b.gal_mbi_hicn_fnl as g_mbi
	, b.tfm_product_new_fnl
	, b.product_level_3_fnl 
	, b.fst_srvc_year
from tmp_1y.2024_2025_HCE_COHORT_6_202503 as a 
left join tadm_tre_cpy.glxy_pr_f_202504 as b
on a.fin_mbi_hicn_fnl = b.gal_mbi_hicn_fnl and b.fst_srvc_year = '2024'
where a.hce_cohort = 'Product_Transition' and b.gal_mbi_hicn_fnl is null




select fin_mbi_hicn_fnl, fin_product_level_3, fin_tfm_product_new, fin_inc_year, fin_inc_month from fichsrv.tre_membership where fin_mbi_hicn_fnl in 
('1A00M09YA10'
,'1A01UJ4KE11'
,'1A05T72AG83'
,'1A07D17KY50'
,'1A42P09GG18'
,'1A44G89KU70'
,'1A46K11RE46'
,'1A47J83KU60'
) and fin_inc_year >= '2024'
order by fin_mbi_hicn_fnl, fin_inc_year








mbi_no2024product_claims
select 
	fin_mbi_hicn_fnl
	, fin_product_level_3
	, fin_tfm_product_new
	, fin_inc_year
	, fin_inc_month 
from fichsrv.tre_membership 
where fin_mbi_hicn_fnl in 
('4AR5QQ5AP53'
,'7EN1H84KW96'
,'8DM4VK6WJ74'
,'1A05T72AG83'
,'4UJ4KE6YY65'
,'5GU8X56HR37'
,'8KK7PT1RP48'
,'5MM1NH1VF12'
) and fin_inc_year = '2024'
order by 
	fin_mbi_hicn_fnl
	, fin_inc_year
	
select 
	fin_mbi_hicn_fnl
	, 2025_anchormonth_fin_product_level_3
	, fin_tfm_product_new 
	, fin_inc_year
from tmp_1y.2024_2025_HCE_COHORT_6
where fin_mbi_hicn_fnl in 
('4AR5QQ5AP53'
,'7EN1H84KW96'
,'8DM4VK6WJ74'
,'1A05T72AG83'
,'4UJ4KE6YY65'
,'5GU8X56HR37'
,'8KK7PT1RP48'
,'5MM1NH1VF12'
)
order by 
	fin_mbi_hicn_fnl
	, fin_inc_year

	
select 
	gal_mbi_hicn_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, fst_srvc_year
	, fst_srvc_month 
from tadm_tre_cpy.glxy_op_f_202504
where gal_mbi_hicn_fnl in 
('4AR5QQ5AP53'
,'7EN1H84KW96'
,'8DM4VK6WJ74'
,'1A05T72AG83'
,'4UJ4KE6YY65'
,'5GU8X56HR37'
,'8KK7PT1RP48'
,'5MM1NH1VF12'
) and fst_srvc_year = '2024'
order by 
	gal_mbi_hicn_fnl
	, fst_srvc_year
	
select 
	gal_mbi_hicn_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, fst_srvc_year
	, fst_srvc_month 
from tadm_tre_cpy.glxy_pr_f_202504
where gal_mbi_hicn_fnl in 
('4AR5QQ5AP53'
,'7EN1H84KW96'
,'8DM4VK6WJ74'
,'1A05T72AG83'
,'4UJ4KE6YY65'
,'5GU8X56HR37'
,'8KK7PT1RP48'
,'5MM1NH1VF12'
) and fst_srvc_year = '2024'
order by 
	gal_mbi_hicn_fnl
	, fst_srvc_year

	
	
	
	
	
	
	
	
	
	
	
select 
	hce_cohort
	, fin_mbi_hicn_fnl
	, 2025_anchormonth_fin_product_level_3
	, fin_tfm_product_new
	, fin_inc_year
	, fin_inc_month from tmp_1y.2024_2025_hce_cohort_6 where fin_mbi_hicn_fnl in 
('4AR5QQ5AP53'
,'7EN1H84KW96'
,'8DM4VK6WJ74'
,'1A05T72AG83'
,'4UJ4KE6YY65'
,'5GU8X56HR37'
,'8KK7PT1RP48'
,'5MM1NH1VF12'
) and fin_inc_year >= '2024'
order by fin_mbi_hicn_fnl, fin_inc_year

describe tmp_1y.2024_2025_hce_cohort_6



create table testmissing as
select 
    a.fin_mbi_hicn_fnl as p_mbi,
    b.fin_mbi_hicn_fnl as m_mbi,
    b.fin_tfm_product,
    b.fin_product_level_3,
    b.fin_inc_year
from tmp_1y.2024_2025_HCE_COHORT_6_202503 as a
left join fichsrv.tre_membership as b
on a.fin_mbi_hicn_fnl = b.fin_mbi_hicn_fnl and b.fin_inc_year = '2024'
where a.hce_cohort = 'Product_Transition' and b.fin_mbi_hicn_fnl is null;

select * from testmissing


select 
	fin_mbi_hicn_fnl
	, fin_tfm_product
	, fin_product_level_3 
	, fin_inc_year
from fichsrv.tre_membership 
where fin_mbi_hicn_fnl = '1A05T72AG83'







select count(distinct sub1.mbi_a) from 
(select 
	a.mbi as mbi_a
	, b.mbi as mbi_b
from tmp_1m.kn_prtr_mbi_claims_members_25_24 as a
join tmp_1m.cl_unique_mbi_2025YTD_wCohort as b
on a.mbi = b.mbi
where b.hce_cohort = 'Product_Transition'
) sub1
where sub1.mbi_a != sub1.mbi_b


select brand_fnl, sum(allowed) from tmp_1m.cl_25_24_claims where hce_cohort = 'Product_Transition' group by brand_fnl
-- 988778461.88 M&R

select brand_fnl, sum(allowed) from tmp_1m.kn_prtr_cost_25_24 group by brand_fnl
-- 13174431756.09 M&R




select fin_mbi_hicn_fnl, fin_product_level_3, fin_tfm_product_new, fin_inc_month from fichsrv.tre_membership 
where fin_mbi_hicn_fnl in ('1A05T72AG83', '1A04MU5EX67')
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA' 
	and fin_inc_year = '2025'

-- Joining product info back

select count(distinct mbi) from tmp_1m.kn_prtr_mbi_claims_members_25_24;
	
describe tmp_1m.kn_prtr_mbi_claims_members_25_24;

select min(fst_srvc_month) as min from tmp_1m.kn_prtr_mbi_claims_members_25_24;

--select * from tmp_1m.kn_prtr_mbi_claims_members_25_24;

	
drop table tmp_1m.kn_prtr_mbi_clean_products_25_24;
create table tmp_1m.kn_prtr_mbi_clean_products_25_24 as
select 
	a.mbi
	, a.category
	, a.brand_fnl
	, a.group_ind_fnl
	, a.tfm_product_new_fnl
	, a.product_level_3_fnl
	, b.old_product
	, b.old_product_month
	, b.new_product
	, b.new_product_month
	, a.fst_srvc_month
	, a.fst_srvc_year
	, a.component
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from tmp_1m.kn_prtr_mbi_claims_members_25_24 as a
inner join clean_product as b
on a.mbi = b.mbi

select 1 as source, count(mbi) as n from tmp_1m.kn_prtr_mbi_clean_products_25_24
union all
select 2 as source, count(distinct mbi) as n from clean_product
order by source

drop table tmp_1m.kn_prtr_mbi_clean_products_25_24;
create table tmp_1m.kn_prtr_mbi_clean_products_25_24 as
select * from default.clean_product




-- Find mms
-- drop table tmp_1m.kn_prtr_mms_25_24;
create table tmp_1m.kn_prtr_mms_25_24 as 
select 
	category
	, brand_fnl
	, group_ind_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, old_product
	, old_product_month
	, new_product
	, new_product_month
	, fst_srvc_year
	, fst_srvc_month
	, count(distinct mbi) as mm
from tmp_1m.kn_prtr_mbi_clean_products_25_24
where category = 'Membership'
group by
	category
	, brand_fnl
	, group_ind_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, old_product
	, old_product_month
	, new_product
	, new_product_month
	, fst_srvc_year
	, fst_srvc_month
;



drop table testmms;
create table testmms as 
select 
	b.fin_brand
	, b.fin_g_i
	, b.fin_tfm_product_new
	, b.fin_inc_year
	, b.fin_inc_month
	, b.fin_product_level_3
	, a.old_product
	, a.old_product_month
	, a.new_product
	, a.new_product_month
	, count(distinct a.mbi) as mm
from clean_product as a
left join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl
group by
	b.fin_brand
	, b.fin_g_i
	, b.fin_tfm_product_new
	, b.fin_inc_year
	, b.fin_inc_month
	, b.fin_product_level_3
	, a.old_product
	, a.old_product_month
	, a.new_product
	, a.new_product_month
;




-- 1504964 M&R
select fin_brand, sum(mm) as mm from testmms
group by fin_brand

-- 377200
select fin_brand, sum(mm) as mm from tmp_1m.cl_cohort_MMs_25_24 where hce_cohort = 'Product_Transition'
group by category



-- Find cost
-- drop table tmp_1m.kn_prtr_cost_25_24;
create table tmp_1m.kn_prtr_cost_25_24 as 
select 
	category
	, brand_fnl
	, group_ind_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, old_product
	, old_product_month
	, new_product
	, new_product_month
	, fst_srvc_year
	, component
	, sum(allw_amt_fnl) as allowed
	, sum(net_pd_amt_fnl) as paid
from tmp_1m.kn_prtr_mbi_clean_products_25_24
where category = 'Claims' and fst_srvc_year >= '2024'
group by
	category
	, brand_fnl
	, group_ind_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, old_product
	, old_product_month
	, new_product
	, new_product_month
	, fst_srvc_year
	, component
;

drop table tmp_1m.kn_prtr_mms_claims_25_24;
create table kn_prtr_mms_claims_25_24 as
select 
	category
	, brand_fnl
	, group_ind_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, old_product
	, old_product_month
	, new_product
	, new_product_month
	, fst_srvc_year
	, component
	, allowed
	, paid
	, 0 as mm
from tmp_1m.kn_prtr_cost_25_24
union all
select 
	category
	, brand_fnl
	, group_ind_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, old_product
	, old_product_month
	, new_product
	, new_product_month
	, fst_srvc_year
	, '' as component
	, 0 as allowed
	, 0 as paid
	, mm
from tmp_1m.kn_prtr_mms_25_24




select category, sum(mm) as mms from tmp_1m.kn_prtr_mms_25_24
where  new_product = 'PPO' and new_product_month in ('202501', '202502')
group by category
	
	

















-- old_product check
select distinct * from clean_product
where mbi in ('1DV7TA1HJ72', '1ER8RM3WR55', '1FV7TU4EE40', '1A35G66AD83')

-- new_product check
select distinct * from clean_product
where mbi in ('1A01GG9YA10', '1A01Q40YE12', '1A03JA7GA89', '1A03QK2EX77')


1A01UA7XE11

select * from 





select distinct
	mbi
	, fst_srvc_month
	, old_product
	, new_product
from kn_prtr_products_25_24
where mbi in (
'1A35G66AD83')
order by mbi, fst_srvc_month desc


select distinct
	mbi
	, fst_srvc_month
	, old_product
	, new_product
from kn_prtr_products_25_24
where fst_srvc_month < '202410' and old_product is not null and new_product is null
order by mbi, fst_srvc_month desc

1A11U12QG25
1A10UF8EH16


select 
	hce_cohort
	, fin_mbi_hicn_fnl
	, 2025_anchormonth_fin_product_level_3
	, 202412_fin_mbi_hicn_fnl
	, 202412_fin_product_level_3 
from tmp_1y.2024_2025_HCE_COHORT_6
where 202412_fin_mbi_hicn_fnl is null and 202412_fin_product_level_3 is null and hce_cohort = 'Product_Transition'


1AA6DE9YT73
1AU7HT8DP45











-- drop table tmp_1m.kn_prtr_25_24;
create table tmp_1m.kn_prtr_25_24 as
select  
	mbi
	, category
	, brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, product
	, fst_srvc_year
	, fst_srvc_month
	, component
	, allw_amt_fnl 
	, net_pd_amt_fnl 
	, rn
from tmp_1m.kn_prtr_mms_claims_25_24
where srvc_month_2025 != '202503'
;




select 1 as source, count(distinct mbi) as n from tmp_1m.kn_prtr_202412_202501_202502
union all
select 2 as source, count(distinct mbi) as n from tmp_1m.kn_prtr_mbi_2024_2025
order by source

jointest




-- Getting MMs
select 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product
	, count(distinct mbi) as Mbrs
from tmp_1m.kn_prtr_mbi_claims_members_25_24
where category = 'Membership' and srvc_month_2025 in ('202501', '202502')
group by 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product
	
	
-- Getting Cost
select 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product
	, sum()
from tmp_1m.kn_prtr_202412_202501_202502
where category = 'Membership'
group by 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product
	
	
	
-- Test PPO
-- 21564 mm
select 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product
	, count(distinct mbi) as Mbrs
from tmp_1m.kn_prtr_2024_2025
where tfm_product_new_fnl = 'PPO' and category = 'Membership' and srvc_month_2024 = '202412' and srvc_month_2025 in ('202501', '202502')
group by 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product

-- Test PPO
-- 
select 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product
	, sum(allw_amt_fnl) as allowed
from tmp_1m.kn_prtr_2024_2025
where tfm_product_new_fnl = 'PPO' and category = 'Claims' and srvc_month_2024 = '202412' and srvc_month_2025 in ('202501', '202502')
group by 
	brand_fnl
	, tfm_product_new_fnl
	, group_ind_fnl
	, product_level_3_fnl
	, srvc_month_2024
	, srvc_month_2025
	, fst_srvc_year
	, product_2024
	, product_2025
	, old24_to_new25_product