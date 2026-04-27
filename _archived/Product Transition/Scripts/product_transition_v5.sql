
-- 304476
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

select * from tmp_1y.2024_2025_HCE_COHORT_6 

create table tmp_1m.kn_prtr_mbi_2024_2025 as
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
		and fin_inc_year in ('2024', '2025')
)
select 
	a.mbi
from unique_mbi_2024_2025 as a
inner join prtr_mbi as b
on a.mbi = b.mbi

-- Getting product for 202412
-- 202412
drop table tmp_1m.kn_prtr_cohort_products_mms_202412;
create table tmp_1m.kn_prtr_cohort_products_mms_202412 as
select
	a.mbi
	, case when b.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then b.fin_tfm_product_new
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'DUAL' then 'DUAL'
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	      else 'OTHER' end as old_product
from tmp_1m.kn_prtr_mbi_2024_2025 as a
join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl
where b.fin_inc_month = '202412'
;
-- Get products info from OP and PR claims
drop table tmp_1m.kn_x_mbi_products_claims_202412;
create table tmp_1m.kn_x_mbi_products_claims_202412 as
select
	a.mbi
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	      else 'OTHER' end as old_product
from tmp_1m.kn_x_mbi_202412_2025ytd as a
join tadm_tre_cpy.glxy_op_f_202503 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month = '202412'
union 
select
	a.mbi
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	      else 'OTHER' end as old_product
from tmp_1m.kn_x_mbi_202412_2025ytd as a
join tadm_tre_cpy.glxy_pr_f_202503 as b
on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month = '202412'
;

select count(*) from tmp_1m.kn_prtr_mbi_2024_2025
select old_product, count(*) from tmp_1m.kn_x_mbi_products_claims_mms_202412 group by old_prod
select count(*) from tmp_1m.kn_x_mbi_products_claims_mms_2025ytd;





create temporary table kn_prtr_cohort_old_product_202412 as
select 
	a.mbi
	, b.old_product
from tmp_1m.kn_prtr_mbi_2024_2025 as a
left join tmp_1m.kn_x_mbi_products_claims_mms_202412 as b
on a.mbi = b.mbi

drop table test_cohort;

create temporary table 2024_2025product as 
select 
	a.mbi as mbi_a
	, b.mbi as mbi_b
	, a.new_product
	, b.old_product 
from tmp_1m.kn_x_mbi_products_claims_mms_2025ytd as a
full outer join tmp_1m.kn_x_mbi_products_claims_mms_202412 as b
on a.mbi = b.mbi
;
-- 304476

drop table test_cohort;
create temporary table test_cohort1 as 
select 
	a.mbi as mbi_c
	, b.mbi_a as mbi_a
	, b.old_product
from tmp_1m.kn_prtr_mbi_2024_2025 as a
left join 2024_2025product as b
on a.mbi = b.mbi_a
;
create temporary table test_cohort_old as select * from test_cohort;

create temporary table test_cohort_new as 
select 
	a.mbi as mbi_c
	, b.mbi_b as mbi_b
	, b.new_product
from tmp_1m.kn_prtr_mbi_2024_2025 as a
left join 2024_2025product as b
on a.mbi = b.mbi_b
;

create table test_all_cohort as 
select 
	a.mbi_c
	, a.old_product
	, a.mbi_a
	, b.mbi_b
	, b.new_product
from test_cohort_old as a
inner join test_cohort_new as b
on a.mbi_c = b.mbi_c
;


select * from test_all_cohort
where mbi_c is not null and old_product is null or new_product is null;




kn_x_mbi_products_claims_mms_202412
