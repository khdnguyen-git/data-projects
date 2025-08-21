-- Unique MBI for 202412
drop table tmp_1m.kn_distinct_mbi_202412;
create table tmp_1m.kn_distinct_mbi_202412 as
select distinct		
	  fin_mbi_hicn_fnl as mbi
from fichsrv.tre_membership
where 1 = 1
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA' 
	and fin_inc_month = '202412'
;

-- Unique MBI for 2025ytd
drop table tmp_1m.kn_distinct_mbi_2025ytd;
create table tmp_1m.kn_distinct_mbi_2025ytd as
select distinct		
	  fin_mbi_hicn_fnl as mbi
from fichsrv.tre_membership
where 1 = 1
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA' 
	and fin_inc_year = '2025'
;

-- Find intersection of members;
create table tmp_1m.kn_x_mbi_202412_2025ytd as
select 
	coalesce(a.mbi, b.mbi) as mbi
from tmp_1m.kn_distinct_mbi_202412 as a
inner join tmp_1m.kn_distinct_mbi_2025ytd as b
on a.mbi = b.mbi
;

-- 202412
-- Get products info from membership
drop table tmp_1m.kn_x_mbi_products_mms_202412;
create table tmp_1m.kn_x_mbi_products_mms_202412 as
select
	a.mbi
	, case when b.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then b.fin_tfm_product_new
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'DUAL' then 'DUAL'
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	      else 'OTHER' end as old_product
from tmp_1m.kn_x_mbi_202412_2025ytd as a
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

select old_product from tmp_1m.kn_x_mbi_products_mms_202412
group by old_product

select old_product from tmp_1m.kn_x_mbi_products_claims_mms_202412
group by old_product


-- Stack 202412
drop table tmp_1m.kn_x_mbi_products_claims_mms_202412;
create table tmp_1m.kn_x_mbi_products_claims_mms_202412 as
select
	mbi
	, 'Claims' as category
	, old_product
from tmp_1m.kn_x_mbi_products_claims_202412
union all
select
	mbi	
	, 'Membership' as category
	, old_product
from tmp_1m.kn_x_mbi_products_mms_202412
;

-- 2025
-- Get products info from membership
drop table tmp_1m.kn_x_mbi_products_mms_2025ytd;
create table tmp_1m.kn_x_mbi_products_mms_2025ytd as
select
	a.mbi
	, case when b.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then b.fin_tfm_product_new
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'DUAL' then 'DUAL'
	      when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	      else 'OTHER' end as new_product
from tmp_1m.kn_x_mbi_202412_2025ytd as a
left join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl
where b.fin_inc_year = '2025'
;
select * from tmp_1m.kn_x_mbi_products_claims_2025ytd limit 100;

-- Get products info from OP and PR claims
drop table tmp_1m.kn_x_mbi_products_claims_2025ytd;
create table tmp_1m.kn_x_mbi_products_claims_2025ytd as
select
	a.mbi
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	      else 'OTHER' end as new_product
from tmp_1m.kn_x_mbi_202412_2025ytd as a
join tadm_tre_cpy.glxy_op_f_202503 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_year = '2025'
union 
select
	a.mbi
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	      when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	      else 'OTHER' end as new_product
from tmp_1m.kn_x_mbi_202412_2025ytd as a
join tadm_tre_cpy.glxy_pr_f_202503 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_year = '2025'
;

-- Stack 2025ytd
drop table tmp_1m.kn_x_mbi_products_claims_mms_2025ytd;
create table tmp_1m.kn_x_mbi_products_claims_mms_2025ytd as
select
	mbi
	, 'Claims' as category
	, new_product
from tmp_1m.kn_x_mbi_products_claims_2025ytd
union all
select
	mbi
	, 'Membership' as category
	, new_product
from tmp_1m.kn_x_mbi_products_mms_2025ytd
;

-- Join 
drop table tmp_1m.kn_x_products_202412_2025ytd;
create table tmp_1m.kn_x_products_202412_2025ytd as
select distinct
	coalesce(a.mbi, b.mbi) as mbi 
	, a.old_product
	, b.new_product
	, concat(a.old_product, "-", b.new_product) as product_old_new
from tmp_1m.kn_x_mbi_products_claims_mms_202412 as a
inner join tmp_1m.kn_x_mbi_products_claims_mms_2025ytd as b
on a.mbi = b.mbi;


-- tmp_1m.kn_x_products_202412_2025ytd
-- for mbi and product_old_new
select 
	product_old_new
	, count(*) as n_category
from tmp_1m.kn_x_products_202412_2025ytd
group by product_old_new 

-- Joining with Sree's cohort table
create table tmp_1m.kn_x_products_cohort_202412_2025ytd as
select 
	a.*
	, b.hce_cohort
from tmp_1m.kn_x_products_202412_2025ytd as a
left join tmp_1y.2024_2025_HCE_COHORT_6 as b
	on a.mbi = b.fin_mbi_hicn_fnl
;

select * from tmp_1y.2024_2025_HCE_COHORT_6 


select mbi, product_old_new, count(*) as n
from tmp_1m.kn_x_products_202412_2025ytd
group by mbi, product_old_new
having count(*) > 1


-- Use these to re-pull claims and membership 

-- OP and PR Claims for 202412 onwards
drop table tmp_1m.kn_products_claims_202412_2025;
create table tmp_1m.kn_products_claims_202412_2025 as
select 
	b.brand_fnl
	, b.tfm_product_new_fnl
	, a.product_old_new
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, b.component
	, b.hce_service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
	, a.hce_cohort	 
 	, sum(b.allw_amt_fnl) as allowed
	, sum(b.net_pd_amt_fnl) as paid	
from tmp_1m.kn_x_products_cohort_202412_2025ytd as a
join tadm_tre_cpy.glxy_op_f_202503 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month >= '202412'
group by 
	b.brand_fnl
	, b.tfm_product_new_fnl
	, a.product_old_new
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, b.component
	, b.hce_service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
	, a.hce_cohort
union 
select 
	b.brand_fnl
	, b.tfm_product_new_fnl
	, a.product_old_new
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, b.component
	, b.service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
	, a.hce_cohort	 
 	, sum(b.allw_amt_fnl) as allowed
	, sum(b.net_pd_amt_fnl) as paid	
from tmp_1m.kn_x_products_cohort_202412_2025ytd as a
join tadm_tre_cpy.glxy_pr_f_202503 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month >= '202412'
group by 
	b.brand_fnl
	, b.tfm_product_new_fnl
	, a.product_old_new
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, b.component
	, b.service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
	, a.hce_cohort
;


-- MMs for 202412 onward
drop table tmp_1m.kn_products_mms_202412_2025;
create table tmp_1m.kn_products_mms_202412_2025 as
select
	b.fin_brand 
	, b.fin_tfm_product_new 
	, a.product_old_new
	, b.fin_g_i 
	, b.fin_product_level_3 
	, b.fin_inc_year
	, b.fin_inc_month
	, a.hce_cohort
	, count(distinct b.fin_mbi_hicn_fnl) as Mbrs
from tmp_1m.kn_x_products_cohort_202412_2025ytd as a
join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl
where b.fin_inc_month >= '202412'
group by
	b.fin_brand 
	, b.fin_tfm_product_new 
	, a.product_old_new
	, b.fin_g_i 
	, b.fin_product_level_3 
	, b.fin_inc_year
	, b.fin_inc_month
	, a.hce_cohort
;

-- 23220 from 
-- select count(*) from kn_products_mms_202412_2025

-- Verify tables
select * from tmp_1m.kn_products_mms_202412_2025 limit 2;
select * from tmp_1m.kn_products_claims_202412_2025 limit 2;

-- Stack claims and mms
create table tmp_1m.kn_products_claims_mms_202412_2025 as
select
	'202412 - 2025ytd' as MoM
	, 'Claims' as category
	, brand_fnl
	, product_old_new 
	, tfm_product_new_fnl
	, group_ind_fnl	
	, product_level_3_fnl
	, hce_cohort
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, allowed
	, paid
	, 0 as mm
from tmp_1m.kn_products_claims_202412_2025
union all
select
	'202412 - 2025ytd' as MoM
	, 'Membership' as category
	, fin_brand
	, product_old_new 
	, fin_tfm_product_new
	, fin_g_i
	, fin_product_level_3
	, '' as component
	, hce_cohort
	, '' as hce_service_code
	, fin_inc_year
	, fin_inc_month
	, 0 as allowed
	, 0 as paid
	, mbrs
from tmp_1m.kn_products_mms_202412_2025
;
-- 24429 from
-- select count(*) from tmp_1m.kn_products_claims_mms_202412_2025;

select * from tmp_1m.kn_products_claims_mms_202412_2025 order by mbrs desc
