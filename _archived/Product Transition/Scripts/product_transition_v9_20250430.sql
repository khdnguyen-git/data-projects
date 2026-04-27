-- Method 1: repeat the making of the mbi table
-- drop table tmp_1m.kn_unique_mbi_2025YTD;
create table tmp_1m.kn_unique_mbi_2025YTD as
select distinct		
	fin_mbi_hicn_fnl as mbi	
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202504
where 1 = 1
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA' 
	and fin_inc_year = '2025'
;

-- Adding cohort info from Sree
-- drop table tmp_1m.kn_unique_mbi_2025YTD_wCohort; 
create table kn_unique_mbi_2025YTD_wCohort as
select 
	a.*
	, b.hce_cohort
	, b.competitorfile_closure_flag
	, b.2025_anchormonth_fin_risk_adj_fctr_b as rafb
from tmp_1m.cl_unique_mbi_2025YTD as a
left join tmp_1y.2024_2025_HCE_COHORT_6 as b    --select * from tmp_1y.2024_2025_HCE_COHORT_6 limit 2;
	on a.mbi = b.fin_mbi_hicn_fnl
where hce_cohort = 'Product_Transition'

-- 153,971
select count(distinct mbi) from kn_unique_mbi_2025YTD_wCohort


-- Method 2: from Caroline's table to get same starting point 
-- drop table tmp_1m.kn_prtr_mbi_2025ytd;
create table tmp_1m.kn_prtr_mbi_2025ytd as 
select 
	* 
from tmp_1m.cl_unique_mbi_2025YTD_wCohort
where hce_cohort = 'Product_Transition'
;




-- 153,971
select count(distinct mbi) from tmp_1m.kn_prtr_mbi_2025ytd;



-- Get product information from membership
create table tmp_1m.kn_prtr_prod_membership_2025ytd as
select 
	a.mbi
	, b.fin_brand 
	, b.fin_g_i 
	, b.fin_tfm_product_new 
	, b.fin_product_level_3
	, case when b.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then b.fin_tfm_product_new
	       when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'DUAL' then 'DUAL'
	       when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	  	   else 'OTHER' end as product
	, b.fin_inc_year
	, b.fin_inc_month
	, a.competitorfile_closure_flag
from tmp_1m.kn_prtr_mbi_2025ytd as a
join tadm_tre_cpy.gl_rstd_gpsgalnce_f_202504 as b
	on a.mbi = b.fin_mbi_hicn_fnl 
where b.fin_inc_month >= '202401'
;

-- 153,971
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_prtr_mbi_2025ytd
union all
select 2 as source, count(distinct mbi) as n from tmp_1m.cl_unique_mbi_2025ytd_wcohort where hce_cohort = 'Product_Transition'
order by source
-- 153,971

with cte_mm as (
select
	count(distinct mbi) as Mbrs
from tmp_1m.kn_prtr_prod_membership_2025ytd
)

select 
	'kn' as source
	, sum(Mbrs) as mm
from cte_mm
union all
select 
	'cl' as source
	, sum(Mbrs) as mm 
from tmp_1m.cl_cohort_mms_25_24
where hce_cohort = 'Product_Transition'
;
-- 153,971 vs 1,904,582

	
-- 1,904,582
select 
	sum(Mbrs) as mm
from tmp_1m.cl_cohort_MMs_25_24
where hce_cohort = 'Product_Transition'


-- Product information from claims
drop table tmp_1m.kn_prtr_prod_claims_2025ytd
create table tmp_1m.kn_prtr_prod_claims_2025ytd as
select 
	a.mbi
	, b.brand_fnl
	, b.group_ind_fnl
	, b.tfm_product_new_fnl
	, b.product_level_3_fnl
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	       else 'OTHER' end as product
	, b.component
	, b.hce_service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
	, a.competitorfile_closure_flag
 	, b.allw_amt_fnl
	, b.net_pd_amt_fnl 
from tmp_1m.kn_prtr_mbi_2025ytd as a
join tadm_tre_cpy.glxy_op_f_202504 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month >= '202401'
union all
select 
	a.mbi
	, b.brand_fnl
	, b.group_ind_fnl
	, b.tfm_product_new_fnl
	, b.product_level_3_fnl
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	       else 'OTHER' end as product
	, b.component
	, b.service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
	, a.competitorfile_closure_flag
 	, b.allw_amt_fnl
	, b.net_pd_amt_fnl
from tmp_1m.kn_prtr_mbi_2025ytd as a
join tadm_tre_cpy.glxy_pr_f_202504 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month >= '202401'
;

-- Stacking membership and claims
drop table tmp_1m.kn_prtr_prod_membership_claims_2025ytd
create table tmp_1m.kn_prtr_prod_membership_claims_2025ytd as
select
	mbi
	, 'Claims' as category
	, brand_fnl
	, group_ind_fnl
	, tfm_product_new_fnl
	, product_level_3_fnl
	, product
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, competitorfile_closure_flag
	, allw_amt_fnl
	, net_pd_amt_fnl
from tmp_1m.kn_prtr_prod_claims_2025ytd
where fst_srvc_month != '202504'
union all
select
	mbi
	, 'Membership' as category
	, fin_brand
	, fin_g_i
	, fin_tfm_product_new
	, fin_product_level_3
	, product
	, '' as component
	, '' as hce_service_code
	, fin_inc_year
	, fin_inc_month
	, competitorfile_closure_flag
	, 0 as allw_amt_fnl
	, 0 as net_pd_amt_fnl
from tmp_1m.kn_prtr_prod_membership_2025ytd
where fin_inc_month != '202504'
;

-- Get old and new product with sorting - most recent old_product and earliest new_product according to fst_srvc_month
drop table clean_product_sorted

create table clean_product_sorted as 
with
mbi_product as (
	select distinct
		mbi
		, fst_srvc_month
		, fst_srvc_year
		, product
	from tmp_1m.kn_prtr_prod_membership_claims_2025ytd
	where product is not null
),
old_product as (
	select 
		mbi
		, product as old_product
		, fst_srvc_month as old_product_month
		, fst_srvc_year as old_product_year
		, row_number() over (partition by mbi order by fst_srvc_month desc) as rn -- latest 2024 product
	from mbi_product
	where fst_srvc_month between '202401' and '202412'
		or fst_srvc_year = '2024'
),
new_product as (
	select 
		mbi
		, product as new_product
		, fst_srvc_month as new_product_month
		, fst_srvc_year as new_product_year
		, row_number() over (partition by mbi order by fst_srvc_month desc) as rn -- latest 2025 product
	from mbi_product
	where fst_srvc_month >= '202501'
		or fst_srvc_year = '2025'
)
select distinct
	a.mbi
	, case 
		when o.old_product is NULL then 'OTHER' 
			else old_product 
	  end as old_product
	, o.old_product_month
	, case 
		when n.new_product is NULL then 'OTHER' 
			else n.new_product 
	  end as new_product
	, n.new_product_month
from mbi_product as a
left join (select * from old_product where rn = 1) as o
	on a.mbi = o.mbi
left join (select * from new_product where rn = 1) as n
	on a.mbi = n.mbi
;

select * from clean_product_sorted

-- Get old and new product without sorting 
-- Not preferred due to duplicates. Only here for completeness
-- drop table clean_product_nosort
--create table clean_product_nosort as 
--with
--mbi_product as (
--	select distinct
--		mbi
--		, fst_srvc_month
--		, fst_srvc_year
--		, product
--	from tmp_1m.kn_prtr_prod_membership_claims_2025ytd
--	where product is not null
--),
--old_product as (
--	select 
--		mbi
--		, product as old_product
--		, fst_srvc_month as old_product_month
--		, fst_srvc_year as old_product_year
--		, row_number() over (partition by mbi order by fst_srvc_month desc) as rn -
--	from mbi_product
--	where fst_srvc_month between '202401' and '202412'
--		or fst_srvc_year = '2024'
--),
--new_product as (
--	select 
--		mbi
--		, product as new_product
--		, fst_srvc_month as new_product_month
--		, fst_srvc_year as new_product_year
--		, row_number() over (partition by mbi order by fst_srvc_month desc) as rn
--	from mbi_product
--	where fst_srvc_month >= '202501'
--		or fst_srvc_year = '2025'
--)
--select distinct
--	a.mbi
--	, case 
--		when o.old_product is NULL then 'OTHER' 
--			else old_product 
--	  end as old_product
--	, o.old_product_month
--	, case 
--		when n.new_product is NULL then 'OTHER' 
--			else n.new_product 
--	  end as new_product
--	, n.new_product_month
--from mbi_product as a
--left join (select * from old_product) as o
--	on a.mbi = o.mbi
--left join (select * from new_product) as n
--	on a.mbi = n.mbi

--	select * from clean_product_nosort

-- Get MMs
--create table nosort_mms as
--select 
--	old_product
--	, new_product
--	, count(distinct mbi) as mm
--from clean_product_nosort
--group by 
--	old_product
--	, new_product
--;
--select * from nosort_mms;


create table sorted_mms as
select 
	old_product
	, new_product
	, count(distinct mbi) as mm
from clean_product_sorted
group by 
	old_product
	, new_product
;
select * from sorted_mms;


-- Rejoin back to claims and mms table
drop table tmp_1m.kn_products_claims_membership_2025ytd;
create table tmp_1m.kn_products_claims_membership_2025ytd as
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
	, a.component
	, a.hce_service_code
	, a.fst_srvc_year
	, a.fst_srvc_month
	, a.competitorfile_closure_flag
	, a.allw_amt_fnl
	, a.net_pd_amt_fnl
from tmp_1m.kn_prtr_prod_membership_claims_2025ytd as a
left join clean_product_sorted as b
	on a.mbi = b.mbi
;

-- Find mms
drop table tmp_1m.kn_products_mms_2025ytd
create table tmp_1m.kn_products_mms_2025ytd as
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
	, '' as component
	, '' as hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, competitorfile_closure_flag
	, 0 as allw_amt_fnl
	, 0 as net_pd_amt_fnl
	, count(distinct mbi) as mm
from tmp_1m.kn_products_claims_membership_2025ytd 
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
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, competitorfile_closure_flag
;

-- Find cost
drop table tmp_1m.kn_products_cost_2025ytd
create table tmp_1m.kn_products_cost_2025ytd as
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
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, competitorfile_closure_flag
	, sum(allw_amt_fnl) as allowed
	, sum(net_pd_amt_fnl) as paid
	, 0 as mm
from tmp_1m.kn_products_claims_membership_2025ytd 
where category = 'Claims'
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
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, competitorfile_closure_flag
;

-- Restack for exporting
select * from tmp_1m.kn_products_mms_cost_2025ytd;

create table tmp_1m.kn_products_mms_cost_2025ytd as
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
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, competitorfile_closure_flag
	, allw_amt_fnl as allowed
	, net_pd_amt_fnl as paid
	, mm
from tmp_1m.kn_products_mms_2025ytd
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
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, competitorfile_closure_flag
	, allowed
	, paid
	, mm
from tmp_1m.kn_products_cost_2025ytd




select * from tmp_1m.kn_products_mms_2025ytd;
select * from tmp_1m.kn_products_cost_2025ytd;
