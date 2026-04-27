

-- Source table check;
-- 324092
select 1 as source, count(distinct fin_mbi_hicn_fnl) as n from tmp_1y.2024_2025_HCE_COHORT_6 where hce_cohort = 'Product_Transition'
union all
select 2 as source, count(distinct fin_mbi_hicn_fnl) as n from tmp_1y.2024_2025_hce_cohort_6_202503 where hce_cohort = 'Product_Transition'
order by source
-- 304476


-- Distinct mbi in Product_Transition cohort from Sree's
drop table tmp_1m.kn_prtr_cohort;
create table tmp_1m.kn_prtr_cohort as 
select 
	fin_mbi_hicn_fnl as mbi
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
where hce_cohort = 'Product_Transition';

select count(*) from tmp_1m.kn_prtr_cohort;

describe tmp_1y.2024_2025_HCE_COHORT_6;
-- Distinct 2025 M&R mbi of Product_Transition cohort;
drop table tmp_1m.kn_prtr_mbi_2025ytd;
create table tmp_1m.kn_prtr_mbi_2025ytd as
with 
unique_mbi_2025 as (
	select distinct		
		fin_mbi_hicn_fnl as mbi	
	from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202504 -- Use most updated membership; usual : fichsrv.tre_membership 
	where 1=1
		and sgr_source_name = 'COSMOS'
		and fin_brand = 'M&R'
		and migration_source not in ('OAH', 'CSP')
		and fin_product_level_3 not in ('INSTITUTIONAL')
		and global_cap = 'NA' 
		and fin_inc_year in ('2025')
),
prtr_mbi as (
	select 
		mbi
	from tmp_1m.kn_prtr_cohort
)
select
	a.mbi
from unique_mbi_2025 as a
left join prtr_mbi as b
	on a.mbi = b.mbi
;
select count(distinct mbi) from tmp_1m.kn_prtr_mbi_2025ytd

-- Check duplicates mbi
-- 153,621
select 1 as source, count(mbi) as n from tmp_1m.kn_prtr_mbi_2025ytd
union all
select 2 as source, count(distinct mbi) as n from tmp_1m.kn_prtr_mbi_2025ytd
order by source
-- 153,621

-- Compare to Caroline's cohort
-- 153,621
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_prtr_mbi_2025ytd
union all
select 2 as source, count(distinct mbi) as n from tmp_1m.cl_unique_mbi_2025YTD_wCohort where hce_cohort = 'Product_Transition'
order by source
-- 163,323


-- Get membership + product information for 2025 M&R mbi of Product_Transition cohort
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
join tadm_tre_cpy.gl_rstd_gpsgalnce_f_202504 as b -- Use most updated membership; usual : fichsrv.tre_membership
	on a.mbi = b.fin_mbi_hicn_fnl
where fin_inc_month >= '202401';




-- From Caroline table, get product_transition
create table tmp_1m.kn_prtr_mbi_2025ytd as 
select 
	* 
from tmp_1m.cl_unique_mbi_2025YTD_wCohort
where hce_cohort = 'Product_Transition'
;

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

-- Product information from claims
create table tmp_1m.kn_prtr_prod_claims_2025ytd as
select 
	b.brand_fnl
	, b.group_ind_fnl
	, b.tfm_product_new_fnl
	, b.product_level_3_fnl
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	       else b.tfm_product_new_fnl end as product
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
	b.brand_fnl
	, b.group_ind_fnl
	, b.tfm_product_new_fnl
	, b.product_level_3_fnl
	, case when b.tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then b.tfm_product_new_fnl
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'DUAL' then 'DUAL'
	       when b.tfm_product_new_fnl = 'DUAL_CHRONIC' and b.product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	       else b.tfm_product_new_fnl end as product
	, b.component
	, b.hce_service_code
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
create table tmp_1m.kn_prtr_prod_membership_claims_2025ytd as
select
	'Claims' as category
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
	'Membership' as category
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
where fst_srvc_month != '202504'


