-- kn_x_mbi_202412_2025ytd
-- 4933904
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_x_mbi_202412_2025ytd
union all
select 2 as source, count(mbi) as n from tmp_1m.kn_x_mbi_202412_2025ytd
-- 4933904

-- kn_x_mbi_products_mms_202412
-- 4933904
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_x_mbi_products_mms_202412
union all
select 2 as source, count(mbi) as n from tmp_1m.kn_x_mbi_products_mms_202412
order by source
-- 4933904

-- kn_x_mbi_products_claims_202412
-- 2994442
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_x_mbi_products_claims_202412
union all
select 2 as source, count(mbi) as n from tmp_1m.kn_x_mbi_products_claims_202412
-- 2994442


-- kn_x_mbi_products_claims_mms_202412
-- 4933904
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_x_mbi_products_claims_mms_202412
union all
select 2 as source, count(mbi) as n from tmp_1m.kn_x_mbi_products_claims_mms_202412
-- 7928346
-- Each member has several

with cte as (
select distinct
	mbi
	, old_product
from tmp_1m.kn_x_mbi_products_claims_mms_202412
order by mbi
)
select mbi, count(mbi) as n from cte group by mbi order by n desc
	

-- kn_x_mbi_202412_2025ytd
-- vs
-- kn_x_products_202412_2025ytd

-- 4933904
select 1 as source, count(mbi) as n from tmp_1m.kn_x_mbi_202412_2025ytd
union all
select 2 as source, count(mbi) as n from tmp_1m.kn_x_products_202412_2025ytd
order by source
-- 4968320



create table jointest as 
select distinct
	a.mbi
	, a.old_product
	, b.new_product 
	, concat(a.old_product, "-", b.new_product) as product_old_new
from tmp_1m.kn_x_mbi_products_claims_mms_202412 as a
join tmp_1m.kn_x_mbi_products_claims_mms_2025ytd as b
on a.mbi = b.mbi
where a.mbi is not null and b.mbi is not null;

mbi month new_product row
1	202503 HMO 			1	
1	2
1

select 
	mbi,
	
row_number() over(partition by mbi order by fin_inc_month desc ) as row from 


select * from tmp_1m.kn_x_mbi_products_claims_mms_2025ytd
limit 10;


-- 4933904
select 1 as source, count(distinct mbi) as n from tmp_1m.kn_x_mbi_202412_2025ytd
union all
select 2 as source, count(distinct mbi) as n from jointest
order by source
-- 4968320


select
	mbi
	, old_product
	, new_product
	, product_old_new
	, count(*)
from jointest
group by 
	mbi
	, old_product
	, new_product
	, product_old_new
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
--- 
	
	-- Counting distinct b.fin_mbi_hicn_fnl
create table count1 as
select 
    b.fin_brand,
    b.fin_tfm_product_new,
    b.fin_g_i,
    b.fin_product_level_3,
    b.fin_inc_year,
    b.fin_inc_month,
    a.hce_cohort,
    a.competitorfile_closure_flag,
    sum(rafb) as rafb_tol,
    count(distinct b.fin_mbi_hicn_fnl) as Mbrs
from tmp_1m.cl_unique_mbi_2025YTD_wCohort a
join fichsrv.tre_membership b
    on a.mbi = b.fin_mbi_hicn_fnl 
where b.fin_inc_month >= '202401'
group by 
    b.fin_brand,
    b.fin_tfm_product_new,
    b.fin_g_i,
    b.fin_product_level_3,
    b.fin_inc_year,
    b.fin_inc_month,
    a.hce_cohort,
    a.competitorfile_closure_flag;

-- Counting distinct a.mbi
create table count2 as
select 
    b.fin_brand,
    b.fin_tfm_product_new,
    b.fin_g_i,
    b.fin_product_level_3,
    b.fin_inc_year,
    b.fin_inc_month,
    a.hce_cohort,
    a.competitorfile_closure_flag,
    sum(rafb) as rafb_tol,
    count(distinct a.mbi) as Mbrs
from tmp_1m.cl_unique_mbi_2025YTD_wCohort a
join fichsrv.tre_membership b
    on a.mbi = b.fin_mbi_hicn_fnl 
where b.fin_inc_month >= '202401'
group by 
    b.fin_brand,
    b.fin_tfm_product_new,
    b.fin_g_i,
    b.fin_product_level_3,
    b.fin_inc_year,
    b.fin_inc_month,
    a.hce_cohort,
    a.competitorfile_closure_flag;
   
select fin_brand, sum(Mbrs) as mm from count1 group by fin_brand
union all
select fin_brand, sum(Mbrs) as mm from count2 group by fin_brand
