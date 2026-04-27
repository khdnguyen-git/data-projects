--March Monthly Close: checking on Jan-March YOY claims

--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Dec 2024 Members and Claims   @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
--#####  Membership extract #####
-- drop table tmp_1m.cl_unique_mbi_2025YTD;
create table tmp_1m.cl_unique_mbi_2025YTD as
select distinct		
	fin_mbi_hicn_fnl as MBI	
from fichsrv.tre_membership
where 1=1
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA' 
	and fin_inc_year = '2025'
;



--Leavers are defined as live in Dec 2024, but termed in 2025; otherwise Stayers
-- DROP TABLE tmp_1m.cl_mbi_202412;
CREATE TABLE tmp_1m.cl_mbi_202412 as
select distinct		
	a.fin_mbi_hicn_fnl as MBI
from fichsrv.tre_membership a
JOIN  tmp_1m.cl_unique_mbi_2025YTD b
ON a.fin_mbi_hicn_fnl = b.mbi
where 1=1
	and a.sgr_source_name = 'COSMOS'
	and a.fin_brand = 'M&R'
	and a.migration_source not in ('OAH', 'CSP')
	and a.fin_product_level_3 not in ('INSTITUTIONAL')
	and a.global_cap = 'NA' 
	and a.fin_inc_month = '202412' 
--5516994		select count(*) from tmp_1m.cl_mbi_202412	
	
	
	
--getting MMs for 2024 ++ 2025
drop table tmp_1m.cl_mbi_202412_MMs;
create table tmp_1m.cl_mbi_202412_MMs as
select 
	b.fin_brand 
	,b.fin_tfm_product_new 
	,b.fin_g_i 
	,b.fin_product_level_3 
	,b.fin_inc_year
	,b.fin_inc_month
	,a.mbi_status	
 	,count(distinct b.fin_mbi_hicn_fnl) as Mbrs
from tmp_1m.cl_mbi_202412 a
join fichsrv.tre_membership b
	on a.mbi = b.fin_mbi_hicn_fnl 
where b.fin_inc_year = '2024'
group BY 
	b.fin_brand 
	,b.fin_tfm_product_new 
	,b.fin_g_i 
	,b.fin_product_level_3 
	,b.fin_inc_year
	,b.fin_inc_month
	,a.mbi_status	
;

-- mbi 202412 
-- mbi YTD2025 ->  
-- keep intersection; join with hce_cohort (prod_trans)
-- claims + membership


select
	,fin_brand
	,case when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	      else fin_tfm_product_new end as Product
	,fin_tfm_product_new
	,fin_g_i
	,fin_product_level_3
	,mbi_status
	,'' as component
	,'' as hce_service_code
	,fin_inc_year
	,fin_inc_month
	,0 as allowed
	,0 as paid
	,mbrs