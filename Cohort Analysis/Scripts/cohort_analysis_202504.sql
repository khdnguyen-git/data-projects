-- $$$$$$$$$  MML: get member level revenue	 $$$$$$$$$$$
-- drop table tmp_1m.kn_cms_revenue;
create table tmp_1m.kn_cms_revenue as
select 
	hicnbr
	, year(incurred_dt)||lpad(month(incurred_dt), 2, '0') as incurred_month
	, sum(raf_ab_adj) as raf_ab_adj
	, sum(`_2_part_c_cms_payments`) as part_c_cms_payments
	, sum(`_2_part_c_cms_raf`) as part_c_cms_raf_retro_adj
	, sum(`_2_part_c_member_premium`) as part_c_member_premium
from tmp_1m.ab_mml_union
where year(incurred_dt) >= '2024'
group by
	hicnbr
	, year(incurred_dt)||lpad(month(incurred_dt), 2, '0')

-- select count(*) from tmp_1m.kn_cms_revenue
-- 142405427
	
--check if every mbi has some revenue
drop table tmp_7d.kn_test_rev;

create table tmp_7d.kn_test_rev as
select 
	a.mbi
	, b.incurred_month
	, sum(b.part_c_cms_payments) as partc_pmt
from tmp_1m.kn_cnsd_unique_mbi_2025YTD as a
left join tmp_1m.kn_cms_revenue as b
	on a.mbi = b.hicnbr 
where incurred_month in ('202501', '202502', '202503')
group by
	a.mbi
	, b.incurred_month
;

-- select count(distinct mbi) from tmp_7d.test_rev where incurred_month is null
-- 0

-- @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 2025 YTD over 2024  @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
-- #####  Membership extract #####
-- Disticnt mbi for C&S Dual membership 2025
-- Non-HCTA
drop table tmp_1m.kn_cnsd_unique_mbi_2025YTD;
create table tmp_1m.kn_cnsd_unique_mbi_2025YTD as
select distinct		
	fin_mbi_hicn_fnl as mbi	
from fichsrv.tre_membership
where 1 = 1
	and fin_brand = 'C&S'
	and global_cap = 'NA'
	and fin_product_level_3 = 'DUAL'
	and sgr_source_name in ('COSMOS', 'NICE', 'CSP') 
	and migration_source not in ('OAH') 
	and fin_inc_year = '2025'
;

-- sgr_source_name in ('NICE') and nce_tadm_dec_risk_type = 'FFS')
-- FOr NICE source

select count(*) from tmp_1m.kn_cnsd_unique_mbi_2025YTD
-- 847864


-- Adding cohort info from Sree
drop table tmp_1m.kn_cnsd_mbi_cohort_2025YTD; 
create table tmp_1m.kn_cnsd_mbi_cohort_2025YTD as
select 
	a.*
	, b.hce_cohort
	, b.competitorfile_closure_flag
	, b.2025_anchormonth_fin_risk_adj_fctr_b as rafb
from tmp_1m.kn_cnsd_unique_mbi_2025YTD as a
left join tmp_1y.2024_2025_HCE_COHORT_6 as b    -- select * from tmp_1y.2024_2025_HCE_COHORT_6 limit 2;
	on a.mbi = b.fin_mbi_hicn_fnl
-- select count(*) from tmp_1m.kn_cnsd_mbi_cohort_2025YTD
-- 847864

-- getting MMs for 2025 and 2024
drop table tmp_1m.kn_cnsd_cohort_MMs_25_24;
create table tmp_1m.kn_cnsd_cohort_MMs_25_24 as
select 
	b.fin_brand 
	, b.sgr_source_name
	, b.global_cap
	, b.migration_source
	, b.fin_tfm_dddddddroduct_new 
	, b.fin_g_i 
	, b.fin_product_level_3 
	, b.fin_inc_year
	, b.fin_inc_month
	, a.hce_cohort
	, a.competitorfile_closure_flag
	, sum(rafb) as rafb_tol
 	, count(distinct b.fin_mbi_hicn_fnl) as Mbrs
	, sum(c.raf_ab_adj) as mml_raf_ab_adj
	, sum(c.part_c_cms_payments) as mml_partc_cms_pmt
	, sum(c.part_c_cms_raf_retro_adj) as mml_partc_cms_raf_pmt
	, sum(c.part_c_member_premium) as mml_partc_mbr_premium
from tmp_1m.kn_cnsd_mbi_cohort_2025YTD as a
join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl 
left join tmp_1m.kn_cms_revenue as c
	on  b.fin_mbi_hicn_fnl = c.hicnbr 
	and b.fin_inc_month = c.incurred_month
where b.fin_inc_month >= '202401'
group by 
	b.fin_brand 
	, b.sgr_source_name
	, b.global_cap
	, b.migration_source
	, b.fin_tfm_product_new 
	, b.fin_g_i 
	, b.fin_product_level_3 
	, b.fin_inc_year
	, b.fin_inc_month
	, a.hce_cohort
	, a.competitorfile_closure_flag
;
-- select count(*) from tmp_1m.kn_cnsd_cohort_MMs_25_24
-- 3826
-- select * from tmp_1m.kn_cnsd_cohort_MMs_25_24


--%%%%%%%%%%%%   Extract Claims: OP and PR  %%%%%%%%%%%%%
drop table tmp_1m.kn_cnsd_cohort_claims_25_24;
create table tmp_1m.kn_cnsd_cohort_claims_25_24 as
select 
	b.brand_fnl
	, b.tfm_product_fnl
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, a.hce_cohort
	, a.competitorfile_closure_flag
	, b.component
	, b.hce_service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
 	, sum(b.allw_amt_fnl) as allowed
	, sum(b.net_pd_amt_fnl) as paid	
from tmp_1m.kn_cnsd_mbi_cohort_2025YTD as a
join tadm_tre_cpy.dcsp_op_f_202504 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_year >= '2024'
group by 
	b.brand_fnl
	, b.tfm_product_fnl
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, a.hce_cohort
	, a.competitorfile_closure_flag
	, b.component
	, b.hce_service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
union all
select 
	b.brand_fnl
	, b.tfm_product_fnl
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, a.hce_cohort
	, a.competitorfile_closure_flag
	, b.component
	, b.service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
 	, sum(b.allw_amt_fnl) as allowed
	, sum(b.net_pd_amt_fnl) as paid	
from tmp_1m.kn_cnsd_mbi_cohort_2025YTD as a
join tadm_tre_cpy.dcsp_pr_f_202504 as b
	on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_year >= '2024'
group by 
	b.brand_fnl
	, b.tfm_product_fnl
	, b.group_ind_fnl
	, b.product_level_3_fnl
	, a.hce_cohort
	, a.competitorfile_closure_flag
	, b.component
	, b.service_code
	, b.fst_srvc_year
	, b.fst_srvc_month
;

-- select count(*) from tmp_1m.kn_cnsd_cohort_claims_25_24
-- 11541	


--union mbrs and claims: drop the most current month
select * from tmp_1m.kn_cnsd_cohort_mms_25_24 limit 2;
select * from tmp_1m.kn_cnsd_cohort_claims_25_24 limit 2;

drop table tmp_1m.kn_cnsd_cohort_claims_mms_25_24;
create table tmp_1m.kn_cnsd_cohort_claims_mms_25_24 as
select
	'25-24' as YOY
	,'Claims' as category
	, brand_fnl
	, case when tfm_product_fnl in ('HMO', 'PPO', 'NPPO') then tfm_product_fnl
	      when tfm_product_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'DUAL' then 'DUAL'
	      when tfm_product_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	      else tfm_product_fnl end as Product
	, tfm_product_fnl
	, group_ind_fnl	
	, product_level_3_fnl
	, hce_cohort
	, competitorfile_closure_flag
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, sum(allowed) as allowed
	, sum(paid) as paid
	, 0 as mm
	, 0 as rafb_tol
	, 0 as mml_raf_ab_adj
	, 0 as mml_partc_cms_pmt
	, 0 as mml_partc_cms_raf_pmt
	, 0 as mml_partc_mbr_premium
from tmp_1m.kn_cnsd_cohort_claims_25_24
where fst_srvc_month != '202504'
group by
	brand_fnl
	, case when tfm_product_fnl in ('HMO', 'PPO', 'NPPO') then tfm_product_fnl
	      when tfm_product_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'DUAL' then 'DUAL'
	      when tfm_product_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
	      else tfm_product_fnl end
	, tfm_product_fnl
	, group_ind_fnl	
	, product_level_3_fnl
	, hce_cohort
	, competitorfile_closure_flag
	, component
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
union all
select
	'25-24' as YOY
	, 'Membership' as category
	, fin_brand
	, case when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	      else fin_tfm_product_new end as Product
	, fin_tfm_product_new
	, fin_g_i
	, fin_product_level_3
	, hce_cohort
	, competitorfile_closure_flag
	, '' as component
	, '' as hce_service_code
	, fin_inc_year
	, fin_inc_month
	, 0 as allowed
	, 0 as paid
	, sum(mbrs) as mbrs
	, sum(rafb_tol) as rafb_tol
	, sum(mml_raf_ab_adj) as mml_raf_ab_adj
	, sum(mml_partc_cms_pmt) as mml_partc_cms_pmt
	, sum(mml_partc_cms_raf_pmt) as mml_partc_cms_raf_pmt
	, sum(mml_partc_mbr_premium) as mml_partc_mbr_premium
from tmp_1m.kn_cnsd_cohort_mms_25_24
where fin_inc_month != '202504'
group by 
	fin_brand
	, case when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
	      when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	      else fin_tfm_product_new end
	, fin_tfm_product_new
	, fin_g_i
	, fin_product_level_3
	, hce_cohort
	, competitorfile_closure_flag
	, fin_inc_year
	, fin_inc_month
-- select count(*) from tmp_1m.kn_cnsd_cohort_claims_mms_25_24

-- 11654	
	
select * from tmp_1m.kn_cnsd_cohort_claims_mms_25_24 order by category asc