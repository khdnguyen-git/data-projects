/*--April Monthly Close: checking on Jan-Apr YOY claims*/
describe formatted tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505; /*--created 3/26*/
/*--$$$$$$$$$  MML: get member level revenue         $$$$$$$$$$$*/
drop table tmp_1m.kn_cms_revenue 
;
 
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
;
 
/*-- 142405427            select count(*) from tmp_1m.kn_cms_revenue*/
/*-- 152377619 20250530*/
/*--check if every mbi has some revenue*/
create table tmp_7d.kn_test_rev as
select
    a.mbi
    , b.incurred_month
    , sum(b.part_c_cms_payments) as partc_pmt
from tmp_1m.kn_unique_mbi_2025ytd_wcohort a
left join tmp_1m.kn_cms_revenue b
on  a.mbi = b.hicnbr
where incurred_month in ('202501', '202502', '202503', '202504')
group by
    a.mbi
    , b.incurred_month
;
 
select 
    * 
from tmp_7d.kn_test_rev 
where incurred_month is null 
;
/*--0*/
/*--check completed*/
/*--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 2025 YTD over 2024 
-- @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*/
/*--#####  Membership extract #####*/
drop table tmp_1m.kn_unique_mbi_2025ytd 
;
 
create table tmp_1m.kn_unique_mbi_2025ytd as
select 
    distinct fin_mbi_hicn_fnl as mbi
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505
where 1 = 1
    and sgr_source_name = 'COSMOS'
    and fin_brand = 'M&R'
    and migration_source not in ('OAH', 'CSP')
    and fin_product_level_3 not in ('INSTITUTIONAL')
    and global_cap = 'NA'
    and fin_inc_year = '2025'
;
 
/*--6120115  select count(*) from tmp_1m.cl_unique_mbi_2025YTD*/
-- 6211145 20250602
/*--adding cohort info from Sree*/
drop table tmp_1m.kn_unique_mbi_2025ytd_wcohort 
;
 
create table tmp_1m.kn_unique_mbi_2025ytd_wcohort as
select
    a.*
    , b.hce_cohort
    , b.competitorfile_closure_flag
    , b.2025_anchormonth_fin_risk_adj_fctr_b as rafb
from tmp_1m.kn_unique_mbi_2025ytd a
left join tmp_1y.2024_2025_hce_cohort_6 b /*--select * from tmp_1y.2024_2025_HCE_COHORT_6 limit 2;*/
on  a.mbi = b.fin_mbi_hicn_fnl 
;
 
/*--6120115   select count(*) from tmp_1m.kn_unique_mbi_2025YTD_wCohort*/
-- 6211145 20250602

select 
    * 
from tmp_1m.kn_unique_mbi_2025ytd_wcohort 
where hce_cohort is null 
; /*--0 (meaning Sree captured all members in 2025 YTD)*/
/*--getting MMs for 2025 and 2024*/
drop table tmp_1m.kn_cohort_mms_25_24 
;
 
create table tmp_1m.kn_cohort_mms_25_24 as
select
    b.fin_brand
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_product_level_3
    , b.fin_inc_year
    , b.fin_inc_month
    , a.hce_cohort
    , a.competitorfile_closure_flag
    , sum(rafb) as rafb_tol
    , count(distinct b.fin_mbi_hicn_fnl) as mbrs
    , sum(c.raf_ab_adj) as mml_raf_ab_adj
    , sum(c.part_c_cms_payments) as mml_partc_cms_pmt
    , sum(c.part_c_cms_raf_retro_adj) as mml_partc_cms_raf_pmt
    , sum(c.part_c_member_premium) as mml_partc_mbr_premium
from tmp_1m.kn_unique_mbi_2025ytd_wcohort a
join tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505 b
on  a.mbi = b.fin_mbi_hicn_fnl
left join tmp_1m.kn_cms_revenue c
on  b.fin_mbi_hicn_fnl = c.hicnbr
    and b.fin_inc_month = c.incurred_month
where b.fin_inc_month >= '202401'
group by
    b.fin_brand
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_product_level_3
    , b.fin_inc_year
    , b.fin_inc_month
    , a.hce_cohort
    , a.competitorfile_closure_flag
;
 
/*--1243  select count(*) from tmp_1m.kn_cohort_MMs_25_24*/
-- 1583 20250602

select 
    * 
from tmp_1m.kn_cohort_mms_25_24 
;

/*
--check membership
drop table tmp_7d.kn_cohort_202412_mbi_retained
create table tmp_7d.kn_cohort_202412_mbi_retained as
select distinct
b.fin_mbi_hicn_fnl as mbi
,a.hce_cohort
from tmp_1m.kn_unique_mbi_2025YTD_wCohort a
join tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505 b
on a.mbi = b.fin_mbi_hicn_fnl
where b.fin_inc_month = '202412'
and a.hce_cohort = 'Retained'
--4195928     select count(*) from tmp_7d.kn_cohort_202412_mbi_retained
drop table tmp_7d.kn_cohort_202501_mbi_retained
create table tmp_7d.kn_cohort_202501_mbi_retained as
select distinct
b.fin_mbi_hicn_fnl as mbi
,a.hce_cohort
from tmp_1m.kn_unique_mbi_2025YTD_wCohort a
join tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505 b
on a.mbi = b.fin_mbi_hicn_fnl
where b.fin_inc_month = '202501' and a.hce_cohort = 'Retained'
--4188476     select count(*) from tmp_7d.kn_cohort_202501_mbi_retained
drop table tmp_7d.kn_cohort_in202412_notin202501
create table tmp_7d.kn_cohort_in202412_notin202501 as
select a.mbi
from tmp_7d.kn_cohort_202412_mbi_retained a
left join tmp_7d.kn_cohort_202501_mbi_retained b
on a.mbi = b.mbi
where b.mbi is null
select * from tmp_7d.kn_cohort_in202412_notin202501  --7452
select * from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505
where fin_mbi_hicn_fnl = '1A00PQ3NF33' and fin_inc_year  = '2025'
*/
/*--%%%%%%%%%%%%   Extract Claims: OP and PR  %%%%%%%%%%%%%*/
/*-- mbi: tmp_1m.kn_unique_mbi_2025YTD_wCohort*/
create table tmp_1m.kn_25_24_claims_marchclose as
select 
    * 
from tmp_1m.kn_25_24_claims 
;

drop table tmp_1m.kn_25_24_claims 
;
 
create table tmp_1m.kn_25_24_claims as
select
    b.brand_fnl
    , b.tfm_product_new_fnl
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
from tmp_1m.kn_unique_mbi_2025ytd_wcohort a
join tadm_tre_cpy.glxy_op_f_202505 b
on  a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_year >= '2024'
group by
    b.brand_fnl
    , b.tfm_product_new_fnl
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
    , b.tfm_product_new_fnl
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
from tmp_1m.kn_unique_mbi_2025ytd_wcohort a
join tadm_tre_cpy.glxy_pr_f_202505 b
on  a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_year >= '2024'
group by
    b.brand_fnl
    , b.tfm_product_new_fnl
    , b.group_ind_fnl
    , b.product_level_3_fnl
    , a.hce_cohort
    , a.competitorfile_closure_flag
    , b.component
    , b.service_code
    , b.fst_srvc_year
    , b.fst_srvc_month
;
 
/*--40949   select count(*) from tmp_1m.kn_25_24_claims*/
-- 49177
/*--union mbrs and claims: drop the most current month*/
select 
    * 
from tmp_1m.kn_cohort_mms_25_24 limit 2 
;
 
select 
    * 
from tmp_1m.kn_25_24_claims limit 2 
;

drop table tmp_1m.kn_cohort_25_24_mbrs_claims 
;

create table tmp_1m.kn_cohort_25_24_mbrs_claims as
select
    '25-24' as yoy
    , 'Claims' as category
    , brand_fnl
    , case 
        when tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then tfm_product_new_fnl
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'DUAL' then 'DUAL'
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
        else tfm_product_new_fnl 
    end as product
    , tfm_product_new_fnl
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
from tmp_1m.kn_25_24_claims
where fst_srvc_month <> '202505'
group by
    brand_fnl
    , case 
        when tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then tfm_product_new_fnl
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'DUAL' then 'DUAL'
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
        else tfm_product_new_fnl 
    end
    , tfm_product_new_fnl
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
    '25-24' as yoy
    , 'Membership' as category
    , fin_brand
    , case 
        when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
        else fin_tfm_product_new 
    end as product
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
from tmp_1m.kn_cohort_mms_25_24
where fin_inc_month <> '202505'
group by
    fin_brand
    , case 
        when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
        else fin_tfm_product_new 
    end
    , fin_tfm_product_new
    , fin_g_i
    , fin_product_level_3
    , hce_cohort
    , competitorfile_closure_flag
    , fin_inc_year
    , fin_inc_month
;
 
/*--39510 39379 35442             select count(*) from tmp_1m.kn_cohort_25_24_mbrs_claims*/
-- 47634 20250602
select 
    * 
from tmp_1m.kn_cohort_25_24_mbrs_claims 
order by 
    category asc
;
 
/*
--CHECKING CLAIMS
drop table tmp_1m.kn_ck
create table tmp_1m.kn_ck as
select
b.brand_fnl
,b.gal_mbi_hicn_fnl
,b.tfm_product_new_fnl
,b.group_ind_fnl
,b.product_level_3_fnl
,a.hce_cohort
,a.competitorfile_closure_flag
,b.component
,b.hce_service_code
,b.fst_srvc_year
,b.fst_srvc_month
,sum(b.allw_amt_fnl) as allowed
,sum(b.net_pd_amt_fnl) as paid
from tmp_1m.kn_unique_mbi_2025YTD_wCohort  a
join tadm_tre_cpy.glxy_op_f_202503 b
on a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month = '202407'
and a.hce_cohort = 'NTU'
and b.tfm_product_new_fnl = 'PPO'
group BY
b.brand_fnl
,b.gal_mbi_hicn_fnl
,b.tfm_product_new_fnl
,b.group_ind_fnl
,b.product_level_3_fnl
,a.hce_cohort
,a.competitorfile_closure_flag
,b.component
,b.hce_service_code
,b.fst_srvc_year
,b.fst_srvc_month
select count(*) from tmp_1m.kn_ck  --243,648
select * from tmp_1m.kn_ck
select * from tmp_1y.2024_2025_HCE_COHORT_6 where fin_mbi_hicn_fnl = '3NG0HQ9KQ36'
select * from  tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505 where  fin_mbi_hicn_fnl = '3NG0HQ9KQ36' and fin_inc_month >= ' 
202401'
select * from tadm_tre_cpy.glxy_op_f_202503 where gal_mbi_hicn_fnl = '3NG0HQ9KQ36' and fst_srvc_month = '202407'
*/
/*--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 2024 YTD over 2023 
-- @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*/
/*--#####  Membership extract #####*/
drop table tmp_1m.kn_unique_mbi_2024ytd 
;
 
create table tmp_1m.kn_unique_mbi_2024ytd as
select 
    distinct fin_mbi_hicn_fnl as mbi
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505
where 1 = 1
    and sgr_source_name = 'COSMOS'
    and fin_brand = 'M&R'
    and migration_source not in ('OAH', 'CSP')
    and fin_product_level_3 not in ('INSTITUTIONAL')
    and global_cap = 'NA'
    and fin_inc_month in ('202401', '202402', '202403', '202404')
;
 
/*--5605394 5,538,564  select count(*) from tmp_1m.kn_unique_mbi_2024YTD*/
/*--adding cohort info from Sree*/
drop table tmp_1m.kn_unique_mbi_2024ytd_wcohort 
;
 
create table tmp_1m.kn_unique_mbi_2024ytd_wcohort as
select
    a.*
    , b.hce_cohort
    , b.competitorfile_closure_flag
    , b.2024_anchormonth_fin_risk_adj_fctr_b as rafb
from tmp_1m.kn_unique_mbi_2024ytd a
left join tmp_1y.2023_2024_hce_cohort_6 b
on  a.mbi = b.fin_mbi_hicn_fnl
;
 
/*
-- 5605394   select count(*) from tmp_1m.kn_unique_mbi_2024YTD_wCohort*/
-- 5674224 20250602

/*--getting MMs for 2024 and 2023*/
drop table tmp_1m.kn_cohort_mms_24_23 
;
 
create table tmp_1m.kn_cohort_mms_24_23 as
select
    b.fin_brand
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_product_level_3
    , b.fin_inc_year
    , b.fin_inc_month
    , a.hce_cohort
    , a.competitorfile_closure_flag
    , sum(rafb) as rafb_tol
    , count(distinct b.fin_mbi_hicn_fnl) as mbrs
    , sum(c.raf_ab_adj) as mml_raf_ab_adj
    , sum(c.part_c_cms_payments) as mml_partc_cms_pmt
    , sum(c.part_c_cms_raf_retro_adj) as mml_partc_cms_raf_pmt
    , sum(c.part_c_member_premium) as mml_partc_mbr_premium
from tmp_1m.kn_unique_mbi_2024ytd_wcohort a
join tadm_tre_cpy.gl_rstd_gpsgalnce_f_202505 b
on  a.mbi = b.fin_mbi_hicn_fnl
left join tmp_1m.kn_cms_revenue c
on  b.fin_mbi_hicn_fnl = c.hicnbr
    and b.fin_inc_month = c.incurred_month
where b.fin_inc_month between '202301' and '202404'
group by
    b.fin_brand
    , b.fin_tfm_product_new
    , b.fin_g_i
    , b.fin_product_level_3
    , b.fin_inc_year
    , b.fin_inc_month
    , a.hce_cohort
    , a.competitorfile_closure_flag
;
 
/*--1568  select count(*) from tmp_1m.kn_cohort_MMs_24_23*/
-- 1939 2050602
/*--%%%%%%%%%%%%   Extract Claims: OP and PR: drop the lastest month  %%%%%%%%%%%%%*/
/*-- mbi: tmp_1m.kn_unique_mbi_2024YTD_wCohort*/
drop table tmp_1m.kn_24_23_claims 
;
 
create table tmp_1m.kn_24_23_claims as
select
    b.brand_fnl
    , b.tfm_product_new_fnl
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
from tmp_1m.kn_unique_mbi_2024ytd_wcohort a
join tadm_tre_cpy.glxy_op_f_202505 b /*--tadm_tre_cpy.glxy_op_f_202503*/
on  a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month between '202301' and '202404'
group by
    b.brand_fnl
    , b.tfm_product_new_fnl
    , b.group_ind_fnl
    , b.product_level_3_fnl
    , a.hce_cohort
    , a.competitorfile_closure_flag
    , b.component
    , b.hce_service_code
    , b.fst_srvc_year
    , b.fst_srvc_month
union
select
    b.brand_fnl
    , b.tfm_product_new_fnl
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
from tmp_1m.kn_unique_mbi_2024ytd_wcohort a
join tadm_tre_cpy.glxy_pr_f_202505 b /*--tadm_tre_cpy.glxy_pr_f_202503*/
on  a.mbi = b.gal_mbi_hicn_fnl
where fst_srvc_month between '202301' and '202404'
group by
    b.brand_fnl
    , b.tfm_product_new_fnl
    , b.group_ind_fnl
    , b.product_level_3_fnl
    , a.hce_cohort
    , a.competitorfile_closure_flag
    , b.component
    , b.service_code
    , b.fst_srvc_year
    , b.fst_srvc_month
;
 
/*--33908  30436  (30683)   select count(*) from tmp_1m.kn_24_23_claims*/
/*--union mbrs and claims*/

-- 45,723 20250602

select 
    * 
from tmp_1m.kn_cohort_mms_24_23 limit 2 
;
 
select 
    * 
from tmp_1m.kn_24_23_claims limit 2 
;

drop table tmp_1m.kn_cohort_24_23_mbrs_claims 
;
 
create table tmp_1m.kn_cohort_24_23_mbrs_claims as
select
    '24-23' as yoy
    , 'Claims' as category
    , brand_fnl
    , case 
        when tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then tfm_product_new_fnl
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'DUAL' then 'DUAL'
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
        else tfm_product_new_fnl 
    end as product
    , tfm_product_new_fnl
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
from tmp_1m.kn_24_23_claims
group by
    brand_fnl
    , case 
        when tfm_product_new_fnl in ('HMO', 'PPO', 'NPPO') then tfm_product_new_fnl
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'DUAL' then 'DUAL'
        when tfm_product_new_fnl = 'DUAL_CHRONIC' and product_level_3_fnl = 'CHRONIC' then 'CHRONIC'
        else tfm_product_new_fnl 
    end
    , tfm_product_new_fnl
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
    '24-23' as yoy
    , 'Membership' as category
    , fin_brand
    , case 
        when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
        else fin_tfm_product_new 
    end as product
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
from tmp_1m.kn_cohort_mms_24_23
group by
    fin_brand
    , case 
        when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
        when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
        else fin_tfm_product_new 
    end
    , fin_tfm_product_new
    , fin_g_i
    , fin_product_level_3
    , hce_cohort
    , competitorfile_closure_flag
    , fin_inc_year
    , fin_inc_month
;
 
/*--35476  31678  (31925)         select count(*) from tmp_1m.kn_cohort_24_23_mbrs_claims*/
-- 47667 20250602
select 
    * 
from tmp_1m.kn_cohort_24_23_mbrs_claims 
order by 
    category asc 
;

/*--export to excel monthly close file, "data" tab*/
create table tmp_1m.kn_cohort_25_23_Jan_April_mbrs_claims as
select 
    * 
from tmp_1m.kn_cohort_25_24_mbrs_claims
union all
select 
    * 
from tmp_1m.kn_cohort_24_23_mbrs_claims
order by 
    yoy 
    , category asc
;

/*--checking
select * from */ 
