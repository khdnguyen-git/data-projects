describe tmp_1m.kn_ip_dataset_loc_07302025
describe 

create table tmp_1y.kn_loc_snapshot_avtar_24_25 as
select 
*
from tmp_1m.ec_avtar_24_25_3;


select 
	min(admit_act_month) as min_month
	, max(admit_act_month) as max_month
from tmp_1m.ec_avtar_24_25_3;

select 
	admit_act_month
from tmp_1m.ec_avtar_24_25_3
group by
	admit_act_month
;

select count(*) from tmp_1m.ec_avtar_24_25_3;



-- ip_dataset_07302025_4_trs + ip_dataset_07302025_mm -> ip_dataset_notif_07302025_trs -> ip_dataset_loc_07302025

 

select product from tmp_1m.kn_ip_dataset_loc_07302025
group by product

drop table tmp_1m.kn_ip_dataset_loc_07302025_dscnp;
create table tmp_1m.kn_ip_dataset_loc_07302025_dscnp as
select 
	admit_week
	,admit_act_month
	,total_oah_flag
	,institutional_flag
	,fin_tfm_product_new
	,fin_product_level_3
	,product
	, case when product in ('DUAL', 'CHRONIC') and admit_act_month <= '202412' then 'Existing Member'
			when product in ('DUAL', 'CHRONIC') and admit_act_month > '202412' then 'New Member'
			else "Others"
	end as dcsnp_cat
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_market
	,fin_brand
	,group_name
	,los_categories
	,respiratory_flag
	,mnr_cosmos_ffs_flag
	,leading_ind_pop
	,mnr_nice_ffs_flag
	,mnr_total_ffs_flag
	,mnr_oah_flag
	,cns_oah_flag
	,mnr_dual_flag
	,cns_dual_flag
	,ocm_migration
	,component
	,sum(case_count) as case_count
	,sum(intital_adr_cnt) as intital_adr_cnt
	,sum(persistent_adr_cnt) as persistent_adr_cnt
	,sum(md_reviewed_cnt) as md_reviewed_cnt
	,sum(appeal_case_cnt) as appeal_case_cnt
	,sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
	,sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
	,sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
	,sum(p2p_case_cnt) as p2p_case_cnt
	,sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
	,sum(other_ovtrns) as other_ovtrns
	,sum(membership) as membership
from tmp_1m.kn_ip_dataset_loc_07302025
where admit_act_month >= '202301'
group by
	admit_week
	,admit_act_month
	,total_oah_flag
	,institutional_flag
	,fin_tfm_product_new
	,fin_product_level_3
	,product
	, case when product in ('DUAL', 'CHRONIC') and admit_act_month <= '202412' then 'Existing Member'
			when product in ('DUAL', 'CHRONIC') and admit_act_month > '202412' then 'New Member'
			else "Others"
	end
	,sgr_source_name
	,nce_tadm_dec_risk_type
	,fin_market
	,fin_brand
	,group_name
	,los_categories
	,respiratory_flag
	,mnr_cosmos_ffs_flag
	,leading_ind_pop
	,mnr_nice_ffs_flag
	,mnr_total_ffs_flag
	,mnr_oah_flag
	,cns_oah_flag
	,mnr_dual_flag
	,cns_dual_flag
	,ocm_migration
	,component
;



select count(*) from tmp_1m.kn_ip_dataset_loc_07302025_dscnp

describe hce_proj_bd.HCE_ADR_AVTAR_Like_24_25_F;

select
	fin_mbi_hicn_fnl as mbi
	, case_id
	, fin_product_level_3
	, fin_tfm_product_new
	, global_cap
	, tfm_include_flag
	, nce_tadm_dec_risk_type
	, fin_market
	, fin_g_i
	, fin_brand
	, notif_yrmonth
	
select entity from hce_proj_bd.hce_adr_avtar_like_24_25_f 
group by entity

drop table tmp_1m.kn_dcsnp_auth_2024_2025;
create table tmp_1m.kn_dcsnp_auth_2024_2025 stored as orc as
select
	fin_mbi_hicn_fnl as mbi
   	, case_id
    , notif_yrmonth
    , business_segment
    , migration_source
    , fin_brand
    , fin_source_name
    , sgr_source_name
    , nce_tadm_dec_risk_type
    , tfm_include_flag
    , global_cap
    , fin_market
    , fin_plan_level_2
    , fin_product_level_3
    , fin_tfm_product_new
    , fin_g_i
	, case when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
	       when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
	       when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	       else 'OTHER' 
	end as product
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 <> 'INSTITUTIONAL' then 1 
		else 0 
	end as mnr_cosmos_ffs_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type = 'FFS' then 1 
		else 0 
	end as mnr_nice_ffs_flag
	, case when (business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 <> 'INSTITUTIONAL') 
       		 or (business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS')) then 1 
		else 0 
	end as mnr_total_ffs_flag
	, case when (substring(notif_yrmonth, 1, 4) = '2024' and business_segment = 'CnS' and fin_brand in ('M&R','C&S') and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD') then 0 
		when (business_segment = 'MnR' and fin_brand = 'M&R' and migration_source = 'OAH')
		  or (business_segment = 'CnS' and fin_brand = 'C&S' and migration_source = 'OAH') then 1 
		else 0 
	end as oah_flag
	, case when 
		(
			(business_segment = 'CnS' and fin_brand in ('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' 
			 and sgr_source_name in ('COSMOS','CSP') and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
			or 
			(substring(notif_yrmonth, 1, 4) = '2024' and business_segment = 'CnS' and fin_brand in ('M&R','C&S') and global_cap = 'NA' 
			 and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')
		) then 1 
		else 0 
	end as cns_dual_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 1 
		else 0 
	end as mnr_dual_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 
		else 0 
	end as isnp_flag
from hce_proj_bd.hce_adr_avtar_like_24_25_f
where business_segment not in ('EnI','ERR','null')
	and medicare_id is not null
	and notif_yrmonth >= '202401'
	and fin_plan_level_2 != 'PFFS'
;

select * from tmp_1m.kn_dcsnp_auth_2024_2025
limit 2;

select * from tmp_1m.kn_dcsnp_mm_2024_2025
limit 2;


drop table tmp_1m.kn_dcsnp_mm_2024_2025;
create table tmp_1m.kn_dcsnp_mm_2024_2025 stored as orc as
select
	fin_mbi_hicn_fnl as mbi
    , fin_inc_month
    , migration_source
    , fin_brand
    , fin_source_name
    , sgr_source_name
    , nce_tadm_dec_risk_type
    , tfm_include_flag
    , global_cap
    , fin_market
    , fin_plan_level_2
    , fin_product_level_3
    , fin_tfm_product_new
    , fin_g_i
	, case when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
	       when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
	       when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	       else 'OTHER' 
	end as product
	, CASE WHEN a.fin_brand='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.fin_product_level_3 <>'INSTITUTIONAL' AND a.tfm_include_flag=1 
		THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
	, CASE WHEN a.fin_brand='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') THEN 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (a.fin_brand='M&R' AND a.global_cap='NA' AND a.sgr_source_name='COSMOS' AND a.fin_product_level_3 <>'INSTITUTIONAL' AND tfm_include_flag=1) 
		OR (a.fin_brand='M&R' AND a.sgr_source_name='NICE' AND a.nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
	, case when a.fin_brand='M&R' and a.migration_source='OAH' then 1 else 0 end as MnR_OAH_flag
 	, case when (a.fin_brand='C&S' and a.migration_source='OAH') then 1 
 		WHEN (a.fin_inc_year='2024' AND a.fin_brand='C&S' AND a.GLOBAL_CAP = 'NA' AND a.SGR_SOURCE_NAME IN ('COSMOS','CSP')
		AND a.MIGRATION_SOURCE = 'OAH' AND a.FIN_STATE = 'MD') THEN 0 else 0 end as CnS_OAH_flag
	, case when a.fin_brand='M&R' and a.fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	, CASE WHEN ((a.fin_brand in('C&S') and a.migration_source <> 'OAH' and a.global_cap = 'NA' and a.fin_product_level_3='DUAL' AND
		a.SGR_SOURCE_NAME in('COSMOS','CSP') AND a.fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (a.fin_inc_year ='2024' AND a.fin_brand in ('C&S')
		AND a.GLOBAL_CAP = 'NA' AND a.SGR_SOURCE_NAME IN ('COSMOS','CSP') AND a.MIGRATION_SOURCE = 'OAH' AND a.FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when a.migration_source='OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when a.fin_brand = 'M&R' and a.fin_product_level_3 = 'INSTITUTIONAL' then 1 
		else 0 
	end as isnp_flag
from fichsrv.tre_membership as a
where fin_inc_month >= '202401'
;


describe fichsrv.tre_membership;




drop table tmp_1m.kn_dcsnp_auth_2024_2025_cleaned;
create table tmp_1m.kn_dcsnp_auth_2024_2025_cleaned as
select
	'Auth' as data_type
	, mbi
	, case_id
	, notif_yrmonth as month
	, migration_source
	, fin_brand
	, fin_source_name
	, sgr_source_name
	, nce_tadm_dec_risk_type
	, tfm_include_flag
	, global_cap
	, fin_market
	, fin_product_level_3
	, fin_tfm_product_new
	, product
	, fin_g_i
    , mnr_cosmos_ffs_flag
    , mnr_nice_ffs_flag
    , mnr_total_ffs_flag
    , oah_flag
    , cns_dual_flag
    , mnr_dual_flag
    , isnp_flag
	, case 
    	when mnr_total_ffs_flag = 1 then 'M&R FFS'
    	when oah_flag = 1 then 'OAH'
    	when cns_dual_flag = 1 then 'C&S Dual'
    	when mnr_dual_flag = 1 then 'M&R Dual'
    	when isnp_flag = 1 then 'ISNP'
		else 'Others'
	end as Population
from tmp_1m.kn_dcsnp_auth_2024_2025;


drop table tmp_1m.kn_dcsnp_mm_2024_2025_cleaned;
create table tmp_1m.kn_dcsnp_mm_2024_2025_cleaned as
select
	'Membership' as data_type
	, mbi
	, '' as case_id
	, max(fin_inc_month) over (partition by mbi) as max_enrollment_month
	, migration_source
	, fin_brand
	, fin_source_name
	, sgr_source_name
	, nce_tadm_dec_risk_type
	, tfm_include_flag
	, global_cap
	, fin_market
	, fin_product_level_3
	, fin_tfm_product_new
	, product
	, fin_g_i
    , mnr_cosmos_ffs_flag
    , mnr_nice_ffs_flag
    , mnr_total_ffs_flag
    , total_oah_flag as oah_flag
    , cns_dual_flag
    , mnr_dual_flag
    , isnp_flag
	, case 
    	when mnr_total_ffs_flag = 1 then 'M&R FFS'
    	when total_oah_flag = 1 then 'OAH'
    	when cns_dual_flag = 1 then 'C&S Dual'
    	when mnr_dual_flag = 1 then 'M&R Dual'
    	when isnp_flag = 1 then 'ISNP'
		else 'Others'
	end as Population
from tmp_1m.kn_dcsnp_mm_2024_2025;


select
	mbi
	, case when product in ('DUAL', 'CHRONIC') and min(month) over (partition by mbi) <= '202412' then 'DCSNP - Existing Member'
			when product in ('DUAL', 'CHRONIC') and min(month) over (partition by mbi) > '202412' then 'DCSNP - New Member'
			else 'Others'
	end as dcsnp_cat
from tmp_1m.kn_dcsnp_auth_2024_2025_cleaned
order by month
limit 100;



drop table tmp_1m.kn_dcsnp_auth_2024_2025_sum;
create table tmp_1m.kn_dcsnp_auth_2024_2025_sum as
with classification as (
select
	mbi
	, case when oah_flag = 0 and product in ('DUAL', 'CHRONIC') and min(month) over (partition by mbi) != '202412' then 'DCSNP - Existing Member'
			when oah_flag = 0 and product in ('DUAL', 'CHRONIC') and min(month) over (partition by mbi) = '202412' then 'DCSNP - New Member'
			else 'Others'
	end as dcsnp_cat
from tmp_1m.kn_dcsnp_auth_2024_2025_cleaned
) 
select 
	a.month
	, a.migration_source
	, a.fin_brand
	, a.fin_source_name
	, a.sgr_source_name
	, a.nce_tadm_dec_risk_type
	, a.tfm_include_flag
	, a.global_cap
	, a.fin_market
	, a.fin_product_level_3
	, a.fin_tfm_product_new
	, a.product
	, b.dcsnp_cat
	, a.fin_g_i
    , a.mnr_cosmos_ffs_flag
    , a.mnr_nice_ffs_flag
    , a.mnr_total_ffs_flag
    , a.oah_flag
    , a.cns_dual_flag
    , a.mnr_dual_flag
    , a.isnp_flag
    , count(distinct a.case_id) as case_count
    , 0 as mm
from tmp_1m.kn_dcsnp_auth_2024_2025_cleaned as a 
left join classification as b
	on a.mbi = b.mbi
group by
	a.month
	, a.migration_source
	, a.fin_brand
	, a.fin_source_name
	, a.sgr_source_name
	, a.nce_tadm_dec_risk_type
	, a.tfm_include_flag
	, a.global_cap
	, a.fin_market
	, a.fin_product_level_3
	, a.fin_tfm_product_new
	, a.product
	, b.dcsnp_cat
	, a.fin_g_i
    , a.mnr_cosmos_ffs_flag
    , a.mnr_nice_ffs_flag
    , a.mnr_total_ffs_flag
    , a.oah_flag
    , a.cns_dual_flag
    , a.mnr_dual_flag
    , a.isnp_flag
;


-- 2025 membership
-- Distinct mbi 
-- 202412 product
-- x join
-- max(month) 2025 product 
-- Complete_new: mbi ne 202412
-- Both DSCNP -> existing
-- Not = -> New


drop table tmp_1m.kn_dcsnp_mm_2024_2025_sum;
create table tmp_1m.kn_dcsnp_mm_2024_2025_sum as
with classification as (
select
	mbi
	, case when oah_flag = 1 and product in ('DUAL', 'CHRONIC') and min(month) over (partition by mbi) < '202412' then 'DCSNP - New Member'
			when oah_flag = 0 and product in ('DUAL', 'CHRONIC') and min(month) over (partition by mbi) >= '202501' then 'DCSNP - New Member'
			when oah_flag = 0 and product in ('DUAL', 'CHRONIC') and min(month) over (partition by mbi) = '202412' then 'DCSNP - Existing Member'
			else 'Others'
	end as dcsnp_cat
from tmp_1m.kn_dcsnp_mm_2024_2025_cleaned
) 
select 
	a.month
	, a.migration_source
	, a.fin_brand
	, a.fin_source_name
	, a.sgr_source_name
	, a.nce_tadm_dec_risk_type
	, a.tfm_include_flag
	, a.global_cap
	, a.fin_market
	, a.fin_product_level_3
	, a.fin_tfm_product_new
	, a.product
	, b.dcsnp_cat
	, a.fin_g_i
    , a.mnr_cosmos_ffs_flag
    , a.mnr_nice_ffs_flag
    , a.mnr_total_ffs_flag
    , a.oah_flag
    , a.cns_dual_flag
    , a.mnr_dual_flag
    , a.isnp_flag
    , 0 as case_count
    , count(distinct a.mbi) as mm
from tmp_1m.kn_dcsnp_mm_2024_2025_cleaned as a 
left join classification as b
	on a.mbi = b.mbi
group by
	a.month
	, a.migration_source
	, a.fin_brand
	, a.fin_source_name
	, a.sgr_source_name
	, a.nce_tadm_dec_risk_type
	, a.tfm_include_flag
	, a.global_cap
	, a.fin_market
	, a.fin_product_level_3
	, a.fin_tfm_product_new
	, a.product
	, b.dcsnp_cat
	, a.fin_g_i
    , a.mnr_cosmos_ffs_flag
    , a.mnr_nice_ffs_flag
    , a.mnr_total_ffs_flag
    , a.oah_flag
    , a.cns_dual_flag
    , a.mnr_dual_flag
    , a.isnp_flag
;
select count(*) from tmp_1m.kn_dcsnp_auth_2024_2025_sum



-- Find 2025 and latest product;
drop table tmp_1m.kn_dcsnp_auth_2025;
create table tmp_1m.kn_dcsnp_auth_2025 stored as orc as
with 2025_auth as (
select
	fin_mbi_hicn_fnl as mbi
	, case_id
	, notif_yrmonth as product_month_2025
	, case when fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then fin_tfm_product_new
	       when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'DUAL' then 'DUAL'
	       when fin_tfm_product_new = 'DUAL_CHRONIC' and fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	       else 'OTHER' 
	end as product_2025
from hce_proj_bd.hce_adr_avtar_like_24_25_f
where business_segment not in ('EnI','ERR','null')
	and medicare_id is not null
	and notif_yrmonth >= '202501'
	and fin_plan_level_2 != 'PFFS'
)
, 
2025_product_ranking as (
select 
	mbi
	, case_id
	, product_month_2025
	, product_2025 
	, row_number() over (partition by mbi order by product_month_2025 desc) as product_order
from 2025_auth
)
select 
	mbi 
	, case_id
	, product_month_2025
	, product_2025
from 2025_product_ranking
where product_order = 1
;

-- select count(*) from tmp_1m.kn_dcsnp_auth_2025; -- 2,565,491
-- select count(distinct mbi) from tmp_1m.kn_dcsnp_auth_2025; -- 2,565,491


drop table tmp_1m.kn_dcsnp_auth_202412;
create table tmp_1m.kn_dcsnp_auth_202412 stored as orc as
select
	a.mbi
	, a.case_id
	, a.product_2025
	, b.notif_yrmonth as product_month_202412
	, case when b.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO') then b.fin_tfm_product_new
	       when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'DUAL' then 'DUAL'
	       when b.fin_tfm_product_new = 'DUAL_CHRONIC' and b.fin_product_level_3 = 'CHRONIC' then 'CHRONIC'
	       else 'OTHER' 
	end as product_202412
from tmp_1m.kn_dcsnp_auth_2025 as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
	on a.mbi = b.fin_mbi_hicn_fnl
where b.business_segment not in ('EnI','ERR','null')
	and b.medicare_id is not null
	and b.notif_yrmonth = '202412'
	and b.fin_plan_level_2 != 'PFFS'
;

drop table tmp_1m.kn_dcsnp_auth_202412_2025;
create table tmp_1m.kn_dcsnp_auth_202412_2025 stored as orc as
select distinct
	a.mbi
	, a.case_id
	, a.product_month_2025
	, a.product_2025
	, b.product_month_202412
	, b.product_202412
	, case 
		when a.product_2025 in ('DUAL', 'CHRONIC') and b.product_202412 is not null 
			and a.product_2025 = b.product_202412 then 'DCSNP - Existing Member'
		when a.product_2025 in ('DUAL', 'CHRONIC') and b.product_202412 is not null 
			and a.product_2025 != b.product_202412 then 'DCSNP - New Member'
		when a.product_2025 in ('DUAL', 'CHRONIC') and b.product_202412 is null then 'DCSNP - New Member'
		when a.product_2025 not in ('DUAL', 'CHRONIC') and b.product_202412 is not null 
			and a.product_2025 = b.product_202412 then 'Other Existing Member'
		when a.product_2025 not in ('DUAL', 'CHRONIC') and b.product_202412 is not null 
			and a.product_2025 != b.product_202412 then 'Other New Member'
		when a.product_2025 not in ('DUAL', 'CHRONIC') and b.product_202412 is null then 'Other New Member'
	end as member_cat
from tmp_1m.kn_dcsnp_auth_2025 as a
left join tmp_1m.kn_dcsnp_auth_202412 as b
	on a.mbi = b.mbi
;

select * from tmp_1m.kn_dcsnp_auth_202412_2025

drop table tmp_1m.kn_dcsnp_auth_202412_2025;
create table tmp_1m.kn_dcsnp_auth_202412_2025 stored as orc as
select distinct
	a.mbi
	, a.case_id
	, a.product_month_2025
	, a.product_2025
	, b.product_month_202412
	, b.product_202412
	, case 
		when a.product_2025 in ('DUAL', 'CHRONIC') and b.product_202412 is null then 'New DCSNP'
		when b.product_202412 is null then 'New Others'
		when a.product_2025 in ('DUAL', 'CHRONIC') and b.product_202412 in ('DUAL', 'CHRONIC') then 'Existing DCSNP'
		when a.product_2025 in ('DUAL', 'CHRONIC') then 'Converted to DCSNP'
		else 'Existing Others'
	end as member_cat
from tmp_1m.kn_dcsnp_auth_2025 as a
left join tmp_1m.kn_dcsnp_auth_202412 as b
	on a.mbi = b.mbi
;

select * from tmp_1m.kn_dcsnp_auth_202412_2025;


drop table tmp_1m.kn_dcsnp_auth_count_2024_2025;
create table tmp_1m.kn_dcsnp_auth_count_2024_2025 stored as orc as
select 
	'Auth' as data_type
	, b.notif_yrmonth as month
	, a.product_2025
	, a.product_202412
	, a.member_cat
	, count(distinct b.case_id) as case_count
	, 0 as mm
from tmp_1m.kn_dcsnp_auth_202412_2025 as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f  as b
	on a.mbi = b.fin_mbi_hicn_fnl
where notif_yrmonth >= '202401'
group by 
	b.notif_yrmonth
	, a.product_2025
	, a.product_202412
	, a.member_cat
;

drop table tmp_1m.kn_dcsnp_mm_count_2024_2025;
create table tmp_1m.kn_dcsnp_mm_count_2024_2025 stored as orc as
select 
	'Membership' as data_type
	, b.fin_inc_month as month
	, a.product_2025
	, a.product_202412
	, a.member_cat
	, 0 as case_count
	, sum(b.fin_member_cnt) as mm
from tmp_1m.kn_dcsnp_auth_202412_2025 as a
left join fichsrv.tre_membership as b
	on a.mbi = b.fin_mbi_hicn_fnl
where fin_inc_month >= '202401'
group by 
	b.fin_inc_month
	, a.product_2025
	, a.product_202412
	, a.member_cat
;

create table tmp_1m.kn_dcsnp_auth_mm_2024_2025 as
select 
	data_type
	, month
	, product_2025
	, product_202412
	, member_cat
	, case_count
	, mm
from tmp_1m.kn_dcsnp_auth_count_2024_2025
union all
	data_type
	, month
	, product_2025
	, product_202412
	, member_cat
	, case_count
	, mm
from tmp_1m.kn_dcsnp_mm_count_2024_2025
;


	


select * from tmp_1m.kn_dcsnp_mm_2024_2025;







