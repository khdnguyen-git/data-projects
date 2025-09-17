use ving_prd_trend_db.tmp_1m;

select * from tmp_1m.KN_MBI_IMRT;

drop table if exists tmp_kn_imrt_check;

create table tmp_1m.kn_imrt_check as
select distinct
	b.*
	, case when fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
	, case when fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1) 
		or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
	, case when ((fin_brand in ('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and 
		sgr_source_name in ('COSMOS','CSP') and fin_state not in ('OK','NC','NM','NV','OH','TX')) or (fin_inc_year = '2024' and fin_brand in ('C&S')
	and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as isnp_flag
from tmp_1m.kn_mbi_imrt as a
left join fichsrv.tre_membership_pr as b 
	on a.mbi = b.fin_mbi_hicn_fnl
where 
	fin_inc_month >= '202401'
;

describe table fichsrv.tre_membership_pr;





drop table tmp_1m.kn_pcgm_mbi_full;
create table tmp_1m.kn_pcgm_mbi_full as
select
	fin_mbi_hicn_fnl 
	, fin_inc_month
	, fin_inc_year
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
	, case when fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1 then 1 else 0 end as MnR_COSMOS_FFS_Flag
	, case when fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN') then 1 else 0 end as MnR_NICE_FFS_Flag
	, case when (fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and fin_product_level_3 <> 'INSTITUTIONAL' and tfm_include_flag = 1) 
		or (fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS','PHYSICIAN')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
	, case when ((fin_brand in ('C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and 
		sgr_source_name in ('COSMOS','CSP') and fin_state not in ('OK','NC','NM','NV','OH','TX')) or (fin_inc_year = '2024' and fin_brand in ('C&S')
	and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 end as CnS_Dual_flag 
	, case when migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_OAH_flag
	, case when fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 else 0 end as isnp_flag
	, fin_member_cnt
from fichsrv.tre_membership  
	a.mbi = b.fin_mbi_hicn_fnl
where 
	fin_inc_month >= '202501'
;