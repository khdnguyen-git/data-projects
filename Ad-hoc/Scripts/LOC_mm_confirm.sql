select * from tmp_1m.ec_ip_dataset_loc_10082025 
limit 100


drop table if exists tmp_1m.kn_loc_mm_2024_202509_v2;
create table tmp_1m.kn_loc_mm_2024_202509_v2 as 
select
	fin_market
	, admit_act_month
	, case when institutional_flag = 'Institutional' then 'ISNP'
		when total_oah_flag = 'OAH' then 'OAH'
		when mnr_total_ffs_flag = 1 then 'MnR FFS'
		when cns_dual_flag = 1 then 'CnS DSNP'
		else 'Others'
	end as population
	, sum(membership) as mm
from tmp_1m.ec_ip_dataset_loc_10082025
group by 
	fin_market
	, admit_act_month
	, case when
		 institutional_flag = 'Institutional' then 'ISNP'
		when total_oah_flag = 'OAH' then 'OAH'
		when mnr_total_ffs_flag = 1 then 'MnR FFS'
		when cns_dual_flag = 1 then 'CnS DSNP'
		else 'Others'
	end
;

select
	admit_act_month
	, population
	, mm
from tmp_1m.kn_loc_mm_2024_202509_v2
where admit_act_month = '202506' and fin_market = 'DC'
;


describe formatted tmp_1m.kn_loc_mm_2024_2025_202509

show create table tmp_1m.kn_loc_mm_2024_2025_202509 



drop table if exists tmp_7d.loc_mm_1008;
create table tmp_7d.loc_mm_1008 as
with mm_pull as (
select
	case when fin_brand = 'M&R' then fin_market
			when fin_brand = 'C&S' then fin_state
	end as fin_market
	, fin_inc_month
	-- Population: OAH
	, case when migration_source = 'OAH' 
		then 'OAH' else 'Non-OAH' 
	end as total_OAH_flag
	-- Population: ISNP
	, case when fin_product_level_3 = 'INSTITUTIONAL' 
		then 'Institutional' else 'Non-Institutional' 
	end as institutional_flag
	-- Sub-Population: MnR COSMOS FFS
	, case when fin_brand = 'M&R'
			and global_cap = 'NA'
			and sgr_source_name = 'COSMOS'
			and fin_product_level_3 != 'INSTITUTIONAL'
	        and tfm_include_flag = 1
		then 1 else 0
	end as MnR_COSMOS_FFS_Flag
	-- Sub-Population: MnR NICE FFS
	, case when fin_brand = 'M&R'
			and sgr_source_name = 'NICE'
			and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')
		then 1 else 0
	end as MnR_NICE_FFS_Flag
	-- Population: MnR FFS
	, case when (fin_brand = 'M&R'
	 		and global_cap = 'NA'
	 		and sgr_source_name = 'COSMOS'
	 		and fin_product_level_3 != 'INSTITUTIONAL'
	     	and tfm_include_flag = 1)
		or (fin_brand = 'M&R'
			and sgr_source_name = 'NICE'
			and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
		then 1 else 0
	end as MnR_TOTAL_FFS_FLAG
	-- Sub-Population: MnR OAH
	, case when fin_brand = 'M&R'
			and migration_source = 'OAH'
		then 1 else 0
	end as MnR_OAH_flag
	-- Sub-Population: CnS OAH
	, case when fin_brand = 'C&S'
			and migration_source = 'OAH'
	   	then 1
	   	   when fin_inc_year = '2024'
			and fin_brand = 'C&S'
			and global_cap = 'NA'
			and sgr_source_name in ('COSMOS', 'CSP')
			and migration_source = 'OAH'
			and fin_state = 'MD'
		then 0 else 0
	end as CnS_OAH_flag
	-- Sub-Population: MnR DSNP
	, case when fin_brand = 'M&R'
			and fin_product_level_3 = 'DUAL'
		then 1 else 0
	end as MnR_Dual_flag
	-- Population: CnS DSNP
	, case when (fin_brand = 'C&S'
			and migration_source != 'OAH'
			and global_cap = 'NA'
			and fin_product_level_3 = 'DUAL'
			and sgr_source_name in ('COSMOS', 'CSP')
			and fin_state not in ('OK', 'NC', 'NM', 'NV', 'OH', 'TX'))
		or (fin_inc_year = '2024'
			and fin_brand = 'C&S'
			and global_cap = 'NA'
			and sgr_source_name in ('COSMOS', 'CSP')
			and migration_source = 'OAH'
			and fin_state = 'MD')
		then 1 else 0
	end as CnS_Dual_flag
	, fin_member_cnt
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202509 -- fichsrv.tre_membership; snapshot from Sep 2025
where fin_inc_year >= '2024'
)
select 
	fin_market
	, fin_inc_month
	, case when institutional_flag = 'Institutional' then 'ISNP'
		   when total_OAH_flag = 'OAH' then 'OAH'
		   when MnR_TOTAL_FFS_FLAG = 1 then 'MnR FFS'
		   when CnS_Dual_flag = 1 then 'CnS DSNP'
		else 'Others'
	end as population
	, sum(fin_member_cnt) as mm
from mm_pull
group by
	fin_market
	, fin_inc_month
	, case when institutional_flag = 'Institutional' then 'ISNP'
		   when total_oah_flag = 'OAH' then 'OAH'
		   when mnr_total_ffs_flag = 1 then 'MnR FFS'
		   when cns_dual_flag = 1 then 'CnS DSNP'
		else 'Others'
	end
;
	
select
	fin_inc_month
	, population
	, mm
from tmp_7d.loc_mm_1008
where fin_inc_month = '202506' and fin_market = 'DC'
;

with mm as (
select
	fin_inc_month
	, fin_market
	, institutional_flag
	, total_oah_flag
	, mnr_total_ffs_flag
	, cns_dual_flag
	, membership
from tmp_1m.ec_ip_dataset_10082025_mm
)
select 
	fin_inc_month
	, fin_market
	, case when institutional_flag = 'Institutional' then 'ISNP'
		   when total_oah_flag = 'OAH' then 'OAH'
		   when mnr_total_ffs_flag = 1 then 'MnR FFS'
		   when cns_dual_flag = 1 then 'CnS DSNP'
		else 'Others'
	end as population
	, sum(membership) as mm
from mm
where fin_inc_month = '202506' and fin_market = 'DC'
group by
	fin_inc_month
	, fin_market
	, case when institutional_flag = 'Institutional' then 'ISNP'
		   when total_oah_flag = 'OAH' then 'OAH'
		   when mnr_total_ffs_flag = 1 then 'MnR FFS'
		   when cns_dual_flag = 1 then 'CnS DSNP'
		else 'Others'
	end
;


