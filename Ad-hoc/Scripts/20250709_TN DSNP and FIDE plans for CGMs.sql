-- 2025
-- H0251-002    DSNP (Coordination Only Plan)
-- H0251-004   DSNP – FIDE
 
-- 2026
-- H0251-008 (FIDE DSNP)
-- H0251-002    DSNP
-- H0251-004   DSNP – FIDE

-- Find mm and savings


-- Context
-- Membership and Annualized savings for plans
-- H0251-002-000
-- H0251-004-000
-- H0251-008 (2026)


-- Location
-- O:\National\Clinical\Prior Authorization Evaluation\2025 Prior Auth Programs\Data Requests\TN DSNP and FIDE plans for CGMs - HPBPs.xlsx



select
	fin_contractpbp
	, fin_inc_month
	 sum(fin_member_cnt) as mm
from fichsrv.tre_membership
where 
	fin_contractpbp in ('H0251-002-000', 'H0251-004-000', 'H0251-008-000')
	and fin_inc_month >= '202401'
group by
	fin_contractpbp
	, fin_inc_month
order by 
	fin_contractpbp
	, fin_inc_month
;


create table tmp_1m.kn_cgm_contract_dsnp_membership as
with mnr_membership as (
select
	'COSMOS' as entity_source
	, fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'M&R OAH'
	       when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	       else 'M&R FFS' 
	end as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source= 'OAH' then 1 else 0 
	end as OAH_flag
	, 0 as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and global_cap = 'NA'
	and fin_inc_month >= '202401'
	and fin_contractpbp in ('H0251-002-000', 'H0251-004-000', 'H0251-008-000')
group by
	fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'M&R OAH'
	       when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	       else 'M&R FFS' 
	end 
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source = 'OAH' then 1 
	       else 0 
	end
),
cns_membership as (
select
	'COSMOS' as entity_source
	, fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source= 'OAH' then 1 else 0 
	end as OAH_flag
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 
	end as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	sgr_source_name = 'COSMOS'
	and fin_brand = 'C&S'
	and global_cap = 'NA'
	and fin_inc_month >= '202401'
	and fin_contractpbp in ('H0251-002-000', 'H0251-004-000', 'H0251-008-000')
group by
	fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source= 'OAH' then 1 
	       else 0 
	end
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 
		else 0 
	end
),
smart_membership as (
select
	'CSP' as entity_source
	, fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap 
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source= 'OAH' then 1 
	       else 0 
	end as OAH_flag
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 
			else 0 
	end as CnS_Dual_flag
	, sum(fin_member_cnt) as mm
from fichsrv.tre_membership 
where		
	fin_brand = 'C&S'	
	and sgr_source_name = 'CSP'
	and global_cap = 'NA'
	and fin_inc_month >= '202401'
	and fin_contractpbp in ('H0251-002-000', 'H0251-004-000', 'H0251-008-000')
group by
	fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source= 'OAH' then 1 else 0 
	end
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 
	end
	
),
nice_membership as (
select
	'NICE' as entity_source
	, fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap 
	, nce_tadm_dec_risk_type
	, 'M&R FFS' as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source= 'OAH' then 1 
	       else 0 
	end as OAH_flag
	, 0 as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	fin_brand = 'M&R'	
	and sgr_source_name = 'NICE'
	and nce_tadm_dec_risk_type = 'FFS'
	and fin_inc_month >= '202401'
	and fin_contractpbp in ('H0251-002-000', 'H0251-004-000', 'H0251-008-000')
group by 
	fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap 
	, nce_tadm_dec_risk_type
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source= 'OAH' then 1 else 0 
	end
)
select * from mnr_membership
union all
select * from cns_membership
union all
select * from smart_membership
union all
select * from nice_membership
;

