{previous_month} = 


create table tmp_1m.kn_nemt_membership_{previous_month} as
with MnR_membership as (
select
	'COSMOS' as entity_source
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
	       when  migration_source = 'OAH' then 1 else 0 
	end as OAH_flag
	, 0 as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
group by
	fin_inc_month
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
CnS_membership as (
select
	'COSMOS' as entity_source
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
	       when migration_source = 'OAH' then 1 else 0 
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
	and fin_inc_month >= '202301'
group by
	fin_inc_month
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
	       when migration_source = 'OAH' then 1 
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
	       when migration_source = 'OAH' then 1 
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
	and fin_inc_month >= '202301'
group by
	fin_inc_month
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
	       when migration_source = 'OAH' then 1 else 0 
	end
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 
	end
),
nice_membership as (
select
	'NICE' as entity_source
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
	       when  migration_source = 'OAH' then 1 
	       else 0 
	end as OAH_flag
	, 0 as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	fin_brand = 'M&R'	
	and sgr_source_name = 'NICE'
	and nce_tadm_dec_risk_type = 'FFS'
	and fin_inc_month >= '202301'
group by 
	fin_inc_month
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
	       when  migration_source = 'OAH' then 1 else 0 
	end
)
select * from MnR_membership
union all
select * from CnS_membership
union all
select * from smart_membership
union all
select * from nice_membership
;









create table tmp_1m.kn_oop_cost_pbp as 
select 
	* 
from fichsrv.tre_membership
where fin_contractpbp in 
( 'H0169-001-000'
, 'H0169-002-000'
, 'H0169-003-000'
, 'H0169-004-000'
, 'H0169-006-000'
, 'H0169-008-000'
, 'H0169-009-000'
, 'H0169-010-000'
, 'H0251-002-000'
, 'H0251-004-000'
, 'H0251-008-000'
, 'H0294-002-000'
, 'H0294-004-000'
, 'H0294-012-000'
, 'H0294-014-000'
, 'H0294-015-000'
, 'H0294-016-000'
, 'H0294-017-000'
, 'H0294-018-000'
, 'H0294-022-000'
, 'H0294-023-000'
, 'H0294-026-000'
, 'H0294-027-000'
, 'H0294-032-000'
, 'H0294-037-000'
, 'H0294-038-000'
, 'H0294-039-000'
, 'H0294-043-000'
, 'H0294-044-000'
, 'H0294-046-000'
, 'H0294-047-000'
, 'H0294-048-000'
, 'H0294-049-000'
, 'H0294-050-000'
, 'H0294-051-000'
, 'H0321-002-000'
, 'H0321-004-000'
, 'H0421-001-000'
, 'H0432-003-000'
, 'H0432-004-000'
, 'H0432-009-000'
, 'H0432-012-000'
, 'H0432-013-000'
, 'H0432-017-000'
, 'H0543-013-000'
, 'H0543-019-000'
, 'H0543-035-000'
, 'H0543-036-000'
, 'H0543-060-000'
, 'H0543-086-000'
, 'H0543-121-000'
, 'H0543-140-000'
, 'H0543-145-000'
, 'H0543-146-000'
, 'H0543-147-000'
, 'H0543-151-000'
, 'H0543-152-000'
, 'H0543-166-000'
, 'H0543-167-000'
, 'H0543-168-000'
, 'H0543-169-000'
, 'H0543-170-000'
, 'H0543-176-000'
, 'H0543-188-000'
, 'H0543-189-000'
, 'H0543-191-000'
, 'H0543-193-000'
, 'H0543-196-000'
, 'H0543-204-000'
, 'H0543-214-000'
, 'H0543-217-000'
, 'H0543-218-000'
, 'H0543-219-000'
, 'H0543-220-000'
, 'H0543-221-000'
, 'H0543-222-000'
, 'H0543-223-000'
, 'H0543-224-000'
, 'H0543-225-000'
, 'H0543-226-000'
, 'H0543-228-000'
, 'H0543-232-000'
, 'H0543-234-000'
, 'H0543-236-000'
, 'H0543-237-000'
, 'H0543-238-000'
, 'H0543-239-000'
, 'H0543-240-000'
, 'H0543-241-000'
, 'H0543-242-000'
, 'H0543-246-000'
, 'H0543-247-000'
, 'H0543-248-000'
, 'H0543-249-000'
, 'H0543-251-000'
, 'H0543-254-000'
, 'H0543-255-000'
, 'H0543-258-000'
, 'H0609-007-000'
, 'H0609-012-000'
, 'H0609-018-000'
, 'H0609-025-000'
, 'H0609-026-000'
, 'H0609-027-000'
, 'H0609-028-000'
, 'H0609-031-000'
, 'H0609-032-000'
, 'H0609-033-000'
, 'H0609-034-001'
, 'H0609-034-002'
, 'H0609-036-001'
, 'H0609-036-002'
, 'H0609-037-000'
, 'H0609-038-000'
, 'H0609-040-000'
, 'H0609-041-000'
, 'H0609-042-000'
, 'H0609-043-000'
, 'H0609-044-000'
, 'H0609-045-000'
, 'H0609-046-000'
, 'H0609-047-000'
, 'H0609-048-000'
, 'H0609-049-000'
, 'H0609-050-000'
, 'H0609-051-000'
, 'H0609-052-000'
, 'H0609-054-000'
, 'H0609-055-000'
, 'H0609-056-000'
, 'H0609-058-000'
, 'H0609-059-000'
, 'H0609-060-000'
, 'H0609-061-000'
, 'H0609-062-000'
, 'H0609-063-000'
, 'H0609-065-000'
, 'H0609-066-000'
, 'H0609-067-000'
, 'H0609-068-000'
, 'H0609-070-000'
, 'H0609-071-000'
, 'H0609-072-000'
, 'H0609-073-000'
, 'H0609-075-000'
, 'H0609-076-000'
, 'H0609-077-000'
, 'H0609-078-000'
, 'H0609-079-000'
, 'H0609-080-000'
, 'H0624-001-000'
, 'H0624-006-000'
, 'H0710-004-000'
, 'H0710-005-000'
, 'H0710-007-000'
, 'H0710-010-000'
, 'H0710-013-000'
, 'H0710-016-000'
, 'H0710-017-000'
, 'H0710-020-000'
, 'H0710-026-000'
, 'H0710-027-000'
, 'H0710-030-000'
, 'H0710-031-000'
, 'H0710-032-000'
, 'H0710-033-000'
, 'H0710-034-000'
, 'H0710-035-000'
, 'H0710-036-000'
, 'H0710-037-000'
, 'H0710-038-000'
, 'H0710-039-000'
, 'H0710-041-000'
, 'H0710-051-000'
, 'H0710-052-000'
, 'H0710-053-000'
, 'H0710-057-000'
, 'H0755-030-000'
, 'H0755-031-000'
, 'H0755-032-000'
, 'H0755-033-000'
, 'H0755-037-000'
, 'H0755-038-000'
, 'H0755-044-000'
, 'H0755-045-000'
, 'H0755-046-000'
, 'H0764-001-000'
, 'H1045-001-000'
, 'H1045-005-000'
, 'H1045-012-000'
, 'H1045-018-000'
, 'H1045-025-000'
, 'H1045-026-000'
, 'H1045-028-000'
, 'H1045-030-000'
, 'H1045-031-000'
, 'H1045-033-000'
, 'H1045-034-000'
, 'H1045-036-000'
, 'H1045-037-000'
, 'H1045-038-000'
, 'H1045-039-000'
, 'H1045-041-000'
, 'H1045-042-000'
, 'H1045-043-000'
, 'H1045-045-000'
, 'H1045-048-001'
, 'H1045-048-002'
, 'H1045-048-003'
, 'H1045-048-004'
, 'H1045-055-000'
, 'H1045-056-000'
, 'H1045-057-000'
, 'H1045-058-000'
, 'H1045-059-000'
, 'H1045-060-000'
, 'H1045-061-000'
, 'H1045-063-000'
, 'H1045-064-000'
, 'H1045-065-000'
, 'H1045-067-000'
, 'H1045-069-000'
, 'H1278-003-000'
, 'H1278-004-000'
, 'H1278-005-000'
, 'H1278-007-000'
, 'H1278-009-000'
, 'H1278-010-000'
, 'H1278-013-000'
, 'H1278-014-000'
, 'H1278-015-000'
, 'H1278-016-000'
, 'H1278-018-000'
, 'H1278-019-000'
, 'H1278-020-000'
, 'H1278-021-000'
, 'H1278-022-000'
, 'H1278-023-000'
, 'H1278-024-000'
, 'H1278-025-000'
, 'H1278-026-000'
, 'H1278-027-000'
, 'H1278-028-000'
, 'H1278-029-000'
, 'H1278-030-000'
, 'H1278-031-000'
, 'H1278-032-000'
, 'H1285-001-000'
, 'H1285-002-000'
, 'H1360-001-000'
, 'H1360-002-000'
, 'H1360-003-000'
, 'H1659-002-000'
, 'H1889-002-001'
, 'H1889-002-002'
, 'H1889-005-000'
, 'H1889-007-000'
, 'H1889-008-000'
, 'H1889-009-000'
, 'H1889-010-000'
, 'H1889-011-000'
, 'H1889-012-000'
, 'H1889-013-000'
, 'H1889-014-000'
, 'H1889-015-000'
, 'H1889-016-000'
, 'H1889-017-000'
, 'H1889-018-000'
, 'H1889-019-000'
, 'H1889-020-000'
, 'H1889-022-000'
, 'H1889-023-000'
, 'H1889-025-000'
, 'H1889-026-000'
, 'H1889-027-000'
, 'H1889-028-000'
, 'H1889-030-000'
, 'H1889-031-000'
, 'H1889-032-000'
, 'H1889-034-000'
, 'H1889-035-000'
, 'H1961-003-000'
, 'H1961-014-001'
, 'H1961-014-002'
, 'H1961-014-003'
, 'H1961-014-004'
, 'H1961-017-000'
, 'H1961-019-000'
, 'H1961-020-000'
, 'H1961-022-000'
, 'H1961-023-000'
, 'H1961-024-000'
, 'H1961-025-000'
, 'H1961-026-000'
, 'H2001-010-000'
, 'H2001-017-000'
, 'H2001-019-000'
, 'H2001-021-000'
, 'H2001-023-000'
, 'H2001-027-000'
, 'H2001-028-000'
, 'H2001-029-000'
, 'H2001-030-000'
, 'H2001-031-000'
, 'H2001-032-000'
, 'H2001-034-000'
, 'H2001-035-000'
, 'H2001-036-000'
, 'H2001-037-000'
, 'H2001-038-000'
, 'H2001-039-000'
, 'H2001-040-000'
, 'H2001-041-000'
, 'H2001-042-000'
, 'H2001-043-000'
, 'H2001-044-000'
, 'H2001-045-000'
, 'H2001-046-000'
, 'H2001-047-000'
, 'H2001-048-000'
, 'H2001-049-000'
, 'H2001-050-000'
, 'H2001-051-000'
, 'H2001-052-000'
, 'H2001-053-000'
, 'H2001-054-000'
, 'H2001-055-000'
, 'H2001-056-000'
, 'H2001-057-000'
, 'H2001-058-000'
, 'H2001-059-000'
, 'H2001-060-000'
, 'H2001-061-000'
, 'H2001-062-000'
, 'H2001-063-001'
, 'H2001-063-002'
, 'H2001-064-000'
, 'H2001-065-000'
, 'H2001-066-000'
, 'H2001-067-000'
, 'H2001-068-001'
, 'H2001-068-002'
, 'H2001-069-001'
, 'H2001-069-002'
, 'H2001-070-000'
, 'H2001-075-000'
, 'H2001-076-000'
, 'H2001-077-000'
, 'H2001-078-000'
, 'H2001-079-000'
, 'H2001-080-000'
, 'H2001-081-000'
, 'H2001-082-000'
, 'H2001-083-001'
, 'H2001-083-002'
, 'H2001-084-000'
, 'H2001-085-000'
, 'H2001-086-000'
, 'H2001-087-000'
, 'H2001-088-001'
, 'H2001-088-002'
, 'H2001-089-000'
, 'H2001-090-000'
, 'H2001-091-000'
, 'H2001-092-000'
, 'H2001-093-000'
, 'H2001-094-000'
, 'H2001-095-000'
, 'H2001-096-000'
, 'H2001-097-000'
, 'H2001-098-000'
, 'H2001-099-000'
, 'H2001-100-000'
, 'H2001-101-000'
, 'H2001-102-000'
, 'H2001-103-000'
, 'H2001-104-000'
, 'H2001-105-000'
, 'H2001-108-000'
, 'H2001-109-000'
, 'H2001-110-000'
, 'H2001-111-000'
, 'H2001-113-000'
, 'H2001-115-000'
, 'H2001-116-000'
, 'H2001-117-000'
, 'H2001-118-000'
, 'H2001-119-000'
, 'H2001-120-000'
, 'H2001-121-000'
, 'H2001-122-000'
, 'H2001-123-000'
, 'H2001-124-000'
, 'H2001-125-000'
, 'H2001-126-000'
, 'H2001-127-000'
, 'H2001-128-000'
, 'H2001-131-000'
, 'H2001-132-000'
, 'H2001-133-000'
, 'H2001-134-000'
, 'H2001-135-000'
, 'H2001-136-000'
, 'H2001-137-000'
, 'H2001-138-000'
, 'H2001-139-000'
, 'H2226-001-000'
, 'H2226-003-000'
, 'H2247-001-000'
, 'H2247-003-000'
, 'H2247-004-000'
, 'H2247-004-000'
, 'H2247-005-000'
, 'H2272-001-000'
, 'H2272-003-000'
, 'H2292-001-000'
, 'H2292-002-000'
, 'H2385-001-000'
, 'H2385-002-000'
, 'H2385-003-000'
, 'H2385-004-000'
, 'H2406-008-000'
, 'H2406-009-000'
, 'H2406-010-000'
, 'H2406-011-000'
, 'H2406-013-000'
, 'H2406-014-000'
, 'H2406-015-000'
, 'H2406-016-000'
, 'H2406-017-000'
, 'H2406-018-000'
, 'H2406-019-000'
, 'H2406-031-000'
, 'H2406-033-000'
, 'H2406-034-000'
, 'H2406-035-000'
, 'H2406-036-000'
, 'H2406-037-000'
, 'H2406-038-000'
, 'H2406-039-000'
, 'H2406-040-000'
, 'H2406-041-000'
, 'H2406-042-000'
, 'H2406-043-000'
, 'H2406-044-000'
, 'H2406-045-000'
, 'H2406-046-000'
, 'H2406-047-000'
, 'H2406-048-000'
, 'H2406-050-000'
, 'H2406-051-000'
, 'H2406-052-000'
, 'H2406-053-000'
, 'H2406-054-000'
, 'H2406-055-000'
, 'H2406-056-000'
, 'H2406-058-000'
, 'H2406-059-000'
, 'H2406-060-000'
, 'H2406-061-000'
, 'H2406-062-000'
, 'H2406-063-000'
, 'H2406-064-000'
, 'H2406-065-000'
, 'H2406-066-000'
, 'H2406-067-000'
, 'H2406-068-000'
, 'H2406-069-000'
, 'H2406-070-000'
, 'H2406-071-000'
, 'H2406-072-000'
, 'H2406-073-000'
, 'H2406-074-000'
, 'H2406-075-000'
, 'H2406-076-000'
, 'H2406-077-000'
, 'H2406-078-000'
, 'H2406-080-000'
, 'H2406-081-000'
, 'H2406-082-000'
, 'H2406-083-000'
, 'H2406-084-000'
, 'H2406-085-000'
, 'H2406-086-000'
, 'H2406-087-000'
, 'H2406-088-000'
, 'H2406-089-000'
, 'H2406-090-000'
, 'H2406-091-000'
, 'H2406-092-000'
, 'H2406-093-000'
, 'H2406-094-000'
, 'H2406-095-000'
, 'H2406-096-000'
, 'H2406-097-000'
, 'H2406-098-000'
, 'H2406-099-000'
, 'H2406-100-000'
, 'H2406-101-000'
, 'H2406-102-000'
, 'H2406-103-000'
, 'H2406-104-000'
, 'H2406-105-000'
, 'H2406-106-000'
, 'H2406-107-000'
, 'H2406-108-000'
, 'H2406-110-000'
, 'H2406-111-000'
, 'H2406-112-000'
, 'H2406-113-000'
, 'H2406-115-000'
, 'H2406-119-000'
, 'H2406-121-000'
, 'H2406-122-000'
, 'H2406-125-000'
, 'H2406-129-000'
, 'H2406-130-000'
, 'H2406-131-000'
, 'H2406-132-000'
, 'H2406-134-000'
, 'H2406-135-000'
, 'H2445-001-000'
, 'H2445-002-000'
, 'H2445-003-000'
, 'H2445-004-000'
, 'H2445-005-000'
, 'H2509-001-000'
, 'H2509-002-000'
, 'H2509-003-000'
, 'H2582-002-000'
, 'H2582-004-000'
, 'H2582-005-000'
, 'H2802-001-000'
, 'H2802-007-000'
, 'H2802-008-000'
, 'H2802-010-000'
, 'H2802-012-000'
, 'H2802-018-000'
, 'H2802-024-000'
, 'H2802-025-000'
, 'H2802-027-000'
, 'H2802-028-000'
, 'H2802-029-000'
, 'H2802-030-000'
, 'H2802-031-000'
, 'H2802-032-000'
, 'H2802-033-000'
, 'H2802-034-000'
, 'H2802-035-000'
, 'H2802-041-000'
, 'H2802-044-000'
, 'H2802-048-000'
, 'H2802-049-000'
, 'H2802-050-000'
, 'H2802-052-000'
, 'H2802-053-000'
, 'H2802-054-000'
, 'H2802-055-000'
, 'H2802-056-000'
, 'H2802-057-000'
, 'H2802-058-000'
, 'H2802-059-000'
, 'H2802-060-000'
, 'H2802-061-000'
, 'H2802-062-000'
, 'H2802-063-000'
, 'H2802-064-000'
, 'H2802-067-000'
, 'H2802-068-000'
, 'H2802-070-000'
, 'H2802-071-000'
, 'H2802-072-000'
, 'H2802-073-000'
, 'H2802-074-000'
, 'H2802-075-000'
, 'H2802-076-000'
, 'H2802-077-000'
, 'H2802-078-000'
, 'H2802-079-000'
, 'H3113-005-000'
, 'H3113-008-000'
, 'H3113-009-000'
, 'H3113-010-000'
, 'H3113-011-000'
, 'H3113-013-000'
, 'H3113-014-000'
, 'H3113-016-000'
, 'H3256-001-000'
, 'H3256-001-000'
, 'H3256-002-000'
, 'H3256-002-000'
, 'H3256-003-000'
, 'H3256-003-000'
, 'H3256-004-001'
, 'H3256-004-002'
, 'H3256-005-001'
, 'H3256-005-002'
, 'H3256-006-001'
, 'H3256-006-002'
, 'H3379-001-000'
, 'H3379-002-000'
, 'H3379-022-000'
, 'H3379-039-000'
, 'H3379-040-000'
, 'H3379-041-000'
, 'H3379-043-000'
, 'H3379-045-000'
, 'H3379-050-000'
, 'H3379-051-000'
, 'H3379-052-000'
, 'H3379-053-000'
, 'H3379-054-000'
, 'H3379-056-000'
, 'H3379-059-000'
, 'H3379-060-000'
, 'H3387-013-000'
, 'H3387-014-001'
, 'H3387-014-002'
, 'H3387-015-001'
, 'H3387-015-002'
, 'H3387-017-000'
, 'H3418-001-000'
, 'H3418-002-000'
, 'H3418-004-000'
, 'H3418-007-000'
, 'H3418-008-000'
, 'H3418-009-000'
, 'H3418-010-000'
, 'H3794-002-000'
, 'H3794-004-000'
, 'H3794-006-000'
, 'H3794-007-000'
, 'H3794-008-000'
, 'H3805-001-000'
, 'H3805-015-000'
, 'H3805-017-000'
, 'H3805-032-000'
, 'H3805-033-000'
, 'H3805-034-000'
, 'H3805-035-000'
, 'H3805-037-000'
, 'H3805-039-001'
, 'H3805-039-002'
, 'H3805-040-000'
, 'H3805-041-000'
, 'H3805-043-000'
, 'H3805-044-000'
, 'H3805-045-000'
, 'H3868-001-000'
, 'H4032-001-000'
, 'H4032-002-000'
, 'H4514-007-000'
, 'H4514-014-000'
, 'H4514-015-000'
, 'H4514-016-000'
, 'H4514-017-000'
, 'H4514-018-000'
, 'H4514-019-000'
, 'H4514-021-000'
, 'H4514-022-000'
, 'H4514-023-000'
, 'H4514-024-000'
, 'H4527-001-000'
, 'H4527-002-000'
, 'H4527-003-000'
, 'H4527-005-000'
, 'H4527-013-000'
, 'H4527-015-000'
, 'H4527-024-000'
, 'H4527-037-000'
, 'H4527-039-000'
, 'H4527-040-000'
, 'H4527-041-000'
, 'H4527-042-000'
, 'H4527-045-000'
, 'H4527-048-000'
, 'H4527-051-000'
, 'H4527-052-000'
, 'H4527-053-000'
, 'H4527-054-000'
, 'H4527-055-000'
, 'H4527-056-000'
, 'H4527-057-000'
, 'H4527-058-000'
, 'H4527-059-000'
, 'H4544-001-000'
, 'H4544-002-000'
, 'H4604-003-000'
, 'H4604-005-000'
, 'H4604-011-000'
, 'H4604-012-000'
, 'H4604-013-000'
, 'H4604-014-000'
, 'H4604-015-000'
, 'H4604-016-000'
, 'H4604-017-000'
, 'H4604-018-000'
, 'H4604-019-000'
, 'H4604-020-000'
, 'H4604-022-000'
, 'H4604-024-000'
, 'H4604-025-000'
, 'H4604-026-000'
, 'H4604-027-000'
, 'H4604-028-000'
, 'H4610-001-000'
, 'H4610-002-000'
, 'H5008-002-000'
, 'H5008-010-000'
, 'H5008-011-000'
, 'H5008-015-000'
, 'H5008-016-000'
, 'H5008-017-000'
, 'H5008-018-000'
, 'H5008-019-000'
, 'H5008-020-000'
, 'H5253-004-000'
, 'H5253-007-000'
, 'H5253-011-000'
, 'H5253-021-000'
, 'H5253-024-000'
, 'H5253-030-000'
, 'H5253-033-000'
, 'H5253-034-000'
, 'H5253-035-000'
, 'H5253-036-000'
, 'H5253-037-000'
, 'H5253-038-000'
, 'H5253-039-000'
, 'H5253-040-000'
, 'H5253-041-000'
, 'H5253-042-000'
, 'H5253-047-000'
, 'H5253-048-000'
, 'H5253-051-000'
, 'H5253-059-000'
, 'H5253-060-000'
, 'H5253-062-000'
, 'H5253-064-000'
, 'H5253-072-000'
, 'H5253-073-000'
, 'H5253-079-000'
, 'H5253-080-000'
, 'H5253-081-000'
, 'H5253-082-000'
, 'H5253-083-000'
, 'H5253-084-000'
, 'H5253-087-000'
, 'H5253-088-000'
, 'H5253-089-000'
, 'H5253-097-000'
, 'H5253-099-000'
, 'H5253-100-000'
, 'H5253-102-000'
, 'H5253-103-000'
, 'H5253-104-000'
, 'H5253-105-000'
, 'H5253-107-001'
, 'H5253-107-002'
, 'H5253-107-003'
, 'H5253-108-001'
, 'H5253-108-002'
, 'H5253-108-003'
, 'H5253-108-004'
, 'H5253-109-001'
, 'H5253-109-002'
, 'H5253-109-004'
, 'H5253-110-000'
, 'H5253-111-001'
, 'H5253-111-002'
, 'H5253-112-001'
, 'H5253-112-002'
, 'H5253-113-000'
, 'H5253-116-000'
, 'H5253-117-000'
, 'H5253-119-000'
, 'H5253-120-000'
, 'H5253-121-000'
, 'H5253-122-000'
, 'H5253-124-001'
, 'H5253-124-002'
, 'H5253-125-001'
, 'H5253-125-002'
, 'H5253-126-001'
, 'H5253-126-002'
, 'H5253-127-000'
, 'H5253-128-000'
, 'H5253-130-000'
, 'H5253-131-000'
, 'H5253-132-000'
, 'H5253-133-000'
, 'H5253-134-000'
, 'H5253-135-000'
, 'H5253-141-000'
, 'H5253-142-000'
, 'H5253-143-000'
, 'H5253-144-001'
, 'H5253-144-002'
, 'H5253-145-000'
, 'H5253-146-000'
, 'H5253-147-000'
, 'H5253-148-000'
, 'H5253-149-000'
, 'H5253-150-000'
, 'H5253-152-000'
, 'H5253-154-000'
, 'H5253-155-000'
, 'H5253-157-001'
, 'H5253-157-002'
, 'H5253-159-000'
, 'H5253-161-000'
, 'H5253-162-001'
, 'H5253-162-002'
, 'H5253-164-001'
, 'H5253-164-002'
, 'H5253-165-001'
, 'H5253-165-002'
, 'H5253-166-000'
, 'H5253-168-000'
, 'H5253-169-000'
, 'H5253-170-000'
, 'H5253-171-000'
, 'H5253-172-000'
, 'H5253-173-000'
, 'H5253-174-000'
, 'H5253-175-000'
, 'H5253-176-000'
, 'H5253-178-000'
, 'H5253-179-000'
, 'H5253-180-000'
, 'H5253-182-000'
, 'H5253-183-000'
, 'H5253-184-000'
, 'H5253-185-000'
, 'H5253-186-000'
, 'H5253-187-000'
, 'H5253-188-000'
, 'H5253-189-000'
, 'H5253-190-000'
, 'H5253-192-000'
, 'H5253-193-001'
, 'H5253-193-002'
, 'H5253-194-001'
, 'H5253-194-002'
, 'H5253-195-000'
, 'H5253-196-000'
, 'H5253-197-000'
, 'H5253-198-000'
, 'H5253-199-000'
, 'H5253-200-000'
, 'H5253-201-000'
, 'H5253-202-000'
, 'H5253-203-000'
, 'H5253-204-000'
, 'H5253-205-000'
, 'H5253-206-000'
, 'H5253-207-000'
, 'H5253-208-000'
, 'H5253-209-000'
, 'H5322-003-000'
, 'H5322-025-000'
, 'H5322-026-000'
, 'H5322-028-000'
, 'H5322-029-000'
, 'H5322-030-000'
, 'H5322-030-000'
, 'H5322-031-000'
, 'H5322-033-000'
, 'H5322-034-000'
, 'H5322-038-000'
, 'H5322-040-000'
, 'H5322-041-000'
, 'H5322-041-000'
, 'H5322-042-000'
, 'H5322-043-000'
, 'H5322-044-000'
, 'H5322-045-000'
, 'H5322-045-000'
, 'H5322-046-000'
, 'H5322-047-001'
, 'H5322-047-002'
, 'H5322-049-001'
, 'H5322-049-002'
, 'H5322-050-001'
, 'H5322-050-002'
, 'H5420-001-000'
, 'H5420-003-000'
, 'H5420-006-000'
, 'H5420-014-000'
, 'H5420-015-000'
, 'H5420-016-000'
, 'H5435-001-000'
, 'H5435-024-000'
, 'H5652-001-000'
, 'H5652-002-000'
, 'H5652-003-000'
, 'H5652-004-000'
, 'H5652-006-000'
, 'H5652-008-000'
, 'H6595-003-000'
, 'H6595-004-000'
, 'H6595-005-000'
, 'H6706-001-000'
, 'H6824-001-000'
, 'H6824-002-000'
, 'H7464-008-001'
, 'H7464-008-002'
, 'H7464-010-000'
, 'H7464-011-000'
, 'H7464-012-000'
, 'H7833-001-000'
, 'H8211-001-000'
, 'H8211-005-000'
, 'H8211-006-000'
, 'H8211-007-000'
, 'H8211-009-000'
, 'H8211-010-000'
, 'H8211-011-000'
, 'H8211-012-000'
, 'H8768-003-000'
, 'H8768-005-000'
, 'H8768-007-000'
, 'H8768-008-000'
, 'H8768-009-000'
, 'H8768-010-000'
, 'H8768-011-000'
, 'H8768-013-000'
, 'H8768-014-000'
, 'H8768-016-000'
, 'H8768-017-001'
, 'H8768-017-002'
, 'H8768-017-003'
, 'H8768-018-000'
, 'H8768-019-000'
, 'H8768-020-000'
, 'H8768-021-000'
, 'H8768-022-000'
, 'H8768-023-000'
, 'H8768-024-000'
, 'H8768-025-000'
, 'H8768-026-000'
, 'H8768-027-000'
, 'H8768-028-000'
, 'H8768-030-000'
, 'H8768-031-000'
, 'H8768-034-000'
, 'H8768-035-000'
, 'H8768-037-001'
, 'H8768-037-002'
, 'H8768-038-001'
, 'H8768-038-002'
, 'H8768-039-000'
, 'H8768-040-000'
, 'H8768-042-000'
, 'H8768-045-000'
, 'H8768-046-000'
, 'H8768-048-000'
, 'H8768-050-000'
, 'H8768-052-001'
, 'H8768-052-002'
, 'H8768-055-001'
, 'H8768-055-002'
, 'H8768-056-001'
, 'H8768-056-002'
, 'H8768-057-001'
, 'H8768-057-002'
, 'H8768-058-000'
, 'H8768-059-000'
, 'H8768-061-000'
, 'H8768-062-000'
, 'H8768-063-000'
, 'H9239-001-000'
, 'R0759-001-000'
, 'R0759-002-000'
, 'R0759-003-000'
, 'R2604-001-000'
, 'R2604-002-000'
, 'R2604-003-000'
, 'R2604-005-000'
, 'R3444-008-000'
, 'R3444-009-000'
, 'R3444-012-000'
, 'R3444-024-000'
, 'R5342-002-000'
, 'R5342-005-000'
, 'R6801-008-000'
, 'R6801-009-000'
, 'R6801-011-000'
, 'R6801-012-000'
) 
and fin_inc_month >= '202501';
;

select count(*) from tmp_1m.kn_oop_cost_pbp;

describe tmp_1m.kn_oop_cost_pbp;

select fin_segment_name from tmp_1m.kn_oop_cost_pbp;

drop table tmp_1m.kn_oop_cost_pbp_sum;
create table tmp_1m.kn_oop_cost_pbp_sum as
select 
	fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_2
	, fin_product_level_3
	, fin_tfm_product_new
	, tfm_include_flag
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, fin_source_name
	, sgr_source_name
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL' THEN 1 else 0 end as MnR_COSMOS_FFS_Flag
	
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type = 'FFS' THEN 1 else 0 end as MnR_NICE_FFS_Flag
	
	, CASE WHEN (fin_segment_name = 'M&R' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL') 
	       OR (fin_segment_name = 'M&R' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS')) then 1 else 0 end as MnR_TOTAL_FFS_FLAG
	
	, CASE WHEN (fin_inc_year ='2024' AND fin_segment_name = 'C&S' AND fin_brand in ('M&R','C&S') AND GLOBAL_CAP = 'NA' AND
		SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') then 0 
		WHEN (fin_segment_name = 'M&R' AND fin_brand='M&R' AND migration_source='OAH')
		OR (fin_segment_name = 'C&S' AND fin_brand='C&S' and migration_source='OAH') then 1 else 0 end as OAH_FLAG
		
	, CASE WHEN ((fin_segment_name = 'C&S' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (fin_inc_year ='2024' AND fin_segment_name = 'C&S' AND fin_brand in ('M&R','C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end as CnS_Dual_Flag	
		
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end as MnR_Dual_flag
	
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' and fin_product_level_3='INSTITUTIONAL' then 1 else 0 end as ISNP_flag
	, sum(fin_member_cnt) as mm 
from tmp_1m.kn_oop_cost_pbp
where fin_inc_month >= '202501'
group by
	fin_contractpbp
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_2
	, fin_product_level_3
	, fin_tfm_product_new
	, tfm_include_flag
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, fin_source_name
	, sgr_source_name
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL' THEN 1 else 0 end
	
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type = 'FFS' THEN 1 else 0 end
	
	, CASE WHEN (fin_segment_name = 'M&R' AND fin_brand='M&R' AND global_cap='NA' AND sgr_source_name='COSMOS' AND tfm_include_flag=1 AND fin_product_level_3 <>'INSTITUTIONAL') 
	       OR (fin_segment_name = 'M&R' AND fin_brand='M&R' AND sgr_source_name='NICE' AND nce_tadm_dec_risk_type in ('FFS')) then 1 else 0 end
	
	, CASE WHEN (fin_inc_year ='2024' AND fin_segment_name = 'C&S' AND fin_brand in ('M&R','C&S') AND GLOBAL_CAP = 'NA' AND
		SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD') then 0 
		WHEN (fin_segment_name = 'M&R' AND fin_brand='M&R' AND migration_source='OAH')
		OR (fin_segment_name = 'C&S' AND fin_brand='C&S' and migration_source='OAH') then 1 else 0 end
		
	, CASE WHEN ((fin_segment_name = 'C&S' and fin_brand in('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3='DUAL' AND
		SGR_SOURCE_NAME in('COSMOS','CSP') AND fin_state NOT IN ('OK','NC','NM','NV','OH','TX')) OR (fin_inc_year ='2024' AND fin_segment_name = 'C&S' AND fin_brand in ('M&R','C&S')
		AND GLOBAL_CAP = 'NA' AND SGR_SOURCE_NAME IN ('COSMOS','CSP') AND MIGRATION_SOURCE = 'OAH' AND FIN_STATE = 'MD')) then 1 else 0 end
		
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' and fin_product_level_3='DUAL' then 1 else 0 end
	
	, CASE WHEN fin_segment_name = 'M&R' AND fin_brand='M&R' and fin_product_level_3='INSTITUTIONAL' then 1 else 0 end
;


select count(*) from tmp_1m.kn_oop_cost_pbp_202506;







