-- COSMOS
drop table if exists tmp_1m.kn_predspike_cosmos_claims;
create table tmp_1m.kn_predspike_cosmos_claims as
select
	'COSMOS' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , case when (brand_fnl = 'M&R' and migration_source = 'OAH') then 'M&R OAH'
	       when (brand_fnl = 'C&S' and migration_source = 'OAH') then 'C&S OAH'
	       when (brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
	       else brand_fnl 
	end as entity1
	, allw_amt_fnl as allw
	, net_pd_amt_fnl as pd
from fichsrv.cosmos_pr
where (proc_1_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_2_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	or proc_3_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	)
	and fst_srvc_month between '202401' and '202512'
	and brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
union all
select
	'COSMOS' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , hce_month as fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl, brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
	, case when (brand_fnl = 'M&R' and migration_source = 'OAH') then 'M&R OAH'
	       when (brand_fnl = 'C&S' and migration_source = 'OAH') then 'C&S OAH'
	       when (brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
	       else brand_fnl 
	end as entity1
	, allw_amt_fnl as allw
	, net_pd_amt_fnl as pd
from fichsrv.cosmos_op
where (proc_1_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_2_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	or proc_3_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	)
	and hce_month between '202401' and '202512'
	and brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
;


-- CSP
drop table if exists tmp_1m.kn_predspike_csp_claims;
create table tmp_1m.kn_predspike_csp_claims as
select
	'CSP' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl, brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
   	, case when migration_source = 'OAH' then 'C&S OAH'
	       else 'C&S DSNP' 
	end as entity1
	, allw_amt_fnl as allw
	, net_pd_amt_fnl as pd
from tadm_tre_cpy.dcsp_pr_f_202506
where (proc_1_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_2_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	or proc_3_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	)
	and fst_srvc_month between '202401' and '202512'
	and brand_fnl = 'C&S' 
	and global_cap = 'NA'
union all
select
	'CSP' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl, brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
   	, case when migration_source = 'OAH' then 'C&S OAH'
	       else 'C&S DSNP' 
	end as entity1
	, allw_amt_fnl as allw
	, net_pd_amt_fnl as pd
from tadm_tre_cpy.dcsp_op_f_202506
where (proc_1_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_2_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	or proc_3_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	)
	and fst_srvc_month between '202401' and '202512'
	and brand_fnl = 'C&S' 
	and global_cap = 'NA'
;


-- NICE
drop table if exists tmp_1m.kn_predspike_nice_claims;
create table tmp_1m.kn_predspike_nice_claims as
select
	'NICE' as entity
	, clm_aud_nbr as clm_id
	, mbi_hicn_fnl as mbi
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , fst_srvc_month
    , fst_srvc_year
	, clm_cap_flag as global_cap
	, clm_dnl_f
    , market_fnl, brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
	, brand_fnl as entity1
	, allw_amt_fnl as allw
	, net_pd_amt_fnl as pd
from fichsrv.nice_pr
where (proc_1_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_2_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	or proc_3_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	)
	and fst_srvc_month between '202401' and '202512'
	and brand_fnl = 'M&R'
	and clm_cap_flag = 'FFS'
union all
select
	'NICE' as entity
	, clm_aud_nbr as clm_id
	, mbi_hicn_fnl as mbi
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , fst_srvc_month
    , fst_srvc_year
	, clm_cap_flag as global_cap
	, clm_dnl_f
    , market_fnl, brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , brand_fnl as entity1
	, allw_amt_fnl as allw
	, net_pd_amt_fnl as pd
from fichsrv.nice_op
where (proc_1_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_2_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	or proc_3_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542') 
	or proc_cd in ('G0299', 'Q5147', 'S9123', 'G0151', '99214', 'J3489', '99215', 'S9131', '67028', 'J7325', 'S9129', 'J2802', 'J1756', '99204', '81542')
	)
	and fst_srvc_month between '202401' and '202512'
	and brand_fnl = 'M&R'
	and clm_cap_flag = 'FFS'
;

drop table if exists tmp_1m.kn_predspike_cosmos_csp_nice_claims;
create table tmp_1m.kn_predspike_cosmos_csp_nice_claims as
select
	entity
	, entity1
	, case 
		when entity1 = 'M&R' then 'M&R FFS'
		when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
		when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
		when entity1 = 'M&R ISNP' then 'ISNP'
	end as Population
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
	, fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , allw
    , pd
from tmp_1m.kn_predspike_cosmos_claims
union all
select 
	entity
	, entity1
	, case 
		when entity1 = 'M&R' then 'M&R FFS'
		when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
		when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
		when entity1 = 'M&R ISNP' then 'ISNP'
	end as Population
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
	, fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , allw
    , pd
from tmp_1m.kn_predspike_csp_claims
union all
select 
	entity
	, entity1
	, case 
		when entity1 = 'M&R' then 'M&R FFS'
		when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
		when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
		when entity1 = 'M&R ISNP' then 'ISNP'
	end as Population
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
	, fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , allw
    , pd
from tmp_1m.kn_predspike_nice_claims
;

drop table tmp_1m.kn_predspike_cosmos_csp_nice_claims_sum;
create table tmp_1m.kn_predspike_cosmos_csp_nice_claims_sum as
select
	entity
	, entity1
	, population
	, component
	, service_code
    , proc_cd
	, fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , sum(allw) as allowed
    , sum(pd) as paid
    , count(unique_id) as n_clm
    , count(distinct unique_id) as n_distinct_clm
from tmp_1m.kn_predspike_cosmos_csp_nice_claims
group by
	entity
	, entity1
	, population
	, component
	, service_code
    , proc_cd
	, fst_srvc_month
    , fst_srvc_year
	, global_cap
	, clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
;

select count(*) from tmp_1m.kn_predspike_cosmos_csp_nice_claims_sum;



-- Membership;
create table tmp_1m.kn_membership as
with mnr_membership as (
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
	and fin_inc_month >= '202401'
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
cns_membership as (
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
	and fin_inc_month >= '202401'
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
	and fin_inc_month >= '202401'
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
	and fin_inc_month >= '202401'
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
select * from mnr_membership
union all
select * from cns_membership
union all
select * from smart_membership
union all
select * from nice_membership
;
