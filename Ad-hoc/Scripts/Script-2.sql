-- COSMOS
drop table if exists tmp_1m.kn_nemt_cosmos_claims;
create table tmp_1m.kn_nemt_cosmos_claims as
select
	'COSMOS' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
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
where (proc_1_cd in ('A0425', 'A0428') 
	or proc_2_cd in ('A0425', 'A0428')
	or proc_3_cd in ('A0425', 'A0428') 
	or proc_cd in ('A0425', 'A0428')
	)
	and fst_srvc_month >= '202301'
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
    , primary_diag_cd, icd_2, icd_3, icd_4
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
where (proc_1_cd in ('A0425', 'A0428') 
	or proc_2_cd in ('A0425', 'A0428')
	or proc_3_cd in ('A0425', 'A0428') 
	or proc_cd in ('A0425', 'A0428')
	)
	and hce_month between '202301' and '202512'
	and brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
;


-- CSP
drop table if exists tmp_1m.kn_nemt_csp_claims;
create table tmp_1m.kn_nemt_csp_claims as
select
	'CSP' as entity
	, clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
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
where (proc_1_cd in ('A0425', 'A0428') 
	or proc_2_cd in ('A0425', 'A0428')
	or proc_3_cd in ('A0425', 'A0428') 
	or proc_cd in ('A0425', 'A0428')
	)
	and fst_srvc_month >= '202301'
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
    , primary_diag_cd, icd_2, icd_3, icd_4
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
where (proc_1_cd in ('A0425', 'A0428') 
	or proc_2_cd in ('A0425', 'A0428')
	or proc_3_cd in ('A0425', 'A0428') 
	or proc_cd in ('A0425', 'A0428')
	)
	and fst_srvc_month >= '202301'
	and brand_fnl = 'C&S' 
	and global_cap = 'NA'
;


-- NICE 
drop table if exists tmp_1m.kn_nemt_nice_claims;
create table tmp_1m.kn_nemt_nice_claims as
select
	'NICE' as entity
	, clm_aud_nbr as clm_id
	, mbi_hicn_fnl as mbi
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
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
where (proc_1_cd in ('A0425', 'A0428') 
	or proc_2_cd in ('A0425', 'A0428')
	or proc_3_cd in ('A0425', 'A0428') 
	or proc_cd in ('A0425', 'A0428')
	)
	and fst_srvc_month >= '202301'
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
    , primary_diag_cd, icd_2, icd_3, icd_4
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
where (proc_1_cd in ('A0425', 'A0428') 
	or proc_2_cd in ('A0425', 'A0428')
	or proc_3_cd in ('A0425', 'A0428') 
	or proc_cd in ('A0425', 'A0428')
	)
	and fst_srvc_month >= '202301'
	and brand_fnl = 'M&R'
	and clm_cap_flag = 'FFS'
;

drop table if exists tmp_1m.kn_nemt_cosmos_csp_nice_claims;
create table tmp_1m.kn_nemt_cosmos_csp_nice_claims as
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
    , primary_diag_cd, icd_2, icd_3, icd_4
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
from tmp_1m.kn_nemt_cosmos_claims
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
    , primary_diag_cd, icd_2, icd_3, icd_4
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
from tmp_1m.kn_nemt_csp_claims
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
    , primary_diag_cd, icd_2, icd_3, icd_4
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
from tmp_1m.kn_nemt_nice_claims
;
