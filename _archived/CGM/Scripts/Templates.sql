drop table 
if exists tmp_1m.kn_<proj>_cosmos_claims;
create table tmp_1m.kn_<proj>_cosmos_claims as
select
    'COSMOS' as entity
    , clm_aud_nbr as clm_id
    , gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , component
    , service_code
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
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
    , case 
        when (brand_fnl = 'M&R' and migration_source = 'OAH') then 'M&R OAH'
        when (brand_fnl = 'C&S' and migration_source = 'OAH') then 'C&S OAH'
        when (brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
        else brand_fnl
    end as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
from fichsrv.cosmos_pr
where (proc_1_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_2_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_3_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') )
    and fst_srvc_month >= '202401'
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
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
    , hce_month as fst_srvc_month
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
    , case 
        when (brand_fnl = 'M&R' and migration_source = 'OAH') then 'M&R OAH'
        when (brand_fnl = 'C&S' and migration_source = 'OAH') then 'C&S OAH'
        when (brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
        else brand_fnl
    end as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
from fichsrv.cosmos_op
where (proc_1_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_2_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_3_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') )
    and hce_month >= '202401'
    and brand_fnl in ('M&R', 'C&S')
    and global_cap = 'NA'
;
drop table 
if exists tmp_1m.kn_<proj>_csp_claims;
create table tmp_1m.kn_<proj>_csp_claims as
select
    'CSP' as entity
    , clm_aud_nbr as clm_id
    , gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , component
    , service_code
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
    , fst_srvc_month
    , fst_srvc_year
    , global_cap
    , clm_dnl_f
    , market_fnl 
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , case 
        when migration_source = 'OAH' then 'C&S OAH'
        else 'C&S DSNP'
    end as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
from tadm_tre_cpy.dcsp_pr_f_202506
where (proc_1_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_2_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_3_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') )
    and fst_srvc_month >= '202401'
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
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
    , fst_srvc_month
    , fst_srvc_year
    , global_cap
    , clm_dnl_f
    , market_fnl 
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , case 
        when migration_source = 'OAH' then 'C&S OAH'
        else 'C&S DSNP'
    end as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
from tadm_tre_cpy.dcsp_op_f_202506
where (proc_1_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_2_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_3_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') )
    and fst_srvc_month >= '202401'
    and brand_fnl = 'C&S'
    and global_cap = 'NA'
;
drop table 
if exists tmp_1m.kn_<proj>_nice_claims;
create table tmp_1m.kn_<proj>_nice_claims as
select
    'NICE' as entity
    , clm_aud_nbr as clm_id
    , mbi_hicn_fnl as mbi
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , component
    , service_code
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
    , fst_srvc_month
    , fst_srvc_year
    , clm_cap_flag as global_cap
    , clm_dnl_f
    , market_fnl 
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , brand_fnl as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
from fichsrv.nice_pr
where (proc_1_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_2_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_3_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') )
    and fst_srvc_month >= '202401'
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
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
    , fst_srvc_month
    , fst_srvc_year
    , clm_cap_flag as global_cap
    , clm_dnl_f
    , market_fnl 
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , brand_fnl as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
from fichsrv.nice_op
where (proc_1_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_2_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_3_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    or proc_cd 
    in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180' 
        , 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') )
    and fst_srvc_month >= '202401'
    and brand_fnl = 'M&R'
    and clm_cap_flag = 'FFS'
;

drop table 
if exists tmp_1m.kn_<proj>_cosmos_csp_nice_claims;
create table tmp_1m.kn_<proj>_cosmos_csp_nice_claims as
select
    entity
    , entity1
    , case
        when entity1 = 'M&R' then 'M&R FFS'
        when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
        when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
        when entity1 = 'M&R ISNP' then 'ISNP'
    end as population
    , clm_id
    , mbi
    , unique_id
    , component
    , service_code
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
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
from tmp_1m.kn_<proj>_cosmos_claims
union all
select
    entity
    , entity1
    , case
        when entity1 = 'M&R' then 'M&R FFS'
        when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
        when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
        when entity1 = 'M&R ISNP' then 'ISNP'
    end as population
    , clm_id
    , mbi
    , unique_id
    , component
    , service_code
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
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
from tmp_1m.kn_<proj>_csp_claims
union all
select
    entity
    , entity1
    , case
        when entity1 = 'M&R' then 'M&R FFS'
        when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
        when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
        when entity1 = 'M&R ISNP' then 'ISNP'
    end as population
    , clm_id
    , mbi
    , unique_id
    , component
    , service_code
    , proc_cd 
    , proc_1_cd 
    , proc_2_cd 
    , proc_3_cd
    , primary_diag_cd 
    , icd_2 
    , icd_3 
    , icd_4
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
from tmp_1m.kn_<proj>_nice_claims
;



create table tmp_1m.kn_<proj>_membership as
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
select * from mnr_membership
union all
select * from cns_membership
union all
select * from smart_membership
union all
select * from nice_membership
;


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
	, case when (notif_year = '2024' and business_segment = 'CnS' and fin_brand in ('M&R','C&S') and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD') then 0 
		when (business_segment = 'MnR' and fin_brand = 'M&R' and migration_source = 'OAH')
		  or (business_segment = 'CnS' and fin_brand = 'C&S' and migration_source = 'OAH') then 1 
		else 0 
	end as oah_flag
	, case when 
		(
			(business_segment = 'CnS' and fin_brand in ('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' 
			 and sgr_source_name in ('COSMOS','CSP') and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
			or 
			(notif_year = '2024' and business_segment = 'CnS' and fin_brand in ('M&R','C&S') and global_cap = 'NA' 
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