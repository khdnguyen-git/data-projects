
;
-- Project: Waterjet ablation for BPH dx;
-- Author: Khang Nguyen

-- Summary
-- PR has approval checks -> Confirm it's working
-- OP doesn't -> Explore savings
-- CPT: 'C2596', '0412T'
-- Dx: 'N401'
-- Population: M&R FFS, C&S DSNP, and OAH
-- Output: Allowed, total claims (count distinct MBI&DOS*PROC)

-- COSMOS
drop table if exists tmp_1m.kn_waterjet_cosmos_claims_check;
create table tmp_1m.kn_waterjet_cosmos_claims_check as
select
	'COSMOS' as entity
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
	, case
        when 'N401' 
        in (primary_diag_cd, icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10 
            , icd_11, icd_12, icd_13, icd_14, icd_15, icd_16, icd_17, icd_18, icd_19, icd_20 
            , icd_21, icd_22, icd_23, icd_24, icd_25) then 'Y'
	    else 'N'
	end as bph
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
where (proc_1_cd in ('C2596', '0421T') 
	or proc_2_cd in ('C2596', '0421T')
	or proc_3_cd in ('C2596', '0421T') 
	or proc_cd in ('C2596', '0421T')
	)
	and fst_srvc_month between '202301' and '202512'
	and brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
union all
select
	'COSMOS' as entity
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
	, case
        when 'N401' 
        in (primary_diag_cd, icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10 
            , icd_11, icd_12, icd_13, icd_14, icd_15, icd_16, icd_17, icd_18, icd_19, icd_20 
            , icd_21, icd_22, icd_23, icd_24, icd_25) then 'Y'
	    else 'N'
	end as bph
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
where (proc_1_cd in ('C2596', '0421T') 
	or proc_2_cd in ('C2596', '0421T')
	or proc_3_cd in ('C2596', '0421T') 
	or proc_cd in ('C2596', '0421T')
	)
	and hce_month between '202301' and '202512'
	and brand_fnl in ('M&R', 'C&S')
	and global_cap = 'NA'
;

-- CSP
drop table if exists tmp_1m.kn_waterjet_csp_claims_check;
create table tmp_1m.kn_waterjet_csp_claims_check as
select
	'CSP' as entity
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
	, case
        when 'N401' 
        in (primary_diag_cd, icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10 
            , icd_11, icd_12, icd_13, icd_14, icd_15, icd_16, icd_17, icd_18, icd_19, icd_20 
            , icd_21, icd_22, icd_23, icd_24, icd_25) then 'Y'
	    else 'N'
	end as bph
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
where (proc_1_cd in ('C2596', '0421T') 
	or proc_2_cd in ('C2596', '0421T')
	or proc_3_cd in ('C2596', '0421T') 
	or proc_cd in ('C2596', '0421T')
	)
	and fst_srvc_month between '202301' and '202512'
	and brand_fnl = 'C&S' 
	and global_cap = 'NA'
union all
select
	'CSP' as entity
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
	, case
        when 'N401' 
        in (primary_diag_cd, icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10 
            , icd_11, icd_12, icd_13, icd_14, icd_15, icd_16, icd_17, icd_18, icd_19, icd_20 
            , icd_21, icd_22, icd_23, icd_24, icd_25) then 'Y'
	    else 'N'
	end as bph
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
where (proc_1_cd in ('C2596', '0421T') 
	or proc_2_cd in ('C2596', '0421T')
	or proc_3_cd in ('C2596', '0421T') 
	or proc_cd in ('C2596', '0421T')
	)
	and fst_srvc_month between '202301' and '202512'
	and brand_fnl = 'C&S' 
	and global_cap = 'NA'
;


-- NICE
drop table if exists tmp_1m.kn_waterjet_nice_claims_check;
create table tmp_1m.kn_waterjet_nice_claims_check as
select
	'NICE' as entity
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
	, case
        when 'N401' 
        in (primary_diag_cd, icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10 
            , icd_11, icd_12, icd_13, icd_14, icd_15, icd_16, icd_17, icd_18, icd_19, icd_20 
            , icd_21, icd_22, icd_23, icd_24, icd_25) then 'Y'
	    else 'N'
	end as bph
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
where (proc_1_cd in ('C2596', '0421T') 
	or proc_2_cd in ('C2596', '0421T')
	or proc_3_cd in ('C2596', '0421T') 
	or proc_cd in ('C2596', '0421T')
	)
	and fst_srvc_month between '202301' and '202512'
	and brand_fnl = 'M&R'
	and clm_cap_flag = 'FFS'
union all
select
	'NICE' as entity
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
	, component
	, hce_service_code as service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
	, case
        when 'N401' 
        in (primary_diag_cd, icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10 
            , icd_11, icd_12, icd_13, icd_14, icd_15, icd_16, icd_17, icd_18, icd_19, icd_20 
            , icd_21, icd_22, icd_23, icd_24, icd_25) then 'Y'
	    else 'N'
	end as bph
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
where (proc_1_cd in ('C2596', '0421T') 
	or proc_2_cd in ('C2596', '0421T')
	or proc_3_cd in ('C2596', '0421T') 
	or proc_cd in ('C2596', '0421T')
	)
	and fst_srvc_month between '202301' and '202512'
	and brand_fnl = 'M&R'
	and clm_cap_flag = 'FFS'
;

drop table if exists tmp_1m.kn_waterjet_cosmos_csp_nice_claims_check;
create table tmp_1m.kn_waterjet_cosmos_csp_nice_claims_check as
select
	entity
	, entity1
	, unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
    , bph
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
from tmp_1m.kn_waterjet_cosmos_claims_check

union all
select 
	entity
	, entity1
	, unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
    , bph
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
from tmp_1m.kn_waterjet_csp_claims_check
union all
select 
	entity
	, entity1
	, unique_id
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
    , bph
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
from tmp_1m.kn_waterjet_nice_claims_check
;

drop table if exists tmp_1m.kn_waterjet_cosmos_csp_nice_claims_check_summary;
create table tmp_1m.kn_waterjet_cosmos_csp_nice_claims_check_summary as
select
	entity
	, entity1
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
    , bph
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
    , sum(pd) as pd
    , count(unique_id) as n
    , count(distinct unique_id) as n_distinct
from tmp_1m.kn_waterjet_cosmos_csp_nice_claims_check
group by 
	entity
	, entity1
	, component
	, service_code
    , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
    , bph
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

