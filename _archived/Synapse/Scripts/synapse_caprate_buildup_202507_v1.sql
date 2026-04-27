/*--######### membership #########*/

select * from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202507
select fin_tfm_include_flag from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202507

drop table 
    tmp_1m.kn_synapse_caprate_membership 
;
 
create table 
    tmp_1m.kn_synapse_caprate_membership stored as orc as
select
    a.fin_brand
    , a.fin_contractpbp as hpbp
    , case 
        when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP'
        when a.fin_product_level_3 = 'DUAL' then a.fin_market ||'-DSNP'
        else a.fin_market 
    end as market_expansion
    , a.fin_product_level_3
    , a.fin_tfm_product_new
    , a.fin_market
    , a.fin_g_i
    , a.migration_source
    , a.hierarchy
    , a.acp_network_name as aco_network
    , a.gal_cust_seg_nbr
    , a.tfm_include_flag
   	, a.fin_source_name
    , case 
        when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || 
            substr(a.nce_src_sys_mdcl_pln_id, 3, 3)
        else substr(a.gal_cust_seg_nbr, 5, 5) 
    end as group_id
    , a.fin_inc_month
    , a.fin_inc_year
    , count(distinct fin_mbi_hicn_fnl) as mbrs
from  tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202507 a
where 1 = 1
    -- and fin_source_name = 'COSMOS'
    /*--and migration_source <> 'OAH'*/
    and a.global_cap = 'NA'
    and a.fin_inc_year > 2020
group by
    a.fin_brand
    , a.fin_contractpbp
    , case 
        when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP'
        when a.fin_product_level_3 = 'DUAL' then a.fin_market ||'-DSNP'
        else a.fin_market 
    end
    , a.fin_product_level_3
    , a.fin_tfm_product_new
    , a.fin_market
    , a.fin_g_i
    , a.migration_source
    , a.hierarchy
    , a.acp_network_name
    , a.gal_cust_seg_nbr
    , a.tfm_include_flag
   	, a.fin_source_name
    , case 
        when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || 
            substr(a.nce_src_sys_mdcl_pln_id, 3, 3)
        else substr(a.gal_cust_seg_nbr, 5, 5) 
    end
    , a.fin_inc_month
    , a.fin_inc_year
;
 
/*-- 2348741 2231088 select count(*) from tmp_1m.kn_synapse_caprate_membership*/
drop table 
    tmp_1m.kn_mbi_aco 
;
 
create table 
    tmp_1m.kn_mbi_aco as
select 
    distinct fin_mbi_hicn_fnl
    , fin_inc_month
    , hierarchy
    , acp_network_number
    , acp_network_name
    , case 
        when fin_tfm_product_new = 'NICE HMO' then nce_purchaser_id || '-' || substr(nce_src_sys_mdcl_pln_id, 3, 3)
        else substr(gal_cust_seg_nbr, 5, 5) 
    end as group_id
from tadm_tre_cpy.gl_rstd_gpsgalnce_f_202507
where 1 = 1
    -- and fin_source_name = 'COSMOS'
    /*--and migration_source <> 'OAH'*/
    and global_cap = 'NA'
    and fin_inc_year > 2020
select count(*) from tmp_1m.kn_mbi_aco -- 418839610 if 07
-- 393603414 393603414 if 04

select count(distinct fin_mbi_hicn_fnl || fin_inc_month) from tmp_1m.kn_mbi_aco 



-- 393603414
-- 393603414
*/
/*--########## claims ##############################################*/
drop table 
    tmp_1m.kn_synapse_caprate_claims 
;

create table 
    tmp_1m.kn_synapse_caprate_claims stored as orc as
select
    a.brand_fnl
    , 'cosmos_op' as data_source
    , a.contractpbp_fnl as hpbp
    , case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end as market_expansion
    , a.product_level_3_fnl as fin_product_level_3
    , a.tfm_product_new_fnl as fin_tfm_product_new
    , a.market_fnl as fin_market
    , a.group_ind_fnl as fin_g_i
    , a.migration_source
    , a.tfm_include_flag
    , c.hierarchy
    , c.acp_network_name as aco_network
    , c.group_id
    , a.fst_srvc_month as fin_inc_month
    , a.fst_srvc_year as fin_inc_year
    , sum(a.allw_amt_fnl) as allowed_amt
    , sum(a.net_pd_amt_fnl) as paid_amt
from fichsrv.cosmos_op a
join tmp_1y.cl_synapse_hcpcs_20250212 b /*--676 (removed the 2 codes for cgm supplies K0553, K0554)*/
    on  a.proc_cd = b.hcpcs
left join tmp_1m.kn_mbi_aco c
    on  a.gal_mbi_hicn_fnl = c.fin_mbi_hicn_fnl
        and a.fst_srvc_month = c.fin_inc_month
where a.global_cap = 'NA'
    /*--and a.migration_source <> 'OAH'*/
    /*--and a.GROUP_IND_FNL = 'I'*/
    and a.hce_service_code = 'OP_DMESUP'
    and a.fst_srvc_year > 2020
    and 
    (a.allw_amt_fnl <> 0 
    or a.tadm_units <> 0)
group by
    a.brand_fnl
    , a.contractpbp_fnl
    , case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end
    , a.product_level_3_fnl
    , a.tfm_product_new_fnl
    , a.market_fnl
    , a.group_ind_fnl
    , a.migration_source
    , c.hierarchy
    , c.acp_network_name
    , c.group_id
    , a.fst_srvc_month
    , a.fst_srvc_year
union all
select
    a.brand_fnl
    , 'smart_op' as data_source
    , a.contractpbp_fnl as hpbp
    , case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end as market_expansion
    , a.product_level_3_fnl as fin_product_level_3
    , a.tfm_product_fnl as fin_tfm_product_new
    , a.market_fnl as fin_market
    , a.group_ind_fnl as fin_g_i
    , a.migration_source
    , c.hierarchy
    , c.acp_network_name as aco_network
    , c.group_id
    , a.fst_srvc_month as fin_inc_month
    , a.fst_srvc_year as fin_inc_year
    , sum(a.allw_amt_fnl) as allowed_amt
    , sum(a.net_pd_amt_fnl) as paid_amt
from tadm_tre_cpy.dcsp_op_f_202506 a
join tmp_1y.cl_synapse_hcpcs_20250212 b /*--676 (removed the 2 codes for cgm supplies K0553, K0554)*/
    on  a.proc_cd = b.hcpcs
left join tmp_1m.kn_mbi_aco c
    on  a.gal_mbi_hicn_fnl = c.fin_mbi_hicn_fnl
        and a.fst_srvc_month = c.fin_inc_month
where a.global_cap = 'NA'
    /*--and a.migration_source <> 'OAH'*/
    /*--and a.GROUP_IND_FNL = 'I'*/
    and a.hce_service_code = 'OP_DMESUP'
    and a.fst_srvc_year > 2020
    and 
    (a.allw_amt_fnl <> 0 
    or a.tadm_units <> 0)
group by
    a.brand_fnl
    , a.contractpbp_fnl
    , case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end
    , a.product_level_3_fnl
    , a.tfm_product_fnl
    , a.market_fnl
    , a.group_ind_fnl
    , a.migration_source
    , c.hierarchy
    , c.acp_network_name
    , c.group_id
    , a.fst_srvc_month
    , a.fst_srvc_year
;
 
describe tadm_tre_cpy.dcsp_op_f_202506

select * from tadm_tre_cpy.dcsp_op_f_202506

-- select max(fst_srvc_month) from tmp_1m.kn_synapse_caprate_claims;
 
;
/*--820022/25616 select data_source, count(*) from tmp_1m.kn_synapse_caprate_claims group by data_source*/
/*--SELECT sum(allowed_amt) FROM tmp_1m.kn_synapse_caprate_claims 
 * 
 * --2894822636.08*/
* -- 3496761616.48
/*--union claims and membership data for cap rate build up excel file*/

select fin_source_name from tmp_1m.kn_synapse_caprate_membership
group by fin_source_name;

drop table 
    tmp_1m.kn_synapse_caprate_membership_claims 
;
 
create table 
    tmp_1m.kn_synapse_caprate_membership_claims as
select
    'Claims' as component
    , market_expansion
    , case 
        when market_expansion 
            in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP' 
                , 'VA-CSNP') then 'exist' 
        else 'new' 
    end as exist_new
    , brand_fnl
    , data_source
    , fin_market
    , migration_source
    , fin_product_level_3
    , hpbp
    , fin_g_i
    /*--,hierarchy*/
    , aco_network
    , group_id
   	, '' as fin_source_name
    , fin_inc_month
    , fin_inc_year
    , sum(allowed_amt) as allowed
    , sum(paid_amt) as paid
    , 0 as member_cnt
from tmp_1m.kn_synapse_caprate_claims
group by
    brand_fnl
    , case 
        when market_expansion 
            in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP' 
                , 'VA-CSNP') then 'exist' 
        else 'new' 
    end
    , data_source
    , market_expansion
    , fin_market
    , migration_source
    , fin_product_level_3
    , hpbp
    , fin_g_i
    /*--,hierarchy*/
    , aco_network
    , group_id
    , fin_inc_month
    , fin_inc_year
union all
select
    'Membership' as component
    , market_expansion
    , case 
        when market_expansion 
            in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP' 
                , 'VA-CSNP') then 'exist' 
        else 'new' 
    end as exist_new
    , fin_brand
    , 'tre_membership' as data_source
    , fin_market
    , migration_source
    , fin_product_level_3
    , hpbp
    , fin_g_i
    /*--,hierarchy*/
    , aco_network
    , group_id
   	, fin_source_name
    , fin_inc_month
    , fin_inc_year
    , 0 as allowed
    , 0 as paid
    , sum(mbrs) as member_cnt
from tmp_1m.kn_synapse_caprate_membership
group by
    market_expansion
    , case 
        when market_expansion 
            in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP' 
                , 'VA-CSNP') then 'exist' 
        else 'new' 
    end
    , fin_brand
    , fin_market
    , migration_source
    , fin_product_level_3
    , hpbp
    , fin_g_i
    /*--,hierarchy*/
    , aco_network
    , group_id
   	, fin_source_name
    , fin_inc_month
    , fin_inc_year
;
 
describe tmp_1m.kn_synapse_caprate_membership_claims;


/*--1756248 select count(*) from tmp_1m.kn_synapse_caprate_membership_claims*/
--select 
--    sum(allowed) 
--from tmp_1m.kn_synapse_caprate_membership_claims /*--3717723357.91 2897669720.86*/
---- 3437640263.72
---- 3477458829.71
---- 3496761616.48
--select 
--    * 
--from tmp_1m.kn_synapse_caprate_membership_claims limit 2 
--;
--select count(*) from tmp_1m.kn_synapse_caprate_membership_claims_sum;

select fin_inc_quarter from tmp_1m.kn_synapse_caprate_membership_claims_sum

drop table 
    tmp_1m.kn_synapse_caprate_membership_claims_sum;
create table 
    tmp_1m.kn_synapse_caprate_membership_claims_sum as
select
    a.component
    , a.market_expansion
    , a.exist_new
    , a.fin_market
    , a.brand_fnl
    , a.data_source
    /*--,b.hpbp_expansion*/
    /*--,a.hpbp as hpbp_actual*/
    /*--,a.ACO_Network*/
    /*--,a.group_id*/
    , a.migration_source
    , a.fin_product_level_3
    , a.fin_g_i
    , a.fin_source_name
    , a.fin_inc_month
    , case 
    	when substr(a.fin_inc_month, 5, 2) in ('01', '02', '03') then concat(a.fin_inc_year, 'Q1')
    	when substr(a.fin_inc_month, 5, 2) in ('04', '05', '06') then concat(a.fin_inc_year, 'Q2')
    	when substr(a.fin_inc_month, 5, 2) in ('07', '08', '09') then concat(a.fin_inc_year, 'Q3')
    	when substr(a.fin_inc_month, 5, 2) in ('10', '11', '12') then concat(a.fin_inc_year, 'Q4')
    end as fin_inc_quarter
    , a.fin_inc_year
    , sum(a.allowed) as allowed
    , sum(a.paid) as paid
    , sum(a.member_cnt) as mm
from tmp_1m.kn_synapse_caprate_membership_claims a
where a.fin_product_level_3 not in ('INSTITUTIONAL')
    and market_expansion <>` ('CA') /*--only in 2022 with less than $2,000 in claims*/
    /*--left join tmp_1y.kn_synapse_hpbp_20250306_inscope b*/
    /*--on a.market_fnl = b.fin_market*/
    /*--and a.hpbp = b.hpbp_actual*/
    /*--and a.fst_srvc_year = b.srvc_year*/
group by
    a.component
    , a.market_expansion
    , a.exist_new
    , a.fin_market
    , a.brand_fnl
    , a.data_source
    , a.migration_source
    , a.fin_product_level_3
    , a.fin_g_i
    , a.fin_source_name
    , a.fin_inc_month
	, case 
    	when substr(a.fin_inc_month, 5, 2) in ('01', '02', '03') then concat(a.fin_inc_year, 'Q1')
    	when substr(a.fin_inc_month, 5, 2) in ('04', '05', '06') then concat(a.fin_inc_year, 'Q2')
    	when substr(a.fin_inc_month, 5, 2) in ('07', '08', '09') then concat(a.fin_inc_year, 'Q3')
    	when substr(a.fin_inc_month, 5, 2) in ('10', '11', '12') then concat(a.fin_inc_year, 'Q4')
    end
    , a.fin_inc_year
;
/*-- 27250 27284 select count(*) from tmp_1m.kn_synapse_caprate_membership_claims_sum*/
select 
    sum(allowed) 
from tmp_1m.kn_synapse_caprate_membership_claims_sum; /*
--$2,815,034,274*/ 
-- 3403900392.63

;
select 
    *
    , case when
    	
from tmp_1m.kn_synapse_caprate_membership_claims_sum



