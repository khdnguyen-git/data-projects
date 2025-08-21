-- CGM: ('A4238', 'A4239', 'E2102', 'E2103') 

drop table tmp_1m.kn_synapse_cgm_membership_202508 
;
create table tmp_1m.kn_synapse_cgm_membership_202508 stored as orc as
select
    a.fin_inc_month
    , a.fin_inc_year
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
    , count(distinct fin_mbi_hicn_fnl) as mbrs
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202507 a 
where 1 = 1
    and a.global_cap = 'NA'
    and a.fin_inc_year > 2020
group by
    a.fin_inc_month
    , a.fin_inc_year
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
;
/*-- 2348741 2231088 select count(*) from tmp_1m.kn_synapse_cgm_membership_202508*/

select * from tmp_1m.kn_synapse_cgm_membership_202508


-- COSMOS_OP
drop table tmp_1m.kn_synapse_cgm_claims_cosmos_op;
create table tmp_1m.kn_synapse_cgm_claims_cosmos_op as
with aco as (
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
    and global_cap = 'NA'
    and fin_inc_year > 2020
)
select 
	'COSMOS_OP' as data_source
    , a.fst_srvc_month as fin_inc_month
    , a.fst_srvc_year as fin_inc_year
	, a.clm_pl_of_srvc_desc
	, a.prov_tin
	, a.prov_prtcp_sts_cd as prov_parstatus
	, a.full_nm
	, a.product_level_3_fnl as fin_product_level_3
    , a.tfm_product_fnl as fin_tfm_product_new
	, a.group_ind_fnl as fin_g_i
	, a.market_fnl as fin_market
	, a.tfm_include_flag
	, case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end as market_expansion
	, a.migration_source
	, a.contractpbp_fnl as hpbp
	, a.proc_cd
    , a.proc_mod1_cd
	, a.proc_mod2_cd
	, a.proc_mod3_cd
	, a.proc_mod4_cd
	, b.hierarchy
    , b.acp_network_name
    , b.group_id
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
from fichsrv.cosmos_op as a 
left join aco as b
	on a.gal_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
		and a.fst_srvc_month = b.fin_inc_month
where
	a.global_cap = 'NA'
    and a.fst_srvc_year > '2020'
    and (a.allw_amt_fnl <> 0 or a.tadm_units <> 0)
    and a.proc_cd in ('A4238', 'A4239', 'E2102', 'E2103')
group by
	a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_year
	, a.clm_pl_of_srvc_desc
	, a.prov_tin
	, a.prov_prtcp_sts_cd
	, a.full_nm
	, a.product_level_3_fnl
    , a.tfm_product_fnl
	, a.group_ind_fnl
	, a.market_fnl
	, a.tfm_include_flag
	, case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end
	, a.migration_source
	, a.contractpbp_fnl
	, a.proc_cd
    , a.proc_mod1_cd
	, a.proc_mod2_cd
	, a.proc_mod3_cd
	, a.proc_mod4_cd
	, b.hierarchy
    , b.acp_network_name
    , b.group_id
;

-- SMART_OP
drop table tmp_1m.kn_synapse_cgm_claims_smart_op;
create table tmp_1m.kn_synapse_cgm_claims_smart_op as
with aco as (
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
    and global_cap = 'NA'
    and fin_inc_year > 2020
)
select 
	'SMART_OP' as data_source
    , a.fst_srvc_month as fin_inc_month
    , a.fst_srvc_year as fin_inc_year
	, a.clm_pl_of_srvc_desc
	, a.tin as prov_tin
	, a.prov_prtcp_sts_cd as prov_parstatus
	, a.full_nm
	, a.product_level_3_fnl as fin_product_level_3
    , a.tfm_product_fnl as fin_tfm_product_new
	, a.group_ind_fnl as fin_g_i
	, a.market_fnl as fin_market
	, a.tfm_include_flag
	, case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end as market_expansion
	, a.migration_source
	, a.contractpbp_fnl as hpbp
	, a.proc_cd
    , a.proc_mod1_cd
	, a.proc_mod2_cd
	, a.proc_mod3_cd
	, a.proc_mod4_cd
	, b.hierarchy
    , b.acp_network_name
    , b.group_id
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
from tadm_tre_cpy.dcsp_op_f_202507 as a 
left join aco as b
	on a.gal_mbi_hicn_fnl = b.fin_mbi_hicn_fnl
		and a.fst_srvc_month = b.fin_inc_month
where
	a.global_cap = 'NA'
    and a.fst_srvc_year > '2020'
    and (a.allw_amt_fnl <> 0 or a.tadm_units <> 0)
    and a.proc_cd in ('A4238', 'A4239', 'E2102', 'E2103')
group by
	a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_year
	, a.clm_pl_of_srvc_desc
	, a.tin
	, a.prov_prtcp_sts_cd
	, a.full_nm
	, a.product_level_3_fnl
    , a.tfm_product_fnl
	, a.group_ind_fnl
	, a.market_fnl
	, a.tfm_include_flag
	, case 
        when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
        when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
        else a.market_fnl 
    end
	, a.migration_source
	, a.contractpbp_fnl
	, a.proc_cd
    , a.proc_mod1_cd
	, a.proc_mod2_cd
	, a.proc_mod3_cd
	, a.proc_mod4_cd
	, b.hierarchy
    , b.acp_network_name
    , b.group_id
;

drop table tmp_1m.kn_synapse_cgm_claims_cosmos_smart_op;
create table tmp_1m.kn_synapse_cgm_claims_cosmos_smart_op as
select * from tmp_1m.kn_synapse_cgm_claims_cosmos_op
union all
select * from tmp_1m.kn_synapse_cgm_claims_smart_op;


select * from tmp_1m.kn_synapse_cgm_claims_cosmos_smart_op;


create table 
    tmp_1m.kn_synapse_cgm_claims_mm as
select
    'Claims' as data_type 
    , data_source 
    , fin_inc_month 
    , fin_inc_year 
    , prov_tin 
    , prov_parstatus 
    , full_nm 
    , fin_product_level_3 
    , fin_tfm_product_new 
    , fin_market 
    , fin_g_i 
    , market_expansion 
    , tfm_include_flag 
    , migration_source 
    , '' as fin_source_name 
    , contractpbp_fnl as hpbp 
    , proc_cd 
    , proc_mod1_cd 
    , proc_mod2_cd 
    , proc_mod3_cd 
    , proc_mod4_cd 
    , acp_network_name as aco_network 
    , group_id 
    , sum(allowed) as allowed 
    , sum(paid) as paid 
    , 0 as mm
from tmp_1m.kn_synapse_cgm_claims_cosmos_smart_op
group by
    data_source 
    , fin_inc_month 
    , fin_inc_year 
    , prov_tin 
    , prov_parstatus 
    , full_nm 
    , fin_product_level_3 
    , fin_tfm_product_new 
    , fin_market 
    , fin_g_i 
    , market_expansion 
    , tfm_include_flag 
    , migration_source 
    , contractpbp_fnl 
    , proc_cd 
    , proc_mod1_cd 
    , proc_mod2_cd 
    , proc_mod3_cd 
    , proc_mod4_cd 
    , acp_network_name 
    , group_id
union all
select
    'Membership' as data_type 
    , 'tre_membership' as data_source 
    , fin_inc_month 
    , fin_inc_year 
    , '' as prov_tin 
    , '' as prov_parstatus 
    , '' as full_nm 
    , fin_product_level_3 
    , fin_tfm_product_new 
    , fin_market 
    , fin_g_i 
    , market_expansion 
    , tfm_include_flag 
    , migration_source 
    , '' as fin_source_name 
    , hpbp 
    , '' as proc_cd 
    , '' as proc_mod1_cd 
    , '' as proc_mod2_cd 
    , '' as proc_mod3_cd 
    , '' as proc_mod4_cd 
    , aco_network 
    , group_id 
    , 0 as allowed 
    , 0 as paid 
    , sum(mbrs) as mm
from tmp_1m.kn_synapse_cgm_membership_202508
group by
    fin_inc_month 
    , fin_inc_year 
    , fin_product_level_3 
    , fin_tfm_product_new 
    , fin_market 
    , fin_g_i 
    , market_expansion 
    , tfm_include_flag 
    , migration_source 
    , hpbp 
    , aco_network 
    , group_id 
;

-- 1,980,930 select count(*) from tmp_1m.kn_synapse_cgm_claims_mm
select
	fin_inc_month
	, sum(allowed) / sum(mm) as pmpm
from tmp_1m.kn_synapse_cgm_claims_mm
where fin_inc_month >= '202401'
group by 
	fin_inc_month
order by 
	fin_inc_month

/*
fin_inc_month|pmpm    |
-------------+--------+
202401       |3.672742|
202402       |3.511487|
202403       |3.485131|
202404       |3.669120|
202405       |3.763107|
202406       |3.547763|
202407       |3.906562|
202408       |3.930930|
202409       |2.234247|
202410       |3.094422|
202411       |2.990759|
202412       |3.471513|
202501       |3.498811|
202502       |2.655911|
202503       |3.161110|
202504       |3.573213|
202505       |2.914672|
202506       |3.056897|
202507       |1.156864|
*/


select
	fin_inc_month
	, market_expansion
	, sum(allowed) / sum(mm) as pmpm
from tmp_1m.kn_synapse_cgm_claims_mm
where fin_inc_month >= '202401'
group by 
	fin_inc_month
	, market_expansion
order by 
	fin_inc_month
	, market_expansion

