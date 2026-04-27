


--######### membership #########
SELECT DISTINCT fin_product_level_3 from tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202504


drop table tmp_1m.cl_synapse_caprate_membership;
create table tmp_1m.cl_synapse_caprate_membership stored as orc as
SELECT
a.fin_brand
,a.fin_contractpbp as hpbp
,case when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP'
when a.fin_product_level_3 = 'DUAL' then a.fin_market ||'-DSNP'
else a.fin_market end as market_expansion
,a.fin_product_level_3
,a.fin_tfm_product_new
,a.fin_market
,a.fin_g_i
,a.fin_source_name
,a.migration_source
,a.hierarchy
,a.acp_network_name as ACO_Network
,a.gal_cust_seg_nbr
,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3)
else substr(a.gal_cust_seg_nbr,5,5) END AS group_id
,a.fin_inc_month
,a.fin_inc_year
,count(distinct fin_mbi_hicn_fnl) as mbrs
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202504 a
where 1=1
--a.fin_source_name = 'COSMOS'
--and migration_source <> 'OAH'
and a.global_cap='NA'
and a.fin_inc_year > 2020
group by
a.fin_brand
,a.fin_contractpbp
,case when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP'
when a.fin_product_level_3 = 'DUAL' then a.fin_market ||'-DSNP'
else a.fin_market end
,a.fin_product_level_3
,a.fin_tfm_product_new
,a.fin_market
,a.fin_g_i
,a.fin_source_name
,a.migration_source
,a.hierarchy
,a.acp_network_name
,a.gal_cust_seg_nbr
,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3)
else substr(a.gal_cust_seg_nbr,5,5) END
,a.fin_inc_month
,a.fin_inc_year
;
--2231088 select count(*) from tmp_1m.cl_synapse_caprate_membership


/*
drop table tmp_7d.cl_mbi_aco
create table tmp_7d.cl_mbi_aco as
select DISTINCT 
fin_mbi_hicn_fnl
,fin_inc_month
,hierarchy
,acp_network_number
,acp_network_name 
,case when fin_tfm_product_new = 'NICE HMO' then nce_purchaser_id || '-' || substr(nce_src_sys_mdcl_pln_id,3,3) 
else substr(gal_cust_seg_nbr,5,5) END AS group_id
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202504
where 1=1 
--fin_source_name = 'COSMOS'
and global_cap='NA'
and fin_inc_year > 2020 
;

select count(*) from tmp_7d.cl_mbi_aco --393603414

select count(distinct fin_mbi_hicn_fnl || fin_inc_month) from tmp_7d.cl_mbi_aco --393603414 
*/

--########## claims ##############################################
drop table tmp_1m.cl_synapse_caprate_claims;
create table tmp_1m.cl_synapse_caprate_claims stored as orc As
SELECT
a.brand_fnl
,'cosmos_op' as data_source
,a.contractpbp_fnl as hpbp
,case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
else a.market_fnl end as market_expansion
,a.product_level_3_fnl as fin_product_level_3
,a.tfm_product_new_fnl as fin_tfm_product_new
,a.market_fnl as fin_market
,a.GROUP_IND_FNL as fin_g_i
,a.migration_source
,c.hierarchy
,c.acp_network_name as ACO_Network
,c.group_id
,a.fst_srvc_month as fin_inc_month
,a.fst_srvc_year as fin_inc_year
,sum(a.allw_amt_fnl) as allowed_amt
,sum(a.net_pd_amt_fnl) as paid_amt
from fichsrv.cosmos_op a
join tmp_1y.cl_synapse_hcpcs_20250212 b --676 (removed the 2 codes for cgm supplies K0553, K0554) 
on a.proc_cd = b.hcpcs
left join tmp_7d.cl_mbi_aco c
on a.gal_mbi_hicn_fnl = c.fin_mbi_hicn_fnl
and a.fst_srvc_month = c.fin_inc_month
where
a.global_cap = 'NA'
--and a.migration_source <> 'OAH'
--and a.GROUP_IND_FNL = 'I'
and a.hce_service_code = 'OP_DMESUP'
and a.fst_srvc_year > 2020
and (a.allw_amt_fnl <> 0 or a.tadm_units <> 0)
group by
a.brand_fnl
,a.contractpbp_fnl
,case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
else a.market_fnl end
,a.product_level_3_fnl
,a.tfm_product_new_fnl
,a.market_fnl
,a.GROUP_IND_FNL
,a.migration_source
,c.hierarchy
,c.acp_network_name
,c.group_id
,a.fst_srvc_month
,a.fst_srvc_year
union all
SELECT
a.brand_fnl
,'smart_op' as data_source
,a.contractpbp_fnl as hpbp
,case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
else a.market_fnl end as market_expansion
,a.product_level_3_fnl as fin_product_level_3
,a.tfm_product_fnl as fin_tfm_product_new
,a.market_fnl as fin_market
,a.GROUP_IND_FNL as fin_g_i
,a.migration_source
,c.hierarchy
,c.acp_network_name as ACO_Network
,c.group_id
,a.fst_srvc_month as fin_inc_month
,a.fst_srvc_year as fin_inc_year
,sum(a.allw_amt_fnl) as allowed_amt
,sum(a.net_pd_amt_fnl) as paid_amt
from tadm_tre_cpy.dcsp_op_f_202503 a
join tmp_1y.cl_synapse_hcpcs_20250212 b --676 (removed the 2 codes for cgm supplies K0553, K0554) 
on a.proc_cd = b.hcpcs
left join tmp_7d.cl_mbi_aco c
on a.gal_mbi_hicn_fnl = c.fin_mbi_hicn_fnl
and a.fst_srvc_month = c.fin_inc_month
where
a.global_cap = 'NA'
--and a.migration_source <> 'OAH'
--and a.GROUP_IND_FNL = 'I'
and a.hce_service_code = 'OP_DMESUP'
and a.fst_srvc_year > 2020
and (a.allw_amt_fnl <> 0 or a.tadm_units <> 0)
group by
a.brand_fnl
,a.contractpbp_fnl
,case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP'
when a.product_level_3_fnl = 'DUAL' then a.market_fnl ||'-DSNP'
else a.market_fnl end
,a.product_level_3_fnl
,a.tfm_product_fnl
,a.market_fnl
,a.GROUP_IND_FNL
,a.migration_source
,c.hierarchy
,c.acp_network_name
,c.group_id
,a.fst_srvc_month
,a.fst_srvc_year
select * from tadm_tre_cpy.dcsp_op_f_202503 limit 2;
;
--820022/25616 select data_source, count(*) from tmp_1m.cl_synapse_caprate_claims group by data_source

--SELECT sum(allowed_amt) FROM tmp_1m.cl_synapse_caprate_claims --2894822636.08

--union claims and membership data for cap rate build up excel file
drop table tmp_1m.cl_synapse_caprate_membership_claims;
create table tmp_1m.cl_synapse_caprate_membership_claims as
select
'Claims' as component
,market_expansion
,case when market_expansion in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP', 'VA-CSNP') then 'exist' else 'new' end as exist_new
,brand_fnl
,data_source
,fin_market
,migration_source
,fin_product_level_3
,hpbp
,fin_g_i
--,hierarchy
,ACO_Network
,group_id
,fin_inc_month
,fin_inc_year
,sum(allowed_amt) as allowed
,sum(paid_amt) as paid
,0 as member_cnt
from tmp_1m.cl_synapse_caprate_claims
group by
brand_fnl
,case when market_expansion in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP', 'VA-CSNP') then 'exist' else 'new' end
,data_source
,market_expansion
,fin_market
,migration_source
,fin_product_level_3
,hpbp
,fin_g_i
--,hierarchy
,ACO_Network
,group_id
,fin_inc_month
,fin_inc_year
union all
select
'Membership' as component
,market_expansion
,case when market_expansion in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP', 'VA-CSNP') then 'exist' else 'new' end as exist_new
,fin_brand
,'tre_membership' as data_source
,fin_market
,migration_source
,fin_product_level_3
,hpbp
,fin_g_i
--,hierarchy
,ACO_Network
,group_id
,fin_inc_month
,fin_inc_year
,0 as allowed
,0 as paid
,sum(mbrs) as member_cnt
from tmp_1m.cl_synapse_caprate_membership
group by
market_expansion
,case when market_expansion in ('GA', 'NC', 'AL', 'SC', 'TN', 'VA', 'GA-CSNP', 'AL-CSNP', 'SC-CSNP', 'TN-CSNP', 'VA-CSNP') then 'exist' else 'new' end
,fin_brand
,fin_market
,migration_source
,fin_product_level_3
,hpbp
,fin_g_i
--,hierarchy
,ACO_Network
,group_id
,fin_inc_month
,fin_inc_year
;
--1756248 select count(*) from tmp_1m.cl_synapse_caprate_membership_claims

select sum(allowed) from tmp_1m.cl_synapse_caprate_membership_claims --3717723357.91 2897669720.86

select * from tmp_1m.cl_synapse_caprate_membership_claims limit 2;

drop table tmp_1m.cl_synapse_caprate_membership_claims_sum
create table tmp_1m.cl_synapse_caprate_membership_claims_sum as
select
a.component
,a.market_expansion
,a.exist_new
,a.fin_market
,a.brand_fnl
,a.data_source
--,b.hpbp_expansion
--,a.hpbp as hpbp_actual
--,a.ACO_Network
--,a.group_id
,a.migration_source
--,a.fin_source_name
,a.fin_product_level_3
,a.fin_g_i
,a.fin_inc_month
,a.fin_inc_year
,sum(a.allowed) as allowed
,sum(a.paid) as paid
,sum(a.member_cnt) as mm
from tmp_1m.cl_synapse_caprate_membership_claims a
WHERE a.fin_product_level_3 NOT IN ('INSTITUTIONAL')
and market_expansion <> ('CA') --only in 2022 with less than $2,000 in claims
--left join tmp_1y.cl_synapse_hpbp_20250306_inscope b
--on a.market_fnl = b.fin_market 
--and a.hpbp = b.hpbp_actual
--and a.fst_srvc_year = b.srvc_year
group BY
a.component
,a.market_expansion
,a.exist_new
,a.fin_market
,a.brand_fnl
,a.data_source
,a.migration_source
,a.fin_product_level_3
,a.fin_g_i
,a.fin_inc_month
,a.fin_inc_year
--27284 select count(*) from tmp_1m.cl_synapse_caprate_membership_claims_sum 

select sum(allowed) from tmp_1m.cl_synapse_caprate_membership_claims_sum --$2,815,034,274

select * from tmp_1m.cl_synapse_caprate_membership_claims_sum
