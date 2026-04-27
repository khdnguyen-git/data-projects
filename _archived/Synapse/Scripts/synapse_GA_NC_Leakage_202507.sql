/*  GA and NC Leakage Reconciliation 

CAP Rate build up using data post Synapse data extract where HPBPs have been internally finalized
Notes when develop cap rate pmpm for an expansion market:
	1. check if the membership pulled using fin_market is reasonably close to fin_state; if so, use fin_market; if not, call a meeting
	2. check claims for ACO vs non_ACO to make sure cap rate for ACO is not lower than non_ACO
	3. for existing markets (after the contract has been signed), filter membership and claims data using HPBP/GroupNumber (omit state altogether)
*/

--HPBP list provided by Beth Ann on 3/6/2025
select * from tmp_1y.cl_synapse_exp_hpbp_20250306;  --for expansion markets  (68)
select * from tmp_1y.cl_synapse_ga_nc_hpbp_grp;     --for existing market (82) --compiled mannually from the pdf in the Documentation folder


--#########   membership  #########
drop table tmp_1m.kn_synapse_GA_NC_mbi;
create table tmp_1m.kn_synapse_GA_NC_mbi stored as orc as 
select
	a.fin_contractpbp as hpbp
	,case when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP' else a.fin_market end as market_expansion
	,a.fin_product_level_3
	,b.fin_state as market
	,a.migration_source
	,a.fin_tfm_product_new
	,a.hierarchy
	,a.acp_network_name as ACO_Network
	,a.gal_cust_seg_nbr
	,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	  else substr(a.gal_cust_seg_nbr,5,5) END AS group_id
	,a.fin_inc_month
	,a.fin_inc_year
	,a.prov_tin_fnl as prov_tin
	,count(distinct fin_mbi_hicn_fnl) as mbrs
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_f_202507 a
join tmp_1y.cl_synapse_ga_nc_hpbp_grp b
on     a.fin_contractpbp = b.fin_contractpbp 
   and a.gal_cust_seg_nbr = b.grp 
   and a.fin_inc_year = b.srvc_year
where 
	a.fin_source_name = 'COSMOS'
	--and migration_source <> 'OAH'
    and a.global_cap='NA'
    and a.fin_inc_month >= 202409
    and a.fin_g_i = 'I'
    and a.fin_product_level_3 not in ('INSTITUTIONAL' , 'CHRONIC', 'DUAL')
group by
	a.fin_contractpbp
	,case when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP' else a.fin_market end
	,a.fin_product_level_3
	,b.fin_state
	,a.migration_source
	,a.fin_tfm_product_new
	,a.hierarchy
	,a.acp_network_name
	,a.gal_cust_seg_nbr
	,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	  else substr(a.gal_cust_seg_nbr,5,5) END 
	,a.fin_inc_month
	,a.fin_inc_year
	,a.prov_tin_fnl
;    


describe tadm_tre_cpy.GL_RSTD_GPSGALNCE_f_202507;

describe fichsrv.tre_membership;

select gal_prov_tin, count(*) from fichsrv.tre_membership
where gal_prov_tin in
('043743987'
,'264279439'
,'351162212'
,'383294263'
,'391735285'
,'450510425'
,'454134007'
,'460517917'
,'562041876'
,'570941194'
,'581612905'
,'581961019'
,'592852900'
,'593493196'
,'593758416'
,'621298835'
,'621474680'
,'621578458'
,'621556783'
,'621578458'
,'621637225'
,'621702327'
,'621722026'
,'621736987'
,'631158411'
,'651118475'
,'710457738'
,'742775696'
,'752236468'
,'752306849'
,'841228046'
)
and fin_inc_year >= '202401'
group by gal_prov_tin;


--6849 5414  4736  select count(*) from tmp_1m.kn_synapse_GA_NC_mbi
--select * from tmp_1m.kn_synapse_GA_NC_mbi

drop table tmp_1m.kn_mbi_ga_nc_aco;
create table tmp_1m.kn_mbi_ga_nc_aco as
select DISTINCT  
	a.fin_mbi_hicn_fnl
	,a.fin_inc_month
	,a.hierarchy
	,a.acp_network_number
	,a.acp_network_name 
	,a.gal_cust_seg_nbr
	,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	  else substr(a.gal_cust_seg_nbr,5,5) END AS group_id
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_f_202507 a
join tmp_1y.cl_synapse_ga_nc_hpbp_grp b
on     a.fin_contractpbp = b.fin_contractpbp 
   and a.gal_cust_seg_nbr = b.grp 
   and a.fin_inc_year = b.srvc_year
where 
	a.fin_source_name = 'COSMOS'
	--and migration_source <> 'OAH'
    and a.global_cap='NA'
    and a.fin_inc_month >= 202409
    and a.fin_g_i = 'I'
    and a.fin_product_level_3 not in ('INSTITUTIONAL' , 'CHRONIC', 'DUAL')
;




describe tmp_1y.cl_synapse_ga_nc_hpbp_grp

describe tmp_1m.kn_mbi_ga_nc_aco;

describe tmp_1m.kn_synapse_ga_nc_claims;

select distinct
	prov_tin
from tmp_1m.kn_synapse_ga_nc_claims
where prov_tin in 
('043743987'
,'264279439'
,'351162212'
,'383294263'
,'391735285'
,'450510425'
,'454134007'
,'460517917'
,'562041876'
,'570941194'
,'581612905'
,'581961019'
,'592852900'
,'593493196'
,'593758416'
,'621298835'
,'621474680'
,'621578458'
,'621556783'
,'621578458'
,'621637225'
,'621702327'
,'621722026'
,'621736987'
,'631158411'
,'651118475'
,'710457738'
,'742775696'
,'752236468'
,'752306849'
,'841228046'
)


select count(*) from tmp_1m.kn_mbi_ga_nc_aco;   -- 2,809,492 2,046,411  1531518

select count(distinct fin_mbi_hicn_fnl ||  fin_inc_month) from tmp_1m.kn_mbi_ga_nc_aco;  -- 2809492 2046411  1531518 


--##########  claims  ##############################################
--filter claims for DME, hcpcs; bring in aco, group id
drop table tmp_1m.kn_synapse_ga_nc_claims;
create table tmp_1m.kn_synapse_ga_nc_claims stored as orc As
SELECT
	case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP' else a.market_fnl end as market_expansion
	,a.contractpbp_fnl as hpbp
	,a.market_fnl
	,a.migration_source
	,a.GROUP_IND_FNL
	,a.hce_service_code
	,a.proc_cd
	,a.prov_tin
	,a.full_nm
--	,c.hierarchy
	,c.acp_network_name as ACO_Network
	,c.gal_cust_seg_nbr
	,c.group_id
    ,a.fst_srvc_month
    ,a.fst_srvc_year
	,sum(a.allw_amt_fnl) as allowed_amt
	,sum(a.net_pd_amt_fnl) as paid_amt
from fichsrv.cosmos_op a
join tmp_1y.cl_synapse_hcpcs_20250212 b    --676 (removed the 2 codes for cgm supplies K0553, K0554) 
	on a.proc_cd = b.hcpcs
join tmp_1m.kn_mbi_ga_nc_aco c   --select * from tmp_1m.kn_mbi_ga_nc_aco
	on  a.gal_mbi_hicn_fnl = c.fin_mbi_hicn_fnl 
	and a.fst_srvc_month = c.fin_inc_month	
where
	a.global_cap = 'NA'
	--and a.migration_source <> 'OAH'
	and a.GROUP_IND_FNL = 'I'
--    and a.hce_service_code = 'OP_DMESUP'  --removed on 5/22/2025
    and a.fst_srvc_month >= 202409
--    and (a.allw_amt_fnl <> 0 or a.tadm_units <> 0)
   	and a.product_level_3_fnl not in ('INSTITUTIONAL' , 'CHRONIC', 'DUAL')
group by
	case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP' else a.market_fnl end
	,a.contractpbp_fnl
	,a.market_fnl
	,a.migration_source
	,a.GROUP_IND_FNL
	,a.hce_service_code
	,a.proc_cd
	,a.prov_tin
	,a.full_nm
--	,c.hierarchy
	,c.acp_network_name 
	,c.gal_cust_seg_nbr
	,c.group_id
    ,a.fst_srvc_month
    ,a.fst_srvc_year
;
--68868           select count(*) from tmp_1m.kn_synapse_ga_nc_claims
-- 95661
SELECT sum(allowed_amt) FROM   tmp_1m.kn_synapse_ga_nc_claims where market_expansion = 'GA' and  hpbp = 'H1889-013-000' and fst_srvc_month = '202409';		--116006.15 111961.82  111053.34

--union claims and membership data for cap rate build up excel file
drop table tmp_1m.kn_synapse_ga_nc_membership_claims; 
create table tmp_1m.kn_synapse_ga_nc_membership_claims  as
select 
	'Claims' as component
	,b.fin_state as recon_market
	,market_expansion as fin_market
	,migration_source
	,hpbp
	,group_id
	--,gal_cust_seg_nbr
	,prov_tin
	,full_nm
	,hce_service_code
	,proc_cd
	--,hierarchy
	,ACO_Network	
	,fst_srvc_month
	,fst_srvc_year
	,sum(allowed_amt) as allowed
	,sum(paid_amt) as paid
	,0 as member_cnt
from tmp_1m.kn_synapse_ga_nc_claims a
join tmp_1y.cl_synapse_ga_nc_hpbp_grp b
on      a.hpbp = b.fin_contractpbp
 	and a.gal_cust_seg_nbr  = b.grp
	and a.fst_srvc_year = b.srvc_year
group by
    b.fin_state
	,market_expansion
	,migration_source
	,hpbp
	,group_id
	--,gal_cust_seg_nbr
	,prov_tin
	,full_nm
	,hce_service_code
	,proc_cd
	--,hierarchy
	,ACO_Network	
	,fst_srvc_month
	,fst_srvc_year
union all
select
	'Membership' as component
	,market as recon_market
	,market_expansion as fin_market
	,migration_source
	,hpbp
	,group_id
	--,gal_cust_seg_nbr
	,prov_tin
	,'' as full_nm
	,'' as hce_service_code
	,'' as proc_cd
	--,hierarchy
	,ACO_Network
	,fin_inc_month
	,fin_inc_year
    ,0 as allowed
    ,0 as paid
    ,sum(mbrs) as member_cnt
from tmp_1m.kn_synapse_GA_NC_mbi
group by
	market
	,market_expansion
	,migration_source
	,hpbp
	,group_id
	,prov_tin
	--,gal_cust_seg_nbr
	--,hierarchy
	,ACO_Network
	,fin_inc_month
	,fin_inc_year
;
--72481  23987  18510    select count(*) from tmp_1m.kn_synapse_ga_nc_membership_claims

SELECT sum(allowed), sum(member_cnt) 
FROM   tmp_1m.kn_synapse_ga_nc_membership_claims
where fin_market = 'GA' and  hpbp = 'H1889-013-000' and fst_srvc_month = '202409';
/*
allowed		member_cnt
111,961.82	32,300    <<< where DMESUP
116006.15	32300	  <<< remove DMESUP condition	

-- 202507
114364.52	32305


*/

select * from tmp_1m.kn_synapse_ga_nc_membership_claims;

drop table tmp_1m.kn_synapse_ga_nc_lincare;
create table tmp_1m.kn_synapse_ga_nc_lincare as
select
	*
from tmp_1m.kn_synapse_ga_nc_membership_claims
where prov_tin in 
('043743987'
,'264279439'
,'351162212'
,'383294263'
,'391735285'
,'450510425'
,'454134007'
,'460517917'
,'562041876'
,'570941194'
,'581612905'
,'581961019'
,'592852900'
,'593493196'
,'593758416'
,'621298835'
,'621474680'
,'621578458'
,'621556783'
,'621578458'
,'621637225'
,'621702327'
,'621722026'
,'621736987'
,'631158411'
,'651118475'
,'710457738'
,'742775696'
,'752236468'
,'752306849'
,'841228046'
)

select count(*) from tmp_1m.kn_synapse_ga_nc_lincare;

select count(distinct prov_tin) from tmp_1m.kn_synapse_ga_nc_lincare;



-- With DMESUP;

/*  GA and NC Leakage Reconciliation 

CAP Rate build up using data post Synapse data extract where HPBPs have been internally finalized
Notes when develop cap rate pmpm for an expansion market:
	1. check if the membership pulled using fin_market is reasonably close to fin_state; if so, use fin_market; if not, call a meeting
	2. check claims for ACO vs non_ACO to make sure cap rate for ACO is not lower than non_ACO
	3. for existing markets (after the contract has been signed), filter membership and claims data using HPBP/GroupNumber (omit state altogether)
*/

--HPBP list provided by Beth Ann on 3/6/2025
select * from tmp_1y.cl_synapse_exp_hpbp_20250306  --for expansion markets  (68)
select * from tmp_1y.cl_synapse_ga_nc_hpbp_grp     --for existing market (82) --compiled mannually from the pdf in the Documentation folder


--#########   membership  #########
drop table tmp_1m.kn_synapse_GA_NC_mbi;
create table tmp_1m.kn_synapse_GA_NC_mbi stored as orc as 
select
	a.fin_contractpbp as hpbp
	,case when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP' else a.fin_market end as market_expansion
	,a.fin_product_level_3
	,b.fin_state as market
	,a.migration_source
	,a.fin_tfm_product_new
	,a.hierarchy
	,a.acp_network_name as ACO_Network
	,a.gal_cust_seg_nbr
	,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	  else substr(a.gal_cust_seg_nbr,5,5) END AS group_id
	,a.fin_inc_month
	,a.fin_inc_year
	,count(distinct fin_mbi_hicn_fnl) as mbrs
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202504 a
join tmp_1y.cl_synapse_ga_nc_hpbp_grp b
on     a.fin_contractpbp = b.fin_contractpbp 
   and a.gal_cust_seg_nbr = b.grp 
   and a.fin_inc_year = b.srvc_year
where 
	a.fin_source_name = 'COSMOS'
	--and migration_source <> 'OAH'
    and a.global_cap='NA'
    and a.fin_inc_month >= 202409
    and a.fin_g_i = 'I'
    and a.fin_product_level_3 not in ('INSTITUTIONAL' , 'CHRONIC', 'DUAL')
group by
	a.fin_contractpbp
	,case when a.fin_product_level_3 = 'CHRONIC' then a.fin_market ||'-CSNP' else a.fin_market end
	,a.fin_product_level_3
	,b.fin_state
	,a.migration_source
	,a.fin_tfm_product_new
	,a.hierarchy
	,a.acp_network_name
	,a.gal_cust_seg_nbr
	,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	  else substr(a.gal_cust_seg_nbr,5,5) END 
	,a.fin_inc_month
	,a.fin_inc_year
;    
--5414  4736  select count(*) from tmp_1m.kn_synapse_GA_NC_mbi
--select * from tmp_1m.kn_synapse_GA_NC_mbi

drop table tmp_1m.kn_mbi_ga_nc_aco;
create table tmp_1m.kn_mbi_ga_nc_aco as
select DISTINCT  
	a.fin_mbi_hicn_fnl
	,a.fin_inc_month
	,a.hierarchy
	,a.acp_network_number
	,a.acp_network_name 
	,a.gal_cust_seg_nbr
	,case when a.fin_tfm_product_new = 'NICE HMO' then a.nce_purchaser_id || '-' || substr(a.nce_src_sys_mdcl_pln_id,3,3) 
     	  else substr(a.gal_cust_seg_nbr,5,5) END AS group_id
from tadm_tre_cpy.GL_RSTD_GPSGALNCE_F_202504 a
join tmp_1y.cl_synapse_ga_nc_hpbp_grp b
on     a.fin_contractpbp = b.fin_contractpbp 
   and a.gal_cust_seg_nbr = b.grp 
   and a.fin_inc_year = b.srvc_year
where 
	a.fin_source_name = 'COSMOS'
	--and migration_source <> 'OAH'
    and a.global_cap='NA'
    and a.fin_inc_month >= 202409
    and a.fin_g_i = 'I'
    and a.fin_product_level_3 not in ('INSTITUTIONAL' , 'CHRONIC', 'DUAL')
;

select count(*) from tmp_1m.kn_mbi_ga_nc_aco   --2046411  1531518

select count(distinct fin_mbi_hicn_fnl ||  fin_inc_month) from tmp_1m.kn_mbi_ga_nc_aco  --2046411  1531518 


--##########  claims  ##############################################
--filter claims for DME, hcpcs; bring in aco, group id
drop table tmp_1m.kn_synapse_ga_nc_claims;
create table tmp_1m.kn_synapse_ga_nc_claims stored as orc As
SELECT
	case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP' else a.market_fnl end as market_expansion
	,a.contractpbp_fnl as hpbp
	,a.market_fnl
	,a.migration_source
	,a.GROUP_IND_FNL
	,a.prov_tin
	,a.full_nm
	,c.hierarchy
	,c.acp_network_name as ACO_Network
	,c.gal_cust_seg_nbr
	,c.group_id
    ,a.fst_srvc_month
    ,a.fst_srvc_year
	,sum(a.allw_amt_fnl) as allowed_amt
	,sum(a.net_pd_amt_fnl) as paid_amt
from fichsrv.cosmos_op a
join tmp_1y.cl_synapse_hcpcs_20250212 b    --676 (removed the 2 codes for cgm supplies K0553, K0554) 
	on a.proc_cd = b.hcpcs
join tmp_1m.kn_mbi_ga_nc_aco c   --select * from tmp_1m.kn_mbi_ga_nc_aco
	on  a.gal_mbi_hicn_fnl = c.fin_mbi_hicn_fnl 
	and a.fst_srvc_month = c.fin_inc_month	
where
	a.global_cap = 'NA'
	--and a.migration_source <> 'OAH'
	and a.GROUP_IND_FNL = 'I'
    and a.hce_service_code = 'OP_DMESUP'
    and a.fst_srvc_month >= 202409
--    and (a.allw_amt_fnl <> 0 or a.tadm_units <> 0)
   	and a.product_level_3_fnl not in ('INSTITUTIONAL' , 'CHRONIC', 'DUAL')
group by
	case when a.product_level_3_fnl = 'CHRONIC' then a.market_fnl ||'-CSNP' else a.market_fnl end
	,a.contractpbp_fnl
	,a.market_fnl
	,a.migration_source
	,a.GROUP_IND_FNL
	,a.prov_tin
	,a.full_nm
	,c.hierarchy
	,c.acp_network_name 
	,c.gal_cust_seg_nbr
	,c.group_id
    ,a.fst_srvc_month
    ,a.fst_srvc_year
;
--18573   44128           select count(*) from tmp_1m.kn_synapse_ga_nc_claims

SELECT sum(allowed_amt) FROM   tmp_1m.kn_synapse_ga_nc_claims where market_expansion = 'GA' and  hpbp = 'H1889-013-000' and fst_srvc_month = '202409'		--111961.82  111053.34

--union claims and membership data for cap rate build up excel file
drop table tmp_1m.kn_synapse_ga_nc_membership_claims; 
create table tmp_1m.kn_synapse_ga_nc_membership_claims  as
select 
	'Claims' as component
	,b.fin_state as recon_market
	,market_expansion as fin_market
	,migration_source
	,hpbp
	,group_id
	,gal_cust_seg_nbr
	,prov_tin
	,full_nm
	,hierarchy
	,ACO_Network	
	,fst_srvc_month
	,fst_srvc_year
	,sum(allowed_amt) as allowed
	,sum(paid_amt) as paid
	,0 as member_cnt
from tmp_1m.kn_synapse_ga_nc_claims a
join tmp_1y.cl_synapse_ga_nc_hpbp_grp b
on      a.hpbp = b.fin_contractpbp
 	and a.gal_cust_seg_nbr  = b.grp
	and a.fst_srvc_year = b.srvc_year
group by
    b.fin_state
	,market_expansion
	,migration_source
	,hpbp
	,group_id
	,gal_cust_seg_nbr
	,prov_tin
	,full_nm
	,hierarchy
	,ACO_Network	
	,fst_srvc_month
	,fst_srvc_year
union all
select
	'Membership' as component
	,market as recon_market
	,market_expansion as fin_market
	,migration_source
	,hpbp
	,group_id
	,gal_cust_seg_nbr
	,'' as prov_tin
	,'' as full_nm
	,hierarchy
	,ACO_Network
	,fin_inc_month
	,fin_inc_year
    ,0 as allowed
    ,0 as paid
    ,sum(mbrs) as member_cnt
from tmp_1m.kn_synapse_GA_NC_mbi
group by
	market
	,market_expansion
	,migration_source
	,hpbp
	,group_id
	,gal_cust_seg_nbr
	,hierarchy
	,ACO_Network
	,fin_inc_month
	,fin_inc_year
;
--23987  18510    select count(*) from tmp_1m.kn_synapse_ga_nc_membership_claims

SELECT sum(allowed), sum(member_cnt) 
FROM   tmp_1m.kn_synapse_ga_nc_membership_claims
where fin_market = 'GA' and  hpbp = 'H1889-013-000' and fst_srvc_month = '202409'
/*
allowed		member_cnt
111,961.82	32,300
*/

select * from tmp_1m.kn_synapse_ga_nc_membership_claims


































