select * from tmp_1m.kn_mcr_ss_join_cosmos_send
limit 100
;


select * from tmp_1m.kn_mcr_ss_join_cosmos 
limit 100
;

select mnr_sbscr_nbr, mnr_site_cd, mnr_site_clm_aud_nbr, mnr_clm_aud_nbr 
from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_clm_aud_nbr ilike '111903' 


select length(mnr_sbscr_nbr), length(mnr_clm_aud_nbr)
from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_clm_aud_nbr ilike '111903' 


select mcr_member_id, length(mcr_member_id), mcr_u_div, mcr_work_item_id_cleaned, length(mcr_work_item_id_cleaned), mcr_work_item_id
from tmp_1m.kn_mcr_202409_2025
where mcr_member_id ilike '960767229' and MCR_WORK_ITEM_ID_CLEANED ilike '111903'




-- mnr_sbscr_nbr + 2 leading 00s = 11
-- 
-- mnr_clm_aud_nbr + 4 leading 00s = 10
-- mcr_sbscr_nbr + 2 leading 00s = 11 
-- mcr_work_item_id_cleaned + 4 leading 00s = 10


select * from tmp_1m.kn_mcr_202409_2025
limit 100
;

select * from tmp_1m.kn_mcr_sent


select * from tmp_1m.kn_ss_claims_202409_2025
where mnr_sbscr_nbr ilike '47876490'


create or replace table tmp_1m.kn_mcr_ss_join_cosmos_dedup_2 as
with cte_clm_status as (
    select distinct
          mnr_sbscr_nbr
        , mnr_site_cd
        , mnr_clm_aud_nbr
        , mnr_clm_dnl_status
        , mnr_clm_pd_dt as max_clm_pd_dt
        , dense_rank() over (
            partition by mnr_sbscr_nbr, mnr_clm_aud_nbr
            order by mnr_clm_pd_dt desc
        ) as drank
    from tmp_1m.kn_mcr_ss_join_cosmos
)
select
      a.mnr_clm_aud_nbr as clm_aud_nbr
    , a.mnr_site_cd as site_cd
    , a.mnr_site_clm_aud_nbr as site_clm_aud_nbr
    , a.mnr_sbscr_nbr as sbscr_nbr
    , a.mnr_entity as entity
    , a.mnr_brand_fnl as brand_fnl
    , a.mnr_product_level_3_fnl as product_level_3_fnl
    , a.mnr_market as market
    , a.mnr_gal_mbi_hicn_fnl as gal_mbi_hicn_fnl
    , a.mnr_component as component
    , a.mnr_lcd_status as lcd_status
    , a.mnr_locationtype as locationtype
    , a.mnr_covered_unproven as covered_unproven
    , a.mnr_migration_source as migration_source
    , a.mnr_proc_cd as proc_cd
    , a.mnr_prov_prtcp_sts_cd as prov_prtcp_sts_cd
    , a.mnr_month as fst_srvc_month
    , b.mnr_clm_dnl_status
    , a.mcr_routed
    , max(b.max_clm_pd_dt) as max_clm_pd_dt
    , sum(a.mnr_allw_amt_fnl) as sum_allowed
    , sum(a.mnr_net_pd_amt_fnl) as sum_paid
    , sum(a.mnr_sbmt_chrg_amt) as sum_billed
    , count(distinct concat(a.mnr_sbscr_nbr, a.mnr_proc_cd, a.mnr_prov_tin, a.mnr_month)) as n_distinct
from tmp_1m.kn_mcr_ss_join_cosmos as a
left join cte_clm_status as b
    on a.mnr_sbscr_nbr = b.mnr_sbscr_nbr
   and a.mnr_clm_aud_nbr = b.mnr_clm_aud_nbr
   and b.drank = 1
group by
      a.mnr_clm_aud_nbr
    , a.mnr_site_cd
    , a.mnr_site_clm_aud_nbr
    , a.mnr_sbscr_nbr
    , a.mnr_entity
    , a.mnr_brand_fnl
    , a.mnr_product_level_3_fnl
    , a.mnr_market
    , a.mnr_gal_mbi_hicn_fnl
    , a.mnr_component
    , a.mnr_lcd_status
    , a.mnr_locationtype
    , a.mnr_covered_unproven
    , a.mnr_migration_source
    , a.mnr_proc_cd
    , a.mnr_prov_prtcp_sts_cd
    , a.mnr_month
    , b.mnr_clm_dnl_status
    , a.mcr_routed
 ;



select max_clm_dnl_status, sum(n_distinct) from tmp_1m.kn_mcr_ss_join_cosmos_dedup_2
group by max_clm_dnl_status
;

select * from tmp_1m.kn_mcr_ss_join_cosmos_dedup_2
where sbscr_nbr = '985723890'


select count(*) from tmp_1m.kn_mcr_ss_join_cosmos_dedup


select mnr_max_clm_dnl_status, sum(n_distinct) from tmp_1m.kn_mcr_ss_join_cosmos_dedup
group by mnr_max_clm_dnl_status
;

select * from tmp_1m.kn_mcr_ss_join_cosmos_dedup_2





create or replace table tmp_1m.kn_mcr_verify as
with cte_window_pd_dt as (
select
	a.CLM_AUD_NBR
    , mnr_site_cd as site_cd
	, mnr_site_clm_aud_nbr as site_clm_aud_nbr
    , a.sbscr_nbr
    , concat_ws('_', mnr_clm_aud_nbr, mnr_site_cd, mnr_sbscr_nbr) as claimkey
    , mnr_entity as entity
	, mnr_brand_fnl as brand_fnl
	, mnr_product_level_3_fnl as product_level_3_fnl
    , mnr_market as market
    , mnr_gal_mbi_hicn_fnl as gal_mbi_hicn_fnl
    , mnr_component as component
    , mnr_lcd_status as lcd_status
    , mnr_locationtype as locationtype
    , mnr_migration_source as migration_source
    , mnr_proc_cd as proc_cd
    , mnr_prov_prtcp_sts_cd as prov_prtcp_sts_cd
    , mnr_allw_amt_fnl as allw_amt_fnl
    , mnr_net_pd_amt_fnl as net_pd_amt_fnl
    , mnr_sbmt_chrg_amt as sbmt_chrg_amt
    , mnr_month as fst_srvc_month
    , max(mnr_clm_pd_dt) over (partition by mnr_clm_aud_nbr) as max_clm_pd_dt1
    , max(mcr_routed) over (partition by mnr_clm_aud_nbr) as max_mcr_routed1
from tmp_1m.kn_mcr_sent as a
left join tmp_1m.kn_mcr_ss_join_cosmos as b
	on a.sbscr_nbr = b.mnr_sbscr_nbr
	and a.CLM_AUD_NBR = b.mnr_clm_aud_nbr
)
select
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, claimkey
	, entity
	, brand_fnl 
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl 
	, component
	, lcd_status 
	, locationtype
	, migration_source 
	, proc_cd 
	, prov_prtcp_sts_cd 
	, fst_srvc_month
	, max(max_clm_pd_dt1) as max_clm_pd_dt
	, max(max_mcr_routed1) as max_mcr_routed
	, sum(allw_amt_fnl) as sum_allowed
	, sum(net_pd_amt_fnl) as sum_paid
	, sum(sbmt_chrg_amt) as sum_billed
	, count(distinct concat(sbscr_nbr, site_clm_aud_nbr, fst_srvc_month)) as n_distinct
from cte_window_pd_dt
group by
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, claimkey
	, entity
	, brand_fnl 
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl 
	, component
	, lcd_status 
	, locationtype
	, brand_fnl 
	, migration_source 
	, proc_cd 
	, prov_prtcp_sts_cd 
	, fst_srvc_month
;

select 
	sum(n_distinct)
from tmp_1m.kn_mcr_verify

select count(*) from tmp_1m.kn_mcr_verify


select sum(allw_amt_fnl)
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where site_clm_aud_nbr = 'STL0054754795'
-- 133,178 


select sum(allw_amt_fnl)
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where site_clm_aud_nbr = 'KLC0068171440'
-- 49,331.52


select sum(allw_amt_fnl)
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where site_clm_aud_nbr = 'KLC0067755191'
-- 84746.05


select sum(allw_amt_fnl)
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where site_clm_aud_nbr = 'KEN0072981454'
-- 110

select sum(allw_amt_fnl)
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where site_clm_aud_nbr = 'NTL0070732898'
-- 1313.69


select sum(allw_amt_fnl)
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where site_clm_aud_nbr = 'STL0050604607'
-- 187293.95



select 
	*
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where regexp_replace(site_clm_aud_nbr, '^[A-Z]+0*', '') in
(
'59938733', '61077092', '95989444', '52012675', '36932019',
'66195910', '41596716', '52265580', '77428430', '54558907',
'66193919', '67852285', '71292898', '51992294', '47876491',
'39947311', '88859693', '47876490', '29219068', '66194710',
'76208652'
)

select 
	*
from tmp_1m.kn_mcr_202409_2025 
where regexp_replace(mcr_work_item_id_cleaned, '^[A-Z]+0*', '') i
(
'59938733', '61077092', '95989444', '52012675', '36932019',
'66195910', '41596716', '52265580', '77428430', '54558907',
'66193919', '67852285', '71292898', '51992294', '47876491',
'39947311', '88859693', '47876490', '29219068', '66194710',
'76208652'
)


select sbscr_nbr from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A limit 100


select * from tmp_1m.kn_mcr_joined limit 100;

select
	*
from tmp_1m.kn_mcr_ss_join_cosmos

drop table if exists tmp_1m.kn_mcr_verify_v1;
create or replace table tmp_1m.kn_mcr_verify_v1 as
with joined as (
select
	a.*
	, b.hcemi_mcr_routed
	, b.hcemi_mcr_lstdecisionoutcome
from tmp_1m.kn_mcr_ss_join_cosmos_dedup as a
right join tmp_1m.kn_mcr_joined_v2 as b
	on a.sbscr_nbr = b.sbscr_nbr
	and a.clm_aud_nbr = b.clm_aud_nbr
)
select 
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, entity
	, brand_fnl
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl
	, component
	, lcd_status
	, locationtype
	, migration_source
	, proc_cd
	, prov_prtcp_sts_cd 
	, fst_srvc_month 
	, mcr_routed
	, mnr_max_clm_pd_dt
	, mnr_max_clm_dnl_status
	, hcemi_mcr_routed 
	, hcemi_mcr_lstdecisionoutcome
	, sum(sum_allowed) as sum_allowed
	, sum(sum_paid) as sum_paid
	, sum(sum_billed) as sum_billed
	, sum(n_distinct) as n_distinct
from joined
group by
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, entity
	, brand_fnl
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl
	, component
	, lcd_status
	, locationtype
	, migration_source
	, proc_cd
	, prov_prtcp_sts_cd 
	, fst_srvc_month 
	, mcr_routed
	, mnr_max_clm_pd_dt
	, mnr_max_clm_dnl_status
	, hcemi_mcr_routed 
	, hcemi_mcr_lstdecisionoutcome
	

select * from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_clm_aud_nbr = '65457401'


select * from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where SBSCR_NBR like '%937975211%'

select * from tmp_1m.kn_mcr_verify_v3
where SBSCR_NBR = '937975211'


select sbscr_nbr, site_clm_aud_nbr, site_cd, clm_dnl_f, allw_amt_fnl, net_pd_amt_fnl, sbmt_chrg_amt
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where SBSCR_NBR like '%937975211%' and site_clm_aud_nbr in ('NTL0065457401', 'NTL0065664837', 'NTL0065972225', 'NTL0047739350') 


	
	
	
drop table if exists tmp_1m.kn_mcr_verify_v3;
create or replace table tmp_1m.kn_mcr_verify_v3 as
with joined as (
select
	a.*
	, b.hcemi_mcr_routed
	, b.hcemi_mcr_lstdecisionoutcome
from tmp_1m.kn_mcr_ss_join_cosmos_dedup_2 as a
left join tmp_1m.kn_mcr_joined_v2 as b
	on a.sbscr_nbr = b.sbscr_nbr
	and a.clm_aud_nbr = b.clm_aud_nbr
)
select 
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, entity
	, brand_fnl
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl
	, component
	, lcd_status
	, locationtype
	, covered_unproven
	, migration_source
	, proc_cd
	, prov_prtcp_sts_cd 
	, fst_srvc_month 
	, mcr_routed
	, max_clm_pd_dt
	, mnr_clm_dnl_status
	, hcemi_mcr_routed 
	, hcemi_mcr_lstdecisionoutcome
	, sum(sum_allowed) as sum_allowed
	, sum(sum_paid) as sum_paid
	, sum(sum_billed) as sum_billed
	, sum(n_distinct) as n_distinct
from joined
group by
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, entity
	, brand_fnl
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl
	, component
	, lcd_status
	, locationtype
	, covered_unproven
	, migration_source
	, proc_cd
	, prov_prtcp_sts_cd 
	, fst_srvc_month 
	, mcr_routed
	, max_clm_pd_dt
	, mnr_clm_dnl_status
	, hcemi_mcr_routed 
	, hcemi_mcr_lstdecisionoutcome
;	
	
select * from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2a
where gal_mbi_hicn_fnl = '6M33FQ6VQ13'



select count(*) from tmp_1m.kn_mcr_verify_v3


select * from tmp_1m.kn_mcr_verify_v3
where sbscr_nbr = '985723890'

	
select * from tmp_1m.kn_mcr_verify_v1
where sum_allowed != mcr_sum_allowed

select * from tmp_1m.KN_MCR_SS_JOIN_COSMOS_DEDUP 



create or replace table tmp_1m.kn_mcr_verify_v2 as
with joined as (
select distinct
	a.*
	, b.hcemi_mcr_routed
	, b.hcemi_mcr_lstdecisionoutcome
from tmp_1m.kn_mcr_ss_join_cosmos_dedup as a
left join tmp_1m.kn_mcr_joined_v2 as b
	on a.sbscr_nbr = b.sbscr_nbr
	and a.clm_aud_nbr = b.clm_aud_nbr
)
select 
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, entity
	, brand_fnl
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl
	, component
	, lcd_status
	, locationtype
	, migration_source
	, proc_cd
	, prov_prtcp_sts_cd 
	, fst_srvc_month 
	, mcr_routed
	, mnr_max_clm_pd_dt
	, mnr_max_clm_dnl_status
	, hcemi_mcr_routed 
	, hcemi_mcr_lstdecisionoutcome
	, sum(sum_allowed) as sum_allowed
	, sum(sum_paid) as sum_paid
	, sum(sum_billed) as sum_billed
	, sum(n_distinct) as n_distinct
from joined
group by
	clm_aud_nbr
	, site_cd
	, site_clm_aud_nbr
	, sbscr_nbr
	, entity
	, brand_fnl
	, product_level_3_fnl
	, market
	, gal_mbi_hicn_fnl
	, component
	, lcd_status
	, locationtype
	, migration_source
	, proc_cd
	, prov_prtcp_sts_cd 
	, fst_srvc_month 
	, mcr_routed
	, mnr_max_clm_pd_dt
	, mnr_max_clm_dnl_status
	, hcemi_mcr_routed 
	, hcemi_mcr_lstdecisionoutcome
;



select count(*) from tmp_1m.kn_mcr_verify_v2


create or replace table tmp_1m.kn_mcr_ss_join_cosmos_notmatched as
select comparison_flag, sum(n_distinct) from

--select * from 
(select
    *
    , case 
        when not hcemi_mcr_routed and not mcr_routed then 'MCR N, MNR N'
        when not hcemi_mcr_routed and mcr_routed then 'MCR N, MNR Y'
        when hcemi_mcr_routed and mcr_routed then 'MCR Y, MNR Y'
        when hcemi_mcr_routed and not mcr_routed then 'MCR Y, MNR N'
        else 'Unknown'
    end as comparison_flag
from tmp_1m.kn_mcr_verify_v3
) as s
--where comparison_flag = 'Unknown'
group by comparison_flag



where comparison_flag not in ('MCR Y, MNR Y', 'MCR N, MNR N')
;




select * from tmp_1m.kn_mcr_joined_v2

select count(*) from tmp_1m.kn_mcr_verify_v1
-- 6837


select * from tmp_1m.kn_mcr_verify_v1




select count(*) from
(
select *
from tmp_1m.kn_mcr_ss_join_cosmos_dedup
qualify count(*) over (partition by clm_aud_nbr, proc_cd, fst_srvc_month) > 1
)
-- 865



select count(clm_aud_nbr) from tmp_1m.kn_mcr_ss_join_cosmos_dedup;
--51880
select count(distinct clm_aud_nbr) from tmp_1m.kn_mcr_ss_join_cosmos_dedup;
--40356



select * from tmp_1m.KN_MCR_JOINED_V2


select * from tmp_1m.kn_mcr_ss_join_cosmos


create or replace table tmp_1m.kn_mcr_ss_join_cosmos_notmatched as
select comparison_flag, sum(n_distinct) from
(select
    *
    , case 
        when not hcemi_mcr_routed and not mcr_routed then 'MCR N, MNR N'
        when not hcemi_mcr_routed and mcr_routed then 'MCR N, MNR Y'
        when hcemi_mcr_routed and mcr_routed then 'MCR Y, MNR Y'
        when hcemi_mcr_routed and not mcr_routed then 'MCR Y, MNR N'
        else 'Unknown'
    end as comparison_flag
from tmp_1m.kn_mcr_verify_v1
) as s
group by comparison_flag
where comparison_flag not in ('MCR Y, MNR Y', 'MCR N, MNR N')
;

select count(*) from tmp_1m.kn_mcr_ss_join_cosmos_notmatched


use secondary role all;
create or replace table tmp_1m.kn_mcr_ss_join_cosmos_notmatched_mcr as 
select 
	*
from tmp_1m.kn_mcr_ss_join_cosmos_notmatched as a
inner join cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work b
	on a.clm_aud_nbr = regexp_substr(b.work_item_id, '^[^-]+')
	and a.sbscr_nbr = b.member_id
;

select * from tmp_1m.kn_mcr_ss_join_cosmos_notmatched_mcr


with cte as (
select 
	regexp_substr(work_item_id, '^[^-]+') as work_item_id_cleaned
	, work_item_id
	, member_id
	, u_div
	, mcr_disposition_code
from cdw_prd_call_db.cdw_ewdo_mcr_base_view_sc.owr_x_uhgen_ewr_work
)
select
	*
from cte
where member_id in ('984021353')



--COMPARISON_FLAG	SUM(N_DISTINCT)
--MCR N, MNR N	4,893
--MCR Y, MNR N	98
--MCR Y, MNR Y	1,815
	
create or replace table tmp_1m.kn_mcr_verify_notmatched as
select
	*
from
(select
    *
    , case 
        when not hcemi_mcr_routed and not mcr_routed then 'MCR N, MNR N'
        when not hcemi_mcr_routed and mcr_routed then 'MCR N, MNR Y'
        when hcemi_mcr_routed and mcr_routed then 'MCR Y, MNR Y'
        when hcemi_mcr_routed and not mcr_routed then 'MCR Y, MNR N'
        else 'Unknown'
    end as comparison_flag
from tmp_1m.kn_mcr_verify
) as s
where 
	comparison_flag not in ('MCR N, MNR N', 'MCR Y, MNR Y')




select count(*) from tmp_1m.kn_mcr_verify_notmatched

select * from tmp_1m.kn_mcr_verify_notmatched




select * from tmp_1m.kn_mcr_ss_join_cosmos limit 100;




select count(distinct mnr_clm_aud_nbr) from tmp_1m.kn_mcr_ss_join_cosmos
-- 18,780


select count(distinct clm_aud_nbr) from tmp_1m.kn_mcr_joined
-- 6,823



