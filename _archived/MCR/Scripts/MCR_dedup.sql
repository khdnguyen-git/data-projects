/*==============================================================================
 * Verify received data with ours
 *================================================================*/

create or replace table tmp_1m.kn_mcr_verify as 
select
    a.sbscr_nbr
	, a.clm_aud_nbr
	, a.gal_mbi_hicn_fnl
 	, a.site_clm_aud_nbr
 	, a.site_cd
 	, a.entity
 	, a.component
 	, a.lcd_status
 	, a.locationtype
 	, a.proc_cd
 	, b.mnr_prov_tin as prov_tin
 	, a.fst_srvc_month
 	, b.mcr_routed as MnR_Routed
 	, b.mnr_covered_unproven
 	, a.hcemi_mcr_routed as MI_Routed
 	, a.hcemi_mcr_lstdecisioned
 	, a.hcemi_mcr_lstdecisionoutcome
 	, iff(sum_allowed > 0.01, 'Paid', 'Denied') as clm_status
    , a.max_clm_pd_dt
	, a.sum_allowed
	, a.sum_paid
	, a.sum_billed
	, count(distinct concat(mnr_sbscr_nbr, mnr_prov_tin, mnr_month, mnr_proc_cd)) as n_distinct
from tmp_1m.kn_mcr_joined_v2 as a -- Received again from HCEMI
left join tmp_1m.kn_mcr_ss_join_cosmos as b
	on a.sbscr_nbr = b.mnr_sbscr_nbr
	and a.clm_aud_nbr = b.mnr_clm_aud_nbr
group by
    a.sbscr_nbr
	, a.clm_aud_nbr
	, a.gal_mbi_hicn_fnl
 	, a.site_clm_aud_nbr
 	, a.site_cd
 	, a.entity
 	, a.component
 	, a.lcd_status
 	, a.locationtype
 	, a.proc_cd
 	, b.mnr_prov_tin 
 	, a.fst_srvc_month
 	, b.mcr_routed
 	, b.mnr_covered_unproven
 	, a.hcemi_mcr_routed 
 	, a.hcemi_mcr_lstdecisioned
 	, a.hcemi_mcr_lstdecisionoutcome
 	, iff(sum_allowed > 0.01, 'Paid', 'Denied')
    , a.max_clm_pd_dt
	, a.sum_allowed
	, a.sum_paid
	, a.sum_billed
;


create or replace table tmp_1m.kn_mcr_verify_cte as 
with joined as (
    select
        a.*
        , b.mnr_sbscr_nbr
        , b.mnr_month
        , b.mnr_proc_cd
        , b.mnr_prov_tin as prov_tin
        , b.mcr_routed as MnR_Routed
        , b.mnr_covered_unproven
    from tmp_1m.kn_mcr_joined_v2 as a
    left join tmp_1m.kn_mcr_ss_join_cosmos as b
        on a.sbscr_nbr = b.mnr_sbscr_nbr
        and a.clm_aud_nbr = b.mnr_clm_aud_nbr
)
select distinct
    sbscr_nbr
    , clm_aud_nbr
    , gal_mbi_hicn_fnl
    , site_clm_aud_nbr
    , site_cd
    , entity
    , component
    , lcd_status
    , locationtype
    , proc_cd
    , prov_tin
    , fst_srvc_month
    , MnR_Routed
    , mnr_covered_unproven
    , hcemi_mcr_routed as mi_routed
    , hcemi_mcr_lstdecisioned
    , hcemi_mcr_lstdecisionoutcome
    , iff(sum(sum_allowed) > 0.01, 'Paid', 'Denied') as clm_status
    , max(max_clm_pd_dt) as max_clm_pd_dt
    , sum(sum_allowed) as sum_allowed
    , sum(sum_paid) as sum_paid
    , sum(sum_billed) as sum_billed
    , count(distinct concat(mnr_sbscr_nbr, prov_tin, mnr_month, mnr_proc_cd)) as n_distinct
from joined
group by
    sbscr_nbr
    , clm_aud_nbr
    , gal_mbi_hicn_fnl
    , site_clm_aud_nbr
    , site_cd
    , entity
    , component
    , lcd_status
    , locationtype
    , proc_cd
    , prov_tin
    , fst_srvc_month
    , MnR_Routed
    , mnr_covered_unproven
    , hcemi_mcr_routed
    , hcemi_mcr_lstdecisioned
    , hcemi_mcr_lstdecisionoutcome
;


select * from tmp_1m.kn_mcr_joined_v2 limit 1

-- Check routed split
select 
	mnr_routed
	, mi_routed
	, sum(n_distinct)
from tmp_1m.kn_mcr_verify
group by 1,2
;

select 
	mnr_routed
	, mi_routed
	, sum(n_distinct)
from tmp_1m.kn_mcr_verify_cte
group by 1,2
;

select fst_srvc_month, sum(sum_allowed) 
from tmp_1m.kn_mcr_verify
where fst_srvc_month >= '202505' and mi_routed = 'N' and lcd_status = 'Non-LCD' and clm_status = 'Paid'
group by 1


select fst_srvc_month, sum(sum_allowed) 
from tmp_1m.kn_mcr_verify_cte
where fst_srvc_month >= '202505' and mi_routed = 'N' and lcd_status = 'Non-LCD' and clm_status = 'Paid'
group by 1

select
	sbscr_nbr
	, site_clm_aud_nbr
	, clm_dnl_f
	, sum_allowed
	, sum_paid
	, sum_billed
from tmp_1m.kn_mcr_verify
where sbscr_nbr in ('126749215', '121568974', '935951163')

-- Denied claims that were paid???
select
	sbscr_nbr
	, clm_aud_nbr
	, clm_dnl_f
	, sum_allowed
	, sum_paid
	, sum_billed
from tmp_1m.kn_mcr_verify
where clm_dnl_f not in ('N', 'P')
qualify sum(sum_allowed) over (partition by sbscr_nbr, clm_aud_nbr) > 0 



select
	sbscr_nbr
	, site_clm_aud_nbr
	, regexp_replace(site_clm_aud_nbr, '^[A-Z]+0*', '') as clm_aud_nbr
	, clm_dnl_f
	, allw_amt_fnl
	, net_pd_amt_fnl
	, sbmt_chrg_amt 
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where regexp_replace(site_clm_aud_nbr, '^[A-Z]+0*', '') in ('26498514', '47427847', '59938733')
qualify sum(allw_amt_fnl) over (partition by sbscr_nbr, site_clm_aud_nbr) > 0 





/*==============================================================================
 * Bring MI_Routed in
 *==============================================================================*/
create or replace table tmp_1m.kn_mcr_verify_v2 as
select
    a.mnr_sbscr_nbr as sbscr_nbr
	, a.mnr_clm_aud_nbr as clm_aud_nbr
 	, a.mnr_site_clm_aud_nbr as site_clm_aud_nbr
 	, a.mnr_site_cd as site_cd
 	, a.mnr_entity as entity
 	, a.mnr_component as component
 	, a.mnr_lcd_status as lcd_status
 	, a.mnr_locationtype as locationtype
 	, a.mnr_proc_cd as proc_cd
 	, a.mnr_prov_tin as prov_tin
 	, a.mnr_prov_prtcp_sts_cd as prov_par_status
 	, a.mnr_month as fst_srvc_month
 	, a.mnr_clm_dnl_f as clm_dnl_f
 	, a.mnr_clm_dnl_status as clm_dnl_status
 	, a.mcr_routed as MnR_Routed
 	, b.hcemi_mcr_routed as MI_Routed
 	, a.mcr_decision as MCR_Decision
 	, a.mcr_resolved_at as MCR_decision_date
 	, b.hcemi_mcr_lstdecisioned as MI_Decision_flag
 	, b.hcemi_mcr_lstdecisionoutcome as MI_Decision
 	, max(mnr_clm_pd_dt) as max_clm_pd_dt
 	, sum(mnr_sbmt_chrg_amt) as sum_billed
 	, sum(mnr_allw_amt_fnl) as sum_allowed
	, sum(mnr_net_pd_amt_fnl) as sum_paid
	, count(distinct concat(mnr_sbscr_nbr, mnr_site_clm_aud_nbr, mnr_month, mnr_proc_cd)) as n_distinct
from tmp_1m.kn_mcr_ss_join_cosmos as a
left join tmp_1m.kn_mcr_joined_v2 as b
	on a.mnr_sbscr_nbr = b.sbscr_nbr 
	and a.mnr_clm_aud_nbr = b.clm_aud_nbr
	and a.mnr_month = b.fst_srvc_month
group by
   a.mnr_sbscr_nbr
	, a.mnr_clm_aud_nbr 
 	, a.mnr_site_clm_aud_nbr 
 	, a.mnr_site_cd 
 	, a.mnr_entity
 	, a.mnr_component 
 	, a.mnr_lcd_status 
 	, a.mnr_locationtype 
 	, a.mnr_proc_cd 
 	, a.mnr_prov_tin 
 	, a.mnr_prov_prtcp_sts_cd 
 	, a.mnr_month 
 	, a.mnr_clm_dnl_f 
 	, a.mnr_clm_dnl_status
 	, a.mcr_routed
 	, b.hcemi_mcr_routed 
 	, a.mcr_decision 
 	, a.mcr_resolved_at
 	, b.hcemi_mcr_lstdecisioned 
 	, b.hcemi_mcr_lstdecisionoutcome
;

select 
	mnr_routed
	, mi_routed
	, sum(n_distinct)
from tmp_1m.kn_mcr_verify_v2
group by 1,2
;



select 
	mnr_routed
	, mi_routed
	, sum(n_distinct)
from tmp_1m.kn_mcr_verify
group by 1,2
;


/*==============================================================================
 * Deduplication
 *==============================================================================*/
-- Exhibit 1: same clm_aud_nbr, same clm_pd_dt
select
	mnr_sbscr_nbr
	, mnr_site_clm_aud_nbr
	, mnr_clm_aud_nbr
	, mnr_site_cd
	, mnr_month
	, mnr_proc_cd
	, mnr_prov_tin
	, mnr_locationtype
	, mnr_lcd_status
	, mcr_routed
	, mnr_clm_pd_dt
	, mnr_clm_dnl_f
	, mnr_clm_dnl_status
	, mnr_sbmt_chrg_amt
	, mnr_allw_amt_fnl
	, mnr_net_pd_amt_fnl
	, mnr_adj_srvc_unit_cnt
	, mnr_tadm_units
from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_SBSCR_NBR = '937975211' -- and mnr_clm_aud_nbr = '51128274' 
order by mnr_clm_aud_nbr, mnr_month, mnr_clm_pd_dt 
;

-- Exhibit 2: same clm_aud_nbr, same clm_pd_dt, different clm_dnl_f
select
	mnr_sbscr_nbr
	, mnr_site_clm_aud_nbr
	, mnr_clm_aud_nbr
	, mnr_site_cd
	, mnr_month
	, mnr_proc_cd
	, mnr_prov_tin
	, mnr_locationtype
	, mnr_lcd_status
	, mcr_routed
	, mnr_clm_pd_dt
	, mnr_clm_dnl_f
	, mnr_clm_dnl_status
	, mnr_sbmt_chrg_amt
	, mnr_allw_amt_fnl
	, mnr_net_pd_amt_fnl
	, mnr_adj_srvc_unit_cnt
	, mnr_tadm_units
from tmp_1m.kn_mcr_ss_join_cosmos
where mnr_clm_aud_nbr in ('51128274', '65457401', '65664837')
order by mnr_clm_aud_nbr, mnr_month, mnr_clm_pd_dt, mnr_clm_dnl_f
;


-- From original ss table
select
	sbscr_nbr
	, site_clm_aud_nbr
	, regexp_replace(site_clm_aud_nbr, '^[A-Z]+0*', '') as clm_aud_nbr
	, site_cd
	, mth
	, proc_cd
	, prov_tin
	, locationtype
	, component
	, clm_pd_dt
	, clm_dnl_f
	, sbmt_chrg_amt
	, allw_amt_fnl
	, net_pd_amt_fnl
	, adj_srvc_unit_cnt
	, tadm_units
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where SBSCR_NBR = '00937975211' and site_clm_aud_nbr in ('NTL0065457401', 'NTL0065664837', 'NTL0051128274') 
order by clm_aud_nbr, mth, clm_pd_dt, clm_dnl_f
;

select * from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where SBSCR_NBR = '00937975211' and site_clm_aud_nbr in ('NTL0065457401', 'NTL0065664837', 'NTL0051128274') 
limit 500

select adjd_dt,site_clm_aud_nbr,dtl_ln_nbr,clm_rec_cd,FNL_RSN_CD_SYS_ID,proc_cd, PROC_MOD1_CD, PROC_MOD1_DESC, proc_mod2_cd, proc_mod2_desc, clm_dnl_f, allw_amt_fnl, * from fichsrv.cosmos_pr
where site_clm_aud_nbr = 'NTL0051128274'

select adjd_dt,site_clm_aud_nbr,dtl_ln_nbr,clm_rec_cd,FNL_RSN_CD_SYS_ID,proc_cd, PROC_MOD1_CD, PROC_MOD1_DESC, proc_mod2_cd, proc_mod2_desc, clm_dnl_f, allw_amt_fnl, * from fichsrv.cosmos_pr
where site_clm_aud_nbr = 'NTL0065664837'


select adjd_dt,site_clm_aud_nbr,dtl_ln_nbr,clm_rec_cd,FNL_RSN_CD_SYS_ID,proc_cd, PROC_MOD1_CD, PROC_MOD1_DESC, proc_mod2_cd, proc_mod2_desc, clm_dnl_f, allw_amt_fnl, * from fichsrv.cosmos_pr
where site_clm_aud_nbr = 'STL0050604607'

select adjd_dt,site_clm_aud_nbr,dtl_ln_nbr,clm_rec_cd,FNL_RSN_CD_SYS_ID,proc_cd, PROC_MOD1_CD, PROC_MOD1_DESC, proc_mod2_cd, proc_mod2_desc, clm_dnl_f, allw_amt_fnl, * from fichsrv.cosmos_pr
where site_clm_aud_nbr = 'STL0050604606'

select adjd_dt, fst_srvc_dt, site_clm_aud_nbr,dtl_ln_nbr,clm_rec_cd,FNL_RSN_CD_SYS_ID,proc_cd, PROC_MOD1_CD, PROC_MOD1_DESC, proc_mod2_cd, proc_mod2_desc, clm_dnl_f, allw_amt_fnl, * from fichsrv.cosmos_pr
where site_clm_aud_nbr in ('STL0050604606', 'STL0050604607')

select distinct proc_cd from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A 
where proc_cd = 'Q4303'


select gal_mbi_hicn_fnl from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A
where sbscr_nbr = '00996625601'

select
	sum(allw_amt_fnl)
from tmp_1y.CL_SS_CLAIMS_OP_PR_COSMOS_SMART_NICE_202CODES_2A
where site_clm_aud_nbr = 'STL0050604606'
	and mth = 202505


select * from fichsrv.cosmos_pr limit 2

select
	*
from hce_ops_fnl.HCE_ADR_AVTAR_LIKE_24_25_F 


select proc_mod1_cd, clm_dnl_f, count(distinct site_clm_aud_nbr), sum(allw_amt_fnl)
from fichsrv.cosmos_pr
where proc_mod1_cd = 'JW' and proc_cd in ('Q4303','Q4239', 'Q4271', 'Q4289', 'Q4304')
group by proc_mod1_cd, clm_dnl_f





where
	FIN_MBI_HICN_FNL = '7QQ2CQ3QQ83'
	and notif_yrmonth >= '202504'
	and proc_cd ilike '%Q%'
order by
	NOTIF_YRMONTH
	



select HCEMI_MCR_ROUTED from tmp_1m.KN_MCR_JOINED_V2
where SBSCR_NBR = '937975211' and clm_aud_nbr = '51128274'

