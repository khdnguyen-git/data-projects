/*==============================================================================
 * SKIN SUBS CHECK — TIN 264341152
 * Compares CL's FNL_SS_SCRIPT logic vs Khang's kn_claims_semantic_base logic
 * Scope: M&R FFS, 2025-2026, monthly rollup
 *==============================================================================*/


/*------------------------------------------------------------------------------
 * VERSION 1: CL's Logic (FNL_SS_SCRIPT)
 * Source: fichsrv direct, 4 legs (COSMOS OP/PR, NICE OP/PR)
 * Denial: clm_dnl_f = 'N' (COSMOS), clm_ln_lvl_dnl_f = 'N' (NICE OP),
 *         clm_dnl_f = 'P' (NICE PR)
 * M&R FFS: brand = M&R, migration_source != OAH, product_level_3 != INSTITUTIONAL
 * Does NOT filter on tfm_include_flag
 *------------------------------------------------------------------------------*/
create or replace table tmp_7d.kn_ss_check_1 as
with cl_raw as (
    -- COSMOS OP
    select
        hce_month as srvc_month
        , allw_amt_fnl
        , adj_srvc_unit_cnt
        , clm_aud_nbr
    from fichsrv.glxy_op_f
    where brand_fnl = 'M&R'
        and global_cap = 'NA'
        and clm_dnl_f = 'N'
        and hce_month >= '202501'
        and prov_tin = '264341152'
        and (migration_source != 'OAH' or migration_source is null)
        and product_level_3_fnl != 'INSTITUTIONAL'
    union all
    -- COSMOS PR
    select
        fst_srvc_month as srvc_month
        , allw_amt_fnl
        , adj_srvc_unit_cnt
        , clm_aud_nbr
    from fichsrv.glxy_pr_f
    where brand_fnl = 'M&R'
        and global_cap = 'NA'
        and clm_dnl_f = 'N'
        and fst_srvc_month >= '202501'
        and prov_tin = '264341152'
        and (migration_source != 'OAH' or migration_source is null)
        and product_level_3_fnl != 'INSTITUTIONAL'
    union all
    -- NICE OP
    select
        hce_month as srvc_month
        , allw_amt as allw_amt_fnl
        , srvc_unit_cnt as adj_srvc_unit_cnt
        , clm_aud_nbr
    from fichsrv.nce_op_f
    where brand_fnl = 'M&R'
        and clm_cap_flag = 'FFS'
        and clm_ln_lvl_dnl_f = 'N'
        and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
        and hce_month >= '202501'
        and tin = '264341152'
    union all
    -- NICE PR
    select
        fst_srvc_month as srvc_month
        , allw_amt as allw_amt_fnl
        , srvc_unit_cnt as adj_srvc_unit_cnt
        , clm_aud_nbr
    from fichsrv.nce_pr_f
    where brand_fnl = 'M&R'
        and clm_cap_flag = 'FFS'
        and clm_dnl_f = 'P'
        and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
        and fst_srvc_month >= '202501'
        and tin = '264341152'
)
select
    srvc_month
    , sum(allw_amt_fnl) as allowed
    , sum(adj_srvc_unit_cnt) as srvc_units
    , count(distinct clm_aud_nbr) as claim_count
from cl_raw
group by srvc_month
order by srvc_month
;


/*------------------------------------------------------------------------------
 * VERSION 2: Khang's Logic (kn_claims_semantic_base)
 * Source: tmp_1m.kn_claims_semantic_base (pre-built)
 * Denial: denial_flag = 'Paid'
 * M&R FFS: population = 'M&R FFS' (includes tfm_include_flag = 1)
 * NICE PR uses calc_allw instead of allw_amt
 *------------------------------------------------------------------------------*/
create or replace table tmp_7d.kn_ss_check_2 as
select
    srvc_month
    , sum(allw_amt_fnl) as allowed
    , sum(adj_srvc_unit_cnt) as srvc_units
    , count(distinct clm_aud_nbr) as claim_count
from tmp_1m.kn_claims_semantic_base
where population = 'M&R FFS'
    and denial_flag = 'Paid'
    and prov_tin = '264341152'
    and srvc_month >= '202501'
group by srvc_month
order by srvc_month
;
select * from tmp_7d.kn_ss_check_1;
-- SRVC_MONTH	ALLOWED	SRVC_UNITS	CLAIM_COUNT
-- 202510	26459.01	201	182
-- 202502	9610.71	107	78
-- 202505	6648.9	83	73
-- 202604	174071.82	1389	73
-- 202603	61917.94	560	123
-- 202602	328495.54	3933	146
-- 202501	6782.72	92	61
-- 202606	604.26	12	12
-- 202511	5178559.67	2967	178
-- 202506	9399.7	150	106
-- 202503	9164.78	145	102
-- 202504	5776.28	122	80
-- 202512	8294965.3	4458	163
-- 202507	9794.21	139	108
-- 202601	2093066.84	6401	146
-- 202509	19945.96	200	170
-- 202605	7560.04	61	55
-- 202508	11504.55	162	127
select * from tmp_7d.kn_ss_check_2;

-- SRVC_MONTH	ALLOWED	SRVC_UNITS	CLAIM_COUNT
-- 202506	9399.7	150	106
-- 202602	328495.54	3933	146
-- 202603	61917.94	560	123
-- 202606	604.26	12	12
-- 202507	9794.21	139	108
-- 202508	11504.55	162	127
-- 202505	6648.9	83	73
-- 202509	19945.96	200	170
-- 202502	9610.71	107	78
-- 202503	9164.78	145	102
-- 202501	6782.72	92	61
-- 202504	5776.28	122	80
-- 202604	174071.82	1389	73
-- 202605	7560.04	61	55
-- 202512	8294965.3	4458	163
-- 202601	2093066.84	6401	146
-- 202510	26459.01	201	182
-- 202511	5178559.67	2967	178


select * 
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b 
limit 5;

select 
    sum(allw_amt_fnl)
    , sum(adj_srvc_unit_cnt)
from tmp_1y.cl_ss_claims_op_pr_cosmos_smart_nice_202codes_2b 
where prov_tin = '264341152' 
    and years = 2026 
    and ekp = '6CW1E56QP37BNA000110939372026-01-16Q4221'
;

select
    sum()
where concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) = '6CW1E56QP37BNA000110939372026-01-16Q4221'
    and prov_tin = '264341152'
    and fst_srvc_year = 2026

