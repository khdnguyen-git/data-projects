/*==============================================================================
 * THERAPY AFFORDABILITY — 202604 REFRESH (REFACTORED)
 *
 * Produces identical final output as Therapy_Savings_202604_Snowflake_stable.sql
 * but with clearer table names, fewer intermediate tables, and no dead steps.
 *
 * Final output: tmp_1q.kn_mbm_aff_202604
 *==============================================================================*/

/*==============================================================================
 * 1. MEMBERSHIP DETAIL
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_mshp_dtl_202604 as
select
    fin_mbi_hicn_fnl
    , fin_inc_month
    , fin_inc_qtr
    , fin_market as market_fnl
    , case when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot' else 'National' end as mbm_deploy_dt
    , fin_g_i as group_ind_fnl
    , migration_source
    , nce_tadm_dec_risk_type
    , case when migration_source = 'CIP' then 'CIP'
        when migration_source in ('PC','MEDICA') then 'SouthFlorida'
        when fin_product_level_3 = 'DUAL' and tfm_include_flag = 1 then 'M&R DUALS'
        when fin_product_level_3 = 'DUAL' and tfm_include_flag = 0 then 'C&S DUALS'
        when migration_source = 'NA' and fin_g_i = 'I' then 'Legacy Individual'
        when fin_g_i = 'G' then 'Group'
        else 'OTHERS'
      end as population
    , global_cap
    , tfm_include_flag
    , fin_product_level_3
    , fin_product_level_2
    , iff(special_network in ('ERICKSON'), 1, 0) as erk
    , sgr_source_name
from hce_ops_archv.gl_rstd_gpsgalnce_f_202604
where fin_inc_year > 2018
    and fin_brand = 'M&R'
    and sgr_source_name in ('COSMOS', 'NICE')
    and ((sgr_source_name = 'COSMOS' and global_cap = 'NA') or (sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')))
    and fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
;

-- QA: membership detail row count
select '202604' as month, count(*) as n from tmp_1q.kn_mbm_aff_mshp_dtl_202604
union all
select '202603' as month, count(*) as n from tmp_1q.kn_mbm_dtl_202603
;


/*==============================================================================
 * 2. MEMBERSHIP SUMMARY (COSMOS only)
 *==============================================================================*/
create or replace temporary table tmp_1q.kn_mbm_aff_mshp_agg_202604 as
select
    fin_inc_month as ep_start_mo
    , substring(market_fnl, 1, 2) as market_fnl
    , mbm_deploy_dt
    , group_ind_fnl
    , population
    , global_cap
    , tfm_include_flag
    , fin_product_level_3
    , fin_product_level_2
    , erk
    , sgr_source_name
    , count(distinct fin_mbi_hicn_fnl) as mm
    , substring(fin_inc_month, 1, 4) as ep_yr
    , substring(fin_inc_month, 5, 2) as ep_mnth
from tmp_1q.kn_mbm_aff_mshp_dtl_202604 as a
where sgr_source_name = 'COSMOS'
group by
    fin_inc_month
    , substring(market_fnl, 1, 2)
    , mbm_deploy_dt
    , group_ind_fnl
    , population
    , global_cap
    , tfm_include_flag
    , fin_product_level_3
    , fin_product_level_2
    , erk
    , sgr_source_name
;


/*==============================================================================
 * 3. MEMBERSHIP MM SUMMARY (excludes DUALS)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_mshp_sum_202604 as
select
    'MM' as data_type
    , ep_start_mo
    , '' as visit_mo
    , mbm_deploy_dt as pilot_nat
    , '' as category
    , '' as claim_status
    , 0 as visit_ep_lag
    , 0 as visit_runout_mo
    , 0 as ep_cnt
    , 0 as visit_cnt
    , 0 as allowed_amt
    , sum(mm) as mms
from tmp_1q.kn_mbm_aff_mshp_agg_202604
where population not in ('M&R DUALS', 'C&S DUALS')
group by
    ep_start_mo
    , mbm_deploy_dt
;


/*==============================================================================
 * 4. LOPA — OP (collapsed from 2 tables to 1)
 *
 * WORKAROUND: The original outer select `total_mbi_dos, still_lopa_mbi_dos,
 *   overturn_lopa_mbi_dos, src.*` causes "duplicate column name 'TOTAL_MBI_DOS'"
 *   because the inner `select ..., *` already adds those computed columns, and
 *   then `src.*` re-exposes them alongside the base table's columns.
 *   Fix: use a CTE so the inner columns are materialized first, then `select *`
 *   from the CTE with only the `ever_lopa` calculation added.
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_lopa_op_202604 as
with lopa_op_base as (
    select
        case when include_non_sug_event = 1 then mbi_dos end as total_mbi_dos
        , case when final_lopa_ind = 1 and mbr_dos_latest_submission = 1 and include_non_sug_event = 1
            then mbi_dos
          end as still_lopa_mbi_dos
        , case when include_non_sug_event = 1 and (final_lopa_ind != 1 or mbr_dos_latest_submission != 1)
            then mbi_dos
          end as overturn_lopa_mbi_dos
        , *
    from hce_ops_stage.pa_trckng_op_evnt_lopa_dtl
)
select
    case when (still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null)
        then mbi_dos else null
      end as ever_lopa
    , *
from lopa_op_base
;

-- QA: LOPA OP row count
select '202604' as month, count(*) as n from tmp_1q.kn_mbm_aff_lopa_op_202604
union all
select '202603' as month, count(*) as n from tmp_1q.kn_lopa_op_202603
;


/*==============================================================================
 * 5. LOPA — PR (collapsed from 2 tables to 1)
 *
 * WORKAROUND: Same duplicate column fix as Step 4 — use CTE instead of
 *   `total_mbi_dos, still_lopa_mbi_dos, overturn_lopa_mbi_dos, src.*`.
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_lopa_pr_202604 as
with lopa_pr_base as (
    select
        mbi_dos as total_mbi_dos
        , case when final_lopa_ind = 1 and mbr_dos_latest_submission = 1 then mbi_dos end as still_lopa_mbi_dos
        , case when final_lopa_ind != 1 or mbr_dos_latest_submission != 1 then mbi_dos end as overturn_lopa_mbi_dos
        , *
    from hce_ops_stage.pa_trckng_pr_evnt_lopa_dtl
)
select
    case when (still_lopa_mbi_dos is not null or overturn_lopa_mbi_dos is not null)
        then mbi_dos else null
      end as ever_lopa
    , *
from lopa_pr_base
;

-- QA: LOPA PR row count
select '202604' as month, count(*) as n from tmp_1q.kn_mbm_aff_lopa_pr_202604
union all
select '202603' as month, count(*) as n from tmp_1q.kn_lopa_pr_202603
;


/*==============================================================================
 * 6. PROFESSIONAL CLAIMS
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_claims_pr_202604 as
select
    a.gal_mbi_hicn_fnl as mbi
    , a.component
    , a.eventkey as id
    , a.service_code
    , a.fst_srvc_dt as start_dt
    , a.fst_srvc_month as serv_month
    , a.fst_srvc_qtr as hce_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hctapaidmonth
    , a.market_fnl
    , a.group_ind_fnl
    , a.proc_cd
    , a.rvnu_cd
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.prov_prtcp_sts_cd
    , a.prov_tin
    , a.full_nm as prov_full_nm
    , case when b.ever_lopa is not null then 1 else 0 end as lopa_flg
    , case when b.still_lopa_mbi_dos is not null then 1 else 0 end as still_lopa
    , case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end as overturn_lopa
    , 0 as apc_pbl_flg
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
    , 0 as tadm_util
    , count(distinct a.eventkey) as visits
    , sum(a.adj_srvc_unit_cnt) as adj_srvc_units
from fichsrv.glxy_pr_f as a
left join tmp_1q.kn_mbm_aff_lopa_pr_202604 as b
    on concat(a.gal_mbi_hicn_fnl, '_', a.fst_srvc_dt) = b.total_mbi_dos
    and a.proc_cd = b.proc_cd
    and a.prov_tin = b.prov_tin
where a.tfm_include_flag = 1
    and a.global_cap = 'NA'
    and a.product_level_3_fnl not in ('INSTITUTIONAL','DUAL')
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.st_abbr_cd = a.market_fnl
    and a.prov_prtcp_sts_cd = 'P'
    and (substring(coalesce(a.bil_typ_cd,'0'), 1, 1) <> '3')
    and (a.ama_pl_of_srvc_cd <> '12')
    and (
        a.proc_cd in
        ('92507','92508','92526','97012','97016','97018','97022','97024','97026','97028',
         '97032','97033','97034','97035','97036','97039','97110','97112','97113','97116',
         '97124','97139','97140','97150','97164','97168','97530','97533','97535','97537',
         '97542','97545','97546','97750','97755','97760','97761','97799','G0283',
         '98940','98941','98942')
        or a.rvnu_cd in
        ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429',
         '0440','0441','0442','0443','0444','0449')
    )
    and a.proc_cd not in
    ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151',
     'G0152','G9041','G9043','G9044','S9128','S9129','S9131')
group by
    a.gal_mbi_hicn_fnl
    , a.component
    , a.eventkey
    , a.service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt))
    , a.market_fnl
    , a.group_ind_fnl
    , a.proc_cd
    , a.rvnu_cd
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.prov_prtcp_sts_cd
    , a.prov_tin
    , a.full_nm
    , case when b.ever_lopa is not null then 1 else 0 end
    , case when b.still_lopa_mbi_dos is not null then 1 else 0 end
    , case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end
;

-- QA: PR claims row count
select '202604' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_aff_claims_pr_202604
union all
select '202603' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_episode_pr_202603
;


/*==============================================================================
 * 7. OUTPATIENT CLAIMS (kn_mbm_op_claims inlined as CTE)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_claims_op_202604 as
with op_claims_base as (
    select
        a.*
        , concat_ws('-', a.clm_rev_rsn_1_cd, a.clm_rev_rsn_2_cd, a.clm_rev_rsn_3_cd, a.clm_rev_rsn_4_cd
              , a.clm_rev_rsn_5_cd, a.clm_rev_rsn_6_cd, a.clm_rev_rsn_7_cd, a.clm_rev_rsn_8_cd
              , a.clm_rev_rsn_9_cd, a.clm_rev_rsn_10_cd) as clm_rev_rsn_1_10
    from fichsrv.glxy_op_f as a
    where (a.proc_cd in
              ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028',
               '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116',
               '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537',
               '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283',
               '98940', '98941', '98942')
          or rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449')
    )
    and proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
)
, op_claims_windowed as (
    select
        e.*
        , max(iff(position('00473-' in clm_rev_rsn_1_10) > 0, 1, 0)) over (partition by site_cd, clm_aud_nbr, sbscr_nbr) as clm_apc_flg
        , sum(allw_amt_fnl) over (partition by site_cd, clm_aud_nbr, sbscr_nbr) as clm_allw_amnt
    from op_claims_base as e
)
select
    a.gal_mbi_hicn_fnl as mbi
    , a.component
    , a.eventkey as id
    , a.hce_service_code as service_code
    , a.fst_srvc_dt as start_dt
    , a.fst_srvc_month as serv_month
    , a.fst_srvc_qtr as hce_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hctapaidmonth
    , a.market_fnl
    , a.group_ind_fnl
    , a.proc_cd
    , a.rvnu_cd
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.prov_prtcp_sts_cd
    , a.prov_tin
    , a.full_nm as prov_full_nm
    , case when b.ever_lopa is not null then 1 else 0 end as lopa_flg
    , case when b.still_lopa_mbi_dos is not null then 1 else 0 end as still_lopa
    , case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end as overturn_lopa
    , case when a.clm_apc_flg = 1 and c.rsn_cd in ('208','176','943') then 1 else 0 end as apc_pbl_flg
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
    , 0 as tadm_util
    , count(distinct a.eventkey) as visits
    , sum(a.adj_srvc_unit_cnt) as adj_srvc_units
from op_claims_windowed as a
left join tmp_1q.kn_mbm_aff_lopa_op_202604 as b
    on concat_ws('_', a.gal_mbi_hicn_fnl, a.fst_srvc_dt) = b.total_mbi_dos
    and a.proc_cd = b.proc_cd
    and a.prov_tin = b.prov_tin
left join fichsrv.tadm_glxy_reason_code as c
    on a.fnl_rsn_cd_sys_id = c.rsn_cd_sys_id
where a.tfm_include_flag = 1
    and a.global_cap = 'NA'
    and a.product_level_3_fnl not in ('INSTITUTIONAL','DUAL')
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.st_abbr_cd = a.market_fnl
    and a.prov_prtcp_sts_cd = 'P'
    and substring(coalesce(a.bil_typ_cd,'0'), 1, 1) != '3'
    and a.ama_pl_of_srvc_cd <> '12'
    and (
        a.proc_cd in
        ('92507','92508','92526','97012','97016','97018','97022','97024','97026','97028',
         '97032','97033','97034','97035','97036','97039','97110','97112','97113','97116',
         '97124','97139','97140','97150','97164','97168','97530','97533','97535','97537',
         '97542','97545','97546','97750','97755','97760','97761','97799','G0283',
         '98940','98941','98942')
        or a.rvnu_cd in
        ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429',
         '0440','0441','0442','0443','0444','0449')
    )
    and a.proc_cd not in
    ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151',
     'G0152','G9041','G9043','G9044','S9128','S9129','S9131')
group by
    a.gal_mbi_hicn_fnl
    , a.component
    , a.eventkey
    , a.hce_service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt))
    , a.market_fnl
    , a.group_ind_fnl
    , a.proc_cd
    , a.rvnu_cd
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.prov_prtcp_sts_cd
    , a.prov_tin
    , a.full_nm
    , case when b.ever_lopa is not null then 1 else 0 end
    , case when b.still_lopa_mbi_dos is not null then 1 else 0 end
    , case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end
    , case when a.clm_apc_flg = 1 and c.rsn_cd in ('208','176','943') then 1 else 0 end
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end
;

-- QA: OP claims row count
select '202604' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_aff_claims_op_202604
union all
select '202603' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_episode_op_202603
;


/*==============================================================================
 * 8. STACK ALL CLAIMS (PR + OP, current + 2018-2020)
 *    References 2018-2020 source tables directly — no copy step
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_claims_all_202604 as
select * from tmp_1q.kn_mbm_aff_claims_pr_202604
union all
select * from tmp_1y.kn_mbm_episode_1_2018_2020
union all
select * from tmp_1q.kn_mbm_aff_claims_op_202604
union all
select * from tmp_1y.kn_mbm_episode_1b_2018_2020
;

-- QA: stacked claims row count
select '202604' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_aff_claims_all_202604
union all
select '202603' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_episode_1c_202603
;


/*==============================================================================
 * 9. CLAIMS FLAGGED (claim_status, optum_flg, mbmserv_dtl, mbm_deploy_dt)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_claims_flagged_202604 as
with episode_base as (
    select *
        , sum(allowed) over (partition by id, start_dt, category) as dnl_allowed
        , max(lopa_flg) over (partition by id, start_dt, category) as max_lopa_flg
    from tmp_1q.kn_mbm_aff_claims_all_202604
)
, joined as (
    select a.*, b.tin_num
    from episode_base as a
    left join tmp_1y.p8001_optum_tin_2 as b
        on a.prov_tin = b.tin_num and b.i = 1
)
select *
    , case when dnl_allowed > 0.01 then 'Paid'
        when still_lopa = 1 then 'LOPA'
        when apc_pbl_flg = 1 then 'APC-Paid'
        else 'Other Denied'
      end as claim_status
    , case when tin_num is null then 0 else 1 end as optum_flg
    , case when proc_cd in ('98940','98941','98942') then 'Chiro'
        when proc_cd in ('97001','97002','97003','97004','97012','97016','97018','97022','97024','97026','97028','97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139','97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532','97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799','G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131') then 'PT-OT'
        when proc_cd in ('70371','92506','92507','92508','92521','92522','92523','92524','92526','92626','92627','92630','92633','96105','S9128') then 'ST'
        else 'Other'
      end as mbmserv_dtl
    , case
        when market_fnl not in ('AR','GA','NJ','SC') or group_ind_fnl <> 'I' then 'National'
        when category = 'OP_REHAB' then 'Phase-II'
        when tin_num is null then 'Phase-II'
        else 'Phase-I'
      end as mbm_deploy_dt
from joined
;

-- QA: flagged claims row count
select '202604' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_aff_claims_flagged_202604
union all
select '202603' as month, count(*) as n, sum(allowed) as allowed from tmp_1q.kn_mbm_episode_2_202603
;


/*==============================================================================
 * 10. VISITS RANKED (collapsed episode_3 + episode_4 into one step)
 *     Aggregates by mbi-category, then adds row_number in one pass
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_visits_ranked_202604 as
with visit_agg as (
    select
        concat(mbi,'-',category) as mbi
        , component
        , id
        , start_dt
        , serv_month
        , hce_qtr
        , min(hctapaidmonth) as hctapaidmonth
        , mbm_deploy_dt
        , market_fnl
        , claim_status
        , cast(mbmserv_dtl as varchar(10)) as mbmserv
        , category
        , sum(allowed) as allowed
        , sum(paid) as paid
        , sum(tadm_util) as tadm_util
        , count(distinct concat(id,start_dt)) as visits
        , count(visits) as vsts
        , sum(adj_srvc_units) as adj_srvc_units
    from tmp_1q.kn_mbm_aff_claims_flagged_202604
    where prov_prtcp_sts_cd = 'P'
    group by
        concat(mbi,'-',category)
        , component
        , id
        , start_dt
        , serv_month
        , hce_qtr
        , mbm_deploy_dt
        , market_fnl
        , claim_status
        , mbmserv_dtl
        , optum_flg
        , category
)
select
    mbi
    , component
    , id
    , start_dt
    , row_number() over (partition by mbi, mbm_deploy_dt order by start_dt) as i
    , serv_month
    , hce_qtr
    , hctapaidmonth
    , mbm_deploy_dt
    , market_fnl
    , claim_status
    , mbmserv
    , category
    , allowed
    , paid
    , tadm_util
    , visits
    , vsts
    , adj_srvc_units
from visit_agg
;

-- QA: visits ranked row count
select '202604' as month, count(*) as n from tmp_1q.kn_mbm_aff_visits_ranked_202604
union all
select '202603' as month, count(*) as n from tmp_1q.kn_mbm_episode_4_202603
;


/*==============================================================================
 * 11. VISIT LAG CALCULATION (self-join for previous visit)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_visits_lag_202604 as
select
    a.mbi
    , a.component
    , a.id
    , a.start_dt
    , b.start_dt as prev_start_dt
    , datediff('day', b.start_dt, a.start_dt) as visit_dy_lag
    , iff(datediff('day', b.start_dt, a.start_dt) > 30, 1, 0) as ep_flag
    , a.i
    , b.i as prev_i
    , a.serv_month
    , a.hce_qtr
    , a.hctapaidmonth
    , a.mbm_deploy_dt
    , a.market_fnl
    , a.claim_status
    , a.mbmserv
    , a.category
    , a.allowed
    , a.paid
    , a.tadm_util
    , a.visits
    , a.vsts
    , a.adj_srvc_units
from tmp_1q.kn_mbm_aff_visits_ranked_202604 as a
left join tmp_1q.kn_mbm_aff_visits_ranked_202604 as b
    on a.mbi = b.mbi
    and a.mbm_deploy_dt = b.mbm_deploy_dt
    and a.i = b.i+1
;

-- QA: visits lag row count
select '202604' as month, count(*) as n from tmp_1q.kn_mbm_aff_visits_lag_202604
union all
select '202603' as month, count(*) as n from tmp_1q.kn_mbm_episode_lag_202603
;


/*==============================================================================
 * 12. EPISODE BOUNDARIES (cumulative episode assignment)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_episodes_202604 as
select
    a.mbi
    , a.component
    , a.id
    , a.start_dt
    , a.prev_start_dt
    , a.visit_dy_lag
    , a.ep_flag
    , min(a.start_dt) over (partition by a.mbi, a.cmltv_episodes) as ep_start_dt
    , a.cmltv_episodes
    , a.i
    , a.prev_i
    , a.serv_month
    , a.hce_qtr
    , a.hctapaidmonth
    , min(a.hctapaidmonth) over (partition by a.mbi, a.cmltv_episodes) as ep_hctapaidmonth
    , a.mbm_deploy_dt
    , a.market_fnl
    , a.claim_status
    , a.mbmserv
    , a.category
    , a.allowed
    , a.paid
    , a.tadm_util
    , a.visits
    , a.vsts
    , a.adj_srvc_units
from (
    select
        *
        , sum(iff(prev_start_dt is null, 1, ep_flag)) over (
            partition by mbi
            order by start_dt
            rows between unbounded preceding and current row
        ) as cmltv_episodes
    from tmp_1q.kn_mbm_aff_visits_lag_202604
) as a
;

-- QA: episodes row count
select '202604' as month, count(*) as n from tmp_1q.kn_mbm_aff_episodes_202604
union all
select '202603' as month, count(*) as n from tmp_1q.kn_mbm_episode_vst_ep_2_202603
;


/*==============================================================================
 * 13. EPISODE RUNOUT (collapsed ro_lag + ro_lag2 into one step)
 *     Computes runout metrics and selects final columns in a single pass
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_ep_runout_202604 as
select
    a.mbi
    , a.id
    , a.ep_start_dt
    , a.cmltv_episodes
    , a.start_dt
    , to_char(a.ep_start_dt,'yyyyMM') as ep_start_mo
    , to_char(a.ep_start_dt,'yyyy') as ep_start_year
    , a.market_fnl
    , a.mbm_deploy_dt
    , a.category
    , a.claim_status
    , a.hctapaidmonth
    , a.mbmserv as visit_mbmserv
    , floor((datediff('day', a.start_dt, a.hctapaidmonth) + 20) / 30.5) as visit_runout_mo
    , 0 as ep_runout_mo
    , to_char(a.start_dt,'yyyyMM') as visit_mo
    , floor(datediff('day', a.ep_start_dt, a.start_dt) / 30.5) as visit_ep_lag
    , iff(a.prev_start_dt is null, 1, a.ep_flag) as episodes
    , a.visits
    , a.allowed
    , 0 as mm
from tmp_1q.kn_mbm_aff_episodes_202604 as a
;

-- QA: ep runout row count
select '202604' as month, count(*) as n from tmp_1q.kn_mbm_aff_ep_runout_202604
union all
select '202603' as month, count(*) as n from tmp_1q.kn_mbm_episode_ro_lag2_202603
;


/*==============================================================================
 * 14. AGG VISITS + EPISODES (single union all — no alter/insert pattern)
 *
 * WORKAROUND: The episodes branch originally had `0 as visit_mo` (numeric).
 *   The visits branch produces varchar visit_mo via to_char(). Snowflake's
 *   UNION ALL resolves the column to DECIMAL, which then breaks Step 15's
 *   UNION with mshp_sum (which has `'' as visit_mo`, a varchar).
 *   Fix: use `'0' as visit_mo` to keep varchar type throughout.
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_agg_202604 as

-- Visits aggregation
select
    cast('VISITS' as varchar(50)) as data_type
    , ep_start_mo
    , concat(ep_start_year,'Q9') as ep_start_qtr
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , visit_mbmserv
    , visit_runout_mo
    , ep_runout_mo
    , visit_mo
    , visit_ep_lag
    , 0 as episodes
    , sum(visits) as visits
    , sum(allowed) as allowed
    , 0 as mm
from tmp_1q.kn_mbm_aff_ep_runout_202604
group by
    ep_start_mo
    , concat(ep_start_year,'Q9')
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , visit_mbmserv
    , visit_runout_mo
    , ep_runout_mo
    , visit_mo
    , visit_ep_lag

union all

-- Episodes aggregation
select
    cast('EPISODES' as varchar(50)) as data_type
    , ep_start_mo
    , concat(ep_start_year,'Q9') as ep_start_qtr
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , '' as visit_mbmserv
    , 0 as visit_runout_mo
    , 0 as ep_runout_mo
    , '0' as visit_mo
    , 0 as visit_ep_lag
    , sum(episodes) as episodes
    , 0 as visits
    , 0 as allowed
    , 0 as mm
from tmp_1q.kn_mbm_aff_ep_runout_202604
where episodes = 1
group by
    ep_start_mo
    , concat(ep_start_year,'Q9')
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
;

-- QA: agg row count + totals
select ep_start_mo, sum(allowed) as allowed, sum(visits) as visits, sum(episodes) as episodes
from tmp_1q.kn_mbm_aff_agg_202604
where ep_start_mo >= '202401'
group by 1
order by 1
;

select ep_start_mo, sum(allowed) as allowed, sum(visits) as visits, sum(episodes) as episodes
from tmp_1q.kn_mbm_episode_agg6_202603
where ep_start_mo >= '202401'
group by 1
order by 1
;


/*==============================================================================
 * 15. FINAL SUMMARY (post-2023 + membership MM, then union with static pre-2023)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_sum_post2023_202604 as
select
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 0, 4) as ep_year
    , substring(ep_start_mo, 5, 2) as ep_month
    , visit_mo
    , case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as pilot_nat
    , category
    , visit_ep_lag
    , visit_runout_mo
    , sum(episodes) as ep_cnt
    , sum(visits) as visit_cnt
    , sum(allowed) as allowed_amt
    , sum(mm) as mms
from tmp_1q.kn_mbm_aff_agg_202604
where ep_start_mo >= '202301'
group by
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 0, 4)
    , substring(ep_start_mo, 5, 2)
    , visit_mo
    , case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end
    , category
    , claim_status
    , visit_ep_lag
    , visit_runout_mo
union
select
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 0, 4) as ep_year
    , substring(ep_start_mo, 5, 2) as ep_month
    , visit_mo
    , pilot_nat
    , category
    , visit_ep_lag
    , visit_runout_mo
    , ep_cnt
    , visit_cnt
    , allowed_amt
    , mms
from tmp_1q.kn_mbm_aff_mshp_sum_202604
;


/*==============================================================================
 * 16. FINAL OUTPUT TABLE
 *     References static before-2023 table directly — no copy step
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_aff_202604 as
select * from tmp_1q.kn_mbm_aff_sum_post2023_202604
union all
select * from tmp_1y.kn_mbm_episode_agg6_sum1_before2023
;


/*==============================================================================
 * FINAL QA: compare current vs previous month
 *==============================================================================*/

-- Row count
select '202604 (refactored)' as month, count(*) as n from tmp_1q.kn_mbm_aff_202604
union all
select '202603 (standard)' as month, count(*) as n from tmp_1q.kn_mbm_202603
;

-- ep_start_mo breakdown
select ep_start_mo, sum(allowed_amt) as allowed, sum(visit_cnt) as visits, sum(ep_cnt) as episodes, sum(mms) as mm
from tmp_1q.kn_mbm_aff_202604
where ep_start_mo >= '202401'
group by 1
order by 1
;

select ep_start_mo, sum(allowed_amt) as allowed, sum(visit_cnt) as visits, sum(ep_cnt) as episodes, sum(mms) as mm
from tmp_1q.kn_mbm_202603
where ep_start_mo >= '202401'
group by 1
order by 1
;
