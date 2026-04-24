--therapy - LOPA count tracking: update monthly
--describe formatted tmp_1m.kn_MBM_EPISODE_RO_LAG2
describe formatted tmp_1y.PA_TRCKNG_op_EVNT_LOPA_DTL;
describe formatted tmp_1m.kn_mbm_episode_2_202508;

--one row per mbi + start_dt + serv_month + proc_cd
--category priority: OP_Rehab > Office > Chiro > Other
--claim_status, allowed_amt, and lopa flags all derived in a single pass
create or replace table tmp_7d.kn_mbm_lopa_base as
select
    mbi
    , start_dt
    , serv_month
    , proc_cd
    , max(case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end) as pilot_national
    , case
        when max(case when category = 'OP_REHAB' then 1 else 0 end) = 1 then 'OP_Rehab'
        when max(case when category = 'Office'   then 1 else 0 end) = 1 then 'Office'
        when max(case when category = 'Chiro'    then 1 else 0 end) = 1 then 'Chiro'
        else 'Other'
      end as category
    , case when max(case when claim_status in ('Paid', 'APC-Paid') then 1 else 0 end) = 1 then 'Paid' else 'Denied' end as claim_status
    , sum(case when claim_status in ('Paid', 'APC-Paid') then allowed else 0 end) as allowed_amt
    , case when max(case when overturn_lopa = 1 then 1 else 0 end) = 1 then 'Y' else 'N' end as overturn_lopa_f
    , case
        when max(case when claim_status in ('Paid', 'APC-Paid') then 1 else 0 end) = 0
            and max(case when overturn_lopa = 1 then 1 else 0 end) = 0
            and max(case when still_lopa    = 1 then 1 else 0 end) = 1
        then 'Y' else 'N'
      end as still_lopa_f
    , case
        when max(case when overturn_lopa = 1 then 1 else 0 end) = 1
            or (
                max(case when claim_status in ('Paid', 'APC-Paid') then 1 else 0 end) = 0
                and max(case when still_lopa = 1 then 1 else 0 end) = 1
            )
        then 'Y' else 'N'
      end as ever_lopa
    , 1 as recordcnt
from tmp_1m.kn_mbm_episode_2_202508
where prov_prtcp_sts_cd = 'P'
    and serv_month > '202109'
group by
    mbi
    , start_dt
    , serv_month
    , proc_cd
;
--select count(*) from tmp_7d.kn_mbm_lopa_base --74609604

--join membership once for both product and market
create or replace table tmp_7d.kn_mbm_lopa_detail as
select
    a.*
    , b.fin_tfm_product_new
    , b.fin_market
from tmp_7d.kn_mbm_lopa_base as a
left join fichsrv.tre_membership as b
    on a.mbi = b.fin_mbi_hicn_fnl
    and a.serv_month = b.fin_inc_month
;
--select count(*) from tmp_7d.kn_mbm_lopa_detail --74609604

--product-level summary for excel
create or replace table tmp_7d.kn_mbm_lopa_summary as
select
    fin_tfm_product_new
    , year(start_dt) as serv_year
    , serv_month
    , pilot_national
    , category
    , claim_status
    , ever_lopa
    , still_lopa_f as still_lopa
    , overturn_lopa_f as overturn_lopa
    , sum(recordcnt) as visit_proc_cnt
from tmp_7d.kn_mbm_lopa_detail
group by
    fin_tfm_product_new
    , year(start_dt)
    , serv_month
    , pilot_national
    , category
    , claim_status
    , ever_lopa
    , still_lopa_f
    , overturn_lopa_f
;
--select count(*) from tmp_7d.kn_mbm_lopa_summary --4437
select * from tmp_7d.kn_mbm_lopa_summary;

--product + market summary, 2024+, excluding Other
create or replace table tmp_7d.kn_mbm_lopa_summary_market as
select
    fin_tfm_product_new
    , fin_market
    , year(start_dt) as serv_year
    , serv_month
    , pilot_national
    , category
    , claim_status
    , ever_lopa
    , still_lopa_f as still_lopa
    , overturn_lopa_f as overturn_lopa
    , sum(recordcnt) as visit_proc_cnt
from tmp_7d.kn_mbm_lopa_detail
where category <> 'Other'
    and year(start_dt) > '2023'
group by
    fin_tfm_product_new
    , fin_market
    , year(start_dt)
    , serv_month
    , pilot_national
    , category
    , claim_status
    , ever_lopa
    , still_lopa_f
    , overturn_lopa_f
;
--select count(*) from tmp_7d.kn_mbm_lopa_summary_market --15304
select * from tmp_7d.kn_mbm_lopa_summary_market;

--checking against Sree's excel
select
    category
    , sum(recordcnt) as visit_proc_cnt
from tmp_7d.kn_mbm_lopa_base
where proc_cd = '97110'
    and still_lopa_f = 'Y'
    and serv_month between '202401' and '202412'
group by category
;

select sum(visit_proc_cnt) from tmp_7d.kn_mbm_lopa_summary;
