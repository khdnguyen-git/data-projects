-- Cadence Solutions TIN investigation
-- Source: 578 TINs provided externally as allegedly associated with Cadence Solutions Inc
-- kn_rpm_cadence_tin was manually loaded (insert/file load); DDL preserved here for reference

create or replace table tmp_1m.kn_rpm_cadence_tin (
    prov_tin varchar(9)
);

-- populate via: insert into tmp_1m.kn_rpm_cadence_tin (prov_tin) values ('381359266'), ('381360562'), ...
-- or: copy into tmp_1m.kn_rpm_cadence_tin from @<stage>/<file>


-- detail table: join cadence TINs to RPM claims + fraud signals
create or replace table tmp_1m.kn_rpm_cadence_tin_detail as
with claims_agg as (
    select
        d.prov_tin
        , coalesce(d.hospital_group, d.full_nm, d.provider_group_name) as provider_name
        , count(distinct d.mbi) as distinct_mbrs
        , count(*) as claim_lines
        , sum(d.allw_amt_fnl) as total_allowed
        , row_number() over (partition by d.prov_tin order by count(*) desc) as rn
    from tmp_1m.kn_rpm_202608_detail as d
    inner join tmp_1m.kn_rpm_cadence_tin as c
        on d.prov_tin = c.prov_tin
    group by
        d.prov_tin
        , coalesce(d.hospital_group, d.full_nm, d.provider_group_name)
)
, claims_final as (
    select
        prov_tin
        , provider_name
        , distinct_mbrs
        , claim_lines
        , total_allowed
    from claims_agg
    where rn = 1
)
, signals as (
    select
        s.prov_tin
        , s.total_signals
        , s.total_domains
        , s.priority_tier
        , array_to_string(array_compact(array_construct(
            case when s.sig_no_prior = 1 then 'no_prior_relationship' end
            , case when s.sig_mbr_multi_tin = 1 then 'member_multi_tin' end
            , case when s.sig_ramp = 1 then 'rapid_ramp' end
            , case when s.sig_billing_conc = 1 then 'billing_concentration' end
            , case when s.sig_cpt_bundle = 1 then 'cpt_bundle' end
            , case when s.sig_bill_order = 1 then 'billing_order' end
            , case when s.sig_no_treat = 1 then 'no_treatment_mgmt' end
            , case when s.sig_churn = 1 then 'member_churn' end
            , case when s.sig_tin_network = 1 then 'tin_network' end
        )), ', ') as flagged_signals
    from tmp_1m.kn_rpm_202608_fraud_scores as s
    inner join tmp_1m.kn_rpm_cadence_tin as c
        on s.prov_tin = c.prov_tin
)
select
    ct.prov_tin
    , cf.provider_name
    , coalesce(cf.distinct_mbrs, 0) as distinct_mbrs
    , coalesce(cf.claim_lines, 0) as claim_lines
    , coalesce(cf.total_allowed, 0) as total_allowed
    , coalesce(si.total_signals, 0) as total_signals
    , coalesce(si.total_domains, 0) as total_domains
    , si.priority_tier
    , si.flagged_signals
from tmp_1m.kn_rpm_cadence_tin as ct
left join claims_final as cf
    on ct.prov_tin = cf.prov_tin
left join signals as si
    on ct.prov_tin = si.prov_tin
order by total_allowed desc
;

select * from tmp_1m.kn_rpm_cadence_tin_detail
order by total_allowed desc
;

select 
    flagged_signals
    , sum(total_allowed)
    , sum(total_signals)
from tmp_1m.kn_rpm_cadence_tin_detail
group by 1
order by sum(total_allowed) desc
;


select
    fin_inc_month
    , sum(fin_member_cnt)
from fichsrv.tre_membership
where 
    global_cap = 'NA'
    and tfm_include_flag = 1
    and fin_brand = 'M&R'
    and fin_product_level_3 != 'INSTITUTIONAL'
    and fin_inc_year >= 2026
group by 1
order by 1
;

select * from fichsrv.tre_membership
limit 5;