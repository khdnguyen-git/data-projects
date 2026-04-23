/*==============================================================================
 * TIN Anomaly Detection — Feature Table
 * Unit: prov_tin x category_1 x market_fnl
 * Source: tmp_1m.knd_mbm_episodes_agg_test (episodes) +
 *         tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3 (denial rate)
 * Output: tmp_1m.knd_mbm_tin_features_202604
 *==============================================================================*/

create or replace table tmp_1m.knd_mbm_tin_features_202602 as

with denial_rates as (
    select
        prov_tin
        , category_1
        , market_fnl
        , count(*) as total_claims
        , count(case when claim_status = 'Denied' then 1 end) as denied_claims
        , count(case when claim_status = 'Denied' then 1 end) / nullif(count(*), 0) as denial_rate
    from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_202603
    group by
        prov_tin
        , category_1
        , market_fnl
)

, market_count_by_tin as (
    select
        prov_tin
        , count(distinct market_fnl) as market_count
    from tmp_1m.knd_mbm_episodes_agg_test_202603
    group by prov_tin
)

, episode_features as (
    select
        a.prov_tin
        , a.category_1
        , a.market_fnl
        , a.optum_tin_flag
        , count(distinct concat(a.mbi_key, '|', cast(a.ep_num as varchar))) as episode_count
        , count(distinct a.mbi_key) as member_count
        , sum(a.n_visits) as total_visits
        , sum(a.allowed) as total_allowed
        , sum(a.n_visits)
            / nullif(count(distinct concat(a.mbi_key, '|', cast(a.ep_num as varchar))), 0)
            as avg_visits_per_ep
        , sum(a.allowed)
            / nullif(sum(a.n_visits), 0)
            as avg_allowed_per_visit
        , sum(a.allowed)
            / nullif(count(distinct concat(a.mbi_key, '|', cast(a.ep_num as varchar))), 0)
            as avg_allowed_per_ep
        , sum(a.allowed)
            / nullif(count(distinct a.mbi_key), 0)
            as allowed_per_member
        , count(distinct a.ahrq_diag_dtl_catgy_desc)
            / nullif(count(distinct concat(a.mbi_key, '|', cast(a.ep_num as varchar))), 0)
            as diag_diversity
    from tmp_1m.knd_mbm_episodes_agg_test_202603 as a
    group by
        a.prov_tin
        , a.category_1
        , a.market_fnl
        , a.optum_tin_flag
)

select
    e.prov_tin
    , e.category_1
    , e.market_fnl
    , e.optum_tin_flag
    , e.episode_count
    , e.member_count
    , e.total_visits
    , e.total_allowed
    , e.avg_visits_per_ep
    , e.avg_allowed_per_visit
    , e.avg_allowed_per_ep
    , e.allowed_per_member
    , e.diag_diversity
    , coalesce(d.denial_rate, 0) as denial_rate
    , m.market_count
from episode_features as e
left join denial_rates as d
    on e.prov_tin = d.prov_tin
    and e.category_1 = d.category_1
    and e.market_fnl = d.market_fnl
left join market_count_by_tin as m
    on e.prov_tin = m.prov_tin
where e.episode_count >= 30
    and e.member_count >= 10
;
