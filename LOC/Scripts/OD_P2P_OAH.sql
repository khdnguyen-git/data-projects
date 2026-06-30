select distinct entity from tmp_1m.ec_ip_dataset_notif_05132026_od;

create or replace table tmp_1m.kn_loc_05132026 as
select
    admit_week
    , hce_admit_month as admit_act_month
    , total_oah_flag
    , institutional_flag
    , fin_tfm_product_new
    , sgr_source_name
    , nce_tadm_dec_risk_type
    , fin_market
    , fin_brand
    , group_name
    , group_number
    , prov_tin
    , par_nonpar
    , hospital_group
    , los_categories
    , respiratory_flag
    , mnr_cosmos_ffs_flag
    , leading_ind_pop
    , mnr_nice_ffs_flag
    , mnr_total_ffs_flag
    , mnr_oah_flag
    , cns_oah_flag
    , mnr_dual_flag
    , cns_dual_flag
    , ocm_migration
    , component
    , entity
    , sum(case_count) as case_count
    , sum(Initial_ADR_cnt) as Initial_ADR_cnt
    , sum(persistent_adr_cnt) as persistent_adr_cnt
    , sum(md_reviewed_cnt) as md_reviewed_cnt
    , sum(appeal_case_cnt) as appeal_case_cnt
    , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
    , sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
    , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
    , sum(p2p_case_cnt) as p2p_case_cnt
    , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
    , sum(other_ovtrns) as other_ovtrns
    , sum(member_appeal_cnt) as member_appeal_cnt
    , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
    , sum(membership) as membership
from tmp_1m.ec_ip_dataset_notif_05132026_od
where ipa_pac_flag in ('IPA', 'MM')
    and hce_admit_month > '202212'
    and loc_flag = 1
group by
    admit_week
    , hce_admit_month
    , total_oah_flag
    , institutional_flag
    , fin_tfm_product_new
    , sgr_source_name
    , nce_tadm_dec_risk_type
    , fin_market
    , fin_brand
    , group_name
    , group_number
    , prov_tin
    , par_nonpar
    , hospital_group
    , los_categories
    , respiratory_flag
    , mnr_cosmos_ffs_flag
    , leading_ind_pop
    , mnr_nice_ffs_flag
    , mnr_total_ffs_flag
    , mnr_oah_flag
    , cns_oah_flag
    , mnr_dual_flag
    , cns_dual_flag
    , ocm_migration
    , component
    , entity
;



create or replace table tmp_1m.kn_loc_od_p2p_oah_05132026 as
select
    admit_act_month
    , admit_week
    , group_name
    , hospital_group
    , fin_market
    , prov_tin
    , sum(case_count) as case_count
    , sum(Initial_ADR_cnt) as initial_adr_count
    , sum(persistent_adr_cnt) as persistent_adr_cnt
    , sum(md_reviewed_cnt) as md_reviewed_cnt
    , sum(appeal_case_cnt) as appeal_case_cnt
    , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
    , sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
    , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
    , sum(p2p_case_cnt) as p2p_case_cnt
    , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
    , sum(member_appeal_cnt) as member_appeal_cnt
    , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
    , sum(membership) as membership
from tmp_1m.kn_loc_05132026
where admit_act_month >= '202501'
    and total_oah_flag = 'OAH'
    and institutional_flag = 'Non-Institutional'
    and mnr_total_ffs_flag = 1
group by 1, 2, 3, 4, 5, 6;


create or replace table tmp_1m.kn_loc_od_p2p_oah_05132026_ranked as
with base as (
    select
        admit_act_month
        , admit_week
        , group_name
        , hospital_group
        , fin_market
        , prov_tin
        , sum(case_count) as case_count
        , sum(initial_adr_cnt) as initial_adr_count
        , sum(persistent_adr_cnt) as persistent_adr_cnt
        , sum(md_reviewed_cnt) as md_reviewed_cnt
        , sum(appeal_case_cnt) as appeal_case_cnt
        , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
        , sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
        , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
        , sum(p2p_case_cnt) as p2p_case_cnt
        , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
        , sum(member_appeal_cnt) as member_appeal_cnt
        , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
        , sum(membership) as membership
    from tmp_1m.kn_loc_05132026
    where admit_act_month >= '202501'
        and total_oah_flag = 'OAH'
    group by
        admit_act_month
        , admit_week
        , group_name
        , hospital_group
        , fin_market
        , prov_tin
)
, hospital_totals as (
    select
        hospital_group
        , sum(member_appeal_cnt) as total_member_appeal_cnt
        , sum(p2p_case_cnt) as total_p2p_case_cnt
    from base
    group by hospital_group
)
, ranked_hospitals as (
    select
        hospital_group
        , dense_rank() over (
            order by total_member_appeal_cnt desc
        ) as member_appeal_rank
        , dense_rank() over (
            order by total_p2p_case_cnt desc
        ) as p2p_rank
    from hospital_totals
)
select
    b.*
    , case
        when r.member_appeal_rank <= 12 then 1
        else 0
      end as top12_member_appeal
    , case
        when r.p2p_rank <= 12 then 1
        else 0
      end as top12_p2p
from base as b
left join ranked_hospitals as r
    on b.hospital_group = r.hospital_group;


select * 


select count(*) from tmp_1m.kn_loc_od_p2p_oah_05132026_ranked;

select * from tmp_1m.kn_loc_od_p2p_oah_05132026_ranked
limit 100;


select distinct * from tmp_1m.kn_loc_if_outlier_hospitals_mnr
where reason ilike 'md_%'
;


select * from tmp_1m.ec_ip_dataset_06102026_3_od
limit 5;