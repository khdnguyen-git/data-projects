/*==============================================================================
 * LOC Valuation — April 2026
 * notifications_date = 04222026
 * membership_month   = 202603
 *==============================================================================*/


/*==============================================================================
 * Step 1: _4_od — roll up auths before join to member months
 * Source: tmp_1m.ec_ip_dataset_04222026_3_od  (IPA's table)
 *==============================================================================*/
create or replace table tmp_1m.kn_ip_dataset_04222026_4_od as
select
    a.admit_week
    , a.hce_admit_month
    , a.admit_year
    , 'Auths' as fst_srvc_month
    , '' as adjd_yrmonth
    , a.component
    , a.entity
    , a.ip_type
    , a.loc_flag
    , a.svc_setting
    , a.case_cur_svc_cat_dtl_cd
    , a.migration_source
    , a.total_oah_flag
    , a.institutional_flag
    , a.fin_tfm_product_new
    , a.tfm_include_flag
    , a.global_cap
    , a.sgr_source_name
    , a.nce_tadm_dec_risk_type
    , a.fin_brand
    , a.fin_g_i
    , a.fin_product_level_3
    , a.fin_plan_level_2
    , a.fin_market
    , a.fin_contractpbp
    , a.group_number
    , a.group_name
    , a.do_ind
    , a.par_nonpar
    , a.prov_tin
    , d.collection as hospital_group
    , a.capitated
    , a.los_categories
    , a.los_exp
    , 0 as length_of_stay
    , a.respiratory_flag
    , a.ipa_li_split
    , a.mnr_cosmos_ffs_flag
    , a.leading_ind_pop
    , a.mnr_nice_ffs_flag
    , a.mnr_total_ffs_flag
    , a.mnr_oah_flag
    , a.cns_oah_flag
    , a.mnr_dual_flag
    , a.cns_dual_flag
    , a.ocm_migration
    , a.swgbed
    , a.mr_cs_other
    , a.admit_type
    , a.ipa_pac_flag
    , a.first_adverse
    , a.first_not_approved_srvc
    , a.first_not_approved_case
    , a.md_review_overturn
    , sum(a.appealed_cases) as appealed_cases
    , sum(a.overturned_cases) as overturned_cases
    , sum(a.md_rev_appeals) as md_rev_appeals
    , sum(a.pre_auth_cases) as pre_auth_cases
    , sum(a.case_count) as case_count
    , sum(a.initial_adr_cnt) as initial_adr_cnt
    , sum(a.persistent_adr_cnt) as persistent_adr_cnt
    , sum(a.md_reviewed_cnt) as md_reviewed_cnt
    , sum(a.appeal_case_cnt) as appeal_case_cnt
    , sum(a.appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
    , sum(a.mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
    , sum(a.mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
    , sum(a.p2p_case_cnt) as p2p_case_cnt
    , sum(a.p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
    , sum(a.other_ovtrns) as other_ovtrns
    , sum(a.member_appeal_cnt) as member_appeal_cnt
    , sum(a.member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
    , sum(a.membership) as membership
    , 0 as days
    , 0 as frank_days
    , 0 as admits
    , 0 as allowed
    , 0 as netpaid
    , 0 as franky_paid
    , 0 as franky_admits
    , 0 as franky_allw
from tmp_1m.ec_ip_dataset_04222026_3_od as a
left join tmp_1y.tin_collection as d
    on a.prov_tin = d.tin
group by
    a.admit_week
    , a.hce_admit_month
    , a.admit_year
    , a.component
    , a.entity
    , a.ip_type
    , a.loc_flag
    , a.svc_setting
    , a.case_cur_svc_cat_dtl_cd
    , a.migration_source
    , a.total_oah_flag
    , a.institutional_flag
    , a.fin_tfm_product_new
    , a.tfm_include_flag
    , a.global_cap
    , a.sgr_source_name
    , a.nce_tadm_dec_risk_type
    , a.fin_brand
    , a.fin_g_i
    , a.fin_product_level_3
    , a.fin_plan_level_2
    , a.fin_market
    , a.fin_contractpbp
    , a.group_number
    , a.group_name
    , a.do_ind
    , a.par_nonpar
    , a.prov_tin
    , d.collection
    , a.capitated
    , a.los_categories
    , a.los_exp
    , a.respiratory_flag
    , a.ipa_li_split
    , a.mnr_cosmos_ffs_flag
    , a.leading_ind_pop
    , a.mnr_nice_ffs_flag
    , a.mnr_total_ffs_flag
    , a.mnr_oah_flag
    , a.cns_oah_flag
    , a.mnr_dual_flag
    , a.cns_dual_flag
    , a.ocm_migration
    , a.swgbed
    , a.mr_cs_other
    , a.admit_type
    , a.ipa_pac_flag
    , a.first_adverse
    , a.first_not_approved_srvc
    , a.first_not_approved_case
    , a.md_review_overturn
;


/*==============================================================================
 * Step 2: _mm_od — member months
 * Source: hce_ops_archv.gl_rstd_gpsgalnce_f_202603
 *==============================================================================*/
create or replace table tmp_1m.kn_loc_mm_04222026_od as
select
    000000 as fin_inc_week
    , a.fin_inc_month
    , a.fin_inc_year
    , 'MM' as fst_srvc_month
    , 'MM' as adjd_yrmonth
    , 'Membership' as component
    , 'MM' as entity
    , 'MM' as ip_type
    , 1 as loc_flag
    , 'MM' as svc_setting
    , 'MM' as case_cur_svc_cat_dtl_cd
    , a.migration_source
    , case when a.migration_source = 'OAH' then 'OAH' else 'Non-OAH' end as total_oah_flag
    , case when a.fin_product_level_3 = 'INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end as institutional_flag
    , a.fin_tfm_product_new
    , a.tfm_include_flag
    , a.global_cap
    , a.sgr_source_name
    , a.nce_tadm_dec_risk_type
    , a.fin_brand
    , a.fin_g_i
    , a.fin_product_level_3
    , a.fin_plan_level_2
    , case when a.fin_brand = 'M&R' then a.fin_market
        when a.fin_brand = 'C&S' then a.fin_state
      end as fin_market
    , a.fin_contractpbp
    , a.tadm_group_nbr_consist
    , b.group_name
    , 'MM' as do_ind
    , 'MM' as par_nonpar
    , 'MM' as prov_tin
    , 'MM' as hospital_group
    , case when (
            (a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP'))
            or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        ) then 0 else 1
      end as capitated
    , 'MM' as los_categories
    , 0 as los_exp
    , 0 as length_of_stay
    , 'MM' as respiratory_flag
    , 'MM' as ipa_li_split
    , case when a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
            and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
        then 1 else 0
      end as mnr_cosmos_ffs_flag
    , case when a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
            and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
            and a.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO', 'DUAL_CHRONIC')
        then 1 else 0
      end as leading_ind_pop
    , case when a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
            and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')
        then 1 else 0
      end as mnr_nice_ffs_flag
    , case when (
            a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
            and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
        ) or (
            a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
            and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')
        ) then 1 else 0
      end as mnr_total_ffs_flag
    , case when a.fin_brand = 'M&R' and a.migration_source = 'OAH' then 1 else 0 end as mnr_oah_flag
    , case when a.fin_brand = 'C&S' and a.migration_source = 'OAH' then 1
        when a.fin_inc_year = '2024' and a.fin_brand = 'C&S' and a.global_cap = 'NA'
            and a.sgr_source_name in ('COSMOS', 'CSP') and a.migration_source = 'OAH'
            and a.fin_state = 'MD' then 0
        else 0
      end as cns_oah_flag
    , case when a.fin_brand = 'M&R' and a.fin_product_level_3 = 'DUAL' then 1 else 0 end as mnr_dual_flag
    , case when (
            a.fin_brand = 'C&S' and a.migration_source != 'OAH' and a.global_cap = 'NA'
            and a.fin_product_level_3 = 'DUAL' and a.sgr_source_name in ('COSMOS', 'CSP')
        ) or (
            a.fin_inc_year = '2024' and a.fin_brand = 'C&S' and a.global_cap = 'NA'
            and a.sgr_source_name in ('COSMOS', 'CSP') and a.migration_source = 'OAH'
            and a.fin_state = 'MD'
        ) then 1 else 0
      end as cns_dual_flag
    , 'NA' as ocm_migration
    , 0 as swgbed
    , case when a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
            and ((a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP'))
                or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')))
            and a.fin_brand = 'M&R' then 'M&R'
        when a.fin_product_level_3 = 'DUAL' and a.tfm_include_flag = 0
            and ((a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP'))
                or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')))
            and (a.migration_source != 'OAH' or a.migration_source is null)
            and a.fin_brand = 'C&S' then 'C&S'
        else 'Other'
      end as mr_cs_other
    , 'MM' as admit_type
    , 'MM' as ipa_pac_flag
    , 0 as first_adverse
    , 0 as first_not_approved_srvc
    , 0 as first_not_approved_case
    , 0 as md_review_overturn
    , 0 as appealed_cases
    , 0 as overturned_cases
    , 0 as md_rev_appeals
    , 0 as pre_auth_cases
    , 0 as case_count
    , 0 as initial_adr_cnt
    , 0 as persistent_adr_cnt
    , 0 as md_reviewed_cnt
    , 0 as appeal_case_cnt
    , 0 as appeal_ovrtn_case_cnt
    , 0 as mcr_reconsideration_case_cnt
    , 0 as mcr_ovrtn_case_cnt
    , 0 as p2p_case_cnt
    , 0 as p2p_ovrtn_case_cnt
    , 0 as other_ovtrns
    , 0 as member_appeal_cnt
    , 0 as member_appeal_ovtn_cnt
    , sum(a.fin_member_cnt) as membership
    , 0 as days
    , 0 as frank_days
    , 0 as admits
    , 0 as allowed
    , 0 as netpaid
    , 0 as franky_paid
    , 0 as franky_admits
    , 0 as franky_allw
from hce_ops_archv.gl_rstd_gpsgalnce_f_202603 as a
left join fichsrv.group_crosswalk as b
    on a.tadm_group_nbr_consist = b.group_number
    and a.fin_inc_year = b.year
where a.fin_inc_year in ('2022', '2023', '2024', '2025', '2026')
group by
    a.fin_inc_month
    , a.fin_inc_year
    , a.migration_source
    , case when a.migration_source = 'OAH' then 'OAH' else 'Non-OAH' end
    , case when a.fin_product_level_3 = 'INSTITUTIONAL' then 'Institutional' else 'Non-Institutional' end
    , a.fin_tfm_product_new
    , a.tfm_include_flag
    , a.global_cap
    , a.sgr_source_name
    , a.nce_tadm_dec_risk_type
    , a.fin_brand
    , a.fin_g_i
    , a.fin_product_level_3
    , a.fin_plan_level_2
    , case when a.fin_brand = 'M&R' then a.fin_market when a.fin_brand = 'C&S' then a.fin_state end
    , a.fin_contractpbp
    , a.tadm_group_nbr_consist
    , b.group_name
    , case when (
            (a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP'))
            or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        ) then 0 else 1
      end
    , case when a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
            and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
        then 1 else 0
      end
    , case when a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
            and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
            and a.fin_tfm_product_new in ('HMO', 'PPO', 'NPPO', 'DUAL_CHRONIC')
        then 1 else 0
      end
    , case when a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
            and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')
        then 1 else 0
      end
    , case when (
            a.fin_brand = 'M&R' and a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
            and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
        ) or (
            a.fin_brand = 'M&R' and a.sgr_source_name = 'NICE'
            and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')
        ) then 1 else 0
      end
    , case when a.fin_brand = 'M&R' and a.migration_source = 'OAH' then 1 else 0 end
    , case when a.fin_brand = 'C&S' and a.migration_source = 'OAH' then 1
        when a.fin_inc_year = '2024' and a.fin_brand = 'C&S' and a.global_cap = 'NA'
            and a.sgr_source_name in ('COSMOS', 'CSP') and a.migration_source = 'OAH'
            and a.fin_state = 'MD' then 0
        else 0
      end
    , case when a.fin_brand = 'M&R' and a.fin_product_level_3 = 'DUAL' then 1 else 0 end
    , case when (
            a.fin_brand = 'C&S' and a.migration_source != 'OAH' and a.global_cap = 'NA'
            and a.fin_product_level_3 = 'DUAL' and a.sgr_source_name in ('COSMOS', 'CSP')
        ) or (
            a.fin_inc_year = '2024' and a.fin_brand = 'C&S' and a.global_cap = 'NA'
            and a.sgr_source_name in ('COSMOS', 'CSP') and a.migration_source = 'OAH'
            and a.fin_state = 'MD'
        ) then 1 else 0
      end
    , case when a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1
            and ((a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP'))
                or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')))
            and a.fin_brand = 'M&R' then 'M&R'
        when a.fin_product_level_3 = 'DUAL' and a.tfm_include_flag = 0
            and ((a.global_cap = 'NA' and a.sgr_source_name in ('COSMOS', 'CSP'))
                or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN')))
            and (a.migration_source != 'OAH' or a.migration_source is null)
            and a.fin_brand = 'C&S' then 'C&S'
        else 'Other'
      end
;


/*==============================================================================
 * Step 3: _notif_od — auths + member months
 *==============================================================================*/
create or replace table tmp_1m.kn_loc_notif_04222026_od as
select * from tmp_1m.kn_ip_dataset_04222026_4_od
union all
select * from tmp_1m.kn_loc_mm_04222026_od
;


/*==============================================================================
 * Step 4: LOC Valuation Table
 *==============================================================================*/
create or replace table tmp_1m.kn_ip_dataset_loc_04222026_od as
with loc_base as (
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
        , ip_type
        , svc_setting
        , prov_tin
        , sum(case_count) as case_count
        , sum(initial_adr_cnt) as initial_adr_cnt
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
    from tmp_1m.kn_loc_notif_04222026_od
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
        , ip_type
        , svc_setting
        , prov_tin
)
select
    *
    , case
        when total_oah_flag = 'OAH' then 'OAH'
        when institutional_flag = 'Institutional' then 'M&R Institutional'
        when mnr_total_ffs_flag = 1 then 'M&R FFS'
        when cns_dual_flag = 1 then 'C&S DSNP'
        when mnr_dual_flag = 1 then 'M&R DSNP'
      end as population
from loc_base
;
