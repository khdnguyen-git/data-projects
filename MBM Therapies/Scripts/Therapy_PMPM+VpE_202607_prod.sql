/*============================================================================================================
 * OP Therapies PMPM + VpE Calculation
 * 06/30: updated to 202607
 * 04/09: changed cmltv_episodes to ep_num for clarification
 * 04/08: changed population definition
 * 04/07: added separate episode-analysis for CpE; now computing VpE/CpE at the episodes level for TIN and market
 * 03/23: added mbm_category for TIN-summary analysis
 * 03/16: removed tin_owner, re-added clm_dnl_f filter + remove claim_status = 'Denied'
 * 03/13: changed optum_tin_flag to Y/N instead of 1/0
 * 03/12: added _202607 suffix to ALL tables, therapy_category, ahrq, optum_tin_flag + tin_owner
 *        via LEFT JOIN to tmp_1y.cl_therapy_optum_tins_202602
 *        renamed mbm_deploy_dt -> national_pilot_flag
 *        final table: tmp_1q.kn_mbm_visits_episodes_extract_202607
 * 03/12: added paidthru suffix + ahrq
 * 02/12: changed hctapaidmonth to hcta_paid_dt because of format
 * 02/11: added visit_paid_month to recalculate 2024Q1Q2 VpE with similar runout to 2025Q1Q2
 * 02/10: added separate VpE analysis, where visits are counted at the episode level to break VpE into 10s tier and count visits/episodes that fall into these levels
 * 02/09: added rvnu_cd to NICE
 * 02/09: fixed substring(coalesce(bil_typ_cd,'0'), 0, 1) != '3' to substring(coalesce(bil_typ_cd,'0'), 1, 1) != '3'
 * 02/09: fixed home health filters
 * 02/09: removed service_code from visit aggregation
 * 02/05: removing claim_status case when (resulted in, same claim, same ID, 2 claim_status) in extraction,
 *    but adding it back after aggregation (not using clm_dnl_f field)
 * 02/05: changed M&R FFS to M&R FFS excl. DSNP (product_level_3 not in ('DUAL', 'INSTITUTIONAL')
 * 02/05: updated script to use lag() window instead of 2 int. tables
 * 03/16: added clm_dnl_f not in ('D', 'Y') filter to all claims extractions
 * Visits definition (from _stable script): count(concat(eventkey, fst_srvc_dt))
 *  Eventkey: field in claims data, equiv. to mbi | fst_srvc_dt | srvc_prov_id 
 * Episode definition
 *	Partition by mbi-category (Office/Chiro/OP_Rehab), national_pilot_flag (National/Pilot)
 *  Order by fst_srvc_dt
 *  If the (current fst_srvc_dt - previous fst_srvc_dt) for this partition > 30 -> New Episode
 *  Or if (current fst_srvc_dt - previous fst_srvc_dt) is NULL -> New Episode
 *===========================================================================================================*/


/*==============================================================================
 * Claims
 *==============================================================================*/


-- All 6 entities in single CTE with therapy-specific filters
create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_claims_202607 as
with claims_raw as (
-- COSMOS PR
select
    'COSMOS' as entity
    , a.component
    , a.eventkey as visit_id
    , a.service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as mbm_category
    , case
        when a.proc_cd in ('98940','98941','98942') then 'Chiro'
        when a.proc_cd in (
            '97001','97002','97003','97004','97012','97014','97016','97018','97022','97024','97026','97028'
            ,'97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139'
            ,'97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532'
            ,'97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799'
            ,'G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'
        ) then 'PT-OT'
        when a.proc_cd in (
            '70371','92506','92507','92508','92521','92522','92523','92524','92526','92609','92626','92627'
            ,'92630','92633','96105','97129','97130','S9128'
        ) then 'ST'
        else 'Other'
    end as therapy_category
    , a.prov_tin
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
    --, case when c.tin is not null then 'Y' else 'N' end as rural_tin_flag
    --, c.hospital as rural_hospital
    , 'N' as rural_tin_flag
    , cast(null as varchar) as rural_hospital
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
        else 'National'
    end as national_pilot_flag
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.glxy_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.prov_tin = b.tin
--left join tmp_1m.THERAPIES_TIN_RURAL as c
--    on a.prov_tin = c.tin
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.prov_prtcp_sts_cd = 'P'
    and a.st_abbr_cd = a.market_fnl
    and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
    and a.ama_pl_of_srvc_cd != '12'
    and (
        a.proc_cd in (
             '97012','97016','97018','97022','97024','97026','97028','97032'
             ,'97033','97034','97035','97036','97039','97110','97112','97113'
             ,'97116','97124','97139','97140','97150','97164','97168','97530'
             ,'97533','97535','97537','97542','97545','97546','97750','97755'
             ,'97760','97761','97799','G0283'
             ,'92507','92508','92526'
             ,'98940','98941','98942'
        )
        or a.rvnu_cd in (
             '0430','0431','0432','0433','0434','0439','0420','0421','0422'
             ,'0423','0424','0429','0440','0441','0442','0443','0444','0449'
        )
    )
    and a.proc_cd not in (
         '92630','92633','97001','97002','97003','97004','97545','97546','98943'
        ,'G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131'
    )
    and a.fst_srvc_year >= '2023'
    and a.clm_dnl_f not in ('D', 'Y')
union all
-- COSMOS OP
select
    'COSMOS' as entity
    , a.component
    , a.eventkey as visit_id
    , a.hce_service_code as service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as mbm_category
    , case
        when a.proc_cd in ('98940','98941','98942') then 'Chiro'
        when a.proc_cd in (
            '97001','97002','97003','97004','97012','97014','97016','97018','97022','97024','97026','97028'
            ,'97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139'
            ,'97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532'
            ,'97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799'
            ,'G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'
        ) then 'PT-OT'
        when a.proc_cd in (
            '70371','92506','92507','92508','92521','92522','92523','92524','92526','92609','92626','92627'
            ,'92630','92633','96105','97129','97130','S9128'
        ) then 'ST'
        else 'Other'
    end as therapy_category
    , a.prov_tin
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
    --, case when c.tin is not null then 'Y' else 'N' end as rural_tin_flag
    --, c.hospital as rural_hospital
    , 'N' as rural_tin_flag
    , cast(null as varchar) as rural_hospital
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
        else 'National'
    end as national_pilot_flag
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.glxy_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.prov_tin = b.tin
--left join tmp_1m.THERAPIES_TIN_RURAL as c
--    on a.prov_tin = c.tin
where a.brand_fnl in ('M&R', 'C&S')
    and a.global_cap = 'NA'
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.prov_prtcp_sts_cd = 'P'
    and a.st_abbr_cd = a.market_fnl
    and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
    and a.ama_pl_of_srvc_cd != '12'
    and (
        a.proc_cd in (
             '97012','97016','97018','97022','97024','97026','97028','97032'
             ,'97033','97034','97035','97036','97039','97110','97112','97113'
             ,'97116','97124','97139','97140','97150','97164','97168','97530'
             ,'97533','97535','97537','97542','97545','97546','97750','97755'
             ,'97760','97761','97799','G0283'
             ,'92507','92508','92526'
             ,'98940','98941','98942'
        )
        or a.rvnu_cd in (
             '0430','0431','0432','0433','0434','0439','0420','0421','0422'
             ,'0423','0424','0429','0440','0441','0442','0443','0444','0449'
        )
    )
    and a.proc_cd not in (
         '92630','92633','97001','97002','97003','97004','97545','97546','98943'
        ,'G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131'
    )
    and a.fst_srvc_year >= '2023'
    and a.clm_dnl_f not in ('D', 'Y')
union all
-- CSP PR
select
    'CSP' as entity
    , a.component
    , a.eventkey as visit_id
    , a.service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as mbm_category
    , case
        when a.proc_cd in ('98940','98941','98942') then 'Chiro'
        when a.proc_cd in (
            '97001','97002','97003','97004','97012','97014','97016','97018','97022','97024','97026','97028'
            ,'97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139'
            ,'97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532'
            ,'97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799'
            ,'G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'
        ) then 'PT-OT'
        when a.proc_cd in (
            '70371','92506','92507','92508','92521','92522','92523','92524','92526','92609','92626','92627'
            ,'92630','92633','96105','97129','97130','S9128'
        ) then 'ST'
        else 'Other'
    end as therapy_category
    , substring(a.tin, 1, 9) as prov_tin
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
    --, case when c.tin is not null then 'Y' else 'N' end as rural_tin_flag
    --, c.hospital as rural_hospital
    , 'N' as rural_tin_flag
    , cast(null as varchar) as rural_hospital
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
        else 'National'
    end as national_pilot_flag
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.dcsp_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on substring(a.tin, 1, 9) = b.tin
--left join tmp_1m.THERAPIES_TIN_RURAL as c
--    on substring(a.tin, 1, 9) = c.tin
where a.brand_fnl = 'C&S'
    and a.global_cap = 'NA'
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.prov_prtcp_sts_cd = 'P'
    and a.st_abbr_cd = a.market_fnl
    and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
    and a.ama_pl_of_srvc_cd != '12'
    and (
        a.proc_cd in (
             '97012','97016','97018','97022','97024','97026','97028','97032'
             ,'97033','97034','97035','97036','97039','97110','97112','97113'
             ,'97116','97124','97139','97140','97150','97164','97168','97530'
             ,'97533','97535','97537','97542','97545','97546','97750','97755'
             ,'97760','97761','97799','G0283'
             ,'92507','92508','92526'
             ,'98940','98941','98942'
        )
        or a.rvnu_cd in (
             '0430','0431','0432','0433','0434','0439','0420','0421','0422'
             ,'0423','0424','0429','0440','0441','0442','0443','0444','0449'
        )
    )
    and a.proc_cd not in (
         '92630','92633','97001','97002','97003','97004','97545','97546','98943'
        ,'G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131'
    )
    and a.fst_srvc_year >= '2023'
    and a.clm_dnl_f not in ('D', 'Y')
union all
-- CSP OP
select
    'CSP' as entity
    , a.component
    , a.eventkey as visit_id
    , a.service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
    , a.gal_mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as mbm_category
    , case
        when a.proc_cd in ('98940','98941','98942') then 'Chiro'
        when a.proc_cd in (
            '97001','97002','97003','97004','97012','97014','97016','97018','97022','97024','97026','97028'
            ,'97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139'
            ,'97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532'
            ,'97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799'
            ,'G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'
        ) then 'PT-OT'
        when a.proc_cd in (
            '70371','92506','92507','92508','92521','92522','92523','92524','92526','92609','92626','92627'
            ,'92630','92633','96105','97129','97130','S9128'
        ) then 'ST'
        else 'Other'
    end as therapy_category
    , substring(a.tin, 1, 9) as prov_tin
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
    --, case when c.tin is not null then 'Y' else 'N' end as rural_tin_flag
    --, c.hospital as rural_hospital
    , 'N' as rural_tin_flag
    , cast(null as varchar) as rural_hospital
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , a.global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , a.migration_source
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
        else 'National'
    end as national_pilot_flag
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
from fichsrv.dcsp_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on substring(a.tin, 1, 9) = b.tin
--left join tmp_1m.THERAPIES_TIN_RURAL as c
--    on substring(a.tin, 1, 9) = c.tin
where a.brand_fnl = 'C&S'
    and a.global_cap = 'NA'
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.prov_prtcp_sts_cd = 'P'
    and a.st_abbr_cd = a.market_fnl
    and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
    and a.ama_pl_of_srvc_cd != '12'
    and (
        a.proc_cd in (
             '97012','97016','97018','97022','97024','97026','97028','97032'
             ,'97033','97034','97035','97036','97039','97110','97112','97113'
             ,'97116','97124','97139','97140','97150','97164','97168','97530'
             ,'97533','97535','97537','97542','97545','97546','97750','97755'
             ,'97760','97761','97799','G0283'
             ,'92507','92508','92526'
             ,'98940','98941','98942'
        )
        or a.rvnu_cd in (
             '0430','0431','0432','0433','0434','0439','0420','0421','0422'
             ,'0423','0424','0429','0440','0441','0442','0443','0444','0449'
        )
    )
    and a.proc_cd not in (
         '92630','92633','97001','97002','97003','97004','97545','97546','98943'
        ,'G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131'
    )
    and a.fst_srvc_year >= '2023'
    and a.clm_dnl_f not in ('D', 'Y')
union all
-- NICE PR
select
    'NICE' as entity
    , a.component
    , a.eventkey as visit_id
    , a.service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
    , a.mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as mbm_category
    , case
        when a.proc_cd in ('98940','98941','98942') then 'Chiro'
        when a.proc_cd in (
            '97001','97002','97003','97004','97012','97014','97016','97018','97022','97024','97026','97028'
            ,'97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139'
            ,'97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532'
            ,'97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799'
            ,'G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'
        ) then 'PT-OT'
        when a.proc_cd in (
            '70371','92506','92507','92508','92521','92522','92523','92524','92526','92609','92626','92627'
            ,'92630','92633','96105','97129','97130','S9128'
        ) then 'ST'
        else 'Other'
    end as therapy_category
    , a.tin as prov_tin
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
    --, case when c.tin is not null then 'Y' else 'N' end as rural_tin_flag
    --, c.hospital as rural_hospital
    , 'N' as rural_tin_flag
    , cast(null as varchar) as rural_hospital
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , iff(a.clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , 'NA' as migration_source
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
        else 'National'
    end as national_pilot_flag
    , a.calc_allw as allw_amt_fnl
    , a.calc_net_pd as net_pd_amt_fnl
from fichsrv.nce_pr_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.tin = b.tin
--left join tmp_1m.THERAPIES_TIN_RURAL as c
--    on a.tin = c.tin
where a.brand_fnl = 'M&R'
    and a.plan_level_2_fnl not in ('PFFS')
    and a.prov_prtcp_sts_cd = 'P'
    and a.st_abbr_cd = a.market_fnl
    and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PHYSICIAN'))
    and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
    and a.claim_place_of_svc_cd != '12'
    and (
        a.proc_cd in (
             '97012','97016','97018','97022','97024','97026','97028','97032'
             ,'97033','97034','97035','97036','97039','97110','97112','97113'
             ,'97116','97124','97139','97140','97150','97164','97168','97530'
             ,'97533','97535','97537','97542','97545','97546','97750','97755'
             ,'97760','97761','97799','G0283'
             ,'92507','92508','92526'
             ,'98940','98941','98942'
        )
        or a.rvnu_cd in (
             '0430','0431','0432','0433','0434','0439','0420','0421','0422'
             ,'0423','0424','0429','0440','0441','0442','0443','0444','0449'
        )
    )
    and a.proc_cd not in (
         '92630','92633','97001','97002','97003','97004','97545','97546','98943'
        ,'G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131'
    )
    and a.fst_srvc_year >= '2023'
    and a.dnl_f not in ('D', 'Y')
union all
-- NICE OP
select
    'NICE' as entity
    , a.component
    , a.eventkey as visit_id
    , a.service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hcta_paid_dt
    , a.fst_srvc_year
    , a.mbi_hicn_fnl as mbi
    , a.proc_cd
    , case
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as mbm_category
    , case
        when a.proc_cd in ('98940','98941','98942') then 'Chiro'
        when a.proc_cd in (
            '97001','97002','97003','97004','97012','97014','97016','97018','97022','97024','97026','97028'
            ,'97032','97033','97034','97035','97036','97039','97110','97112','97113','97116','97124','97139'
            ,'97140','97150','97161','97162','97163','97164','97165','97166','97167','97168','97530','97532'
            ,'97533','97535','97537','97542','97545','97546','97750','97755','97760','97761','97762','97799'
            ,'G0129','G0151','G0152','G0281','G0282','G0283','G9041','G9043','G9044','S9129','S9131'
        ) then 'PT-OT'
        when a.proc_cd in (
            '70371','92506','92507','92508','92521','92522','92523','92524','92526','92609','92626','92627'
            ,'92630','92633','96105','97129','97130','S9128'
        ) then 'ST'
        else 'Other'
    end as therapy_category
    , a.tin as prov_tin
    , case when b.tin is not null then 'Y' else 'N' end as optum_tin_flag
    --, case when c.tin is not null then 'Y' else 'N' end as rural_tin_flag
    --, c.hospital as rural_hospital
    , 'N' as rural_tin_flag
    , cast(null as varchar) as rural_hospital
    , a.primary_diag_cd
    , a.ahrq_diag_genl_catgy_desc
    , a.ahrq_diag_dtl_catgy_desc
    , iff(a.clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , a.market_fnl
    , a.st_abbr_cd
    , a.brand_fnl
    , a.group_ind_fnl
    , a.tfm_include_flag
    , 'NA' as migration_source
    , a.product_level_3_fnl
    , case when a.market_fnl in ('AR','GA','NJ','SC') and a.group_ind_fnl = 'I' then 'Pilot'
        else 'National'
    end as national_pilot_flag
    , a.allw_amt as allw_amt_fnl
    , a.net_pd_amt as net_pd_amt_fnl
from fichsrv.nce_op_f as a
left join tmp_1y.cl_therapy_optum_tins_202602 as b
    on a.tin = b.tin
--left join tmp_1m.THERAPIES_TIN_RURAL as c
--    on a.tin = c.tin
where a.brand_fnl = 'M&R'
    and a.plan_level_2_fnl not in ('PFFS')
    and a.prov_prtcp_sts_cd = 'P'
    and a.st_abbr_cd = a.market_fnl
    and (a.clm_cap_flag = 'FFS' and a.dec_risk_type_fnl in ('FFS', 'PHYSICIAN'))
    and substring(coalesce(a.bil_typ_cd,'0'),1,1) != '3'
    and a.claim_place_of_svc_cd != '12'
    and (
        a.proc_cd in (
             '97012','97016','97018','97022','97024','97026','97028','97032'
             ,'97033','97034','97035','97036','97039','97110','97112','97113'
             ,'97116','97124','97139','97140','97150','97164','97168','97530'
             ,'97533','97535','97537','97542','97545','97546','97750','97755'
             ,'97760','97761','97799','G0283'
             ,'92507','92508','92526'
             ,'98940','98941','98942'
        )
        or a.rvnu_cd in (
             '0430','0431','0432','0433','0434','0439','0420','0421','0422'
             ,'0423','0424','0429','0440','0441','0442','0443','0444','0449'
        )
    )
    and a.proc_cd not in (
         '92630','92633','97001','97002','97003','97004','97545','97546','98943'
        ,'G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131'
    )
    and a.fst_srvc_year >= '2023'
    and a.dnl_f not in ('D', 'Y')
)
select
    *
    , case
        when migration_source = 'OAH'
            and not (brand_fnl = 'C&S' and fst_srvc_year = '2024' and st_abbr_cd = 'MD')
            and not (brand_fnl != 'C&S' and fst_srvc_year = '2024' and market_fnl = 'MD')
        then 'OAH'
        when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 'M&R ISNP'
        when entity in ('COSMOS', 'NICE')
            and brand_fnl = 'M&R'
            and global_cap = 'NA'
            and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL')
            and tfm_include_flag = 1
        then 'M&R FFS (excl. DSNP)'
        when entity in ('COSMOS', 'CSP')
            and global_cap = 'NA'
            and (
                (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
                or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD')
                or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
            )
        then 'C&S DSNP'
        when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 'M&R DSNP'
        else 'N/A'
      end as population
from claims_raw
;


-- Aggregate to sum(allowed) and sum(paid) before VpE analysis
-- Adding claim_status
create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_claims_aggregated_202607 as
with aggregated as (
select
	population
	, entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
	, fst_srvc_month
	, fst_srvc_qtr
	, hcta_paid_dt
	, fst_srvc_year
	, mbi
	, proc_cd
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, primary_diag_cd
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	--, tfm_product_new_fnl
	, product_level_3_fnl
	, national_pilot_flag
    , sum(allw_amt_fnl) as allw_amt_fnl
    , sum(net_pd_amt_fnl) as net_pd_amt_fnl
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_202607
group by
	population
	, entity
	, component
	, visit_id
	, service_code
	, fst_srvc_dt
	, fst_srvc_month
	, fst_srvc_qtr
	, hcta_paid_dt
	, fst_srvc_year
	, mbi
	, proc_cd
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, primary_diag_cd
	, ahrq_diag_dtl_catgy_desc
	, global_cap
	, market_fnl
	, st_abbr_cd
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	--, tfm_product_new_fnl
	, product_level_3_fnl
	, national_pilot_flag
)
select
	*
	, iff(sum(allw_amt_fnl) over (partition by visit_id, fst_srvc_dt, mbm_category)  > 0.01, 'Paid', 'Denied') as claim_status -- reserving logic from original script
from aggregated
;

-- select sum(allowed) from tmp_1m.kn_mbm_cosmos_csp_nice_claims_aggregated_202607
-- where population = 'M&R FFS (excl. DSNP)' and fst_srvc_month between '202501' and '202512' 


-- Defining visits grouping structure
create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_1_202607 as
select 
    entity
    , concat(mbi, '-', mbm_category) as mbi_key
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , min(hcta_paid_dt) as min_hcta_paid_dt
    , fst_srvc_year
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_aggregated_202607
group by
    entity
    , concat(mbi, '-', mbm_category)
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_year
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
;

-- Flag new episode
create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_2_202607 as
select
	mbi_key
	, entity
	, component
	, visit_id
	, fst_srvc_dt
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , allowed
    , paid
 	, lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt) 
 	as prev_srvc_dt
    , datediff('day'
    		, lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt)
    		, fst_srvc_dt) 
    as visit_day_diff
    , iff(datediff('day'
    		, lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt)
    		, fst_srvc_dt) > 30, 1 , 0) 
    as ep_start_flag
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_1_202607
;


-- Count episodes per group + define episode boundary
create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_3_202607 as
with ep_numbering as 
(
select
	*
  	, sum(iff(prev_srvc_dt is null, 1, ep_start_flag)) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt rows between unbounded preceding and current row) 
  	as ep_num
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_2_202607
)
select 
	mbi_key
	, fst_srvc_dt
	, prev_srvc_dt
	, visit_day_diff
	, iff(prev_srvc_dt is null, 1, ep_start_flag) as ep_start_flag 
	, ep_num
	, min(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag, ep_num) as ep_start_dt
	, min(min_hcta_paid_dt) over (partition by mbi_key, national_pilot_flag, ep_num) as ep_hcta_paid_dt
	, entity
	, visit_id
    , fst_srvc_month
    , min_hcta_paid_dt
    , fst_srvc_year
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
    , market_fnl
	, national_pilot_flag
    , population
    , claim_status
    , allowed
    , paid    
from ep_numbering
;
-- Episodes summary
create or replace table tmp_1q.kn_mbm_episodes_summary_202607 as
select 
	'EPISODES' as data_type
	, to_char(ep_start_dt, 'yyyyMM') as ep_start_month
	, to_char(ep_start_dt, 'yyyy') as ep_start_year
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
	, cast(null as varchar) as visit_month
	, cast(null as varchar) as visit_year
	, cast(null as varchar) as visit_paid_month
	, ep_hcta_paid_dt as ep_paid_month
	, entity
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
	, 0 as visit_ep_runout_month
	, 0 as visit_runout_month
	, sum(ep_start_flag) as n_episodes
	, 0 as n_visits
	, 0 as sum_allowed
	, 0 as sum_paid
	, 0 as mbr_count
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_3_202607
where ep_start_flag = 1  -- Filter for only episode-starting visits
group by 
	to_char(ep_start_dt, 'yyyyMM')
	, to_char(ep_start_dt, 'yyyy')
	, substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
	, ep_hcta_paid_dt
	, entity
	, mbm_category
	, therapy_category
	, prov_tin
    , rural_hospital
	, optum_tin_flag
	, rural_tin_flag
	, ahrq_diag_dtl_catgy_desc
	, market_fnl
	, national_pilot_flag
	, population
	, claim_status
;

-- Visits summary
create or replace table tmp_1q.kn_mbm_visits_summary_202607 as
select
    'VISITS' as data_type
    , to_char(ep_start_dt, 'yyyyMM') as ep_start_month
    , to_char(ep_start_dt, 'yyyy') as ep_start_year
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2) as ep_start_month_num
    , fst_srvc_month as visit_month
    , fst_srvc_year as visit_year
    , min_hcta_paid_dt as visit_paid_month
    , cast(null as varchar) as ep_paid_month
    , entity
    , mbm_category
    , therapy_category
    , prov_tin
    -- , rural_hospital
    , optum_tin_flag
    -- , rural_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5) as visit_ep_runout_month
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5) as visit_runout_month
    , 0 as n_episodes
    , count(distinct concat(visit_id, fst_srvc_dt)) as n_visits
    , sum(allowed) as sum_allowed
    , sum(paid) as sum_paid
    , count(distinct mbi_key) as mbr_count
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_vpe_3_202607
group by
    to_char(ep_start_dt, 'yyyyMM')
    , to_char(ep_start_dt, 'yyyy')
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
    , fst_srvc_month
    , fst_srvc_year
    , min_hcta_paid_dt
    , entity
    , mbm_category
    , therapy_category
    , prov_tin
    -- , rural_hospital
    , optum_tin_flag
    -- , rural_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5)
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5)
;


-- Stack VISITS and EPISODES
create or replace table tmp_1q.kn_mbm_visits_episodes_stacked_202607 as
select * from tmp_1q.kn_mbm_visits_summary_202607
union all
select * from tmp_1q.kn_mbm_episodes_summary_202607
;

-- Summary 1
create or replace table tmp_1q.kn_mbm_vpe_summary_202607 as
select
    ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , mbm_category
    , therapy_category
    , prov_tin
    -- , rural_hospital
    , optum_tin_flag
    -- , rural_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
    , sum(n_episodes) as total_episodes
    , sum(n_visits) as total_visits
    , sum(sum_allowed) as allowed
    , sum(sum_paid) as paid
    , sum(mbr_count) as mbr_count
from tmp_1q.kn_mbm_visits_episodes_stacked_202607
where population != 'NA'
group by
    ep_start_month
    , ep_start_year
    , ep_start_month_num
    , visit_month
    , visit_year
    , visit_paid_month
    , ep_paid_month
    , entity
    , mbm_category
    , therapy_category
    , prov_tin
    , rural_hospital
    , optum_tin_flag
    , rural_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
;

select * from tmp_1q.kn_mbm_pmpm_summary_202607;




-- Summary for Rural TIN
create or replace table tmp_1m.knd_mbm_rural_tin_extract_202607 as
select
    ep_start_month
    , therapy_category
    , prov_tin
    , rural_hospital
    , market_fnl
    , rural_tin_flag
    , sum(total_episodes) as episodes
    , sum(total_visits) as visits
    , sum(allowed) as allowed
from tmp_1q.kn_mbm_vpe_summary_202607
where population = 'M&R FFS (excl. DSNP)'
    and ep_start_month >= '202501'
group by
    ep_start_month
    , therapy_category
    , prov_tin
    , rural_hospital
    , market_fnl
    , rural_tin_flag
    , ahrq_diag_dtl_catgy_desc
;
select * from tmp_1m.knd_mbm_rural_tin_extract_202607
-- where ep_start_month between '202501' and '202512' and therapy_category in ('PT-OT', 'ST', 'Chiro')
where ep_start_month >= '202501' and therapy_category in ('PT-OT', 'ST', 'Chiro')







-- Excel table 1
create or replace table tmp_1q.kn_mbm_visits_episodes_extract_202607 as
select sum(allowed)  from (
select
	population
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , therapy_category as category
    , market_fnl
    , sum(mbr_count) as unique_member_count
    , sum(total_episodes) as episode_count
    , sum(total_visits) as visit_count
    , sum(allowed) as allowed
from tmp_1q.kn_mbm_vpe_summary_202607
where ep_start_month between '202409' and '202508' 
group by 
	population
	, prov_tin
	, optum_tin_flag
	, rural_tin_flag
    , ahrq_diag_dtl_catgy_desc
    , therapy_category
    , market_fnl
)
where population = 'M&R FFS (excl. DSNP)' and category = 'Office'
-- group by 1
-- order by 1



select sum(allowed)
from tmp_1q.kn_mbm_visits_episodes_extract_202607 where 


-- Summary 2
create or replace table tmp_1q.kn_mbm_vpe_tin_summary_202607 as
with agg as (
select
    population
    , prov_tin
    , ep_start_month
    , optum_tin_flag
    , rural_tin_flag
    , therapy_category as category
    , ahrq_diag_dtl_catgy_desc
    , market_fnl    
    , sum(n_episodes) as total_episodes
    , sum(n_visits) as total_visits
    , sum(sum_allowed) as allowed
    , sum(sum_paid) as paid
    , sum(mbr_count) as mbr_count
from tmp_1q.kn_mbm_visits_episodes_stacked_202607
where population != 'NA'
group by
    population
    , prov_tin
    , ep_start_month
    , optum_tin_flag
    , rural_tin_flag
    , therapy_category
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
)
select 
    population
    , prov_tin
    , optum_tin_flag
    , rural_tin_flag
    , category
    , ahrq_diag_dtl_catgy_desc
    , market_fnl    
    , sum(total_episodes) as episode_count
    , sum(total_visits) as visit_count
    , sum(allowed) as allowed
    , sum(paid) as paid
    , sum(mbr_count) as unique_member_count
from agg
where ep_start_month >= '202501'
group by
    population
    , prov_tin
    , optum_tin_flag
    , rural_tin_flag
    , category
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
;







-- Episodes-level summary + VpE Tiers moved to: therapy_VpE_Episodes_adhoc.sql
-- (Run that script after this one completes)





/*==============================================================================
 * Membership
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_mm_202607 as 
with mm_raw as (
select
	sgr_source_name as entity
	, '' as component
	, '' as service_code
	, fin_inc_month as fst_srvc_month
	, fin_inc_year as fst_srvc_year
	, global_cap
	, nce_tadm_dec_risk_type
	, fin_market as market_fnl
	, fin_state as st_abbr_cd
	, fin_brand as brand_fnl
	, fin_g_i as group_ind_fnl
	, tfm_include_flag
	, migration_source
	, fin_tfm_product_new as tfm_product_new_fnl 
	, fin_product_level_3 as product_level_3_fnl
	, fin_member_cnt
	, fin_mbi_hicn_fnl
from fichsrv.tre_membership
where		
	sgr_source_name in ('COSMOS', 'CSP', 'NICE')
	and fin_inc_month >= '202301'
)
, 
mm_flag as (
select
    *
	, case when brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' then 0
			when brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' then 0
			when migration_source = 'OAH' then 1
			else 0
		end as OAH_flag
	, case when (
			   (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL' and global_cap = 'NA')
			or (entity in ('COSMOS', 'CSP') and brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and st_abbr_cd = 'MD' and global_cap = 'NA') 
			or (entity in ('COSMOS', 'CSP') and brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD' and global_cap = 'NA')
			) then 1
		else 0
	end as CnS_DSNP_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'DUAL' then 1
		else 0
	end as MnR_DSNP_flag
	, case when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 1
		else 0
	end as MnR_ISNP_flag
	, case when (
			(entity = 'COSMOS' and brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1)
 		 or (entity = 'NICE' and brand_fnl = 'M&R' and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN') and product_level_3_fnl != 'INSTITUTIONAL' and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_flag
 	, case when (
			(entity = 'COSMOS' and brand_fnl = 'M&R' and global_cap = 'NA' and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 or (entity = 'NICE' and brand_fnl = 'M&R' and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN') and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL') and tfm_include_flag = 1) 
 		 ) then 1 
 		else 0 
 	end as MnR_FFS_noDSNP_flag
from mm_raw
)
select
    *
    , case
        when oah_flag = 1 then 'OAH'
        when mnr_DSNP_flag = 1 then 'M&R DSNP'
        when mnr_isnp_flag = 1 then 'M&R ISNP'
        when cns_DSNP_flag = 1 then 'C&S DSNP'
        when mnr_ffs_nodsnp_flag = 1 then 'M&R FFS (excl. DSNP)'
        else 'N/A'
      end as population
from mm_flag;


-- (removed: second CREATE for kn_mbm_cosmos_csp_nice_mm was referencing nonexistent tables)


create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_mm_summary_202607 as 
select 
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
    , sum(fin_member_cnt) as sum_mm
from tmp_1q.kn_mbm_cosmos_csp_nice_mm_202607
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_new_fnl
    , product_level_3_fnl
;


/*==============================================================================
 * Union Claims and Membership
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_cosmos_csp_nice_claims_mm_summary_202607 as
select
	'Claims' as data_type
	, entity
	, population
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , product_level_3_fnl
    , sum(allw_amt_fnl) as sum_allowed
    , sum(net_pd_amt_fnl) as sum_paid
    , 0 as sum_mm
from tmp_1q.kn_mbm_cosmos_csp_nice_claims_aggregated_202607
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , product_level_3_fnl
union all
select
	'Membership' as data_type
	, entity
	, population
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , product_level_3_fnl
    , 0 as sum_allowed
    , 0 as sum_paid
    , sum(sum_mm) as sum_mm
from tmp_1q.kn_mbm_cosmos_csp_nice_mm_summary_202607
group by
	entity
	, population
	, component
	, service_code
    , fst_srvc_month
    , fst_srvc_year
	, global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , product_level_3_fnl
;

