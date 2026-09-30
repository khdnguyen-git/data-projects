/*============================================================================================================
 * OP Therapies PMPM + VpE Calculation — DEV
 * Purpose: Same as _prod but WITHOUT clm_dnl_f filter, to match _stable's approach
 *          (denied claims stay in extraction, claim_status assigned post-aggregation)
 *          Also removed: optum_tin_flag, rural_tin_flag, rural_hospital (not needed for VpE comparison)
 * Compare: VpE between this dev, prod, and _stable for 202601-202604
 *===========================================================================================================*/


/*==============================================================================
 * Claims — no clm_dnl_f filter, no optum/rural columns
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_202608 as
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


/*==============================================================================
 * Aggregation + claim_status
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_202608 as
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
    , global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , product_level_3_fnl
    , national_pilot_flag
    , sum(allw_amt_fnl) as allw_amt_fnl
    , sum(net_pd_amt_fnl) as net_pd_amt_fnl
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_202608
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
    , primary_diag_cd
    , ahrq_diag_dtl_catgy_desc
    , global_cap
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , product_level_3_fnl
    , national_pilot_flag
)
select
    *
    , iff(sum(allw_amt_fnl) over (partition by visit_id, fst_srvc_dt, mbm_category) > 0.01, 'Paid', 'Denied') as claim_status
from aggregated
;


/*==============================================================================
 * VpE Step 1: visits grouping
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_202608 as
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
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_aggregated_202608
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
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
;


/*==============================================================================
 * VpE Step 2: flag new episode
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_202608 as
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
        , fst_srvc_dt) > 30, 1, 0)
      as ep_start_flag
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_1_202608
;


/*==============================================================================
 * VpE Step 3: episode numbering + ep_start_dt
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_202608 as
with ep_numbering as (
select
    *
    , sum(iff(prev_srvc_dt is null, 1, ep_start_flag)) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt rows between unbounded preceding and current row)
      as ep_num
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_2_202608
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
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , allowed
    , paid
from ep_numbering
;


/*==============================================================================
 * Episodes summary
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_episodes_summary_202608 as
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
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_202608
where ep_start_flag = 1
group by
    to_char(ep_start_dt, 'yyyyMM')
    , to_char(ep_start_dt, 'yyyy')
    , substring(to_char(ep_start_dt, 'yyyyMM'), 5, 2)
    , ep_hcta_paid_dt
    , entity
    , mbm_category
    , therapy_category
    , prov_tin
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
;


/*==============================================================================
 * Visits summary
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_visits_summary_202608 as
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
from tmp_1m.knd_mbm_cosmos_csp_nice_claims_vpe_3_202608
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
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5)
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5)
;


/*==============================================================================
 * Stack visits + episodes
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_visits_episodes_stacked_202608 as
select * from tmp_1m.knd_mbm_visits_summary_202608
union all
select * from tmp_1m.knd_mbm_episodes_summary_202608
;


/*==============================================================================
 * VpE summary
 *==============================================================================*/
create or replace table tmp_1m.knd_mbm_vpe_summary_202608 as
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
from tmp_1m.knd_mbm_visits_episodes_stacked_202608
where population != 'N/A'
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
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , national_pilot_flag
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
;


/*==============================================================================
 * COMPARISON QUERIES: VpE for 202601-202604
 *==============================================================================*/

-- 1) DEV vs PROD (PMPM+VpE): full population, all statuses
--    Expect: dev has MORE visits/episodes (denied claims now included)
select
    'PROD' as source
    , ep_start_month
    , mbm_category
    , sum(total_visits) as visits
    , sum(total_episodes) as episodes
    , sum(total_visits) / nullif(sum(total_episodes), 0) as vpe
from tmp_1q.kn_mbm_vpe_summary_202608
where population = 'M&R FFS (excl. DSNP)'
    and national_pilot_flag = 'National'
    and entity = 'COSMOS'
    and mbm_category in ('Office', 'OP_REHAB', 'Chiro')
    and ep_start_month between '202601' and '202604'
group by 2, 3
union all
select
    'DEV' as source
    , ep_start_month
    , mbm_category
    , sum(total_visits) as visits
    , sum(total_episodes) as episodes
    , sum(total_visits) / nullif(sum(total_episodes), 0) as vpe
from tmp_1m.knd_mbm_vpe_summary_202608
where population = 'M&R FFS (excl. DSNP)'
    and national_pilot_flag = 'National'
    and entity = 'COSMOS'
    and mbm_category in ('Office', 'OP_REHAB', 'Chiro')
    and ep_start_month between '202601' and '202604'
group by 2, 3
order by 2, 3, 1
;


-- 2) DEV vs STABLE: matching filters for apples-to-apples
--    DEV filtered to COSMOS + M&R FFS + National + all claim statuses
--    STABLE filtered to National + all categories (no claim_status filter)
select
    'DEV' as source
    , ep_start_month
    , mbm_category as category
    , sum(total_visits) as visits
    , sum(total_episodes) as episodes
    , sum(total_visits) / nullif(sum(total_episodes), 0) as vpe
from tmp_1m.knd_mbm_vpe_summary_202608
where population = 'M&R FFS (excl. DSNP)'
    and national_pilot_flag = 'National'
    and entity = 'COSMOS'
    and mbm_category in ('Office', 'OP_REHAB', 'Chiro')
    and ep_start_month between '202601' and '202604'
group by 2, 3
union all
select
    'STABLE' as source
    , ep_start_mo as ep_start_month
    , category
    , sum(visit_cnt) as visits
    , sum(ep_cnt) as episodes
    , sum(visit_cnt) / nullif(sum(ep_cnt), 0) as vpe
from tmp_1q.kn_mbm_202608
where pilot_nat = 'National'
    and category in ('Office', 'OP_REHAB', 'Chiro')
    and ep_start_mo between '202601' and '202604'
group by 2, 3
order by 2, 3, 1
;


-- 3) Summary totals (all categories combined)
select
    'DEV' as source
    , sum(total_visits) as visits
    , sum(total_episodes) as episodes
    , sum(total_visits) / nullif(sum(total_episodes), 0) as vpe
from tmp_1m.knd_mbm_vpe_summary_202608
where population = 'M&R FFS (excl. DSNP)'
    and national_pilot_flag = 'National'
    and entity = 'COSMOS'
    and mbm_category in ('Office', 'OP_REHAB', 'Chiro')
    and ep_start_month between '202601' and '202604'
union all
select
    'PROD' as source
    , sum(total_visits) as visits
    , sum(total_episodes) as episodes
    , sum(total_visits) / nullif(sum(total_episodes), 0) as vpe
from tmp_1q.kn_mbm_vpe_summary_202607
where population = 'M&R FFS (excl. DSNP)'
    and national_pilot_flag = 'National'
    and entity = 'COSMOS'
    and mbm_category in ('Office', 'OP_REHAB', 'Chiro')
    and ep_start_month between '202601' and '202604'
union all
select
    'STABLE' as source
    , sum(visit_cnt) as visits
    , sum(ep_cnt) as episodes
    , sum(visit_cnt) / nullif(sum(ep_cnt), 0) as vpe
from tmp_1q.kn_mbm_202608
where pilot_nat = 'National'
    and category in ('Office', 'OP_REHAB', 'Chiro')
    and ep_start_mo between '202601' and '202604'
;
