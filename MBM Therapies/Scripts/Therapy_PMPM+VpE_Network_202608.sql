/*============================================================================================================
 * OP Therapies PMPM + VpE — Network Analysis
 * Based on: Therapy_PMPM+VpE_202608_prod.sql
 * Added: ntwk_nbr (claims-side), acp_network_name + acp_network_number (membership-side)
 * Purpose: Compare therapies PMPM/VpE across ACO networks (e.g., DuPage Medical vs IL market)
 *===========================================================================================================*/


/*==============================================================================
 * Claims
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_claims_202608 as
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
    , a.ntwk_nbr
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
    , case when a.clm_dnl_f in ('D', 'Y') then 'Denied' else 'Paid' end as claim_status
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
    , a.ntwk_nbr
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
    , case when a.clm_dnl_f in ('D', 'Y') then 'Denied' else 'Paid' end as claim_status
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
-- CSP PR (no ntwk_nbr on CSP)
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
    , cast(null as number(38,0)) as ntwk_nbr
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
    , case when a.clm_dnl_f in ('D', 'Y') then 'Denied' else 'Paid' end as claim_status
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
-- CSP OP (no ntwk_nbr on CSP)
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
    , cast(null as number(38,0)) as ntwk_nbr
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
    , case when a.clm_dnl_f in ('D', 'Y') then 'Denied' else 'Paid' end as claim_status
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
    , a.ntwk_nbr
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
    , case when a.dnl_f in ('D', 'Y') then 'Denied' else 'Paid' end as claim_status
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
    , a.ntwk_nbr
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
    , case when a.dnl_f in ('D', 'Y') then 'Denied' else 'Paid' end as claim_status
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
 * Aggregate claims to visit level
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_claims_aggregated_202608 as
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
    , ntwk_nbr
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
    , claim_status
    , sum(allw_amt_fnl) as allw_amt_fnl
    , sum(net_pd_amt_fnl) as net_pd_amt_fnl
from tmp_1q.kn_mbm_network_claims_202608
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
    , ntwk_nbr
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
    , claim_status
;


/*==============================================================================
 * VpE Step 1: Visit-level grouping
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_vpe_1_202608 as
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
    , national_pilot_flag
    , population
    , claim_status
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
from tmp_1q.kn_mbm_network_claims_aggregated_202608
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
    , national_pilot_flag
    , population
    , claim_status
;


/*==============================================================================
 * VpE Step 2: Flag new episodes (30-day gap)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_vpe_2_202608 as
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
    , national_pilot_flag
    , population
    , claim_status
    , allowed
    , paid
    , lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt) as prev_srvc_dt
    , datediff('day'
        , lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt)
        , fst_srvc_dt) as visit_day_diff
    , iff(datediff('day'
        , lag(fst_srvc_dt) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt)
        , fst_srvc_dt) > 30, 1, 0) as ep_start_flag
from tmp_1q.kn_mbm_network_vpe_1_202608
;


/*==============================================================================
 * VpE Step 3: Number episodes
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_vpe_3_202608 as
with ep_numbering as (
select
    *
    , sum(iff(prev_srvc_dt is null, 1, ep_start_flag)) over (partition by mbi_key, national_pilot_flag order by fst_srvc_dt rows between unbounded preceding and current row) as ep_num
from tmp_1q.kn_mbm_network_vpe_2_202608
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
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
create or replace table tmp_1q.kn_mbm_network_episodes_summary_202608 as
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
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
from tmp_1q.kn_mbm_network_vpe_3_202608
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
    , national_pilot_flag
    , population
    , claim_status
;


/*==============================================================================
 * Visits summary
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_visits_summary_202608 as
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
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
from tmp_1q.kn_mbm_network_vpe_3_202608
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
    , national_pilot_flag
    , population
    , claim_status
    , floor(datediff('day', ep_start_dt, fst_srvc_dt) / 30.5)
    , floor((datediff('day', fst_srvc_dt, min_hcta_paid_dt) + 20) / 30.5)
;


/*==============================================================================
 * Stack visits + episodes
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_vpe_stacked_202608 as
select * from tmp_1q.kn_mbm_network_visits_summary_202608
union all
select * from tmp_1q.kn_mbm_network_episodes_summary_202608
;


/*==============================================================================
 * VpE Summary (with ntwk_nbr)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_vpe_summary_202608 as
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
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
from tmp_1q.kn_mbm_network_vpe_stacked_202608
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
    , ntwk_nbr
    , ahrq_diag_dtl_catgy_desc
    , market_fnl
    , group_ind_fnl
    , national_pilot_flag
    , population
    , claim_status
    , visit_ep_runout_month
    , visit_runout_month
;


/*==============================================================================
 * Membership (with acp_network_name + acp_network_number)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_mm_202608 as
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
    , acp_network_name
    , acp_network_number
    , fin_member_cnt
    , fin_mbi_hicn_fnl
from fichsrv.tre_membership
where
    sgr_source_name in ('COSMOS', 'CSP', 'NICE')
    and fin_inc_month >= '202301'
)
, mm_flag as (
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
from mm_flag
;


/*==============================================================================
 * Membership summary (grouped by acp_network)
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_mm_summary_202608 as
select
    entity
    , population
    , fst_srvc_month
    , fst_srvc_year
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , acp_network_name
    , acp_network_number
    , sum(fin_member_cnt) as sum_mm
from tmp_1q.kn_mbm_network_mm_202608
group by
    entity
    , population
    , fst_srvc_month
    , fst_srvc_year
    , market_fnl
    , st_abbr_cd
    , brand_fnl
    , group_ind_fnl
    , acp_network_name
    , acp_network_number
;


/*==============================================================================
 * PMPM: Union claims + membership (network-level)
 * Join claims to membership via ntwk_nbr = acp_network_number for PMPM calc
 *==============================================================================*/
create or replace table tmp_1q.kn_mbm_network_pmpm_202608 as
with claims_agg as (
select
    'Claims' as data_type
    , population
    , therapy_category
    , claim_status
    , market_fnl
    , ntwk_nbr
    , fst_srvc_month
    , fst_srvc_year
    , sum(allw_amt_fnl) as sum_allowed
    , sum(net_pd_amt_fnl) as sum_paid
    , 0 as sum_mm
from tmp_1q.kn_mbm_network_claims_aggregated_202608
where population != 'N/A'
group by
    population
    , therapy_category
    , claim_status
    , market_fnl
    , ntwk_nbr
    , fst_srvc_month
    , fst_srvc_year
)
, mm_agg as (
select
    'Membership' as data_type
    , population
    , cast(null as varchar) as therapy_category
    , cast(null as varchar) as claim_status
    , market_fnl
    , acp_network_number as ntwk_nbr
    , fst_srvc_month
    , fst_srvc_year
    , 0 as sum_allowed
    , 0 as sum_paid
    , sum(sum_mm) as sum_mm
from tmp_1q.kn_mbm_network_mm_summary_202608
where population != 'N/A'
group by
    population
    , market_fnl
    , acp_network_number
    , fst_srvc_month
    , fst_srvc_year
)
select * from claims_agg
union all
select * from mm_agg
;


/*==============================================================================
 * Excel Export: PMPM + VpE Summary — DuPage vs Rest of IL (Individual only)
 *==============================================================================*/
create or replace table tmp_1m.kn_mbm_network_dupage_summary_202608 as
with claims as (
select
    fst_srvc_month
    , case when ntwk_nbr = 4821000 then 'DUPAGE' else 'REST OF IL' end as group_flag
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
    , count(distinct visit_id) as visits
from tmp_1q.kn_mbm_network_claims_aggregated_202608
where population = 'M&R FFS (excl. DSNP)'
    and market_fnl = 'IL'
    and group_ind_fnl = 'I'
    and claim_status = 'Paid'
group by fst_srvc_month, group_flag
)
, mm as (
select
    fst_srvc_month
    , case when acp_network_number = 4821000 then 'DUPAGE' else 'REST OF IL' end as group_flag
    , sum(sum_mm) as member_months
from tmp_1q.kn_mbm_network_mm_summary_202608
where population = 'M&R FFS (excl. DSNP)'
    and market_fnl = 'IL'
    and group_ind_fnl = 'I'
group by fst_srvc_month, group_flag
)
, vpe as (
select
    ep_start_month as fst_srvc_month
    , case when ntwk_nbr = 4821000 then 'DUPAGE' else 'REST OF IL' end as group_flag
    , sum(total_visits) as vpe_visits
    , sum(total_episodes) as episodes
from tmp_1q.kn_mbm_network_vpe_summary_202608
where population = 'M&R FFS (excl. DSNP)'
    and market_fnl = 'IL'
    and group_ind_fnl = 'I'
    and claim_status = 'Paid'
group by ep_start_month, group_flag
)
select
    c.fst_srvc_month
    , c.group_flag
    , round(c.allowed, 2) as allowed
    , round(c.paid, 2) as paid
    , m.member_months
    , round(c.allowed / nullif(m.member_months, 0), 2) as pmpm_allowed
    , round(c.paid / nullif(m.member_months, 0), 2) as pmpm_paid
    , v.vpe_visits
    , v.episodes
    , round(v.vpe_visits / nullif(v.episodes, 0), 2) as vpe
from claims as c
left join mm as m
    on c.fst_srvc_month = m.fst_srvc_month
    and c.group_flag = m.group_flag
left join vpe as v
    on c.fst_srvc_month = v.fst_srvc_month
    and c.group_flag = v.group_flag
order by c.fst_srvc_month, c.group_flag
;

select * from tmp_1m.kn_mbm_network_dupage_summary_202608;


/*==============================================================================
 * Excel Export 2: Claims by proc_cd — DuPage vs Rest of IL (Individual only)
 *==============================================================================*/
create or replace table tmp_1m.kn_mbm_network_dupage_proccd_202608 as
with claims as (
select
    fst_srvc_month
    , case when ntwk_nbr = 4821000 then 'DUPAGE' else 'REST OF IL' end as group_flag
    , case when component = 'OP' then 'OP' else 'Physician' end as service_setting
    , proc_cd
    , therapy_category
    , sum(allw_amt_fnl) as allowed
from tmp_1q.kn_mbm_network_claims_aggregated_202608
where population = 'M&R FFS (excl. DSNP)'
    and market_fnl = 'IL'
    and group_ind_fnl = 'I'
    and claim_status = 'Paid'
group by fst_srvc_month, group_flag, service_setting, proc_cd, therapy_category
)
, mm as (
select
    fst_srvc_month
    , case when acp_network_number = 4821000 then 'DUPAGE' else 'REST OF IL' end as group_flag
    , sum(sum_mm) as member_months
from tmp_1q.kn_mbm_network_mm_summary_202608
where population = 'M&R FFS (excl. DSNP)'
    and market_fnl = 'IL'
    and group_ind_fnl = 'I'
group by fst_srvc_month, group_flag
)
select
    c.fst_srvc_month
    , c.group_flag
    , c.service_setting
    , c.proc_cd
    , d.proc_desc
    , c.therapy_category
    , round(c.allowed, 2) as allowed
    , m.member_months
    , round(c.allowed / nullif(m.member_months, 0), 4) as pmpm
from claims as c
left join mm as m
    on c.fst_srvc_month = m.fst_srvc_month
    and c.group_flag = m.group_flag
left join hce_ops_archv.tadm_glxy_procedure_code_tmp as d
    on c.proc_cd = d.proc_cd
order by c.fst_srvc_month, c.group_flag, c.proc_cd
;

select * from tmp_1m.kn_mbm_network_dupage_proccd_202608;