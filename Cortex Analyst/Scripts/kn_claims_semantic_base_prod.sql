/*==============================================================================
 * CLAIMS SEMANTIC — PROD
 * Layer 1: Raw (UNION ALL from fichsrv, no derivation)
 * Layer 2: Derivation (population, flags, joins)
 * Time range: 2024+
 *==============================================================================*/


/*------------------------------------------------------------------------------
 * PROD RAW: kn_claims_semantic_raw
 * 6-table UNION ALL, keeps intermediate fields for derivation.
 * ~9 min on Large. Run biweekly or when adding source fields.
 *------------------------------------------------------------------------------*/
create or replace table tmp_1m.kn_claims_semantic_raw as
select
    'COSMOS' as claim_platform
    , 'OP' as component
    , eventkey as visit_id
    , fst_srvc_dt as srvc_dt
    , hce_month as srvc_month
    , hce_qtr as srvc_qtr
    , substr(hce_month, 1, 4) as srvc_year
    , gal_mbi_hicn_fnl as mbi
    , proc_cd
    , prov_tin
    , market_fnl
    , contract_fnl
    , allw_amt_fnl
    , net_pd_amt_fnl
    , primary_diag_cd
    , rvnu_cd
    , ama_pl_of_srvc_cd
    , bil_typ_cd
    , srvc_prov_npi_nbr
    , full_nm
    , site_clm_aud_nbr
    , hce_service_code as service_code
    , proc_mod1_cd
    , proc_mod2_cd
    , proc_mod3_cd
    , proc_mod4_cd
    , clm_dnl_f
    , tadm_hcta_util as tadm_unit_cnt
    , adj_srvc_unit_cnt
    , st_abbr_cd
    , brand_fnl
    , global_cap
    , group_ind_fnl
    , product_level_3_fnl
    , migration_source
    , tfm_include_flag
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as proc_srvc_id
    , sbmt_chrg_amt
    , clm_pd_dt
from fichsrv.glxy_op_f
where brand_fnl in ('M&R', 'C&S')
    and global_cap = 'NA'
    and substr(hce_month, 1, 4) >= '2024'
union all
select
    'COSMOS' as claim_platform
    , 'PR' as component
    , eventkey as visit_id
    , fst_srvc_dt as srvc_dt
    , fst_srvc_month as srvc_month
    , fst_srvc_qtr as srvc_qtr
    , substr(fst_srvc_month, 1, 4) as srvc_year
    , gal_mbi_hicn_fnl as mbi
    , proc_cd
    , prov_tin
    , market_fnl
    , contract_fnl
    , allw_amt_fnl
    , net_pd_amt_fnl
    , primary_diag_cd
    , rvnu_cd
    , ama_pl_of_srvc_cd
    , bil_typ_cd
    , srvc_prov_npi_nbr
    , full_nm
    , site_clm_aud_nbr
    , service_code
    , proc_mod1_cd
    , proc_mod2_cd
    , proc_mod3_cd
    , proc_mod4_cd
    , clm_dnl_f
    , tadm_units as tadm_unit_cnt
    , adj_srvc_unit_cnt
    , st_abbr_cd
    , brand_fnl
    , global_cap
    , group_ind_fnl
    , product_level_3_fnl
    , migration_source
    , tfm_include_flag
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as proc_srvc_id
    , sbmt_chrg_amt
    , clm_pd_dt
from fichsrv.glxy_pr_f
where brand_fnl in ('M&R', 'C&S')
    and global_cap = 'NA'
    and substr(fst_srvc_month, 1, 4) >= '2024'
union all
select
    'CSP' as claim_platform
    , 'OP' as component
    , eventkey as visit_id
    , fst_srvc_dt as srvc_dt
    , hce_month as srvc_month
    , hce_qtr as srvc_qtr
    , substr(hce_month, 1, 4) as srvc_year
    , gal_mbi_hicn_fnl as mbi
    , proc_cd
    , tin as prov_tin
    , market_fnl
    , contract_fnl
    , allw_amt_fnl
    , net_pd_amt_fnl
    , primary_diag_cd
    , rvnu_cd
    , ama_pl_of_srvc_cd
    , bil_typ_cd
    , srvc_prov_npi_nbr
    , full_nm
    , concat(site_cd, clm_aud_nbr) as site_clm_aud_nbr
    , hce_service_code as service_code
    , proc_mod1_cd
    , proc_mod2_cd
    , proc_mod3_cd
    , proc_mod4_cd
    , clm_dnl_f
    , tadm_hcta_util as tadm_unit_cnt
    , adj_srvc_unit_cnt
    , st_abbr_cd
    , brand_fnl
    , global_cap
    , group_ind_fnl
    , product_level_3_fnl
    , migration_source
    , tfm_include_flag
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as proc_srvc_id
    , sbmt_chrg_amt
    , clm_pd_dt
from fichsrv.dcsp_op_f
where brand_fnl in ('M&R', 'C&S')
    and global_cap = 'NA'
    and substr(hce_month, 1, 4) >= '2024'
union all
select
    'CSP' as claim_platform
    , 'PR' as component
    , eventkey as visit_id
    , fst_srvc_dt as srvc_dt
    , fst_srvc_month as srvc_month
    , fst_srvc_qtr as srvc_qtr
    , substr(fst_srvc_month, 1, 4) as srvc_year
    , gal_mbi_hicn_fnl as mbi
    , proc_cd
    , tin as prov_tin
    , market_fnl
    , contract_fnl
    , allw_amt_fnl
    , net_pd_amt_fnl
    , primary_diag_cd
    , rvnu_cd
    , ama_pl_of_srvc_cd
    , bil_typ_cd
    , srvc_prov_npi_nbr
    , full_nm
    , concat(site_cd, clm_aud_nbr) as site_clm_aud_nbr
    , service_code
    , proc_mod1_cd
    , proc_mod2_cd
    , proc_mod3_cd
    , proc_mod4_cd
    , clm_dnl_f
    , tadm_units as tadm_unit_cnt
    , adj_srvc_unit_cnt
    , st_abbr_cd
    , brand_fnl
    , global_cap
    , group_ind_fnl
    , product_level_3_fnl
    , migration_source
    , tfm_include_flag
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as proc_srvc_id
    , sbmt_chrg_amt
    , clm_pd_dt
from fichsrv.dcsp_pr_f
where brand_fnl in ('M&R', 'C&S')
    and global_cap = 'NA'
    and substr(fst_srvc_month, 1, 4) >= '2024'
union all
select
    'NICE' as claim_platform
    , 'OP' as component
    , eventkey as visit_id
    , fst_srvc_dt as srvc_dt
    , hce_month as srvc_month
    , hce_qtr as srvc_qtr
    , substr(hce_month, 1, 4) as srvc_year
    , mbi_hicn_fnl as mbi
    , proc_cd
    , tin as prov_tin
    , market_fnl
    , contract_fnl
    , allw_amt as allw_amt_fnl
    , net_pd_amt as net_pd_amt_fnl
    , primary_diag_cd
    , rvnu_cd
    , ama_pl_of_srvc_cd
    , bil_typ_cd
    , srvc_prov_npi_nbr
    , full_nm
    , concat(site_cd, clm_aud_nbr) as site_clm_aud_nbr
    , hce_service_code as service_code
    , proc_mod1_cd
    , proc_mod2_cd
    , null as proc_mod3_cd
    , null as proc_mod4_cd
    , clm_ln_lvl_dnl_f as clm_dnl_f
    , procedure_unit as tadm_unit_cnt
    , srvc_unit_cnt as adj_srvc_unit_cnt
    , st_abbr_cd
    , brand_fnl
    , iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , group_ind_fnl
    , product_level_3_fnl
    , 'NA' as migration_source
    , tfm_include_flag
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as proc_srvc_id
    , sbmt_chrg_amt
    , clm_pd_dt
from fichsrv.nce_op_f
where brand_fnl in ('M&R', 'C&S')
    and clm_cap_flag = 'FFS'
    and substr(hce_month, 1, 4) >= '2024'
    and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
union all
select
    'NICE' as claim_platform
    , 'PR' as component
    , eventkey as visit_id
    , fst_srvc_dt as srvc_dt
    , fst_srvc_month as srvc_month
    , fst_srvc_qtr as srvc_qtr
    , substr(fst_srvc_month, 1, 4) as srvc_year
    , mbi_hicn_fnl as mbi
    , proc_cd
    , tin as prov_tin
    , market_fnl
    , contract_fnl
    , calc_allw as allw_amt_fnl
    , calc_net_pd as net_pd_amt_fnl
    , primary_diag_cd
    , rvnu_cd
    , ama_pl_of_srvc_cd
    , bil_typ_cd
    , srvc_prov_npi_nbr
    , full_nm
    , concat(site_cd, clm_aud_nbr) as site_clm_aud_nbr
    , service_code
    , proc_mod1_cd
    , proc_mod2_cd
    , null as proc_mod3_cd
    , null as proc_mod4_cd
    , clm_dnl_f
    , tadm_units as tadm_unit_cnt
    , srvc_unit_cnt as adj_srvc_unit_cnt
    , st_abbr_cd
    , brand_fnl
    , iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
    , group_ind_fnl
    , product_level_3_fnl
    , 'NA' as migration_source
    , tfm_include_flag
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as proc_srvc_id
    , sbmt_chrg_amt
    , clm_pd_dt
from fichsrv.nce_pr_f
where brand_fnl in ('M&R', 'C&S')
    and clm_cap_flag = 'FFS'
    and substr(fst_srvc_month, 1, 4) >= '2024'
    and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
;


/*------------------------------------------------------------------------------
 * PROD DERIVATION: kn_claims_semantic_base
 * Reads from kn_claims_semantic_raw. Applies population, flags, joins.
 * ~1-2 min on Large. Run when changing derivation logic.
 *------------------------------------------------------------------------------*/
create or replace table tmp_1m.kn_claims_semantic_base as
select
    a.claim_platform
    , a.component
    , a.visit_id
    , a.srvc_dt
    , a.srvc_month
    , a.srvc_qtr
    , a.srvc_year
    , a.mbi
    , a.proc_cd
    , a.proc_mod1_cd
    , a.proc_mod2_cd
    , a.proc_mod3_cd
    , a.proc_mod4_cd
    , a.prov_tin
    , a.market_fnl
    , a.contract_fnl
    , a.allw_amt_fnl
    , a.net_pd_amt_fnl
    , a.primary_diag_cd
    , a.rvnu_cd
    , a.ama_pl_of_srvc_cd
    , a.bil_typ_cd
    , a.srvc_prov_npi_nbr
    , a.full_nm
    , a.site_clm_aud_nbr
    , a.service_code
    , a.tadm_unit_cnt
    , a.adj_srvc_unit_cnt
    , a.st_abbr_cd
    , a.brand_fnl
    , a.global_cap
    , a.group_ind_fnl
    , a.product_level_3_fnl
    , a.migration_source
    , a.tfm_include_flag
    , a.proc_srvc_id
    , a.sbmt_chrg_amt
    , a.clm_pd_dt
    , to_char(a.clm_pd_dt, 'YYYYMM') as clm_pd_month
    , to_char(a.clm_pd_dt, 'YYYY') || 'Q' || quarter(a.clm_pd_dt) as clm_pd_qtr
    , to_char(a.clm_pd_dt, 'YYYY') as clm_pd_year
    , case when a.clm_dnl_f in ('D', 'Y') then 'Denied'
           when a.clm_dnl_f = 'N' then 'Paid'
      end as denial_flag
    , t.collection as hospital_group
    , case when pc.category = 'Therapies'
            and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
            and left(coalesce(a.bil_typ_cd, '0'), 1) != '3'
            and coalesce(a.ama_pl_of_srvc_cd, '') != '12'
            and coalesce(a.product_level_3_fnl, '') not in ('DUAL', 'INSTITUTIONAL')
           then 'Y'
           when a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449')
            and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
            and left(coalesce(a.bil_typ_cd, '0'), 1) != '3'
            and coalesce(a.ama_pl_of_srvc_cd, '') != '12'
            and coalesce(a.product_level_3_fnl, '') not in ('DUAL', 'INSTITUTIONAL')
           then 'Y'
           else 'N'
      end as therapies_flag
    , pc.subcategory
    , case when pc.category = 'Skin Subs' then 'Y' else 'N' end as skin_sub_flag
    , case
        when a.migration_source = 'OAH'
            and not (a.brand_fnl = 'C&S' and a.srvc_year = '2024' and a.st_abbr_cd = 'MD')
            and not (a.brand_fnl != 'C&S' and a.srvc_year = '2024' and a.market_fnl = 'MD')
        then 'OAH'
        when a.product_level_3_fnl = 'INSTITUTIONAL'
        then 'INSTITUTIONAL'
        when a.claim_platform in ('COSMOS', 'NICE')
            and a.brand_fnl = 'M&R'
            and a.global_cap = 'NA'
            and a.product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL')
            and a.tfm_include_flag = 1
        then 'M&R FFS'
        when a.claim_platform in ('COSMOS', 'CSP')
            and a.global_cap = 'NA'
            and (
                (a.brand_fnl = 'C&S' and a.migration_source != 'OAH' and a.product_level_3_fnl = 'DUAL')
                or (a.brand_fnl = 'C&S' and a.srvc_year = '2024' and a.migration_source = 'OAH' and a.st_abbr_cd = 'MD')
                or (a.brand_fnl != 'C&S' and a.srvc_year = '2024' and a.migration_source = 'OAH' and a.market_fnl = 'MD')
            )
        then 'C&S DSNP'
        else 'Other'
      end as population
from tmp_1m.kn_claims_semantic_raw as a
left join tmp_1y.tin_collection as t
    on a.prov_tin = t.tin
left join (select distinct proc_cd, category, subcategory from tmp_1y.kn_proc_category) as pc
    on a.proc_cd = pc.proc_cd
;

alter table tmp_1m.kn_claims_semantic_base
cluster by (population, denial_flag, srvc_month, component);


/*------------------------------------------------------------------------------
 * COMMENTS: Table + column metadata for Cortex Code discoverability.
 * Run after every rebuild.
 *------------------------------------------------------------------------------*/
comment on table tmp_1m.kn_claims_semantic_base is
'Outpatient (OP) and Professional/Physician (PR) medical claims, 2024+. ~2.2B rows. Default filters: population = ''M&R FFS'' AND denial_flag = ''Paid''. Always include component in GROUP BY. For PMPM, join to kn_claims_semantic_membership on (population, srvc_month, market_fnl, brand_fnl, product_level_3_fnl) using aggregated CTEs — never row-level join.';

alter table tmp_1m.kn_claims_semantic_base alter column
    claim_platform comment 'Source system. Values: COSMOS, CSP (aka SMART), NICE.'
    , component comment 'Claim type. Values: OP (outpatient/facility), PR (professional/physician/provider). Always include in GROUP BY.'
    , visit_id comment 'Unique visit identifier (concatenated key). Use COUNT(DISTINCT visit_id) for visit counts.'
    , srvc_dt comment 'Service date (first date of service on the claim line).'
    , srvc_month comment 'Service month (YYYYMM format, e.g. 202501). Primary time dimension.'
    , srvc_qtr comment 'Service quarter (e.g. 2026Q1).'
    , srvc_year comment 'Service year (e.g. 2025). Derived from srvc_month.'
    , mbi comment 'Member identifier (fin_mbi_hicn_fnl). Use COUNT(DISTINCT mbi) for unique member counts.'
    , proc_cd comment 'CPT/HCPCS procedure code. Use with COUNT(DISTINCT proc_srvc_id) for procedure counts.'
    , proc_mod1_cd comment 'Procedure modifier position 1 (e.g. KF, GP, GO, GN). Check ALL 4 modifier columns when filtering.'
    , proc_mod2_cd comment 'Procedure modifier position 2. Check all 4 modifier columns when filtering for a modifier.'
    , proc_mod3_cd comment 'Procedure modifier position 3. NULL for NICE claims.'
    , proc_mod4_cd comment 'Procedure modifier position 4. NULL for NICE claims.'
    , prov_tin comment 'Provider Tax Identification Number.'
    , market_fnl comment 'Geographic market (state-level). Join key to membership table.'
    , contract_fnl comment 'Contract identifier.'
    , allw_amt_fnl comment 'Allowed amount in dollars — the plan financial exposure. Use for spend/cost queries. This is the standard dollar metric.'
    , net_pd_amt_fnl comment 'Net paid amount (after member cost-sharing). Only use when user explicitly says net paid or after cost sharing.'
    , primary_diag_cd comment 'Primary ICD-10 diagnosis code. Use LEFT(primary_diag_cd, 3) for category-level filtering (e.g. E11 = Type 2 diabetes).'
    , rvnu_cd comment 'Revenue code (facility billing).'
    , ama_pl_of_srvc_cd comment 'Place of service code. Office = 11/49. Telehealth = 02/10.'
    , bil_typ_cd comment 'Bill type code (facility claims).'
    , srvc_prov_npi_nbr comment 'Servicing provider NPI number.'
    , full_nm comment 'Servicing provider full name.'
    , site_clm_aud_nbr comment 'Site-prefixed claim audit number. Unique claim identifier within platform. Use COUNT(DISTINCT site_clm_aud_nbr) for claim counts.'
    , service_code comment 'Categorized service type (e.g. PR_OFFVISIT, OP_EMERG, PR_TELEHEALTH, OP_SURG). Prefix indicates component (OP_ or PR_).'
    , tadm_unit_cnt comment 'Total admission units (legacy). Prefer adj_srvc_unit_cnt for utilization.'
    , adj_srvc_unit_cnt comment 'Adjusted service units — standard volume metric. Use for utilization per K = SUM(adj_srvc_unit_cnt) / SUM(member_months) * 12000.'
    , st_abbr_cd comment 'State abbreviation (e.g. TX, FL, CA).'
    , brand_fnl comment 'Product brand. Join key to membership table.'
    , global_cap comment 'Global capitation flag. Values: NA (fee-for-service), ENC (encounter/capitated).'
    , group_ind_fnl comment 'Group/Individual indicator.'
    , product_level_3_fnl comment 'Product sub-type. Values include DUAL, INSTITUTIONAL, FFS. Join key to membership table.'
    , migration_source comment 'Migration source indicator. Values: OAH or NA. Used in population derivation.'
    , tfm_include_flag comment 'TFM inclusion flag (1 = included in standard M&R FFS reporting).'
    , proc_srvc_id comment 'Unique procedure-service identifier (mbi+provider+date+proc). Use COUNT(DISTINCT proc_srvc_id) for procedure counts.'
    , sbmt_chrg_amt comment 'Submitted/billed charge amount. Provider-reported pre-adjudication amount.'
    , clm_pd_dt comment 'Claim paid/processed date. Use for runout/lag analysis (time between service and payment).'
    , clm_pd_month comment 'Claim paid month (YYYYMM). Use for paid-month reporting or incurred-but-not-reported (IBNR) analysis.'
    , clm_pd_qtr comment 'Claim paid quarter (e.g. 2026Q1).'
    , clm_pd_year comment 'Claim paid year (e.g. 2025).'
    , denial_flag comment 'Claim payment status. Values: Paid, Denied. Default filter: denial_flag = ''Paid'' for standard analytics.'
    , hospital_group comment 'Hospital system/group name (from TIN collection). NULL for non-hospital providers. Filter IS NOT NULL when grouping.'
    , therapies_flag comment 'Physical/occupational/speech therapy indicator. Values: Y, N. Excludes DSNP and INSTITUTIONAL. Filter = ''Y'' for therapy claims.'
    , subcategory comment 'Therapy or skin sub type. Values: Chiro, PT-OT, ST (therapies); Covered, Unproven (skin subs). NULL otherwise.'
    , skin_sub_flag comment 'Skin substitute procedure indicator. Values: Y, N. Filter = ''Y'' for skin substitute claims.'
    , population comment 'Population segment. Values: M&R FFS (default — includes DSNP), C&S DSNP, INSTITUTIONAL, OAH, Other. Always filter to one population for PMPM.'
;


/*------------------------------------------------------------------------------
 * MEMBERSHIP COMMENTS
 *------------------------------------------------------------------------------*/
comment on table tmp_1m.kn_claims_semantic_membership is
'Pre-aggregated member months for PMPM/utilization calculations. Grain: population + srvc_month + market_fnl + brand_fnl + product_level_3_fnl. Join to claims ONLY after aggregating claims first (CTE pattern). PMPM = SUM(allw_amt_fnl) / SUM(member_months). Do NOT multiply by 1000.';

alter table tmp_1m.kn_claims_semantic_membership alter column
    srvc_month comment 'Month (YYYYMM). Join key to claims. Range: 202401-202606.'
    , population comment 'Population segment. Must match claims table population values exactly.'
    , market_fnl comment 'Geographic market. Join key to claims.'
    , brand_fnl comment 'Product brand. Join key to claims.'
    , product_level_3_fnl comment 'Product sub-type. Join key to claims.'
    , member_months comment 'Count of member-months at this grain. Denominator for PMPM = SUM(allw_amt_fnl) / SUM(member_months). Do NOT multiply by 1000.'
;