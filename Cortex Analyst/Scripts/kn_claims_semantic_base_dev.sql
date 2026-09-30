/*==============================================================================
 * CLAIMS SEMANTIC — DEV
 * Filtered to M&R FFS criteria + 2025+ for fast iteration.
 * Layer 1: Raw (~1-2 min on Large)
 * Layer 2: Derivation (~10-20s on Large)
 *==============================================================================*/


/*------------------------------------------------------------------------------
 * DEV RAW: knd_claims_semantic_raw
 * Same as prod raw but filtered to M&R FFS criteria + 2025+.
 *------------------------------------------------------------------------------*/
create or replace table tmp_1m.knd_claims_semantic_raw as
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
    , tadm_hcta_util as tadm_unit_count
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
where brand_fnl = 'M&R'
    and global_cap = 'NA'
    and substr(hce_month, 1, 4) >= '2025'
    and product_level_3_fnl != 'INSTITUTIONAL'
    and tfm_include_flag = 1
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
    , tadm_units as tadm_unit_count
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
where brand_fnl = 'M&R'
    and global_cap = 'NA'
    and substr(fst_srvc_month, 1, 4) >= '2025'
    and product_level_3_fnl != 'INSTITUTIONAL'
    and tfm_include_flag = 1
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
    , tadm_hcta_util as tadm_unit_count
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
where brand_fnl = 'M&R'
    and global_cap = 'NA'
    and substr(hce_month, 1, 4) >= '2025'
    and product_level_3_fnl != 'INSTITUTIONAL'
    and tfm_include_flag = 1
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
    , tadm_units as tadm_unit_count
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
where brand_fnl = 'M&R'
    and global_cap = 'NA'
    and substr(fst_srvc_month, 1, 4) >= '2025'
    and product_level_3_fnl != 'INSTITUTIONAL'
    and tfm_include_flag = 1
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
    , procedure_unit as tadm_unit_count
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
where brand_fnl = 'M&R'
    and clm_cap_flag = 'FFS'
    and substr(hce_month, 1, 4) >= '2025'
    and product_level_3_fnl != 'INSTITUTIONAL'
    and tfm_include_flag = 1
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
    , tadm_units as tadm_unit_count
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
where brand_fnl = 'M&R'
    and clm_cap_flag = 'FFS'
    and substr(fst_srvc_month, 1, 4) >= '2025'
    and product_level_3_fnl != 'INSTITUTIONAL'
    and tfm_include_flag = 1
    and dec_risk_type_fnl in ('FFS', 'PCP', 'PHYSICIAN')
;


/*------------------------------------------------------------------------------
 * DEV DERIVATION: knd_claims_semantic_base
 * Same derivation logic, reads from dev raw.
 *------------------------------------------------------------------------------*/
create or replace table tmp_1m.knd_claims_semantic_base as
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
    , a.tadm_unit_count
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
    , case when pc_therapy.proc_cd is not null
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
    , case when pc_skin.proc_cd is not null then 'Y' else 'N' end as skin_sub_flag
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
from tmp_1m.knd_claims_semantic_raw as a
left join tmp_1y.tin_collection as t
    on a.prov_tin = t.tin
left join (select distinct proc_cd from tmp_1y.kn_proc_category where category = 'Therapies') as pc_therapy
    on a.proc_cd = pc_therapy.proc_cd
left join (select distinct proc_cd from tmp_1y.kn_proc_category where category = 'Skin Subs') as pc_skin
    on a.proc_cd = pc_skin.proc_cd
;
