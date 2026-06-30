# Data Dictionary

Cross-project reference for all source tables used across HCE analyses.
All tables live in `VING_PRD_TREND_DB` unless otherwise noted.

---

## Table of Contents

- [Fact Tables](#fact-tables)
  - [Claims (OP/PR)](#claims-oppr)
  - [Claims (IP)](#claims-ip)
  - [Claims (Dental)](#claims-dental)
  - [Authorizations](#authorizations)
  - [Membership](#membership)
  - [PA Tracking / LOPA](#pa-tracking--lopa)
- [Dimension & Reference Tables](#dimension--reference-tables)
  - [HCE_OPS_LKUP](#hce_ops_lkup-lookupreference-schema)
  - [FICHSRV](#fichsrv-enterprise-reference-data)
  - [TMP_2Y](#tmp_2y-multi-year-reference)
  - [TMP_1Y](#tmp_1y-analyst-working-tables)
  - [External Databases](#external-database-references)
- [Population Segmentation Logic](#population-segmentation-logic)
- [Common Join Patterns](#common-join-patterns)
- [Schema Conventions](#schema-naming-conventions)
- [Session Variables](#session-variables)

---

# Fact Tables

## Claims (OP/PR)

Six source tables across three entities, UNION ALL'd together in most pulls.

### COSMOS

| Table | Type | Rows |
|-------|------|------|
| `fichsrv.glxy_op_f` | Outpatient | 1.2B |
| `fichsrv.glxy_pr_f` | Provider/Professional | 1.25B |

### CSP

| Table | Type | Rows |
|-------|------|------|
| `fichsrv.dcsp_op_f` | Outpatient | 437M |
| `fichsrv.dcsp_pr_f` | Provider/Professional | 338M |

### NICE

| Table | Type | Rows |
|-------|------|------|
| `fichsrv.nce_op_f` | Outpatient | 27M |
| `fichsrv.nce_pr_f` | Provider/Professional | 27M |

### Key Columns

| Column | COSMOS/CSP name | NICE name | Description |
|--------|----------------|-----------|-------------|
| MBI | `gal_mbi_hicn_fnl` | `mbi_hicn_fnl` | Member identifier |
| Claim ID | `eventkey` | `eventkey` | Visit/event identifier |
| Service date | `fst_srvc_dt` | `fst_srvc_dt` | First service date |
| Service month | `fst_srvc_month` | `fst_srvc_month` | YYYYMM format |
| Procedure code | `proc_cd` | `proc_cd` | CPT/HCPCS |
| Revenue code | `rev_cd` | `rev_cd` | Facility revenue code |
| Place of service | `pos_cd` | `pos_cd` | Place of service code |
| TIN | `prov_tin` | `tin` | Provider Tax ID |
| Brand | `brand_fnl` | `brand_fnl` | M&R or C&S |
| Product | `product_level_3_fnl` | `product_level_3_fnl` | FFS, DUAL, INSTITUTIONAL, etc. |
| Market | `market_fnl` | `market_fnl` | Geographic market |
| State | `st_abbr_cd` | `st_abbr_cd` | State abbreviation |
| Global cap | `global_cap` | derived from `clm_cap_flag` | NA = FFS, ENC = capitated |
| Migration source | `migration_source` | N/A (hardcode `'NA'`) | OAH/NA |
| TFM include | `tfm_include_flag` | `tfm_include_flag` | Total fund management flag |
| Group/Individual | `group_ind_fnl` | `group_ind_fnl` | Group vs individual |
| Entity | `sgr_source_name` | (hardcode `'NICE'`) | COSMOS/CSP/NICE |
| Allowed (OP) | `allw_amt_fnl` | `allw_amt` | Allowed amount |
| Paid (OP) | `net_pd_amt_fnl` | `net_pd_amt` | Net paid amount |
| Allowed (PR) | `allw_amt_fnl` | `calc_allw` | Allowed amount |
| Paid (PR) | `net_pd_amt_fnl` | `calc_net_pd` | Net paid amount |
| Denial flag | `clm_dnl_f` | `dnl_f` | D/Y = denied |

### Standard Filters

```sql
where brand_fnl in ('M&R', 'C&S')
  and global_cap = 'NA'                    -- COSMOS/CSP: global_cap; NICE: clm_cap_flag = 'FFS'
  and clm_dnl_f not in ('D', 'Y')         -- NICE: dnl_f not in ('D', 'Y')
  and fst_srvc_dt between :start and :end
```

### NICE-Specific Derivations

```sql
-- Global cap equivalent for NICE
iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap

-- Migration source doesn't exist in NICE
'NA' as migration_source
```

---

## Claims (IP)

### Encounter-Level (FICHGRP)

| Table | Rows | Description |
|-------|------|-------------|
| `fichgrp.cosmos_arsg` | 810M | COSMOS All Service Groups (full claim-line detail) |
| `fichgrp.glxy_ip_f_enc` | 14.6M | Galaxy IP encounters (grouped by encounter) |
| `fichgrp.glxy_op_f_enc` | 263M | Galaxy OP encounters (grouped) |
| `fichgrp.glxy_pr_f_enc` | 293M | Galaxy PR encounters (grouped) |

### Admit-Level (FICHSRV)

| Table | Rows | Description |
|-------|------|-------------|
| `fichsrv.glxy_ip_admit_f` | 137M | Galaxy IP admissions |
| `fichsrv.glxy_ip_admit_f_denial` | 137M | Galaxy IP admissions with denial info |
| `fichsrv.dcsp_ip_admit_f` | 59M | CSP IP admissions |
| `fichsrv.cosmos_ip_w_dnls_clm` | 8.5M | COSMOS IP with denial claims |

**Note**: IP data in HCE analyses primarily flows through the authorization path (AVTAR/ADR tables) rather than standalone IP claims tables.

---

## Claims (Dental)

| Table | Schema | Description |
|-------|--------|-------------|
| `HCE_DENTAL_CLMS_FNL` | `tmp_1y` | Finalized dental claims (FACETS source) |
| `HCE_DENTAL_FACETS_CLM_MBR_ROLLUP` | `tmp_1y` | Dental claims rolled up by member |
| `hce_dental_facets_skygen_membership_2` | `tmp_1y` | Dental membership mapping |

### Dental Key Columns

`MBI`, `fin_inc_month`, `svc_month`, `paid_month`, `proc_cd`, `dental_clm_source`, `temp_provparstatus` (TIN-level PAR status)

### Dental Filters

```sql
where dental_clm_source = 'FACETS'
  and fin_inc_month between 202301 and 202412
```

**Unit of analysis**: distinct `MBI | service_date | proc_cd`

---

## Authorizations

### Master Table (Current Fiscal Year)

| Table | Schema |
|-------|--------|
| `hce_adr_avtar_like_25_26_f` | `hce_ops_fnl` (read-only) |

### Prior Years

| Table | Schema | Period |
|-------|--------|--------|
| `hce_adr_avtar_like_24_25_f` | `hce_ops_fnl` or `hce_proj_bd` | FY 2024-2025 |
| `hce_adr_avtar_like_2023_f` | `hce_proj_bd` | FY 2023 |

### LOC Pipeline Upstream

| Table | Schema | Notes |
|-------|--------|-------|
| `ec_avtar_25_26_3_od` | `tmp_1m` | IPA's raw AVTAR table. LOC pipeline only. |

### Key Columns

| Column | Description |
|--------|-------------|
| `case_id` | Unique authorization case |
| `fin_mbi_hicn_fnl` | Member MBI |
| `sgr_source_name` | Entity (COSMOS/CSP/NICE) |
| `fin_brand` | M&R or C&S |
| `fin_market` | Geographic market |
| `fin_state` | State |
| `migration_source` | OAH/NA |
| `fin_product_level_3` | FFS, DUAL, INSTITUTIONAL |
| `global_cap` | NA = FFS, ENC = capitated |
| `tfm_include_flag` | Total fund management |
| `svc_setting` | Inpatient/Outpatient |
| `plc_of_svc_cd` | Place of service (e.g., 21 - Acute Hospital) |
| `admit_cat_cd` | Admit category (e.g., 17 - Medical, 30 - Surgical) |
| `ip_type` | Medical/Surgical/Transplant/LTAC/SNF/AIR/NA |
| `admit_dt_act` | Actual admit date |
| `dschg_dt_act` | Actual discharge date |
| `admit_dt_exp` | Expected admit date |
| `dschg_dt_exp` | Expected discharge date |
| `case_status_cd` | Case status |
| `case_init_decn_cd` | Initial decision code |
| `fa_prov_id` | FA provider ID (derive TIN: `substr(fa_prov_id, 2, 9)`) |
| `fnl_drg_cd` | Final DRG code |
| `transplant_flag` | Y/N transplant indicator |
| `initial_adr_ind` | Initial ADR (0/1) |
| `persistent_adr_ind` | Persistent ADR (0/1) |
| `md_review_ind` | MD review (0/1) |
| `appeal_ind` | Appeal filed (0/1) |
| `appeal_overturn_ind` | Appeal overturned (0/1) |
| `p2p_ind` | Peer-to-peer review (0/1) |
| `p2p_overturn_ind` | P2P overturned (0/1) |
| `mcr_reconsideration_ind` | MCR reconsideration (0/1) |
| `mcr_overturn_ind` | MCR overturned (0/1) |
| `member_appeal_ind` | Member appeal (0/1) — only from 202601+ |
| `member_appeal_overturn_ind` | Member appeal overturned (0/1) |

### Standard Filters

```sql
where fin_brand in ('M&R', 'C&S')
  and (transplant_flag != 'Y' or transplant_flag is null)
  and (
      (ip_type in ('Medical', 'Surgical', 'Transplant')
       and to_varchar(admit_dt_act, 'MM/dd/yyyy') is not null)
      or ip_type in ('LTAC', 'SNF', 'AIR')
  )
```

### LOC Filter (loc_flag = 1)

```sql
and svc_setting = 'Inpatient'
and plc_of_svc_cd = '21 - Acute Hospital'
and admit_cat_cd in ('17 - Medical', '30 - Surgical')
```

### Rate Calculations

| Metric | Formula |
|--------|---------|
| Initial ADR rate | `sum(initial_adr_ind) / count(case_id)` |
| Persistent ADR rate | `sum(persistent_adr_ind) / count(case_id)` |
| Persistency | `sum(persistent_adr_ind) / nullif(sum(initial_adr_ind), 0)` |
| MD review rate | `sum(md_review_ind) / count(case_id)` |
| Appeal rate | `sum(appeal_ind) / nullif(sum(initial_adr_ind), 0)` |
| Appeal overturn rate | `sum(appeal_overturn_ind) / nullif(sum(initial_adr_ind), 0)` |
| P2P rate | `sum(p2p_ind) / nullif(sum(initial_adr_ind), 0)` |
| P2P overturn rate | `sum(p2p_overturn_ind) / nullif(sum(initial_adr_ind), 0)` |
| Auth per K | `count(case_id) / (membership / 1000)` |

### Known Data Issues

- **202403-202404**: Prior auth turned off — ADR artificially low
- **`member_appeal_ind`**: Only available from 202601 onward
- **Data maturity lag**: Last 3-4 months have incomplete outcomes
- **Earliest usable month**: 202301
- **Always use** `count(distinct case_id)` not `count(*)`

---

## Membership

### Current Enrollment

| Table | Schema |
|-------|--------|
| `tre_membership` | `fichsrv` (read-only) |

### Monthly Archive Snapshots

| Table | Schema | Notes |
|-------|--------|-------|
| `gl_rstd_gpsgalnce_f_<YYYYMM>` | `hce_ops_archv` (read-only) | Table name is dynamic — suffix = `membership_month` variable |

### Key Columns

| Column | Description |
|--------|-------------|
| `fin_mbi_hicn_fnl` | Member MBI |
| `fin_inc_month` | Enrollment month (YYYYMM) |
| `fin_inc_year` | Enrollment year |
| `sgr_source_name` | Entity (COSMOS/CSP/NICE) |
| `fin_brand` | M&R or C&S |
| `fin_market` | Geographic market |
| `fin_state` | State |
| `fin_product_level_3` | FFS, DUAL, INSTITUTIONAL |
| `migration_source` | OAH/NA |
| `global_cap` | NA = FFS, ENC = capitated |
| `tfm_include_flag` | Total fund management |
| `fin_member_cnt` | Member count |
| `fin_g_i` | Group/Individual |
| `nce_tadm_dec_risk_type` | NICE risk type |
| `fin_tfm_product_new` | TFM product classification |

### Archive-Only Shortcut Flags

Available in `gl_rstd_gpsgalnce_f_<YYYYMM>` but prefer explicit logic:

| Flag | Description |
|------|-------------|
| `mnr_total_ffs_flag` | M&R Total FFS indicator |
| `cns_dual_flag` | C&S DSNP indicator |
| `mnr_dual_flag` | M&R DSNP indicator |
| `total_oah_flag` | OAH indicator |
| `institutional_flag` | ISNP indicator |

### Standard Filters

```sql
where sgr_source_name in ('COSMOS', 'CSP', 'NICE')
  and fin_brand in ('M&R', 'C&S')
  and fin_inc_month >= '202301'
```

---

## PA Tracking / LOPA

| Table | Schema | Description |
|-------|--------|-------------|
| `pa_trckng_op_evnt_lopa_dtl` | `hce_ops_stage` | PA tracking LOPA event details (outpatient) |
| `pa_trckng_pr_evnt_lopa_dtl` | `hce_ops_stage` | PA tracking LOPA event details (professional) |

Used in Therapies savings scripts for LOPA (Lack of Prior Authorization) analysis.

---

# Dimension & Reference Tables

## HCE_OPS_LKUP (Lookup/Reference Schema)

Schema description: "Lookup/reference tables used in operations or other processes"

### Calendar & Time

| Table | Rows | Columns | Description |
|-------|------|---------|-------------|
| `DATE_WEEK` | 3,015 | `MONTH`, `DAY`, `WEEK`, `DATE` | Calendar date to reporting week (e.g., `25W8`) |
| `WEEK_MONTH_LASTWEEKDAY_CUTOFF_FNL` | 432 | | Week-to-month mapping with last weekday cutoff |

### Procedure & Diagnosis Codes

| Table | Rows | Columns | Description |
|-------|------|---------|-------------|
| `TADM_GLXY_PROCEDURE_CODE` | 341,902 | `PROC_CD`, `PROC_DESC`, `PROC_TYP_CD`, `AHRQ_PROC_GENL_CATGY_CD/DESC`, `AHRQ_PROC_DTL_CATGY_CD/DESC`, `SRVC_CATGY_CD/DESC`, `GDR_LMT_CD`, `VST_CD`, `PROC_END_DT` | Canonical procedure code reference with AHRQ categories |
| `TADM_GLXY_DRG_CODE` | 891 | `DRG_CD`, `DRG_DESC`, `DRG_ROW_EFF_DT`, `DRG_WGT_FCT`, `MDC_CD`, `MDC_DESC`, `SERVICECATG` | DRG code to description, weight, MDC, med/surg category |
| `SOS_MAPPING_2026` | 2,690 | `CATEGORY`, `PROC_CD` | Procedure code to Site of Service category |
| `PROC_DESC` | 72 | | Compact procedure descriptions |

### Prior Auth & Medical Necessity

| Table | Rows | Description |
|-------|------|-------------|
| `CATEGORY_MAPPING` | 1,948 | Proc code to PA vendor/program. Columns: `PROC_CODE`, `PROC_DESC`, `VENDOR`, `PROGRAM`, `VOLUME_REPORT`, `AFFORDABILITY_EXCLUDE`, `LATEST_EFFECTIVE_DATE`, `LATEST_EXPIRATION_DATE` |
| `PA2020_CODE_LISTS_2019` | 1,372 | Prior Authorization code lists |
| `PA_ADJUNCT_PROCS` | 296 | PA adjunct procedure list |
| `MCR_MEDNEC_CPT_LIST_1` | 174 | CPT codes for MCR medical necessity |
| `MCR_MEDNEC_DOL_LIST_1` | 38 | Dollar threshold list |
| `MCR_MEDNEC_DNL_DOL_LIST_2` | 187 | Denial dollar threshold list |
| `MCR_MEDNEC_PROCMOD_LIST_1` | 7 | Procedure modifier list |

### AVTAR Lookups

| Table | Rows | Description |
|-------|------|-------------|
| `AVTAR_PROC_CATG_SUBCATG_DESC` | 9,046 | AVTAR procedure to category + subcategory + description |
| `COVID_FLAG_AVTAR` | 4 | COVID ICD codes flagged in AVTAR |
| `FLU_FLAG_AVTAR` | 58 | Flu/respiratory ICD codes for AVTAR |
| `DX240_AVTAR_LIST` | 243 | DX-240 diagnosis list for AVTAR |

### Pricing & Cost

| Table | Rows | Description |
|-------|------|-------------|
| `OPPS_SI_WEIGHTINGS_2016_2026` | 188,061 | OPPS Status Indicator weightings by year |
| `AMR_COST_FORMATTED_202602` | 16,864 | AMR cost reference (latest) |
| `DRG_AVG_ADMIT_PRC_2025_JMS_3` | 15,703 | DRG to average admission price (2025) |
| `DIAG_AVG_ADMIT_PRC_2025_JMS_3` | 142,367 | Diagnosis to average admission price (2025) |
| `ADMITYR_AVG_ADMIT_PRC_2025_JMS_3` | 72 | Admit year average admission price |

### Geographic

| Table | Rows | Description |
|-------|------|-------------|
| `INCOME_BY_ZIP` | 41,702 | ZIP-level socioeconomic: `ZIPCODE`, `PERSONS_PER_HOUSEHOLD`, `AVERAGE_HOUSEVALUE`, `INCOME_PER_HOUSEHOLD`, `STATE`, `COUNTY`, `COUNTYFIPS`, `CBSA_TYPE` |
| `SG_ZIP_ZCTA` | 41,062 | ZIP to ZCTA crosswalk: `ZIP_CODE`, `PO_NAME`, `STATE`, `ZIP_TYPE`, `ZCTA`, `ZIP_JOIN_TYPE` |
| `ZCTA_NEW` | 32,990 | ZCTA geographic reference |
| `NCHSURCODES2013` | 3,151 | NCHS urbanization codes |
| `NCHS_TYPE_DESCRIPTION` | 7 | NCHS type to description |

### Operational & Other

| Table | Rows | Description |
|-------|------|-------------|
| `HCEOPS_CAT_DIMENSION` | 37 | HCE Operations category dimension |
| `HCEOPS_2022_2026_DENTAL_MARKET_PROFILE` | 5,342 | Dental market profiling by year |
| `PREVENTIVE_COMPREHENSIVE` | 702 | Preventive care comprehensive code list |
| `OPS_PARAM_VALUE` | 9 | Operational parameter values |
| `MR_CS_MACRA_JOIN` | 303,230 | MACRA join reference |
| `CATASTROPHIC_RUN_OUT_XREF` | 12 | Catastrophic claims run-out crosswalk |
| `TRR_RACE_REF` | 9 | Race code reference |
| `RETAIN_LIST_24_ADJ_DX` | 25 | Retained diagnosis list for 2024 adjustments |
| `UCS_TB866_TIN_EXCLU_TIMELINE` | 131 | TIN exclusion timeline |
| `UCS_TB866_PROV_EXCLU_TIMELINE` | 57 | Provider exclusion timeline |

---

## FICHSRV (Enterprise Reference Data)

389 tables total. Key reference/dimension tables (fact tables listed above):

### Member Identity

| Table | Rows | Description |
|-------|------|-------------|
| `MACRA_CROSSWALK` | 28,147,190 | `MASTERID`, `MBI`, `HICN`, `PAID_THRU_DATE` — MBI to HICN identity crosswalk |
| `MNR_CCW_CONDITIONS_202502` | varies | Medicare Chronic Conditions Warehouse flags per member |

### Group & Provider

| Table | Rows | Description |
|-------|------|-------------|
| `GROUP_CROSSWALK` | 18,033 | `YEAR`, `GROUP_NUMBER`, `GROUP_NAME`, `BID_GROUP_NAME`, `ORG_TYPE`, `MEMBERSHIP_JANUARY`, `JUMBO_GROUP` |
| `ACO_NAMES` | 196 | `ACO_DESC`, `ACONAME` — ACO description to ACO number |
| `DAVITA_TINS` | 883 | `TIN` — DaVita facility TINs (presence = DaVita) |
| `NICE_COPY_PROVIDER_GROUP` | 31,765 | Provider group directory: `PRVDR_GRP`, `PRVDR_GRP_NAME`, `PRVDR_GRP_TYPE`, address fields, `PMG_NETWORK_CODE`, `BEG_EFF_DATE`, `END_EFF_DATE` |
| `NATIONAL_MPINS_TINS_202509` | 8,561 | National MPIN-TIN mapping |

### Drug / Product

| Table | Rows | Description |
|-------|------|-------------|
| `NDC_CLASSIFICATION_MAP` | 284 | `manufacturer_name`, `product_name`, `product_id`, `Classification`, `Certainty` |
| `MARD_DIM_SPEC_ELIG_DRUG` | 79,796 | MARD specialty eligibility drug dimension |

### Plan / Market

| Table | Rows | Description |
|-------|------|-------------|
| `MARKET_TABLE` | 2,174,859 | Market-level reference data |
| `GATEKEEP_PLANS_26F` | 501 | Gatekeeper plan PBPs (2026 forward) |

---

## TMP_2Y (Multi-Year Reference)

55 tables. Mix of reference tables and longer-lived analytical outputs.

### Calendar / Time

| Table | Rows | Columns | Description |
|-------|------|---------|-------------|
| `EC_LOC_WEEK_ASSIGN` | 3,288 | `DATE`, `WEEK` | Date to LOC reporting week (e.g., `201901`) |
| `EC_FRANKY_EXTRAP_2026` | 1,830 | | Admit month to adjudication month (Franky extrapolation) |

### DRG / Procedure

| Table | Rows | Description |
|-------|------|-------------|
| `EC_GLXY_DRG_CD_2026` | 3,846 | DRG code + fiscal year to description (2026) |
| `EC_GLXY_DRG_CODE` | 21,321 | Expanded DRG code reference (all FYs) |

### Geographic

| Table | Rows | Columns | Description |
|-------|------|---------|-------------|
| `ZIP_COUNTY_LOOKUP` | 39,501 | `ZIP_CODE`, `COUNTY`, `STATE_ABBREVIATION`, `USPS_DEFAULT_CITY_FOR_ZIP` | ZIP to county/state/city |
| `COUNTY_SSA_FIPS_CBSA` | 3,283 | `FIPS`, `COUNTYNAME_FIPS`, `STATE`, `CBSA_CODE`, `CBSA_NAME`, `SSA`, `STATE_NAME` | FIPS/SSA/CBSA Rosetta Stone |
| `BP_COUNTY_SSA` | 3,185 | | County-level SSA reference |
| `URBAN_RURAL_FLAG` | 3,144 | `COUNTYYEAR`, `FIPSSCC`, `CMSSCC`, `URBAN_RURAL`, `URBAN_RURAL_DETAIL` | County+year to Urban/Rural (Isolated/Large Rural/Urban) |

### Provider

| Table | Rows | Columns | Description |
|-------|------|---------|-------------|
| `BP_GENESIS_CROSSWALK` | 149,339 | `PARENT_ENTITY_ID`, `TIN`, `PARENT_ENTITY_NM` | TIN to parent entity (Genesis/Optum grouping) |

### DME & Procedure Mapping

| Table | Rows | Columns | Description |
|-------|------|---------|-------------|
| `DME_MAPPING` | 12,237 | `PROC_CD`, `CATEGORY_1`, `CATEGORY_2`, `PA_FLAG`, `CATEGORY_3` | DME procedure to multi-level category + PA flag |
| `CL_COVERAGE_INDICATOR_HCPCS` | 19,773 | | HCPCS coverage indicator reference |
| `CL_SS_CODELIST_20260312` | 302 | | Skin Substitutes procedure code list |

### Synapse (Capitated Rate Network)

| Table | Rows | Description |
|-------|------|-------------|
| `CL_SYNAPSE_HCPCS_BY_LOB_20260211` | 1,434 | Synapse HCPCS codes by Line of Business |
| `CL_SYNAPSE_HCPCS_MNR_679` | 679 | M&R Synapse HCPCS inclusion list |
| `CL_SYNAPSE_HCPCS_CNS_755` | 755 | C&S Synapse HCPCS inclusion list |
| `CL_SYNAPSE_HPBP_GRPID_BY_WAVE` | 412 | HPBP to group ID + implementation wave |
| `CL_SYNAPSE_HPBP_GROUPID_W4_20251111` | 175 | Wave 4 HPBP-to-group mapping |
| `CL_SYNAPSE_HPBP_GROUPID_W5_OAH_20260410` | 29 | Wave 5 OAH HPBP-to-group mapping |
| `CL_SYNAPSE_HPBP_GROUPID_W6` | 162 | Wave 6 HPBP-to-group mapping |

---

## TMP_1Y (Analyst Working Tables)

1,012 tables total. Key dimension/reference:

### Provider / TIN Mapping

| Table | Columns | Description |
|-------|---------|-------------|
| `TIN_COLLECTION` | `TIN`, `COLLECTION` | TIN to hospital group name. Most-used dimension. |
| `CL_THERAPY_OPTUM_TINS_202602` | `TIN` | Optum-owned TINs (presence = Optum) |
| `HK_SWINGBED_2026` | `TIN`, `TYPE_F` | Swing bed facility TINs |
| `HK_NAVI_CONTRACTS` | `CONTRACT`, `YR`, `MARKET` | Navi PAC risk-bearing contracts |
| `P8001_OPTUM_TIN_2` | `TIN` | Older Optum TIN list |

### Member Cohort

| Table | Columns | Description |
|-------|---------|-------------|
| `2024_2025_HCE_COHORT_6` | `FIN_MBI_HICN_FNL`, `HCE_COHORT` | Member to HCE cohort (e.g., Product_Transition) |
| `2023_2024_HCE_COHORT_6` | | Prior year cohort |

### Synapse Reference

| Table | Rows | Description |
|-------|------|-------------|
| `CL_SYNAPSE_HCPCS_20250212` | 676 | HCPCS inclusion list for cap rate |
| `CL_SYNAPSE_GA_NC_HPBP_GRP` | 82 | Existing GA/NC market H-PBPs |
| `CL_SYNAPSE_EXP_HPBP_20250306` | 68-102 | Expansion market H-PBPs |
| `KN_SYNAPSE_HPBP_WAVES2AND3_GRP` | varies | HPBP to market_expansion + wave |

### Therapies

| Table | Schema | Columns | Description |
|-------|--------|---------|-------------|
| `THERAPIES_TIN_RURAL` | `TMP_1M` | `TIN`, `HOSPITAL` | Rural therapy provider TIN to hospital name |

### Other

| Table | Columns | Description |
|-------|---------|-------------|
| `CL_SKIN_SUBS_MAPPING` | `ICD_CODE`, `LOCATION_TYPE` | ICD to body location (Skin Subs) |
| `HCE_RESP_2024` / `HCE_RESP_2025` | | Respiratory diagnosis ICD flags |

---

## External Database References

Tables referenced in scripts that live outside VING_PRD_TREND_DB:

| Full Path | Description |
|-----------|-------------|
| `ublia_prd_isdc_prd_db.bdr_conf.d_geo_xref` | Geographic surrogate key to state/zip |
| `ublia_prd_isdc_prd_db.bdr_dm.d_cpt_look` | CPT/HCPCS code lookup |
| `ublia_prd_isdc_prd_db.bdr_conf.d_pln_ben_mod` | Plan benefit model dimension |
| `ublia_prd_isdc_prd_db.bdr_conf.d_ben` | Benefit dimension |
| `ublia_prd_isdc_prd_db.bdr_conf.d_calc_rt` | Calculation rate dimension |
| `fichsrv.tadm_glxy_diagnosis_code` (may be in TADM_TRE_CPY) | Diagnosis code reference |

---

# Population Segmentation Logic

Priority-ordered classification applied to claims, membership, and auth pulls.
Prefer explicit `CASE WHEN` logic over shortcut flags.

```sql
case
    -- OAH (Optum At Home)
    when migration_source = 'OAH' then 'OAH'
    -- M&R ISNP (Institutional)
    when fin_brand = 'M&R'
        and fin_product_level_3 = 'INSTITUTIONAL'
        then 'M&R ISNP'
    -- M&R FFS
    when sgr_source_name in ('COSMOS', 'NICE')
        and fin_brand = 'M&R'
        and global_cap = 'NA'
        and fin_product_level_3 not in ('DUAL', 'INSTITUTIONAL')
        and tfm_include_flag = 1
        then 'M&R FFS'
    -- C&S DSNP
    when sgr_source_name in ('COSMOS', 'CSP')
        and global_cap = 'NA'
        and fin_brand = 'C&S'
        and [DUAL conditions]
        then 'C&S DSNP'
    -- M&R DSNP
    when fin_brand = 'M&R'
        and fin_product_level_3 = 'DUAL'
        then 'M&R DSNP'
    else 'N/A'
end as population
```

**Exception**: Therapies project excludes DSNP from M&R FFS definition (adds `fin_product_level_3 not in ('DUAL', 'INSTITUTIONAL')` to the M&R FFS condition).

---

# Common Join Patterns

```sql
-- Claims → Hospital group (TIN_COLLECTION)
join tmp_1y.tin_collection d on a.prov_tin = d.tin

-- Claims → Procedure description
join hce_ops_lkup.tadm_glxy_procedure_code b on a.proc_cd = b.proc_cd

-- Membership → Group attributes
join fichsrv.group_crosswalk b
  on a.tadm_group_nbr_consist = b.group_number
  and a.fin_inc_year = b.year

-- Member identity (MBI → HICN)
join fichsrv.macra_crosswalk b on a.fin_mbi_hicn_fnl = b.mbi

-- Auth → LOC week assignment
join tmp_2y.ec_loc_week_assign c on a.pd_dn_ol_admit_start_dt = c.date

-- Auth → DRG (fiscal year logic)
join tmp_2y.ec_glxy_drg_cd_2026 b
  on a.fnl_drg_cd = b.drg_cd
  and case when month(a.admit_dt) >= 10 then year(a.admit_dt) + 1
           else year(a.admit_dt) end = b.fy

-- Claims → Optum TIN flag
left join tmp_1y.cl_therapy_optum_tins_202602 b on a.prov_tin = b.tin

-- Claims → Swing bed flag
left join tmp_1y.hk_swingbed_2026 b on a.prov_tin = b.tin

-- Claims → Navi contracts
left join tmp_1y.hk_navi_contracts b
  on a.fin_contractpbp = replace(b.contract, ' ', '')
  and substr(a.fst_srvc_month, 1, 4) = replace(b.yr, ' ', '')
  and substr(a.fin_market, 1, 2) = replace(b.market, ' ', '')

-- ZIP → County
join tmp_2y.zip_county_lookup b on a.zip_code = b.zip_code

-- County → Urban/Rural
join tmp_2y.urban_rural_flag b on a.fips_code = b.fipsscc
```

---

# Schema Naming Conventions

| Schema | Retention | Purpose |
|--------|-----------|---------|
| `HCE_OPS_LKUP` | Permanent | Curated lookup/reference tables |
| `HCE_OPS_FNL` | Permanent | Finalized operational outputs (auth master tables) |
| `HCE_OPS_STAGE` | Permanent | Operational staging (PA tracking, LOPA) |
| `HCE_OPS_ARCHV` | Permanent | Monthly membership archive snapshots |
| `FICHSRV` | Permanent | Enterprise fact + reference tables (team-maintained) |
| `FICHGRP` | Permanent | Group-level encounter fact tables |
| `TMP_1Y` | 1 year | Analyst working tables (purged annually) |
| `TMP_2Y` | 2 years | Longer-lived reference + analytical outputs |
| `TMP_1Q` | Quarterly | Quarterly purge temp tables |
| `TMP_1M` | 30 days | Monthly purge temp tables |
| `TMP_7D` | 7 days | Weekly purge temp tables |
| `HCEPUB` | Permanent | External-facing schema for cross-team sharing |
| `HCE_MISC` | Permanent | Miscellaneous |
| `MARD` | Permanent | MARD-specific tables |
| `TADM_TRE_CPY` | Permanent | TADM/TRE copied reference tables |

---

# Session Variables

Defined in `AGENTS.md` — used as parameters in templated queries:

| Variable | Format | Description | Current Value |
|----------|--------|-------------|---------------|
| `notifications_date` | YYYYMMDD | IPA upstream date suffix | Check AGENTS.md |
| `membership_month` | YYYYMM | Archive table suffix (`gl_rstd_gpsgalnce_f_<YYYYMM>`) | Check AGENTS.md |
| `claims_month` | YYYYMM | Upper bound for claims service date | Check AGENTS.md |
