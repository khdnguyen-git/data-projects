---
name: "icm check notebook"
created: "2026-06-30T14:34:45.522Z"
status: pending
---

# Plan: Create LOC\_IF\_SHAP\_icm-check.ipynb

## Context

The user wants a new notebook that runs the Isolation Forest pipeline on **ICM-ever-owned cases only**, sourced from `tmp_7d.hce_adr_avtar_like_25_26_f` (which has the `icm_ever_owned` column that `ec_avtar` lacks).

**Key findings from exploration:**

- `tmp_7d.hce_adr_avtar_like_25_26_f` has 194 columns, including `ICM_EVER_OWNED` (NUMBER type)
- Uses `ADMIT_ACT_MONTH` (not `hce_admit_month`)
- Does NOT have `ip_type` or `loc_flag` — but has `ADMIT_CAT_CD` (e.g. `'17 - Medical'`, `'30 - Surgical'`, `'33 - Transplant'`) and `SVC_SETTING` (`'Inpatient'`)
- All ICM-owned M\&R rows have `PLC_OF_SVC_CD = '21 - Acute Hospital'` — effectively already LOC-eligible
- Has same FFS columns as ec\_avtar: `sgr_source_name`, `global_cap`, `fin_product_level_3`, `tfm_include_flag`, `nce_tadm_dec_risk_type`
- Does NOT have LTAC/SNF/AIR rows (those aren't ICM-owned)
- `ec_avtar` does NOT have `icm_ever_owned`

**Filter mapping (ec\_avtar → tmp\_7d):**

| ec\_avtar filter                                     | tmp\_7d equivalent                                                     |
| ---------------------------------------------------- | ---------------------------------------------------------------------- |
| `fin_brand = 'M&R'`                                  | Same                                                                   |
| `ip_type IN ('Medical', 'Surgical', 'Transplant')`   | `ADMIT_CAT_CD IN ('17 - Medical', '30 - Surgical', '33 - Transplant')` |
| `to_varchar(admit_dt_act, 'MM/dd/yyyy') IS NOT NULL` | Same column exists                                                     |
| `loc_flag = 1`                                       | Implicit — all ICM rows are POS 21 Acute                               |
| FFS logic (global\_cap, sgr, etc.)                   | Same columns                                                           |
| `hce_admit_month BETWEEN ...`                        | `admit_act_month BETWEEN ...`                                          |
| N/A                                                  | `icm_ever_owned = 1` (additional filter)                               |

## Implementation

### Cell 1: QA Check — Case Count Comparison

Run two queries side-by-side for `202602` M\&R FFS:

1. `ec_avtar` with full LOC filters (no icm\_ever\_owned — doesn't exist)
2. `tmp_7d` with `icm_ever_owned = 1` + equivalent filters

Print both counts and the ratio (tmp\_7d should be a subset).

### Cell 2: Imports

Same as production: sqlalchemy, pandas, numpy, sklearn, shap, matplotlib.

### Cell 3: Snowflake Connection

Standard `create_engine` block.

### Cell 4: Data Pull (from tmp\_7d)

Two queries (2025 and 2026) pulling from `tmp_7d.hce_adr_avtar_like_25_26_f`:

- Filter: `icm_ever_owned = 1`, `fin_brand = 'M&R'`, FFS logic, `ADMIT_CAT_CD IN ('17 - Medical', '30 - Surgical', '33 - Transplant')`, `to_varchar(admit_dt_act, 'MM/dd/yyyy') IS NOT NULL`
- Join to `tmp_1y.tin_collection` on `substr(fa_prov_id, 2, 9) = tin`
- Month ranges: 2025 = `admit_act_month BETWEEN '202501' AND '202512'`, 2026 = `admit_act_month BETWEEN '202601' AND '202604'`
- Same aggregation: hospital\_group x market with count columns
- `HAVING COUNT(DISTINCT case_id) >= 30`

### Cells 5-9: Same Pipeline as Production

Copy the pipeline logic from `LOC_IF_SHAP_production.ipynb`:

- k estimation (pooled, 5 features)
- Empirical Bayes shrinkage
- IF models (6 feat 2025, 7 feat 2026)
- Bootstrap stability (200x, 0.50 consensus)
- SHAP attribution

### Cell 10: Output Summary

Print flagged hospitals with SHAP drivers.

## Verification

- QA cell confirms tmp\_7d is a strict subset of ec\_avtar (fewer cases for same month/population)
- IF pipeline should produce fewer hospital-markets (ICM filter reduces volume, some may drop below 30-case threshold)
- Sanity check: flagged ICM hospitals should overlap significantly with production flagged set

## Critical Files

- `c:\Users\knguy139\Documents\Projects\LOC\Scripts\LOC_IF_SHAP_production.ipynb` — source pipeline to replicate
- `c:\Users\knguy139\Documents\Projects\LOC\Scripts\LOC_IF_README.md` — reference for methodology
