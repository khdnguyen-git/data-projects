# LOC Project — Context & Objectives

## What This Project Does

Produces the **LOC (Level of Care) Valuation** output used for leading indicator reporting. The table tracks acute inpatient medical/surgical authorization decisions and member months across M&R and C&S populations.

## Key Variables (update each run)

| Variable | Example | Notes |
|---|---|---|
| `notifications_date` | `04222026` | drives all `kn_` table names |
| `membership_month` | `202603` | confirm with Pradeepa that enrollment table is updated |
| `claims_month` | `202603` | update to match current claims run |

## Table Naming

Output tables use `kn_` initials (prod) or `knd_` (dev), not `ec_` (IPA's prefix).

```
tmp_1m.kn_loc_*_${notifications_date}_od
```

## Pipeline Overview

| Step | Table | Source |
|---|---|---|
| 1 | `kn_ip_dataset_${notifications_date}_4_od` | `ec_ip_dataset_${notifications_date}_3_od` (IPA's upstream) |
| 2 | `kn_loc_mm_${notifications_date}_od` | `hce_ops_archv.gl_rstd_gpsgalnce_f_${membership_month}` |
| 3 | `kn_loc_notif_${notifications_date}_od` | union of steps 1 + 2 |
| 4 | `kn_ip_dataset_loc_${notifications_date}_od` | step 3, filtered to `loc_flag = 1` |

## loc_flag Definition

`loc_flag = 1` means the case is **acute inpatient, place of service 21 (Acute Hospital), medical or surgical admit category**. Defined in IPA's `ec_avtar_23_1_od`. Member month rows are hardcoded to `loc_flag = 1`.

The LOC table filters to `ipa_pac_flag in ('IPA', 'MM')` — claims are excluded (source is `_notif_`, not `_all_`).

## Population Segmentation (priority order)

```
OAH               → total_oah_flag = 'OAH'
M&R Institutional → institutional_flag = 'Institutional'
M&R FFS           → mnr_total_ffs_flag = 1
C&S DSNP          → cns_dual_flag = 1
M&R DSNP          → mnr_dual_flag = 1
```

---

## Anomaly Detection Pipeline

Two-script pipeline: `loc_agg.sql` builds pre-aggregated rate tables in Snowflake,
then `loc_anomaly.ipynb` runs a tiered detection pipeline in Python.

### Scripts

| Script | Purpose |
|---|---|
| `Scripts/loc_agg.sql` | Creates 3 population-specific aggregation tables (MNR, CNS, OAH) |
| `Scripts/loc_anomaly.ipynb` | Reads agg tables, runs Tier 1 + Tier 2 detection, exports CSVs |

### Run Order

1. Run `loc_agg.sql` in DBeaver (uses `@set notifications_date = ...` client-side variable)
2. Run `loc_anomaly.ipynb` (reads the 3 `kn_loc_*_agg_` tables, outputs to `C:\Users\knguy139\Documents\Projects\Data\Output`)

### Aggregation Tables (`loc_agg.sql`)

Each population gets its own table with dimension blocks unioned together.
Each block aggregates the source data by one dimension and computes 14 rate features.
A `global` block cross-cuts all dimensions via `concat_ws`.

| Population | Table | Filter | Dimensions |
|---|---|---|---|
| M&R FFS | `kn_loc_mnr_agg_${notifications_date}` | `mnr_total_ffs_flag = 1` | 11 + global: prov_tin, svc_setting, fin_market, fin_product_level_3, fin_plan_level_2, fin_contractpbp, hospital_group, los_categories, admit_type, ipa_li_split, ip_type |
| C&S DSNP | `kn_loc_cns_agg_${notifications_date}` | `cns_dual_flag = 1` | 8 + global: prov_tin, svc_setting, fin_market, hospital_group, los_categories, admit_type, ipa_li_split, ip_type |
| OAH | `kn_loc_oah_agg_${notifications_date}` | `total_oah_flag = 'OAH'` | 8 + global: prov_tin, svc_setting, fin_market, hospital_group, los_categories, admit_type, ipa_li_split, ip_type |

CNS and OAH use a reduced dimension set — `fin_product_level_3`, `fin_plan_level_2`, and `fin_contractpbp` are excluded.

### Output Columns (all agg tables)

```
_dimension, _dim_value, case_count, membership,
adr_rate, persistent_adr_rate, persistency, md_review_rate,
appeal_rate, appeal_overturn_rate, p2p_rate, p2p_overturn_rate,
mcr_reconsideration_rate, mcr_overturn_rate,
member_appeal_rate, member_appeal_overturn_rate,
pre_auth_rate, auth_per_k
```

- `having sum(case_count) >= 30` filters low-volume entities from every block
- Rate denominators use `nullif(..., 0)` to avoid division by zero

### 14 Rate Features

| Feature | Denominator | Description |
|---|---|---|
| `adr_rate` | case_count | Initial ADR rate |
| `persistent_adr_rate` | case_count | Persistent ADR rate |
| `persistency` | initial_adr_cnt | Persistent / initial ADR |
| `md_review_rate` | case_count | MD review rate |
| `appeal_rate` | initial_adr_cnt | Appeal rate (% of ADRs) |
| `appeal_overturn_rate` | initial_adr_cnt | Appeal overturn rate |
| `p2p_rate` | initial_adr_cnt | P2P rate |
| `p2p_overturn_rate` | initial_adr_cnt | P2P overturn rate |
| `mcr_reconsideration_rate` | initial_adr_cnt | MCR reconsideration rate |
| `mcr_overturn_rate` | initial_adr_cnt | MCR overturn rate |
| `member_appeal_rate` | initial_adr_cnt | Member appeal rate |
| `member_appeal_overturn_rate` | initial_adr_cnt | Member appeal overturn rate |
| `pre_auth_rate` | case_count | Pre-auth rate |
| `auth_per_k` | membership | Auth per 1,000 members |

### Tiered Detection (`loc_anomaly.ipynb`)

**Tier 1 — Percentile flagging** (primary): For each dimension × rate feature,
rank entities within their peer group. Flag top/bottom 10th percentile.
No model needed — directly answers "who has a high overturn rate?"

**Tier 2 — Isolation Forest** (secondary): Catches entities whose *combination*
of rates is unusual even when no single rate triggers a Tier 1 flag.
Features scaled with `StandardScaler`, `contamination=0.05`, `n_estimators=200`.
Z-scores added post-hoc for interpretability; top 3 reasons per entity.

**Combined flagging**: `tier` column marks origin — `"percentile"`, `"isolation_forest"`, or `"both"`.

### Notebook Output Files

All written to `C:\Users\knguy139\Documents\Projects\Data\Output`:

| File | Contents |
|---|---|
| `loc_pctl_{pop}_{YYYYMM}.csv` | Tier 1 percentile flags (long format: entity × feature) |
| `loc_if_{pop}_{YYYYMM}.csv` | Tier 2 Isolation Forest scores + top reasons |
| `loc_narratives_{pop}_{YYYYMM}.csv` | Plain-English narratives for top 20 anomalies per dimension |
| `loc_combined_{pop}_{YYYYMM}.csv` | Merged flags with `tier` column |

### Notebook Connection

Uses SQLAlchemy `create_engine` with `snowflake-sqlalchemy` (not `snowflake.connector`):

```python
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

engine = create_engine(URL(
    account   = "UHG-UHGDWAAS",
    user      = "...",
    authenticator = "externalbrowser",
    warehouse = "...",
    database  = "VING_PRD_TREND_DB",
    schema    = "TMP_1M",
))
```

Queries use `engine.connect()` context manager; cleanup uses `engine.dispose()`.
