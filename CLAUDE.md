# UHC Project — Claude Code Guidelines

## SQL Formatting Rules

1. **Table creation**: always use `create or replace table`, never `drop table if exists` + `create table`
2. **Commas**: leading commas (`, column_name`), never trailing
3. **Aliases**: lowercase single-letter aliases with `as` — e.g., `left join <table> as a`
4. **Keywords**: all SQL syntax in lowercase (`select`, `from`, `left join`, `where`, `group by`, etc.)
5. **`case` statements**: single space before `then`, no column-aligning padding — `when x = 1 then 'y'`, not `when x = 1          then 'y'`

See `_templates/` for canonical examples of these patterns.

## Table Naming Convention

```
<schema>.<initials>_<projectname>_<topic>_<YYYYMM>
```

- **Schema**: `tmp_1m` (preferred write target)
- **Initials**: `kn` for prod, `knd` for dev
- **Example**: `tmp_1m.kn_loc_valuation_202604`

The date suffix uses the run/notification month in `YYYYMM` format.

## Canonical Table Sources

### Claims
Pull from `fichsrv.*` tables. Use `union all` across entities as needed:
- `fichsrv.glxy_op_f`  — COSMOS outpatient
- `fichsrv.glxy_pr_f`  — COSMOS professional
- `fichsrv.dcsp_op_f`  — CSP outpatient
- `fichsrv.dcsp_pr_f`  — CSP professional
- `fichsrv.nce_op_f`   — NICE outpatient
- `fichsrv.nce_pr_f`   — NICE professional

See `_templates/claims_template.sql`.

### Membership
Pull from `fichsrv.tre_membership`.
See `_templates/membership_template.sql`.

---

## Isolation Forest — Therapy Provider Anomaly Detection

### Objective
Identify therapy providers whose behavioral patterns are unusual relative to peers
in the same specialty, market, and network. This is an unsupervised model — there
is no Y variable. The anomaly score is an output of the algorithm, not a trained
prediction.

### Unit of Analysis
One row = one unique `prov_tin × category_1 × market` combination.
Each row is one observation fed into the model as X variables.

### Stratification
Run separate Isolation Forest models for each `optum_tin_flag` population:
- `optum_tin_flag = 1` — Optum providers compared to other Optum providers
- `optum_tin_flag = 0` — UHC providers compared to other UHC providers

Do not mix populations in the same model.

### Identifiers (not X variables)
| Variable | Description |
|---|---|
| `prov_tin` | Provider identifier |
| `category_1` | Specialty — PT, OT, ST, Chiro |
| `market` | Geographic market |
| `optum_tin_flag` | Stratification variable — defines which model the row goes into |

### Filters (not X variables — used for data quality only)
| Variable | Rule |
|---|---|
| `episode_count` | Exclude prov_tin × category_1 × market rows with fewer than 30 episodes |
| `member_count` | Exclude rows with fewer than 10 distinct members |

Threshold TBD — adjust based on data distribution.

### Features (X variables — behavioral metrics only)
| Feature | SQL Expression | Notes |
|---|---|---|
| `avg_visits_per_ep` | SUM(n_visits) / episode_count | Core utilization intensity signal |
| `avg_allowed_per_visit` | SUM(allowed) / SUM(n_visits) | Unit price signal |
| `avg_allowed_per_ep` | SUM(allowed) / episode_count | Episode cost signal |
| `allowed_per_member` | SUM(allowed) / member_count | Member-level cost signal |
| `denial_rate` | COUNT(denied claims) / COUNT(total claims) | Auth/billing behavior signal |
| `market_count` | COUNT(DISTINCT market_fnl) | Provider geographic spread — property of prov_tin not the market row |
| `diag_diversity` | COUNT(DISTINCT ahrq_diag_dtl_catgy_desc) / episode_count | Include only if diagnosis field is reliable |

### Model Specs
- Algorithm: `IsolationForest` (sklearn)
- Scale all continuous features with `StandardScaler` before fitting
- `contamination=0.05` (adjustable — this is a business decision not a statistical one)
- Output: `raw_score` from `decision_function` (continuous ranking, not binary predict)
- Final output: provider-market rows ranked by anomaly score descending

### Output Table
Save scored results as `tmp_1m.knd_mbm_tin_features_<YYYYMM>` (e.g., `202604`)
Columns: `prov_tin, category_1, market, optum_tin_flag, all features, raw_score`

### Narrative Output
For the top 20 flagged providers per model run, generate a plain English paragraph
per provider explaining what is unusual about their pattern relative to peers in the
same category and market.
