# LOC Anomaly Detection — Session Handoff

## What This Project Is
Monthly pipeline that identifies unusual LOC (Level of Care) KPI patterns across
multiple grouping dimensions (provider, market, hospital group, etc.) for three
populations (M&R FFS, C&S DSNP, OAH). Uses a tiered approach:

1. **Tier 1 — Percentile flagging** (primary): flags entities in the top/bottom 10th
   percentile of each rate feature within their dimension. Directly actionable, no model.
2. **Tier 2 — Isolation Forest** (secondary sweep): catches entities that don't trigger
   any single-KPI flag but have an unusual *combination* of rates.

---

## Files in This Project

| File | Purpose |
|---|---|
| `loc_anomaly.ipynb` | Main notebook — Tier 1 percentile flagging, Tier 2 Isolation Forest, exports |
| `loc_anomaly.py` | Legacy Python script (Isolation Forest only — superseded by the notebook) |
| `loc_agg.sql` | Snowflake SQL that pre-aggregates raw data and computes all rates, one table per population |
| `loc_anomaly_guide.md` | Beginner-friendly walkthrough of the Python script with R↔Python equivalents |

---

## Pipeline Architecture

### Run order
```
1. Run loc_agg.sql in Snowflake       (creates the 3 kn_loc_*_agg tables)
2. Run loc_anomaly.ipynb              (Tier 1 percentile → Tier 2 IF → exports)
```

### SQL does aggregation + rate calculation (not Python)
- `loc_agg.sql` creates 3 population-specific tables
- One `UNION ALL` block per dimension (prov_tin, svc_setting, fin_market, etc.)
- Each block: `GROUP BY` that dimension, `HAVING case_count >= 30`, rates computed via `/ NULLIF(..., 0)`
- Python only connects, reads the pre-aggregated table, and runs the detection tiers

### Why separate UNION ALL blocks instead of one GROUP BY
Each dimension answers a different question. `GROUP BY prov_tin` collapses across
all markets/products. `GROUP BY fin_market` collapses across all providers. These
cannot come from a single GROUP BY — you'd get the cross-product, not per-dimension summaries.
The `global` block at the bottom does the all-dimensions-combined version intentionally.

### Why three separate population tables
Both tiers run within each population — peers compared to peers only. Mixing
populations would cause OAH providers to be flagged simply for being OAH, not for
being unusual within OAH.

### Global dimension block
The last UNION ALL block in each table groups by ALL dimensions simultaneously —
catches anomalies that only appear when dimensions interact (e.g., a provider that
looks normal overall but is anomalous within a specific market + product combination).

---

## Tier 1 — Percentile Flagging + IQR Flagging

Two complementary rule-based methods run in parallel for each dimension × rate feature.
Both are computed within the dimension's peer group only — never across dimensions.

### Method A — Percentile flagging
- Compute percentile rank within the dimension's peer group
- Flag entities in the top 10th or bottom 10th percentile
- Record `peer_median`, `percentile`, `flag_high`, `flag_low`

### Method B — IQR flagging
- Compute Q1, Q3, and IQR within the dimension's peer group
- Flag entities below `Q1 - 1.5×IQR` or above `Q3 + 1.5×IQR` (standard Tukey rule)
- Record `iqr_low_bound`, `iqr_high_bound`, `iqr_flag_high`, `iqr_flag_low`

### Why both
Percentile flagging always flags the top/bottom 10% — even in a well-behaved peer group
with no real outliers. IQR only fires when a value is a genuine statistical outlier
relative to the spread of the group. Together they answer different questions:
- Percentile: "who is at the extreme end of this distribution?"
- IQR: "who is far enough outside the bulk of peers to be unusual?"

Entities flagged by both methods are higher priority than those flagged by one only.
The `tier` column will reflect `"percentile"`, `"iqr"`, or `"both"` for Tier 1 flags.

### Output
Long-format table — one row per entity × feature × dimension, with both flag sets included.

---

## Tier 2 — Isolation Forest

### Role
Catches entities whose *combination* of rates is unusual even when no single rate
is extreme. Runs after Tier 1 on the same pre-aggregated data.

### Model specs
- Algorithm: `IsolationForest` (sklearn)
- Fit separately per dimension and per population — peers compared to peers only
- Scale all features with `StandardScaler` before fitting
- `contamination=0.05` — business decision, not statistical
- Output: `raw_score` from `decision_function` (continuous ranking; lower = more anomalous)
- Use `raw_score` for ranking, not binary `predict()` output

### Features (X variables — rates only, no identifiers)
| Feature | Notes |
|---|---|
| `adr_rate` | |
| `persistent_adr_rate` | Key signal — VP priority |
| `md_review_rate` | |
| `pre_auth_rate` | |
| `appeal_overturn_rate` | Key signal — VP priority |
| `mcr_overturn_rate` | Key signal — VP priority |
| `p2p_overturn_rate` | Key signal — VP priority |
| `member_appeal_rate` | Key signal — VP priority |

Identifiers (`prov_tin`, `_dimension`, `_dim_value`, `population`) are carried
through but never fed into the model.

### Analyst-facing output (matplotlib/seaborn)
End output is for the analyst to inspect before deciding what to communicate.
- Score distribution plot per dimension (histogram of `raw_score`)
- Flagged entity scatter — one dot per entity, x = raw_score, y = a key rate (e.g. `appeal_overturn_rate`)
- Z-score heatmap for top flagged entities — shows which features drove the anomaly

### Combined output table
A `tier` column marks each flag's origin (`"percentile"`, `"iqr"`, `"percentile+iqr"`, `"isolation_forest"`, or combinations thereof) so analysts can prioritize by consensus across methods.

---

## Key Business Context

### What the VP actually wants
Exploratory surveillance — no hypothesis. Priority signals:
- High `member_appeal_rate`
- Low `persistent_adr_rate`
- High overturn rates (`appeal_overturn_rate`, `mcr_overturn_rate`, `p2p_overturn_rate`)

Across dimensions: markets, providers (`prov_tin`), hospital groups, service settings.

### Why Tier 1 is primary
Isolation Forest limitations for this use case:
1. **Subpopulation blindness** — no concept of groups; SNF providers flagged just for being SNFs
2. **Multivariate black box** — a score of -0.043 is not actionable on its own
3. **Wrong tool for no-hypothesis surveillance** — percentile ranking produces "top 10% of appeal overturn rate among prov_tins" — a sentence a VP can act on

---

## Parameters

| Parameter | Value | Notes |
|---|---|---|
| `IQR_MULTIPLIER` | 1.5 | Tier 1 IQR: standard Tukey rule — adjust if too many/few flags |
| `TOP_PCT` / `BOTTOM_PCT` | 0.10 | Tier 1: flag top/bottom 10th percentile |
| `CONTAMINATION` | 0.05 | Tier 2: expected anomaly rate — business decision |
| `n_estimators` | 200 | Tier 2: more trees = more stable scores |
| `random_state` | 42 | Reproducibility within a run |
| Volume filter | `HAVING case_count >= 30` | Applied in SQL, not Python |

---

## Rate Denominator Reference

| Rate | Denominator |
|---|---|
| `adr_rate`, `persistent_adr_rate`, `md_review_rate`, `pre_auth_rate`, `auth_per_k` | `case_count` |
| All appeal, overturn, p2p, mcr, member appeal rates | `initial_adr_cnt` |
| `persistency` | `initial_adr_cnt` |

---

## Snowflake Table Naming

Source table: `tmp_1m.kn_loc_notif_${notifications_date}_od`
All prep tables written to: `tmp_1m`

| | M&R FFS | C&S DSNP | OAH |
|---|---|---|---|
| SQL filter | `mnr_total_ffs_flag = 1` | `cns_dual_flag = 1` | `total_oah_flag = 'OAH'` |
| Prep table | `kn_loc_mnr_agg_04152026` | `kn_loc_cns_agg_04152026` | `kn_loc_oah_agg_04152026` |
| Tier 1 CSV | `loc_pctl_{pop}_202604.csv` | | |
| Tier 2 CSV | `loc_if_{pop}_202604.csv` | | |
| Combined CSV | `loc_combined_{pop}_202604.csv` | | |

---

## Future Enhancements

**Tier 3 — Trend layer**: month-over-month delta is often more actionable than the
level. Requires storing prior month output and joining on `_dimension` + `_dim_value`.

**Within-group Isolation Forest**: run one model per dimension *value* (e.g., SNFs
vs. SNFs only). Only worth doing if Tier 1 flags produce too many false positives.

**CUSUM control charts**: standard methodology in health plan quality surveillance.
Flags when a metric crosses a statistically meaningful threshold relative to its own
historical baseline. More credible to clinical/actuarial audience than IF scores.