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
| `loc_anomaly.ipynb` | Main notebook — Tier 1 percentile flagging, Tier 2 Isolation Forest, narratives, exports |
| `loc_anomaly.py` | Legacy Python script (Isolation Forest only — superseded by the notebook) |
| `loc_agg.sql` | Snowflake SQL that pre-aggregates raw data and computes all rates, one table per population |
| `loc_anomaly_guide.md` | Beginner-friendly walkthrough of the Python script with R↔Python equivalents |

---

## Pipeline Architecture

### Tiered detection (what the notebook does)

**Tier 1 — Percentile flagging** runs first. For each dimension × rate feature:
- Compute percentile rank within the dimension's peer group
- Flag entities in the top 10th or bottom 10th percentile
- Record `peer_median`, `percentile`, `flag_high`, `flag_low`
- Output: long-format table (one row per entity × feature × dimension)

This directly answers "who has a high overturn rate" — immediately usable in VP meetings.

**Tier 2 — Isolation Forest** runs second on the same data. Its role is catching
entities whose *combination* of rates is unusual even when no single rate is extreme.
- Fit per dimension, `StandardScaler` + `contamination=0.05`
- Z-scores added for interpretability; top 3 reasons per flagged entity
- Output: scored table + narrative paragraphs for top 20 anomalies per dimension

**Combined output**: a `tier` column marks each flag's origin (`"percentile"`,
`"isolation_forest"`, or `"both"`) so stakeholders can prioritize.

### SQL does aggregation + rate calculation (not Python)
- `loc_agg.sql` creates 3 population-specific tables — see naming below
- One `UNION ALL` block per dimension (prov_tin, svc_setting, fin_market, etc.)
- Each block: `GROUP BY` that dimension, `HAVING case_count >= 30`, rates computed via `/ NULLIF(..., 0)`
- Python only connects, reads the pre-aggregated table, and runs the detection tiers

### Why separate UNION ALL blocks instead of one GROUP BY
Each dimension answers a different question. `GROUP BY prov_tin` collapses across
all markets/products. `GROUP BY fin_market` collapses across all providers. These
cannot come from a single GROUP BY — you'd get the cross-product, not per-dimension summaries.
The `global` block at the bottom does the all-dimensions-combined version intentionally.

### Three separate population tables (not one combined)
Both tiers run within each population — peers compared to peers only. Mixing
populations would cause OAH providers to be flagged simply for being OAH, not for
being unusual within OAH.

### Global dimension block
The last UNION ALL block in each table groups by ALL dimensions simultaneously —
catches anomalies that only appear when dimensions interact (e.g., a provider that
looks normal overall but is anomalous within a specific market + product combination).

---

## Key Business Context

### What the VP actually wants
Exploratory surveillance — no hypothesis. Looking for:
- High `member_appeal_rate`
- Low `persistent_adr_rate`
- High overturn rates (`appeal_overturn_rate`, `mcr_overturn_rate`, `p2p_overturn_rate`)

Across dimensions: markets, providers (`prov_tin`), hospital groups, service settings, etc.

### Why Tier 1 percentile flagging is the primary approach
Isolation Forest has limitations for this use case:
1. **Subpopulation blindness** — no concept of groups; SNF providers flagged just for being SNFs
2. **Multivariate black box** — VP can't act on a score of -0.043
3. **Wrong tool for no-hypothesis surveillance** — percentile ranking is more actionable

Percentile flagging solves all three: it's within-dimension, single-KPI, and
produces "top 10% of appeal overturn rate among prov_tins" — a sentence a VP can act on.

---

## Future Enhancements (not yet implemented)

### Tier 3 — Trend layer
Month-over-month delta is often more actionable than the level.
A provider jumping from 8% to 22% appeal rate is more urgent than one stable at 22%.
Requires storing prior month output — add a `prior_month_table` parameter and join on
`_dimension` + `_dim_value` to compute deltas.

### Within-group Isolation Forest (statistical upgrade)
Instead of one model per dimension, run one model per dimension *value* for large
dimensions like `svc_setting`. This ensures SNFs are only compared to SNFs, acute
to acute. Only worth doing if Tier 1 flags are producing too many false positives.

### CUSUM control charts (longer term)
Standard methodology in health plan quality surveillance (same family as HEDIS
outlier reporting). Flags when a metric crosses a statistically meaningful threshold
relative to its own historical baseline. More credible to clinical/actuarial audience
than Isolation Forest scores.

---

## Rate Denominator Reference

| Rate | Denominator |
|---|---|
| `adr_rate`, `persistent_adr_rate`, `md_review_rate`, `pre_auth_rate`, `auth_per_k` | `case_count` |
| All appeal, overturn, p2p, mcr, member appeal rates | `initial_adr_cnt` |
| `persistency` | `initial_adr_cnt` |

---

## Parameters (loc_anomaly.ipynb)

| Parameter | Value | Notes |
|---|---|---|
| `TOP_PCT` / `BOTTOM_PCT` | 0.10 | Tier 1: flag top/bottom 10th percentile |
| `CONTAMINATION` | 0.05 | Tier 2: expected 5% anomaly rate — business decision, not statistical |
| `TOP_N` | 20 | Tier 2: narratives written for top 20 most anomalous per dimension |
| `n_estimators` | 200 | Tier 2: more trees = more stable scores |
| `random_state` | 42 | Reproducibility within a run |
| Volume filter | `HAVING case_count >= 30` | Applied in SQL, not Python |

---

## Snowflake Table Naming Convention

| | M&R FFS | C&S DSNP | OAH |
|---|---|---|---|
| SQL filter | `mnr_total_ffs_flag = 1` | `cns_dual_flag = 1` | `total_oah_flag = 'OAH'` |
| Prep table | `kn_loc_mnr_agg_04152026` | `kn_loc_cns_agg_04152026` | `kn_loc_oah_agg_04152026` |
| Tier 1 CSV | `loc_pctl_{pop}_202604.csv` | `loc_pctl_{pop}_202604.csv` | `loc_pctl_{pop}_202604.csv` |
| Tier 2 scored CSV | `loc_if_{pop}_202604.csv` | `loc_if_{pop}_202604.csv` | `loc_if_{pop}_202604.csv` |
| Narratives CSV | `loc_narratives_{pop}_202604.csv` | `loc_narratives_{pop}_202604.csv` | `loc_narratives_{pop}_202604.csv` |
| Combined CSV | `loc_combined_{pop}_202604.csv` | `loc_combined_{pop}_202604.csv` | `loc_combined_{pop}_202604.csv` |

Source table: `tmp_1m.kn_loc_notif_${notifications_date}_od`
All prep tables written to schema: `tmp_1m`

---

## Run Order

```
1. Run loc_agg.sql in Snowflake       (creates the 3 kn_loc_*_agg tables)
2. Run loc_anomaly.ipynb              (Tier 1 percentile → Tier 2 IF → combined export)
```
