# LOC Anomaly Detection — Session Handoff

## What This Project Is
Monthly pipeline that detects anomalous LOC (Level of Care) KPI patterns across
multiple grouping dimensions (provider, market, hospital group, etc.) for three
populations (M&R FFS, C&S DSNP, OAH). Uses Isolation Forest + z-score explanation.

---

## Files in This Project

| File | Purpose |
|---|---|
| `loc_anomaly.py` | Main Python pipeline — loads data, runs Isolation Forest, exports results |
| `loc_agg.sql` | Snowflake SQL that pre-aggregates raw data and computes all rates, one table per population |
| `loc_anomaly_guide.md` | Beginner-friendly walkthrough of the Python script with R↔Python equivalents |

---

## Architecture Decisions Made This Session

### SQL does aggregation + rate calculation (not Python)
- `loc_agg.sql` creates 3 population-specific tables — see naming below
- One `UNION ALL` block per dimension (prov_tin, svc_setting, fin_market, etc.)
- Each block: `GROUP BY` that dimension, `HAVING case_count >= 30`, rates computed via `/ NULLIF(..., 0)`
- Python `build_rates()` and `aggregate_dimension()` functions were removed — SQL replaces them entirely
- Python now only connects, reads the pre-aggregated table, and runs the model

### Why separate UNION ALL blocks instead of one GROUP BY
Each dimension answers a different question. `GROUP BY prov_tin` collapses across
all markets/products. `GROUP BY fin_market` collapses across all providers. These
cannot come from a single GROUP BY — you'd get the cross-product, not per-dimension summaries.
The `global` block at the bottom does the all-dimensions-combined version intentionally.

### Three separate population tables (not one combined)
IF models within each population compare peers to peers. Mixing populations would
cause OAH providers to be flagged simply for being OAH, not for being unusual within OAH.

### Global model
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

### Statistical critique of current approach
Isolation Forest has limitations for this use case:
1. **Subpopulation blindness** — no concept of groups; SNF providers flagged just for being SNFs
2. **Multivariate black box** — VP can't act on a score of -0.043
3. **Wrong tool for no-hypothesis surveillance** — percentile ranking is more actionable

---

## Recommended Next Steps (priority order)

### 1. Add Tier 1 percentile flagging (highest business value, no model needed)
For each dimension × rate feature combination, compute:
- Entity's rate value
- Peer median within that dimension
- Percentile rank within that dimension
- Flag if top/bottom 10th percentile

This directly answers "who has a high overturn rate" and is immediately usable in VP meetings.

```python
def percentile_flags(df, rate_features, top_pct = 0.10, bottom_pct = 0.10):
    """
    For each dimension group, rank all entities on each rate feature.
    Flag top/bottom percentile. Returns long-format table.
    """
    rows = []
    for dim, group in df.groupby("_dimension"):
        for feat in rate_features:
            if feat not in group.columns:
                continue
            ranked = group[["_dim_value", "case_count", feat]].dropna(subset = [feat]).copy()
            ranked["percentile"]  = ranked[feat].rank(pct = True)
            ranked["peer_median"] = ranked[feat].median()
            ranked["flag_high"]   = ranked["percentile"] >= (1 - top_pct)
            ranked["flag_low"]    = ranked["percentile"] <= bottom_pct
            ranked["_dimension"]  = dim
            ranked["_feature"]    = feat
            rows.append(ranked)
    return pd.concat(rows, ignore_index = True)
```

### 2. Keep Isolation Forest as Tier 2 (secondary sweep only)
Run after percentile flagging. Its legitimate role is catching entities that don't
trigger any single-KPI flag but have an unusual *combination* of rates. Re-frame
output to stakeholders as "flagged for unusual pattern, see z-scores for detail"
rather than presenting the raw score.

### 3. Add trend layer (Tier 3)
Month-over-month delta is often more actionable than the level.
A provider jumping from 8% to 22% appeal rate is more urgent than one stable at 22%.
Requires storing prior month output — add a `prior_month_table` parameter and join on
`_dimension` + `_dim_value` to compute deltas.

### 4. Within-group Isolation Forest (statistical upgrade)
Instead of one model per dimension, run one model per dimension *value* for large
dimensions like `svc_setting`. This ensures SNFs are only compared to SNFs, acute
to acute. Only worth doing if Tier 1 flags are producing too many false positives.

### 5. Consider CUSUM control charts (longer term)
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

## Model Parameters (loc_anomaly.py)

| Parameter | Value | Notes |
|---|---|---|
| `CONTAMINATION` | 0.05 | Expected 5% anomaly rate — business decision, not statistical |
| `TOP_N` | 20 | Narratives written for top 20 most anomalous per dimension |
| `n_estimators` | 200 | More trees = more stable scores |
| `random_state` | 42 | Reproducibility within a run |
| Volume filter | `HAVING case_count >= 30` | Applied in SQL, not Python |

---

## Snowflake Table Naming Convention

| | M&R FFS | C&S DSNP | OAH |
|---|---|---|---|
| SQL filter | `mnr_total_ffs_flag = 1` | `cns_dual_flag = 1` | `total_oah_flag = 'OAH'` |
| Prep table | `kn_loc_mnr_agg_04152026` | `kn_loc_cns_agg_04152026` | `kn_loc_oah_agg_04152026` |
| Scored CSV | `loc_anomaly_mnr_202604.csv` | `loc_anomaly_cns_202604.csv` | `loc_anomaly_oah_202604.csv` |
| Narratives CSV | `loc_anomaly_mnr_narratives_202604.csv` | `loc_anomaly_cns_narratives_202604.csv` | `loc_anomaly_oah_narratives_202604.csv` |

Source table: `tmp_1m.kn_loc_notif_${notifications_date}_od`
All prep tables written to schema: `tmp_1m`

---

## Run Order

```
1. Run loc_agg.sql in Snowflake   (creates the 3 kn_loc_*_agg tables)
2. Run loc_anomaly.py             (reads those tables, outputs 6 CSVs)
```
