# Plan: Promote Reduced IF Model as Production

## Context

The `LOC_IF_SHAP_reduced.ipynb` notebook (6 features for 2025, 7 for 2026 — dropping `appeal_overturn` and `mcr_overturn`) will become the canonical production notebook. The sensitivity analysis confirmed these removed features don't drive the core anomaly signal (31/19 flags vs 30/19 in the full model).

## Changes

### 1. Create `Scripts/Archive/` subfolder

Move deprecated notebooks out of the working directory to reduce clutter while preserving audit trail.

### 2. Move deprecated notebooks

**Move to Archive/:**
- `LOC_IF_SHAP.ipynb` (original)
- `LOC_IF_SHAP_v1.ipynb` through `LOC_IF_SHAP_v5.ipynb`
- `LOC_IF_SHAP_fix.ipynb` + `(Knguy139 2nd copy)` + `(2)` duplicates
- `LOC_IF_SHAP_fix_v2.ipynb`
- `LOC_IF_SHAP_fix_v3.ipynb` (doesn't exist per glob — skip if absent)
- `LOC_IF_SHAP_fix_v4.ipynb`
- `LOC_IF_SHAP_fix_v4_sensitivity.ipynb`
- `LOC_IF_SHAP_fix_v5.ipynb`
- `LOC_IF_simple.ipynb`
- `LOC_IF_SHAP_v8.ipynb` (now reference only)
- `LOC_IF_SHAP_final.ipynb` (now reference only)
- `LOC_ML_expanded.ipynb`
- `loc_if_expanded.ipynb`
- `LOC_IF_shrinkage.ipynb`
- `LOC_IF_shrinkage_MnR.ipynb`
- `LOC_IF_SHAP_v5 (Knguy139 2nd copy).ipynb`

**Keep in Scripts/:**
- `LOC_IF_SHAP_reduced.ipynb` → rename to `LOC_IF_SHAP_production.ipynb`
- `LOC_IF_validation.ipynb`
- `LOC_IF_decision_log.md`
- `LOC_IF_README.md`
- `LOC_IF_KPI_joinback.sql`
- `LOC_ICM_validation.sql`
- All non-IF scripts (loc_*.py, OD_P2P_*.sql, Leading Indicator*.sql, etc.)

### 3. Rename production notebook

```
LOC_IF_SHAP_reduced.ipynb → LOC_IF_SHAP_production.ipynb
```

### 4. Update LOC_IF_README.md

Key changes:
- Production notebook: `LOC_IF_SHAP_production.ipynb`
- Features: 6 (2025) / 7 (2026) — removes `appeal_overturn_adj_rate` and `mcr_overturn_adj_rate`
- k estimation: All pooled (no per-year k needed — divergence was only in the removed features)
- Rate table: Remove `appeal_overturn_rate` and `mcr_overturn_rate` rows
- Output schema: Remove `appeal_ovrtn_cnt`, `mcr_ovrtn_cnt`, `appeal_overturn_rate`, `mcr_overturn_rate`, `appeal_overturn_adj_rate`, `mcr_overturn_adj_rate`
- Notebook inventory: Update to reflect archive
- Version history: Add entry for promotion of reduced model
- Cumulative fixes: Add fix #14 (dropped low-signal features)

### 5. Delete `CHANGELOG.md`

Empty file with just a header — no information.

### 6. Update project memory

Sync `/memories/projects/.../isolation_forest.md` to reflect new production state.

## Not Changing

- `_CLAUDE_ARCHIVE.md` — covers the LOC Valuation pipeline, not IF. Still useful.
- `analyses.md` — planned backlog, independent of IF.
- `LOC_IF_decision_log.md` — kept for narrative reference.
- Non-IF scripts in `Scripts/` — untouched.
