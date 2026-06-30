# LOC IF — Decision Log

How we arrived at the v7 methodology. Each section is a decision point, what we tried, what the evidence showed, and what we chose.

---

## 1. Anomaly Threshold: contamination vs percentile vs 2σ

**Problem:** How do we decide which hospitals are "outliers"?

**Tried:**
- `contamination = 0.05` (sklearn default) — forces exactly 5% flagged regardless of actual distribution
- 2σ from mean score — assumes Gaussian scores (they're not; IF scores are bounded and skewed)
- Fixed percentile (2.5th) of decision scores — controls screening rate by design

**Evidence:** IF decision scores are not Gaussian. The 2σ approach flagged inconsistent counts depending on score skew. Contamination forces a fixed fraction which doesn't reflect reality — some years may have more/fewer genuine outliers.

**Decision:** 2.5th percentile. Interpretable ("bottom 2.5% of scores"), consistent across years, doesn't assume distributional form. Applied in fix_v2.

---

## 2. Feature Scaling: StandardScaler vs raw

**Problem:** Should we normalize features before IF?

**Tried:**
- StandardScaler → IF → SHAP (v5 approach)
- Raw features → IF → SHAP (fix_v2 approach)

**Evidence:** IF is tree-based — splits are rank-based, so scaling cannot change the model's decisions. But scaling *does* affect SHAP interpretation: SHAP values in scaled space are unintuitive (a "1 standard deviation" shift in appeal_overturn_rate means nothing to clinicians). Removing the scaler gave identical IF results with interpretable SHAP in rate units.

**Decision:** No scaler. Fix #1 (fix_v2). Trees don't need it; SHAP is cleaner without it.

---

## 3. Shrinkage: none vs fixed k vs per-feature k

**Problem:** Small hospitals have noisy rates that dominate IF splits.

**Tried:**
- No shrinkage (v1) — 3/10 = 30% treated same as 300/1000 = 30%
- Fixed k = 50 for all features (v2) — uniform prior strength
- Per-feature MoM Beta-Binomial k (fix_v3) — each rate gets its own empirically estimated prior

**Evidence:** Fixed k = 50 over-shrinks features with high true between-hospital variance (e.g., initial ADR, k_optimal ≈ 8) and under-shrinks features with low variance (e.g., mcr overturn, k_optimal ≈ 150+). Per-feature k correctly adapts: features where hospitals genuinely differ get less shrinkage; features where they're homogeneous get more.

**Decision:** Per-feature k via MoM. Fix_v3, refined in fix_v4.

---

## 4. MoM Estimator: arithmetic vs harmonic mean

**Problem:** The sampling variance formula uses `n̄` (average denominator). Which average?

**Tried:**
- Arithmetic mean of denominators
- Harmonic mean of denominators

**Evidence:** Hospital volumes are right-skewed (few large systems, many small ones). Arithmetic mean is dominated by large hospitals → underestimates typical sampling noise → overestimates signal variance → underestimates k → under-shrinks. Harmonic mean correctly weights toward smaller hospitals where noise actually lives.

**Decision:** Harmonic mean. Fix #8 (fix_v4).

---

## 5. Shrinkage Targets: filtered vs full population

**Problem:** We filter to `initial_adr_cnt >= 10` for IF input. Should global rates (shrinkage targets) use the same filtered subset?

**Tried:**
- Global rates from ≥10 ADR subset only
- Global rates from full ≥30 case population

**Evidence:** The ≥10 filter is for ensuring stable rate denominators *for the hospital being adjusted*. But the shrinkage target (population mean) should reflect the full population — otherwise you're shrinking toward the mean of "hospitals that already have enough ADR volume," which biases upward. Survivorship bias.

**Decision:** Global rates from full ≥30 population. Fix #9 (fix_v4).

---

## 6. Model Stability: single run vs bootstrap consensus

**Problem:** IF is stochastic — different random seeds give different flags. How do we know which hospitals are reliably anomalous?

**Tried:**
- Single seed (random_state = 42) — one realization
- 200× bootstrap with consensus voting

**Evidence:** Single-seed pairwise Jaccard across 10 seeds: ~0.74 for 2025, ~0.86 for 2026. This means ~25% of 2025 flags are seed-dependent noise. Bootstrap consensus (flag only if flagged in majority of runs) filters out these unstable flags.

**Decision:** 200× bootstrap, flag if ≥50% consensus. Fix #10 (fix_v4), threshold calibrated in v7.

---

## 7. Consensus Threshold: 0.80 vs 0.60 vs 0.50

**Problem:** What fraction of bootstrap runs must flag a hospital for it to make the final list?

**Tried:**
- 0.80 (v4 original) — "stable outlier" only
- 0.60 (v7 initial) — calibrated to stability floor
- 0.50 (v7 final) — majority vote

**Evidence:** Peak/valley detection on the smoothed bootstrap distribution: both years are **unimodal** — no natural gap separating stable from unstable flags. When there's no bimodal separation, any threshold is arbitrary. Majority vote (0.50 = "flagged more often than not") is the most defensible default.

Sensitivity table:

| Threshold | 2025 flagged | 2026 flagged |
|---|---|---|
| 0.50 | 33 | 21 |
| 0.60 | 29 | 20 |
| 0.70 | 26 | 20 |
| 0.80 | 22 | 17 |

2026 is stable (20–21) across thresholds; sensitivity is in 2025's borderline hospitals.

**Decision:** 0.50 (majority vote). Clinical review is the downstream filter for borderline cases.

---

## 8. Year Architecture: pooled vs separate models

**Problem:** 2025 doesn't have `member_appeal` data. How do we handle the missing feature?

**Tried:**
- Pooled 9-feat model, impute member_appeal for 2025 at 2026 global mean (v4 baseline)
- Pooled 8-feat model, drop member_appeal entirely (Option 1)
- Separate models: 2025 = 8-feat, 2026 = 9-feat (Option 3)

**Evidence (v5 sensitivity):**

| Option | 2025 flagged | 2026 flagged | Jaccard vs Baseline (25/26) |
|---|---|---|---|
| Baseline (imputed) | 35 | 16 | 1.0 / 1.0 |
| Option 1 (8feat pooled) | 43 | 8 | 0.814 / 0.412 |
| Option 3 (separate) | 31 | 20 | 0.692 / 0.636 |

Multi-seed stability:

| Approach | 2025 Jaccard | 2026 Jaccard |
|---|---|---|
| Baseline (pooled) | 0.754 ± 0.091 | 0.862 ± 0.065 |
| Option 3 (separate) | 0.737 ± 0.071 | 0.862 ± 0.065 |

Key findings:
- Imputation creates zero-variance dimension that distorts IF geometry (all 2025 rows identical on member_appeal)
- Pooling doesn't improve 2025 stability (0.754 vs 0.737 = within noise)
- Option 1 dramatically shifts flags between years (2026 drops 50%)
- Option 3 gives most balanced distribution and avoids imputation artifact

**Decision:** Separate per-year models. v7.

---

## 9. k Pooling: all-pooled vs per-year for divergent features

**Problem:** We pool both years for k estimation (more data = more stable hyperparameter). But does this assumption hold for all features?

**Tried:**
- All features use pooled k
- Compare per-year k estimates to pooled

**Evidence:**

| Feature | k_2025 | k_2026 | k_pooled | Divergence |
|---|---|---|---|---|
| initial_adr | 8.3 | 8.5 | 8.3 | 2.2% |
| persistent_adr | 15.0 | 15.3 | 15.1 | 2.3% |
| p2p_overturn | 8.3 | 9.5 | 8.8 | 13.5% |
| appeal_overturn | 1.3 | 0.7 | 1.1 | 57.5% |
| mcr_overturn | 150.4 | 50.0 | 204.8 | 49.0% |
| md_review | 2.1 | 1.9 | 2.0 | 7.2% |

- `appeal_overturn`: 57.5% divergence sounds bad, but k=1.3 vs 0.7 are both near-zero → shrinkage ≈ 0 for everyone regardless. Practical impact negligible.
- `mcr_overturn`: k=150 vs k=50 is meaningful — 150 means "shrink aggressively" while 50 means "moderate shrinkage." Pooled (204.8) overshoots both, likely because pooling introduces apparent homogeneity. The k=50 in 2026 may reflect MoM degeneration (rare event) or genuine policy-driven variation.

**Decision:** Per-year k for `mcr_overturn` only. All other features stay pooled. v7.

---

## Summary: v7 Method

```
Data Pull (hospital_group × market, ≥30 cases)
  → Per-feature k estimation (pooled except mcr_overturn = per-year)
  → Empirical Bayes shrinkage (harmonic-mean MoM, global rates from full ≥30 pop)
  → Separate Isolation Forest per year (2025: 8 feat, 2026: 9 feat)
  → 200× Bootstrap stability (flag if ≥50% consensus)
  → SHAP TreeExplainer per-year model
  → Output table
```
