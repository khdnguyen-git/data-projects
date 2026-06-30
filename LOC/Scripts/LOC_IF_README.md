# LOC Isolation Forest + SHAP — Hospital Outlier Detection

## Overview

**Objective:** Identify hospital-market combinations with anomalous utilization review patterns that may warrant Level of Care (LOC) clinical intervention.

**Approach:** Empirical Bayes shrinkage → Isolation Forest → SHAP attribution

**Production notebook:** `LOC_IF_SHAP_production.ipynb` (reduced feature set — drops `appeal_overturn` and `mcr_overturn`)

### Pipeline

```
Raw Counts → Rate Computation → Per-Feature MoM k (pooled) → Bayes Shrinkage
→ Separate IF per Year (2025: 6feat, 2026: 7feat) → Bootstrap Stability (200×) → SHAP Attribution → Output
```

---

## Input

| Source Table | Schema | Description |
|---|---|---|
| `ec_avtar_25_26_3_od` | `TMP_1M` | AVTAR case-level data (2025–2026), pre-joined with LOC flags |
| `tin_collection` | `TMP_1Y` | TIN-to-hospital-group crosswalk (maps FA provider TIN to `collection`) |

### Filters Applied in SQL

- `fin_brand = 'M&R'` (Medicare & Retirement only)
- LOC-eligible IP types: Medical, Surgical, Transplant (with valid admit date), LTAC, SNF, AIR
- `loc_flag = 1`
- Exclusion logic for global cap / SGR source / product level
- `HAVING count(distinct case_id) >= 30` (minimum volume threshold)
- **2025**: `hce_admit_month BETWEEN '202501' AND '202512'`
- **2026**: `hce_admit_month BETWEEN '202601' AND '202604'`

### Eligibility (Python-side)

After SQL pull, an additional filter is applied:
- `case_count >= 30` AND `initial_adr_cnt >= 10`

This ensures the shrinkage estimator has enough denominator mass.

---

## Pipeline Steps

### 1. Data Pull

Aggregates case-level AVTAR data to **hospital_group × market** grain:
- **Denominators:** `case_count` (total eligible LOC cases), `initial_adr_cnt` (cases with initial ADR)
- **Numerators:** persistent ADR, P2P overturn, appeal overturn, MCR overturn, MD review, member appeal (2026 only)
- **Filters:** M&R brand, LOC-eligible IP types, COSMOS FFS/NICE physician, ≥30 cases per hospital-market
- **Grain:** One row = one hospital system × one market × one year

### 2. Empirical Bayes Shrinkage

**Problem:** Small hospitals have noisy rates — a 3/10 ADR rate (30%) shouldn't be treated the same as 300/1000 (30%).

**Solution:** Shrink each hospital's rate toward the population mean, proportional to its sample size.

```
p_adj = (successes + k · p_global) / (trials + k)
```

Where **k** (prior pseudo-sample-size) is estimated per-feature via Method-of-Moments Beta-Binomial:
- Decompose observed rate variance into **sampling noise** + **true between-hospital signal**
- High signal variance → small k → less shrinkage (hospitals genuinely differ)
- Low signal variance → large k → more shrinkage (differences are mostly noise)
- Uses **harmonic mean** of denominators in the sampling variance estimate (corrects bias from right-skewed hospital volumes)
- Global rates (shrinkage targets) computed from the full ≥30-case population *before* the ≥10 ADR filter (removes survivorship bias)

| Rate | Numerator | Denominator |
|---|---|---|
| `initial_adr_rate` | Initial full ADR cases | Total cases |
| `persistent_adr_rate` | Persistent full ADR cases | Total cases |
| `p2p_overturn_rate` | P2P full overturns | Initial ADR count |
| `md_review_rate` | MD reviewed cases | Total cases |
| `member_appeal_rate` | Member appeals | Initial ADR count (2026 only) |

*Removed in production:* `appeal_overturn_rate` and `mcr_overturn_rate` — low base rates with problematic k divergence; confirmed via sensitivity analysis to not drive core anomaly signal.

### 3. Per-Feature k Estimation

Pool both years to maximize sample for the hyperprior. Each rate feature gets its own optimal k:
- Features where hospitals genuinely differ (high v_signal) → small k → preserve individuality
- Features where hospitals are homogeneous (low v_signal) → large k → shrink aggressively

Fallback: k=50 if signal variance ≤ 0 or parameters degenerate (no detectable between-hospital variation).

**k pooling:** All features use pooled k (no per-year override needed — the divergent features `mcr_overturn` and `appeal_overturn` were removed).

**MoM Beta-Binomial procedure:**
1. Compute observed variance of raw rates across hospitals
2. Subtract expected sampling variance: `p̄(1-p̄) / n̄_harmonic` (harmonic mean handles skewed volumes)
3. The residual is true between-hospital signal variance
4. Back-solve for α, β from signal variance and mean rate
5. `k_optimal = α + β`

**Log-volume features**: `log_case_count = log1p(case_count)`, `log_initial_adr_cnt = log1p(initial_adr_cnt)` — compresses the volume dimension so high-volume hospitals don't dominate splits purely on size.

### 4. Isolation Forest (Separate Per-Year Models)

**Why IF?** Unsupervised anomaly detection that handles multivariate outliers without distributional assumptions. Tree-based → scale-invariant, handles correlations naturally.

**Architecture (separate models):**
- **2025 model**: 6 features (4 shrinkage-adjusted rates + 2 log-volume). Excludes `member_appeal_adj_rate` (unavailable for 2025 data).
- **2026 model**: 7 features (5 shrinkage-adjusted rates + 2 log-volume). Includes `member_appeal_adj_rate` with observed data.
- Models are fit independently — each year's anomalies are relative to that year's own distribution.

**Features:**
- `log_case_count`, `log_initial_adr_cnt`
- `initial_adr_adj_rate`, `persistent_adr_adj_rate`, `p2p_overturn_adj_rate`, `md_review_adj_rate`
- `member_appeal_adj_rate` (2026 only)

**Hyperparameters:** `n_estimators=300`, `contamination='auto'`

**Threshold:** Fixed 2.5th percentile of IF decision scores (not contamination-driven). Applied per-year model.

**Note:** `contamination='auto'` sets sklearn's internal offset but is irrelevant here since we apply our own percentile cutoff.

### 5. Bootstrap Stability

**Purpose:** Quantify how robust the flagged set is to sampling variation. A hospital flagged in 95% of bootstraps is a strong signal; one flagged in 30% is borderline.

**Method:** Per-year: resample hospitals 200× with replacement → refit IF → score *all* hospitals in that year → apply 2.5th pct threshold → record flag frequency.

**Consensus threshold:** `bootstrap_flag_pct >= 0.50` (majority vote).

**Calibration:** The threshold was empirically tested via peak/valley detection on the smoothed bootstrap distribution (see v8 notebook, "Bootstrap Distribution Plots" cell). Both 2025 and 2026 distributions are **unimodal** — no natural gap separating stable outliers from borderline ones. When there's no bimodal separation, the most defensible default is majority vote (0.50): "flagged in more bootstrap runs than not."

**Last-run results (production):**

| Year | Hospital-markets | Consensus flagged |
|---|---|---|
| 2025 | 1226 | 31 |
| 2026 | 790 | 19 |

**Interpretation:**
- `bootstrap_flag_pct ≥ 0.80` → stable outlier, high confidence
- `0.50 ≤ bootstrap_flag_pct < 0.80` → included in flagged set, borderline confidence scales with pct
- `bootstrap_flag_pct < 0.50` → excluded (flagged in minority of runs)

### 6. SHAP Attribution

**Purpose:** Explain *why* each hospital was flagged — which features drove the anomaly score.

**Method:** TreeExplainer on the fitted IF model → per-hospital, per-feature SHAP values.

**Key outputs:**
- `shap_driver`: The feature with the most negative SHAP value (strongest push toward anomaly)
- `driver_direction`: Whether the driver value is above/below population median (HIGH vs LOW)
- Summary plots: Feature importance by year
- Waterfall plots: Per-hospital SHAP decomposition

---

## Output

### Snowflake Table (STALE — needs refresh)

**Table**: `TMP_1M.KN_LOC_IF_OUTLIER_HOSPITALS_MNR_2025_2026`

**Status:** STALE — needs refresh from `LOC_IF_SHAP_production.ipynb`. Current table predates bootstrap consensus and uses the full 8/9-feature model.

**Target schema (after refresh):**

| Column | Description |
|---|---|
| `hospital_group` | Hospital collection name |
| `fin_market` | Financial market |
| `year` | `"2025"` or `"2026"` |
| `case_count`, `initial_adr_cnt`, `persistent_adr_cnt`, `p2p_ovrtn_cnt`, `md_reviewed_cnt` | Raw counts |
| `member_appeal_cnt` | Raw count (2026 only) |
| `initial_adr_rate`, `persistent_adr_rate`, `p2p_overturn_rate`, `md_review_rate`, `member_appeal_rate` | Raw rates |
| `log_case_count`, `log_initial_adr_cnt` | Log-volume features |
| `*_adj_rate` columns | Shrinkage-adjusted rates (model inputs) |
| `anomaly_score` | IF decision function score (lower = more anomalous) |
| `anomaly_rank` | Dense rank by anomaly_score ascending |
| `percentile_rank` | Percentile rank (0 = most anomalous) |
| `anomaly_flag` | Boolean — TRUE if below 2.5th percentile threshold |
| `bootstrap_flag_pct` | Fraction of 200 bootstrap resamples flagged |
| `consensus_flag` | Boolean — TRUE if `bootstrap_flag_pct >= 0.50` |
| `driver` | Feature name most responsible for flag (human-readable) |
| `direction` | `"high"` or `"low"` relative to median |
| `reason` | `driver (direction)` concatenated |
| `driver_value` | Hospital's value on driver feature |
| `driver_adj_median` | Population median (adj rate space) |
| `driver_dev_pct` | `(value - median) / median` |

---

## Design Rationale

### Why Isolation Forest?

- **Unsupervised**: No labeled "outlier" ground truth exists for hospitals
- **Multivariate**: Captures hospitals that are unusual across the *combination* of rates (not just one extreme metric)
- **Non-parametric**: No distributional assumptions about rates
- **Interpretable via SHAP**: TreeExplainer provides exact per-feature attribution for each hospital's anomaly score

### Why Empirical Bayes Shrinkage?

A hospital with 31 cases and 30 ADRs has a 97% initial ADR rate — but this is almost certainly noise from small sample size. Shrinkage pulls this toward the population mean proportionally to the sample size, so:
- Large hospitals: shrinkage ≈ 0 (raw rate dominates)
- Small hospitals: shrinkage → global rate (noise damped)

Per-feature k resolves the fixed-k limitation by calibrating shrinkage to each rate's actual signal-to-noise ratio.

### Why Separate Per-Year Models? (v7 decision)

v4 pooled 2025+2026 into one IF and imputed `member_appeal` for 2025 at the 2026 global mean. The v5 sensitivity notebook tested 3 approaches:
- Baseline: 9 feat pooled, member_appeal imputed for 2025
- Option 1: 8 feat pooled, member_appeal dropped entirely
- Option 3: Separate models (2025=8feat, 2026=9feat)

**Sensitivity results (v5):**
- Jaccard vs Baseline: Option 1 = 0.814/0.412, Option 3 = 0.692/0.636
- All options produced materially different flag sets (no Jaccard > 0.85)
- Option 1 shifted flags heavily toward 2025 (+23%) and away from 2026 (-50%)
- Option 3 produced the most balanced distribution across years

**Multi-seed stability (10 seeds, pairwise Jaccard):**

| Approach | 2025 | 2026 |
|---|---|---|
| Baseline (pooled 9feat) | 0.754 ± 0.091 | 0.862 ± 0.065 |
| Option 3 (separate) | 0.737 ± 0.071 | 0.862 ± 0.065 |

**Rationale for Option 3:**
- Avoids the imputation artifact (constant member_appeal for 2025 creates zero-variance dimension that distorts IF geometry)
- Each year uses its natural feature space — 2026 gets full 9-feat, 2025 uses 8-feat without data fabrication
- Equivalent stability to pooled approach; downstream bootstrap consensus is the true robustness mechanism
- Cleaner methodology that holds up to scrutiny (no "we made up a feature value" caveat)

---

## Notebook Inventory

| Notebook | Status | Purpose |
|---|---|---|
| `LOC_IF_SHAP_production.ipynb` | **Current production** | Reduced 6/7 features, pooled k, bootstrap consensus, SHAP attribution |
| `LOC_IF_validation.ipynb` | Validation artifact | 5 structured tests: rate instability, MoM k divergence, shrinkage sanity, bootstrap stability, separate-model sensitivity |
| `LOC_IF_decision_log.md` | Reference | Prose decision log: 9 decision points |
| `LOC_IF_README.md` | Reference | This document |
| `LOC_IF_KPI_joinback.sql` | Utility | Join IF flags back to KPI table |
| `Archive/` (22 notebooks) | Deprecated | Prior iterations (v1–v8, fix_v2–fix_v5, final, shrinkage explorations) |

---

## Version History

| Version | Notebook | Key Change |
|---|---|---|
| v1 | `LOC_IF_shap.ipynb` | Original prototype |
| v2 | `LOC_IF_SHAP_v2.ipynb` | Added shrinkage (fixed k=50) |
| v3 | `LOC_IF_SHAP_v3.ipynb` | Two-year comparison |
| v4 | `LOC_IF_SHAP_v4_2026.ipynb` | 2026-only experiment |
| v5 | `LOC_IF_SHAP_v5.ipynb` | Per-year models, StandardScaler, 2σ threshold |
| fix_v2 | `LOC_IF_SHAP_fix_v2.ipynb` | Pooled model, no scaler, percentile threshold (fixes #1–7) |
| fix_v3 | `LOC_IF_SHAP_fix_v3.ipynb` | Per-feature optimal k via MoM Beta-Binomial |
| fix_v4 | `LOC_IF_SHAP_fix_v4.ipynb` | Harmonic-mean MoM, bootstrap stability, survivorship-bias-free targets (fixes #8–10) |
| fix_v4_sens | `LOC_IF_SHAP_fix_v4_sensitivity.ipynb` | Option comparison (baseline vs opt1 vs opt3) |
| fix_v5 | `LOC_IF_SHAP_fix_v5.ipynb` | Simplified sensitivity (3-way + multi-seed stability) |
| v7/v8 | `Archive/LOC_IF_SHAP_v8.ipynb` | Separate per-year models, majority-vote consensus (0.50), 8/9 features |
| final | `Archive/LOC_IF_SHAP_final.ipynb` | Cleaner API with tuple-returning `mom_optimal_k` |
| **production** | **`LOC_IF_SHAP_production.ipynb`** | **Current** — reduced to 6/7 features (drops appeal/mcr overturn), all k pooled |
| validation | `LOC_IF_validation.ipynb` | All diagnostic tests consolidated |

---

## Cumulative Fixes (all incorporated into v7/v8)

| # | Fix | Version Introduced | Problem Solved |
|---|---|---|---|
| 1 | Removed StandardScaler | fix_v2 | IF is tree-based, scaling unnecessary; distorted SHAP interpretation |
| 2 | Percentile-based threshold (2.5th pct) | fix_v2 | Replaced contamination=0.05 and 2σ (non-Gaussian scores) |
| 3 | SHAP in original feature space | fix_v2 | Consequence of #1 — values now in rate units |
| 4 | Single pooled model | fix_v2 | Cross-year score comparability (superseded by v7 separate models) |
| 5 | `driver_dev` in raw count space | fix_v2 | Log-space deviation unintuitive for stakeholders |
| 6 | SQL HAVING clauses aligned | fix_v2 | Asymmetric eligibility between years |
| 7 | 2026 month range bounded | fix_v2 | Prevented pulling incomplete future months |
| 8 | Harmonic mean in MoM k estimator | fix_v4 | Arithmetic mean underestimates sampling variance with skewed volumes |
| 9 | Global rates before ≥10 ADR filter | fix_v4 | Survivorship bias in shrinkage targets |
| 10 | Bootstrap stability (200×) | fix_v4 | No stability assessment despite per-feature k |
| 11 | Separate per-year models | v7/v8 | Imputation artifact; pooling doesn't improve stability |
| 12 | Majority-vote consensus (0.50) | v7/v8 | Data-driven calibration — unimodal distribution, no natural cutpoint |
| 13 | Per-year k for divergent features | v7/v8 | `mcr_overturn` and `appeal_overturn` have >30% k divergence between years |
| 14 | Dropped low-signal features | production | Removed `appeal_overturn` and `mcr_overturn` — confirmed via sensitivity to not drive core signal; eliminates per-year k complexity |

---

## Reduced Feature Set (Production)

Drops `appeal_overturn_adj_rate` and `mcr_overturn_adj_rate` — features with problematic k divergence and low base rates.

**Rationale:** Sensitivity analysis confirmed 31/19 flags (reduced) vs 30/19 (full model) — these features are not driving the core anomaly signal. Removing them eliminates the per-year k complexity and produces a cleaner, more defensible model.

**Output filtering:** Surfaces hospitals driven by `md_review`, `persistent_adr`, `initial_adr`, `p2p_overturn`, or `member_appeal`.

**All k pooled:** Without the divergent features, no per-year k overrides are needed.
