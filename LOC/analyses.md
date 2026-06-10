# LOC Exploratory Analyses

## Data Constraints

- **Time range**: 202401–present (exclude 202403–202404: prior auth off)
- **Member appeal**: Only available from 202601 onward
- **Data maturity**: Recent ~3-4 months have incomplete overturn outcomes; acceptable for member appeal filing analysis
- **Primary source**: `tmp_1m.ec_avtar_25_26_3_od` (43M rows, 201 columns, case-level)
- **Aggregated source**: `tmp_1m.kn_loc_mnr_agg_*`, `kn_loc_cns_agg_*`, `kn_loc_oah_agg_*`
- **Population**: M&R FFS unless otherwise noted

---

## Cluster 1: Persistency Drivers

### 1. Persistency × Diagnosis

| | |
|---|---|
| **Question** | Which AHRQ diagnosis categories have unusually low/high persistency? |
| **Method** | Logistic regression + chi-squared; rate tables by `PRIM_DIAG_AHRQ_GENL_CATGY_DESC` |
| **Data** | Raw AVTAR, filter to `INITIALFULLADR_CASES = 1`; outcome = `PERSISTENTFULLADR_CASES` |
| **Output** | Rate table (diagnosis × persistency rate), chi-squared p-values, logistic model coefficients |
| **Status** | Not started |

### 2. Case-Level Persistency Model

| | |
|---|---|
| **Question** | What case features predict whether a denial sticks? |
| **Method** | XGBoost classification + SHAP feature importance |
| **Data** | Raw AVTAR; outcome = `PERSISTENTFULLADR_CASES`; features = `PRIM_DIAG_AHRQ_GENL_CATGY_DESC`, `LOS`, `IP_TYPE`, `CHANNEL_CD`, `AUTH_TYP_CD`, `FA_PROV_PAR_STATUS_IND`, `FIN_MARKET`, `ADMIT_CAT_CD`, `SVC_SETTING` |
| **Output** | SHAP summary plot, feature importance ranking, AUC-ROC, partial dependence plots for top features |
| **Status** | Not started |

---

## Cluster 2: Member Appeal Deep-Dive

### 5a. Member Appeal Overturn Predictors

| | |
|---|---|
| **Question** | What makes member appeals succeed (get overturned)? |
| **Method** | Logistic regression; outcome = `MEMBER_APPEAL_OVTN_IND` among cases where `MEMBER_APPEAL_IND = 1` |
| **Data** | Raw AVTAR, 202601+; features = diagnosis, LOS, hospital, market, par status, whether P2P was attempted |
| **Output** | Logistic coefficients + odds ratios, model performance metrics |
| **Status** | Not started |

### 5b. P2P → Member Appeal Channel Migration

| | |
|---|---|
| **Question** | Is volume shifting from P2P to member appeal? |
| **Method** | Monthly share-of-pathway: among cases with initial ADR, what % go P2P vs. member appeal? Compare pre-202601 (P2P only) to post-202601 (both available) |
| **Data** | Raw AVTAR; `P2P_FULL_EVERTOUCHED_CNT`, `MEMBER_APPEAL_IND` by `HCE_ADMIT_MONTH` |
| **Constraint** | Only ~5 months of member appeal data; trend is limited; primarily a pre/post comparison |
| **Output** | Monthly share chart; volume decomposition (rate × volume) |
| **Status** | Not started |

### 5c. Who Shifted? (Entity-Level Detection)

| | |
|---|---|
| **Question** | Which hospitals/markets have highest member appeal rates? |
| **Method** | Rank entities by member_appeal_rate (member appeals / initial ADRs); compare to P2P rate |
| **Data** | Raw AVTAR, 202601+; group by `hospital_group` (via TIN collection join) and `FIN_MARKET` |
| **Output** | Ranked table of entities by member appeal share; scatter plot of P2P rate vs. member appeal rate |
| **Status** | Not started |

### 5d. Pathway Choice Model

| | |
|---|---|
| **Question** | What case characteristics predict member appeal vs. P2P pathway? |
| **Method** | Logistic regression; outcome = chose member appeal (vs. P2P) among cases with initial ADR that entered at least one pathway |
| **Data** | Raw AVTAR, 202601+; features = diagnosis, LOS, hospital, market, par status, ip_type |
| **Output** | Logistic coefficients identifying what drives pathway choice |
| **Status** | Not started |

### 5e. Case Profile Comparison

| | |
|---|---|
| **Question** | Do member appeal cases look clinically different from P2P cases? |
| **Method** | Stratified comparison: diagnosis distribution, LOS distribution, par status, ip_type |
| **Data** | Raw AVTAR, 202601+; split into P2P-only vs. member-appeal-only vs. both |
| **Output** | Comparative tables + chi-squared tests for categorical differences; KS test for LOS |
| **Status** | Not started |

---

## Cluster 3: Provider Patterns

### 9. Hospital Behavioral Typing

| | |
|---|---|
| **Question** | What behavioral archetypes exist among hospitals? |
| **Method** | K-means clustering (k=3-6, silhouette-optimized) on standardized rate features |
| **Data** | `kn_loc_mnr_agg_*` hospital_group dimension; features = 14 rate columns |
| **Output** | Cluster assignments, cluster centers (archetype profiles), cluster stability across months |
| **Status** | Not started |

### 19. ADR Control Charts by Hospital

| | |
|---|---|
| **Question** | Which hospitals are statistically out of control (volume-adjusted)? |
| **Method** | Shewhart p-chart for ADR rate; CUSUM for trend detection. Accounts for volume (small hospitals have wider natural variation) |
| **Data** | Raw AVTAR or `kn_loc_notif`, monthly grain by hospital_group; need ≥30 cases/month |
| **Output** | Control chart per hospital (flagged as in-control / out-of-control); list of breaching entities |
| **Status** | Not started |

---

## Cluster 4: Drivers of Change

### 16. Rate Change Decomposition (Mix vs. Behavior)

| | |
|---|---|
| **Question** | Is rate movement compositional (case mix shifted) or behavioral (same cases, different decisions)? |
| **Method** | Kitagawa decomposition: Δ_total = Δ_composition + Δ_rate. Decompose by AHRQ category, market, or hospital |
| **Data** | Raw AVTAR; compare period 1 vs. period 2 (e.g., Q1 2025 vs. Q1 2026) |
| **Output** | Decomposition table: how much of rate change is mix vs. behavior; applicable to persistency, ADR rate, overturn rates |
| **Status** | Not started |

---

## Cluster 5: Prediction & Timing

### 18. Time-to-Overturn Survival Analysis

| | |
|---|---|
| **Question** | How fast do overturns happen? What accelerates/delays them? |
| **Method** | Kaplan-Meier curves (time from `INITIAL_DNL_DECN_DTTM` to overturn event); Cox PH regression with hospital, diagnosis, par status as covariates |
| **Data** | Raw AVTAR; cases with `INITIALFULLADR_CASES = 1`; event = any overturn; censored = still persistent at data pull |
| **Output** | Survival curves by hospital/market; Cox hazard ratios; median time-to-overturn |
| **Status** | Not started |

### 20. Case-Level Overturn Classifier

| | |
|---|---|
| **Question** | At point of initial ADR, how likely is the case to eventually get overturned (any pathway)? |
| **Method** | XGBoost + SHAP; binary outcome = eventually overturned via P2P, appeal, MCR, or member appeal |
| **Data** | Raw AVTAR; features available at time of initial ADR (diagnosis, LOS-to-date, hospital, par status, channel, admit type) |
| **Output** | Predicted overturn probability per case; SHAP importance; AUC-ROC; calibration curve |
| **Status** | Not started |
