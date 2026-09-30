import pickle
import numpy as np
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold

with open(r"C:\Users\knguy139\Documents\Projects\RPM\rpm_cadence_checkpoint.pkl", "rb") as f:
    ckpt = pickle.load(f)

df = ckpt["df"]
y = ckpt["y"]
rf_vendor_score = ckpt["rf_vendor_score"]

pos_idx = np.where(y == 1)[0]
unl_idx = np.where(y == 0)[0]
n_pos = len(pos_idx)

continuous_features = [
    "avg_member_age", "std_member_age", "pct_female", "pct_htn", "pct_dm", "pct_hf", "pct_invalid_dx",
    "avg_plan_tenure_months",
    "total_mbrs", "months_active", "avg_rpm_tenure_months", "single_month_churn_rate", "peak_mom_growth_pct",
    "pct_mgmt_lines", "pct_device_lines", "pct_setup_lines", "pct_99458", "proc_hhi",
    "avg_allowed_per_line", "avg_lines_per_mbr", "top_dom_pct", "weekend_pct", "avg_submission_lag",
    "distinct_srvc_npis", "mbrs_per_srvc_npi", "pct_srvc_ne_bil",
    "pct_no_prior_rel", "pct_no_treatment_mgmt", "pct_multi_practice_overlap", "max_jaccard", "pct_shared_npis",
]
binary_features = [
    "sig_ramp", "sig_no_prior", "sig_no_treat", "sig_mbr_multi_tin",
    "sig_billing_conc", "sig_churn", "sig_tin_network",
]
skew_features = ["total_mbrs", "peak_mom_growth_pct", "distinct_srvc_npis",
                 "mbrs_per_srvc_npi", "avg_lines_per_mbr", "avg_submission_lag"]

# known from original run
holdout_recalls_rf = [0.583, 0.750, 0.792, 0.708, 0.625]

# --- ablation ---
drop_features = [
    "sig_ramp", "sig_no_prior", "sig_no_treat", "sig_mbr_multi_tin",
    "sig_billing_conc", "sig_churn", "sig_tin_network",
    "max_jaccard", "pct_shared_npis", "pct_multi_practice_overlap",
]

abl_continuous = [f for f in continuous_features if f not in drop_features]
abl_features = abl_continuous + [f for f in binary_features if f not in drop_features]
print(f"Ablation features: {len(abl_features)} ({len(abl_continuous)} continuous + {len(abl_features) - len(abl_continuous)} binary)")

# prep
skew_to_transform_abl = [f for f in skew_features if f in abl_continuous]
X_abl_raw = df[abl_features].copy()
for f in skew_to_transform_abl:
    X_abl_raw[f] = np.log1p(X_abl_raw[f].clip(lower = 0))
scaler_abl = StandardScaler()
X_abl_raw[abl_continuous] = scaler_abl.fit_transform(X_abl_raw[abl_continuous])
X_abl = X_abl_raw.values
print(f"X_abl shape: {X_abl.shape}")

# PU bagging
K_abl = 100
np.random.seed(42)
abl_rf_scores = np.zeros((K_abl, len(y)))

for k in range(K_abl):
    neg_sample = np.random.choice(unl_idx, size = n_pos, replace = False)
    train_idx = np.concatenate([pos_idx, neg_sample])
    X_train = X_abl[train_idx]
    y_train = np.concatenate([np.ones(n_pos), np.zeros(n_pos)])
    rf = RandomForestClassifier(n_estimators = 200, max_depth = 6, min_samples_leaf = 5, random_state = k)
    rf.fit(X_train, y_train)
    abl_rf_scores[k] = rf.predict_proba(X_abl)[:, 1]
    if (k + 1) % 25 == 0:
        print(f"  Bagging iteration {k + 1}/{K_abl}")

abl_vendor_score = abl_rf_scores.mean(axis = 0)
print(f"\nAblation RF scores: mean = {abl_vendor_score.mean():.4f}, std = {abl_vendor_score.std():.4f}")

# 5-fold CV
skf_abl = StratifiedKFold(n_splits = 5, shuffle = True, random_state = 42)
abl_holdout_recalls = []

for fold, (train_pos_rel, test_pos_rel) in enumerate(skf_abl.split(pos_idx, np.ones(n_pos))):
    train_pos = pos_idx[train_pos_rel]
    test_pos = pos_idx[test_pos_rel]
    n_train_pos = len(train_pos)
    fold_rf_scores = np.zeros((50, len(y)))

    for k in range(50):
        neg_sample = np.random.choice(unl_idx, size = n_train_pos, replace = False)
        train_idx = np.concatenate([train_pos, neg_sample])
        X_train = X_abl[train_idx]
        y_train = np.concatenate([np.ones(n_train_pos), np.zeros(n_train_pos)])
        rf = RandomForestClassifier(n_estimators = 200, max_depth = 6, min_samples_leaf = 5,
                                    random_state = fold * 100 + k)
        rf.fit(X_train, y_train)
        fold_rf_scores[k] = rf.predict_proba(X_abl)[:, 1]

    fold_avg = fold_rf_scores.mean(axis = 0)
    top_200 = np.argsort(-fold_avg)[:200]
    recall = len(set(test_pos) & set(top_200)) / len(test_pos)
    abl_holdout_recalls.append(recall)
    print(f"  Fold {fold + 1}: RF recall@200 = {recall:.3f}")

print(f"\nAblation RF mean recall@200: {np.mean(abl_holdout_recalls):.3f} +/- {np.std(abl_holdout_recalls):.3f}")

# comparison
print("\n=== Ablation vs Original (RF) ===\n")

print("Recall@200 (5-fold CV):")
print(f"  Original (38 features): {np.mean(holdout_recalls_rf):.3f} +/- {np.std(holdout_recalls_rf):.3f}")
print(f"  Ablation (28 features): {np.mean(abl_holdout_recalls):.3f} +/- {np.std(abl_holdout_recalls):.3f}")

print("\nPrecision@k:")
for k in [100, 200, 300, 500]:
    orig_top = np.argsort(-rf_vendor_score)[:k]
    abl_top = np.argsort(-abl_vendor_score)[:k]
    orig_prec = y[orig_top].sum() / k
    abl_prec = y[abl_top].sum() / k
    print(f"  Top {k}: Original = {orig_prec:.1%}, Ablation = {abl_prec:.1%}")

print("\nScore distribution:")
for label, scores in [("Original", rf_vendor_score), ("Ablation", abl_vendor_score)]:
    c_scores = scores[y == 1]
    nc_scores = scores[y == 0]
    print(f"  {label} Cadence:     median = {np.median(c_scores):.4f}, IQR = [{np.percentile(c_scores, 25):.4f}, {np.percentile(c_scores, 75):.4f}]")
    print(f"  {label} Non-Cadence: median = {np.median(nc_scores):.4f}, IQR = [{np.percentile(nc_scores, 25):.4f}, {np.percentile(nc_scores, 75):.4f}]")

rho, _ = stats.spearmanr(rf_vendor_score, abl_vendor_score)
print(f"\nSpearman correlation (original vs ablation scores): {rho:.4f}")

