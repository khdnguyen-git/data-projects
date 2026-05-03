# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
from scipy.spatial.distance import mahalanobis
from scipy.stats import chi2
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt
import seaborn as sns

# %%
engine = create_engine(URL(
    account = "UHG-UHGDWAAS",
    user = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database = "VING_PRD_TREND_DB",
    schema = "TMP_1M"
))

mnr = pd.read_sql("select * from tmp_1m.kn_loc_mnr_agg_202604", engine)

engine.dispose()
print(mnr.shape)

# %%
mnr["_dimension"].value_counts()

# %%
mnr[["adr_rate", "persistent_adr_rate", "appeal_overturn_rate",
     "member_appeal_rate", "auth_per_k"]].describe().round(3)

# %%
mnr_tin = (
    mnr
    .query("_dimension == 'prov_tin'")
    .sort_values("auth_per_k", ascending = False)
    .head(50)
    .reset_index(drop = True)
)

mnr_hosp = (
    mnr
    .query("_dimension == 'hospital_group'")
    .dropna(subset = ["_dim_value"])
    .sort_values("auth_per_k", ascending = False)
    .head(50)
    .reset_index(drop = True)
)

mnr_mkt = (
    mnr
    .query("_dimension == 'fin_market'")
    .sort_values("auth_per_k", ascending = False)
    .head(30)
    .reset_index(drop = True)
)

print(mnr_tin.shape, mnr_hosp.shape, mnr_mkt.shape)

# %%
util_features = ["auth_per_k", "adr_rate", "pre_auth_rate"]
pers_features = ["persistent_adr_rate", "appeal_overturn_rate",
                 "p2p_overturn_rate", "mcr_overturn_rate", "member_appeal_rate"]
all_features = util_features + pers_features


# %% [markdown]
# ## prov_tin

# %% mahalanobis — utilization
X = mnr_tin[util_features].dropna()
mu = X.mean().values
VI = np.linalg.pinv(np.cov(X.values.T))

dists = X.apply(lambda row: mahalanobis(row.values, mu, VI), axis = 1)
pvals = chi2.sf(dists ** 2, df = len(util_features))

mnr_tin["mahal_dist_util"] = dists
mnr_tin["chi2_pval_util"] = pvals
mnr_tin["mahal_flag_util"] = pvals < 0.05

mnr_tin["mahal_flag_util"].value_counts()

# %% mahalanobis — persistency
X = mnr_tin[pers_features].dropna()
mu = X.mean().values
VI = np.linalg.pinv(np.cov(X.values.T))

dists = X.apply(lambda row: mahalanobis(row.values, mu, VI), axis = 1)
pvals = chi2.sf(dists ** 2, df = len(pers_features))

mnr_tin["mahal_dist_pers"] = dists
mnr_tin["chi2_pval_pers"] = pvals
mnr_tin["mahal_flag_pers"] = pvals < 0.05

mnr_tin["mahal_flag_pers"].value_counts()

# %% isolation forest — utilization
X = mnr_tin[util_features].dropna()
X_scaled = StandardScaler().fit_transform(X)
iso = IsolationForest(n_estimators = 200, contamination = 0.05, random_state = 42)
iso.fit(X_scaled)

mnr_tin.loc[X.index, "if_score_util"] = iso.decision_function(X_scaled)
mnr_tin.loc[X.index, "if_flag_util"] = iso.predict(X_scaled) == -1

mnr_tin["if_flag_util"].value_counts()

# %% isolation forest — persistency
X = mnr_tin[pers_features].dropna()
X_scaled = StandardScaler().fit_transform(X)
iso = IsolationForest(n_estimators = 200, contamination = 0.05, random_state = 42)
iso.fit(X_scaled)

mnr_tin.loc[X.index, "if_score_pers"] = iso.decision_function(X_scaled)
mnr_tin.loc[X.index, "if_flag_pers"] = iso.predict(X_scaled) == -1

mnr_tin["if_flag_pers"].value_counts()

# %% z-scores + consensus + financial impact
for col in all_features:
    mnr_tin[f"z_{col}"] = (mnr_tin[col] - mnr_tin[col].mean()) / mnr_tin[col].std()

mnr_tin["consensus_util"] = mnr_tin["mahal_flag_util"] & mnr_tin["if_flag_util"]
mnr_tin["consensus_pers"] = mnr_tin["mahal_flag_pers"] & mnr_tin["if_flag_pers"]
mnr_tin["consensus_both"] = mnr_tin["consensus_util"] & mnr_tin["consensus_pers"]

mnr_tin["est_persistent_denials"] = mnr_tin["persistent_adr_rate"] * mnr_tin["case_count"]
mnr_tin["financial_impact"] = mnr_tin["est_persistent_denials"] * 8600  # 2025 rate; use 8300 for 2024, 8900 for 2026

print(mnr_tin["consensus_both"].sum(), "providers flagged by both methods on both feature groups")

view_cols = ["_dim_value", "case_count", "mahal_dist_pers", "chi2_pval_pers",
             "financial_impact", "consensus_pers", "consensus_both"]
mnr_tin.sort_values("mahal_dist_pers", ascending = False).head(10)[view_cols]

# %% heatmap
z_cols = [f"z_{c}" for c in all_features]

top20 = (
    mnr_tin
    .nlargest(20, "mahal_dist_pers")
    .set_index("_dim_value")[z_cols]
)
top20.columns = all_features

fig, ax = plt.subplots(figsize = (12, 8))
sns.heatmap(top20, cmap = "RdBu_r", center = 0, annot = True, fmt = ".1f",
            linewidths = 0.5, ax = ax)
ax.set_title("MNR prov_tin — Top 20 by Mahalanobis (Persistency): Z-scores")
plt.tight_layout()
plt.show()

# %% scatter — method agreement
colors = mnr_tin["consensus_pers"].map({True: "crimson", False: "steelblue"}).fillna("steelblue")
sizes = mnr_tin["case_count"] / mnr_tin["case_count"].max() * 180 + 20

fig, ax = plt.subplots(figsize = (8, 6))
ax.scatter(
    mnr_tin["mahal_dist_pers"],
    mnr_tin["if_score_pers"],
    c = colors,
    s = sizes,
    alpha = 0.7,
    edgecolors = "k",
    linewidths = 0.4
)
ax.axhline(0, color = "grey", linestyle = "--", linewidth = 0.8)
ax.set_xlabel("Mahalanobis distance (persistency)")
ax.set_ylabel("IF anomaly score — lower = more anomalous")
ax.set_title("MNR prov_tin — Mahalanobis vs Isolation Forest (persistency)")
plt.tight_layout()
plt.show()


# %% [markdown]
# ## hospital_group

# %% mahalanobis — utilization
X = mnr_hosp[util_features].dropna()
mu = X.mean().values
VI = np.linalg.pinv(np.cov(X.values.T))

dists = X.apply(lambda row: mahalanobis(row.values, mu, VI), axis = 1)
pvals = chi2.sf(dists ** 2, df = len(util_features))

mnr_hosp["mahal_dist_util"] = dists
mnr_hosp["chi2_pval_util"] = pvals
mnr_hosp["mahal_flag_util"] = pvals < 0.05

mnr_hosp["mahal_flag_util"].value_counts()

# %% mahalanobis — persistency
X = mnr_hosp[pers_features].dropna()
mu = X.mean().values
VI = np.linalg.pinv(np.cov(X.values.T))

dists = X.apply(lambda row: mahalanobis(row.values, mu, VI), axis = 1)
pvals = chi2.sf(dists ** 2, df = len(pers_features))

mnr_hosp["mahal_dist_pers"] = dists
mnr_hosp["chi2_pval_pers"] = pvals
mnr_hosp["mahal_flag_pers"] = pvals < 0.05

mnr_hosp["mahal_flag_pers"].value_counts()

# %% isolation forest — utilization
X = mnr_hosp[util_features].dropna()
X_scaled = StandardScaler().fit_transform(X)
iso = IsolationForest(n_estimators = 200, contamination = 0.05, random_state = 42)
iso.fit(X_scaled)

mnr_hosp.loc[X.index, "if_score_util"] = iso.decision_function(X_scaled)
mnr_hosp.loc[X.index, "if_flag_util"] = iso.predict(X_scaled) == -1

mnr_hosp["if_flag_util"].value_counts()

# %% isolation forest — persistency
X = mnr_hosp[pers_features].dropna()
X_scaled = StandardScaler().fit_transform(X)
iso = IsolationForest(n_estimators = 200, contamination = 0.05, random_state = 42)
iso.fit(X_scaled)

mnr_hosp.loc[X.index, "if_score_pers"] = iso.decision_function(X_scaled)
mnr_hosp.loc[X.index, "if_flag_pers"] = iso.predict(X_scaled) == -1

mnr_hosp["if_flag_pers"].value_counts()

# %% z-scores + consensus + financial impact
for col in all_features:
    mnr_hosp[f"z_{col}"] = (mnr_hosp[col] - mnr_hosp[col].mean()) / mnr_hosp[col].std()

mnr_hosp["consensus_util"] = mnr_hosp["mahal_flag_util"] & mnr_hosp["if_flag_util"]
mnr_hosp["consensus_pers"] = mnr_hosp["mahal_flag_pers"] & mnr_hosp["if_flag_pers"]
mnr_hosp["consensus_both"] = mnr_hosp["consensus_util"] & mnr_hosp["consensus_pers"]

mnr_hosp["est_persistent_denials"] = mnr_hosp["persistent_adr_rate"] * mnr_hosp["case_count"]
mnr_hosp["financial_impact"] = mnr_hosp["est_persistent_denials"] * 8600  # 2025 rate; use 8300 for 2024, 8900 for 2026

print(mnr_hosp["consensus_both"].sum(), "hospital groups flagged by both methods on both feature groups")

view_cols = ["_dim_value", "case_count", "mahal_dist_pers", "chi2_pval_pers",
             "financial_impact", "consensus_pers", "consensus_both"]
mnr_hosp.sort_values("mahal_dist_pers", ascending = False).head(10)[view_cols]

# %% heatmap
z_cols = [f"z_{c}" for c in all_features]

top20 = (
    mnr_hosp
    .nlargest(20, "mahal_dist_pers")
    .set_index("_dim_value")[z_cols]
)
top20.columns = all_features

fig, ax = plt.subplots(figsize = (12, 8))
sns.heatmap(top20, cmap = "RdBu_r", center = 0, annot = True, fmt = ".1f",
            linewidths = 0.5, ax = ax)
ax.set_title("MNR hospital_group — Top 20 by Mahalanobis (Persistency): Z-scores")
plt.tight_layout()
plt.show()


# %% [markdown]
# ## fin_market

# %% mahalanobis — utilization
X = mnr_mkt[util_features].dropna()
mu = X.mean().values
VI = np.linalg.pinv(np.cov(X.values.T))

dists = X.apply(lambda row: mahalanobis(row.values, mu, VI), axis = 1)
pvals = chi2.sf(dists ** 2, df = len(util_features))

mnr_mkt["mahal_dist_util"] = dists
mnr_mkt["chi2_pval_util"] = pvals
mnr_mkt["mahal_flag_util"] = pvals < 0.05

mnr_mkt["mahal_flag_util"].value_counts()

# %% mahalanobis — persistency
X = mnr_mkt[pers_features].dropna()
mu = X.mean().values
VI = np.linalg.pinv(np.cov(X.values.T))

dists = X.apply(lambda row: mahalanobis(row.values, mu, VI), axis = 1)
pvals = chi2.sf(dists ** 2, df = len(pers_features))

mnr_mkt["mahal_dist_pers"] = dists
mnr_mkt["chi2_pval_pers"] = pvals
mnr_mkt["mahal_flag_pers"] = pvals < 0.05

mnr_mkt["mahal_flag_pers"].value_counts()

# %% isolation forest — utilization
X = mnr_mkt[util_features].dropna()
X_scaled = StandardScaler().fit_transform(X)
iso = IsolationForest(n_estimators = 200, contamination = 0.05, random_state = 42)
iso.fit(X_scaled)

mnr_mkt.loc[X.index, "if_score_util"] = iso.decision_function(X_scaled)
mnr_mkt.loc[X.index, "if_flag_util"] = iso.predict(X_scaled) == -1

mnr_mkt["if_flag_util"].value_counts()

# %% isolation forest — persistency
X = mnr_mkt[pers_features].dropna()
X_scaled = StandardScaler().fit_transform(X)
iso = IsolationForest(n_estimators = 200, contamination = 0.05, random_state = 42)
iso.fit(X_scaled)

mnr_mkt.loc[X.index, "if_score_pers"] = iso.decision_function(X_scaled)
mnr_mkt.loc[X.index, "if_flag_pers"] = iso.predict(X_scaled) == -1

mnr_mkt["if_flag_pers"].value_counts()

# %% z-scores + consensus + financial impact
for col in all_features:
    mnr_mkt[f"z_{col}"] = (mnr_mkt[col] - mnr_mkt[col].mean()) / mnr_mkt[col].std()

mnr_mkt["consensus_util"] = mnr_mkt["mahal_flag_util"] & mnr_mkt["if_flag_util"]
mnr_mkt["consensus_pers"] = mnr_mkt["mahal_flag_pers"] & mnr_mkt["if_flag_pers"]
mnr_mkt["consensus_both"] = mnr_mkt["consensus_util"] & mnr_mkt["consensus_pers"]

mnr_mkt["est_persistent_denials"] = mnr_mkt["persistent_adr_rate"] * mnr_mkt["case_count"]
mnr_mkt["financial_impact"] = mnr_mkt["est_persistent_denials"] * 8600  # 2025 rate; use 8300 for 2024, 8900 for 2026

print(mnr_mkt["consensus_both"].sum(), "markets flagged by both methods on both feature groups")

view_cols = ["_dim_value", "case_count", "mahal_dist_pers", "chi2_pval_pers",
             "financial_impact", "consensus_pers", "consensus_both"]
mnr_mkt.sort_values("mahal_dist_pers", ascending = False).head(10)[view_cols]

# %% heatmap
z_cols = [f"z_{c}" for c in all_features]

top_n = (
    mnr_mkt
    .nlargest(20, "mahal_dist_pers")
    .set_index("_dim_value")[z_cols]
)
top_n.columns = all_features

fig, ax = plt.subplots(figsize = (12, 8))
sns.heatmap(top_n, cmap = "RdBu_r", center = 0, annot = True, fmt = ".1f",
            linewidths = 0.5, ax = ax)
ax.set_title("MNR fin_market — Top 20 by Mahalanobis (Persistency): Z-scores")
plt.tight_layout()
plt.show()


# %% [markdown]
# ## export

# %%
mnr_tin["_dim_type"] = "prov_tin"
mnr_hosp["_dim_type"] = "hospital_group"
mnr_mkt["_dim_type"] = "fin_market"

out = pd.concat([mnr_tin, mnr_hosp, mnr_mkt], ignore_index = True)

output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
out.to_csv(f"{output_dir}\\loc_outlier_mnr_202604.csv", index = False)
print(out.shape)
