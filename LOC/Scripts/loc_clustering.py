# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

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

query = """
    select *
    from tmp_1m.kn_loc_mnr_agg_202604
    where _dimension = 'hospital_group'
"""

df = pd.read_sql(query, engine)
engine.dispose()
print(df.shape)

# %%
df.describe().round(3)

# %%
df[["_dim_value", "case_count", "auth_per_k", "adr_rate", "persistent_adr_rate",
    "appeal_overturn_rate", "p2p_overturn_rate", "mcr_overturn_rate",
    "member_appeal_rate", "pre_auth_rate"]].head(20)

# %%
# scope to top 50 hospital groups by auth/k — same peer group as outlier analysis
features = ["auth_per_k", "adr_rate", "pre_auth_rate",
            "persistent_adr_rate", "appeal_overturn_rate",
            "p2p_overturn_rate", "mcr_overturn_rate", "member_appeal_rate"]

top50 = (
    df
    .sort_values("auth_per_k", ascending = False)
    .head(50)
    .dropna(subset = features)
    .reset_index(drop = True)
)
print(top50.shape)

# %%
# scale features — K-means is distance-based, unscaled auth_per_k would dominate
X = top50[features].copy()
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# %%
# elbow plot — inertia (within-cluster sum of squares) by K
inertias = []
sil_scores = []
K_range = range(2, 11)

for k in K_range:
    km = KMeans(n_clusters = k, random_state = 42, n_init = 10)
    km.fit(X_scaled)
    inertias.append(km.inertia_)
    sil_scores.append(silhouette_score(X_scaled, km.labels_))

fig, axes = plt.subplots(1, 2, figsize = (12, 4))

axes[0].plot(list(K_range), inertias, marker = "o", color = "steelblue", linewidth = 1.5)
axes[0].set_xlabel("K (number of clusters)")
axes[0].set_ylabel("Inertia (WCSS)")
axes[0].set_title("Elbow Plot — K-Means Inertia")

axes[1].plot(list(K_range), sil_scores, marker = "o", color = "crimson", linewidth = 1.5)
axes[1].set_xlabel("K (number of clusters)")
axes[1].set_ylabel("Silhouette score")
axes[1].set_title("Silhouette Score by K (higher = better-separated clusters)")

plt.tight_layout()
plt.show()

# %%
# fit final model — pick K from elbow/silhouette above; default to 4
K = 4

km_final = KMeans(n_clusters = K, random_state = 42, n_init = 10)
top50["cluster"] = km_final.fit_predict(X_scaled)

top50[["_dim_value", "cluster", "case_count", "auth_per_k", "adr_rate", "persistent_adr_rate"]].sort_values("cluster")

# %%
# cluster profiles — mean of each feature per cluster
profile = (
    top50
    .groupby("cluster")[features]
    .mean()
    .round(3)
)
profile["n"] = top50.groupby("cluster")["_dim_value"].count()
profile

# %%
# heatmap of cluster means (z-scored so all features are on the same scale)
profile_z = profile[features].apply(lambda col: (col - col.mean()) / col.std(), axis = 0)

fig, ax = plt.subplots(figsize = (11, 4))
sns.heatmap(profile_z, annot = profile[features], fmt = ".2f", cmap = "RdBu_r", center = 0,
            linewidths = 0.5, ax = ax,
            annot_kws = {"size": 8})
ax.set_title(f"MNR hospital_group — K-Means Cluster Profiles (K={K})\n(color = z-score; labels = raw mean)")
ax.set_xlabel("")
ax.set_ylabel("Cluster")
plt.tight_layout()
plt.show()

# %%
# scatter: auth/k vs persistent_adr_rate, colored by cluster
palette = {0: "#3d8bbf", 1: "#e07b6a", 2: "#6abf69", 3: "#b07bbf",
           4: "#f0c060", 5: "#7bbfbf"}

fig, ax = plt.subplots(figsize = (9, 7))
for c, grp in top50.groupby("cluster"):
    ax.scatter(grp["auth_per_k"], grp["persistent_adr_rate"],
               s = grp["case_count"] / grp["case_count"].max() * 200 + 20,
               color = palette.get(c, "grey"), alpha = 0.8,
               edgecolors = "k", linewidths = 0.4, label = f"Cluster {c}")
    for _, row in grp.iterrows():
        ax.annotate(row["_dim_value"], (row["auth_per_k"], row["persistent_adr_rate"]),
                    fontsize = 6, xytext = (4, 2), textcoords = "offset points", alpha = 0.7)

ax.set_xlabel("Auth per 1k members")
ax.set_ylabel("Persistent ADR rate")
ax.set_title(f"MNR hospital_group — K-Means Clusters (K={K})\n(size = case_count)")
ax.legend(fontsize = 9)
plt.tight_layout()
plt.show()

# %%
# scatter: adr_rate vs appeal_overturn_rate, colored by cluster
fig, ax = plt.subplots(figsize = (9, 7))
for c, grp in top50.groupby("cluster"):
    ax.scatter(grp["adr_rate"], grp["appeal_overturn_rate"],
               s = grp["case_count"] / grp["case_count"].max() * 200 + 20,
               color = palette.get(c, "grey"), alpha = 0.8,
               edgecolors = "k", linewidths = 0.4, label = f"Cluster {c}")
    for _, row in grp.iterrows():
        ax.annotate(row["_dim_value"], (row["adr_rate"], row["appeal_overturn_rate"]),
                    fontsize = 6, xytext = (4, 2), textcoords = "offset points", alpha = 0.7)

ax.set_xlabel("ADR rate")
ax.set_ylabel("Appeal overturn rate")
ax.set_title(f"MNR hospital_group — K-Means Clusters (K={K})\n(size = case_count)")
ax.legend(fontsize = 9)
plt.tight_layout()
plt.show()

# %%
# full member list per cluster
top50[["cluster", "_dim_value", "case_count", "auth_per_k", "adr_rate",
       "persistent_adr_rate", "appeal_overturn_rate", "p2p_overturn_rate",
       "mcr_overturn_rate", "member_appeal_rate", "pre_auth_rate"]].sort_values(["cluster", "auth_per_k"], ascending = [True, False])

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
top50.sort_values(["cluster", "auth_per_k"], ascending = [True, False]).to_csv(
    f"{output_dir}\\loc_clusters_mnr_202604.csv", index = False
)
profile.to_csv(f"{output_dir}\\loc_cluster_profiles_mnr_202604.csv")
print(top50.shape)
