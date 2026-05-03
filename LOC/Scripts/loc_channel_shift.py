# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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
    select
        hce_admit_month
        , hospital_group
        , sum(initial_adr_cnt) as initial_adr_cnt
        , sum(p2p_case_cnt) as p2p_case_cnt
        , sum(member_appeal_cnt) as member_appeal_cnt
        , sum(case_count) as case_count
    from tmp_1m.kn_loc_notif_04222026_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'IPA'
        and loc_flag = 1
        and hce_admit_month >= '202511'
        and hospital_group is not null
    group by 1, 2
    order by 1, 2
"""

raw = pd.read_sql(query, engine)
engine.dispose()
print(raw.shape)

# %%
raw["hce_admit_month"].value_counts().sort_index()

# %%
raw.describe().round(3)

# %%
# rates per hospital_group per month — both as share of initial ADRs
raw["p2p_rate"] = raw["p2p_case_cnt"] / raw["initial_adr_cnt"].replace(0, np.nan)
raw["member_appeal_rate"] = raw["member_appeal_cnt"] / raw["initial_adr_cnt"].replace(0, np.nan)

# pre = 202511-202512 baseline; post = 202601+ when member appeal came into effect
raw["era"] = np.where(raw["hce_admit_month"] < "202601", "pre", "post")

raw.head(10)

# %%
# which hospital groups have ANY member appeal activity post-202601?
post = raw[raw["era"] == "post"]
hosp_appeal = (
    post
    .groupby("hospital_group")["member_appeal_cnt"]
    .sum()
    .sort_values(ascending = False)
)
hosp_appeal[hosp_appeal > 0]

# %%
# pre vs post comparison per hospital group
era_stats = (
    raw
    .groupby(["hospital_group", "era"])
    .agg(
        initial_adr_cnt = ("initial_adr_cnt", "sum"),
        p2p_case_cnt = ("p2p_case_cnt", "sum"),
        member_appeal_cnt = ("member_appeal_cnt", "sum")
    )
    .reset_index()
)

era_stats["p2p_rate"] = era_stats["p2p_case_cnt"] / era_stats["initial_adr_cnt"].replace(0, np.nan)
era_stats["member_appeal_rate"] = era_stats["member_appeal_cnt"] / era_stats["initial_adr_cnt"].replace(0, np.nan)

pre_post = era_stats.pivot(index = "hospital_group", columns = "era",
                           values = ["p2p_rate", "member_appeal_rate", "initial_adr_cnt"])
pre_post.columns = ["_".join(col) for col in pre_post.columns]
pre_post = pre_post.reset_index()

# volume filter: >= 10 initial ADRs in both eras
pre_post = pre_post[
    (pre_post["initial_adr_cnt_pre"] >= 10) &
    (pre_post["initial_adr_cnt_post"] >= 10)
].copy()

# positive p2p_delta = P2P usage dropped; positive appeal_delta = member appeal usage rose
pre_post["p2p_delta"] = pre_post["p2p_rate_pre"] - pre_post["p2p_rate_post"]
pre_post["appeal_delta"] = pre_post["member_appeal_rate_post"] - pre_post["member_appeal_rate_pre"]

# shifting = P2P went down AND member appeal went up
pre_post["shifting"] = (pre_post["p2p_delta"] > 0) & (pre_post["appeal_delta"] > 0)

pre_post.sort_values("appeal_delta", ascending = False)

# %%
# scatter: P2P delta vs member appeal delta
# top-right quadrant = P2P dropped AND member appeal rose = channel shift
colors = pre_post["shifting"].map({True: "crimson", False: "steelblue"})

fig, ax = plt.subplots(figsize = (9, 7))
ax.scatter(pre_post["p2p_delta"], pre_post["appeal_delta"],
           c = colors, s = 60, alpha = 0.8, edgecolors = "k", linewidths = 0.4)
ax.axhline(0, color = "grey", linewidth = 0.8, linestyle = "--")
ax.axvline(0, color = "grey", linewidth = 0.8, linestyle = "--")

for _, row in pre_post[pre_post["shifting"]].iterrows():
    ax.annotate(row["hospital_group"], (row["p2p_delta"], row["appeal_delta"]),
                fontsize = 7, xytext = (4, 4), textcoords = "offset points")

ax.set_xlabel("P2P rate: pre − post (positive = P2P usage dropped)")
ax.set_ylabel("Member appeal rate: post − pre (positive = member appeal usage rose)")
ax.set_title("MNR hospital_group — Channel Shift: P2P → Member Appeal\n(red = P2P dropped AND member appeal rose post-202601)")
plt.tight_layout()
plt.show()

# %%
# monthly trend lines for shifting hospital groups
shifters = pre_post[pre_post["shifting"]]["hospital_group"].tolist()

if len(shifters) > 0:
    ts = raw[raw["hospital_group"].isin(shifters)].sort_values("hce_admit_month")

    n_cols = min(3, len(shifters))
    n_rows = int(np.ceil(len(shifters) / n_cols))
    fig, axes = plt.subplots(n_rows, n_cols, figsize = (14, 5 * n_rows), squeeze = False)
    axes = axes.flatten()

    for i, hosp in enumerate(shifters):
        grp = ts[ts["hospital_group"] == hosp]
        ax = axes[i]
        ax.plot(grp["hce_admit_month"], grp["p2p_rate"],
                marker = "o", label = "P2P rate", color = "steelblue", linewidth = 1.5)
        ax.plot(grp["hce_admit_month"], grp["member_appeal_rate"],
                marker = "s", label = "Member appeal rate", color = "crimson", linewidth = 1.5)
        ax.axvline("202601", color = "grey", linestyle = "--", linewidth = 0.8)
        ax.set_title(hosp, fontsize = 9)
        ax.set_xlabel("Month")
        ax.tick_params(axis = "x", rotation = 45)
        if i == 0:
            ax.legend(fontsize = 8)

    for j in range(len(shifters), len(axes)):
        axes[j].set_visible(False)

    plt.suptitle("MNR hospital_group — P2P vs Member Appeal Rate by Month (dashed = 202601 effective date)")
    plt.tight_layout()
    plt.show()

# %%
# summary table
output_cols = [
    "hospital_group",
    "initial_adr_cnt_pre", "initial_adr_cnt_post",
    "p2p_rate_pre", "p2p_rate_post", "p2p_delta",
    "member_appeal_rate_pre", "member_appeal_rate_post", "appeal_delta",
    "shifting"
]
summary = pre_post[output_cols].sort_values("appeal_delta", ascending = False)
summary

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
summary.to_csv(f"{output_dir}\\loc_channel_shift_mnr_202604.csv", index = False)
print(summary.shape)
