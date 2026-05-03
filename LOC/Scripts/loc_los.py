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

# kn_ip_dataset_04222026_3_od is the case-level table with hospital_group added
# los_exp = actual LOS for discharged, days since admit for open — use it as the primary LOS field
# filter to discharged cases only to avoid inflating LOS with still-open stays
query = """
    select
        hospital_group
        , ipa_li_split
        , los_categories
        , do_ind
        , los_exp
        , initial_adr_cnt
        , persistent_adr_cnt
        , case_count
        , hce_admit_month
    from tmp_1m.kn_ip_dataset_04222026_3_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'IPA'
        and loc_flag = 1
        and hospital_group is not null
        and do_ind = 'DISCHARGED'
        and los_exp > 0
        and los_exp < 180
"""

df = pd.read_sql(query, engine)
engine.dispose()
print(df.shape)

# %%
df["los_exp"].describe().round(2)

# %%
df["ipa_li_split"].value_counts()

# %%
# ADR flag — row is an initial denial
df["is_adr"] = df["initial_adr_cnt"] > 0
df["is_persistent"] = df["persistent_adr_cnt"] > 0

# %%
# LOS distribution: ADR cases vs non-ADR cases
fig, ax = plt.subplots(figsize = (8, 5))

for label, grp in df.groupby("is_adr"):
    grp["los_exp"].hist(bins = 40, alpha = 0.6, ax = ax,
                        label = "ADR" if label else "No ADR", density = True)

ax.set_xlabel("Length of stay (days)")
ax.set_ylabel("Density")
ax.set_title("MNR FFS — LOS Distribution: ADR Cases vs Non-ADR Cases")
ax.legend()
plt.tight_layout()
plt.show()

# %%
# median LOS by hospital group — top 30 by case volume
hosp_los = (
    df
    .groupby("hospital_group")
    .agg(
        case_count = ("case_count", "sum"),
        median_los = ("los_exp", "median"),
        mean_los = ("los_exp", "mean"),
        p75_los = ("los_exp", lambda x: x.quantile(0.75)),
        adr_rate = ("is_adr", "mean"),
        pct_long_stay = ("los_exp", lambda x: (x > 10).mean())
    )
    .reset_index()
)

top30 = hosp_los.nlargest(30, "case_count")["hospital_group"]
top_los = hosp_los[hosp_los["hospital_group"].isin(top30)].sort_values("median_los", ascending = False)

fig, ax = plt.subplots(figsize = (10, 9))
colors = top_los["adr_rate"].apply(
    lambda r: "crimson" if r > top_los["adr_rate"].quantile(0.75) else "steelblue"
)
ax.barh(top_los["hospital_group"], top_los["median_los"],
        xerr = top_los["p75_los"] - top_los["median_los"],
        color = colors, height = 0.7, capsize = 3, error_kw = {"linewidth": 0.8})
ax.axvline(top_los["median_los"].median(), color = "k", linewidth = 0.8, linestyle = "--")
ax.set_xlabel("Median LOS (days), error bar = 75th pct")
ax.set_title("MNR FFS — Median LOS by Hospital Group (top 30 by volume)\n(red = top quartile ADR rate)")
plt.tight_layout()
plt.show()

# %%
# LOS by clinical category — do Surgical cases get denied more at specific LOS bands?
los_split = (
    df[df["ipa_li_split"].isin(["Medical", "Surgical"])]
    .groupby(["ipa_li_split", "los_categories"])
    .agg(
        case_count = ("case_count", "sum"),
        adr_rate = ("is_adr", "mean"),
        persistency = ("is_persistent", "mean")
    )
    .reset_index()
)

# order los_categories logically
cat_order = ["1", "2", "3", "4-5", "6-10", "11-30", "31+"]
los_split["los_categories"] = pd.Categorical(los_split["los_categories"], categories = cat_order, ordered = True)
los_split = los_split.sort_values(["ipa_li_split", "los_categories"])

fig, axes = plt.subplots(1, 2, figsize = (13, 5))

for ax, metric, title in zip(
    axes,
    ["adr_rate", "persistency"],
    ["ADR Rate by LOS Band", "Persistency Rate by LOS Band"]
):
    for label, grp in los_split.groupby("ipa_li_split"):
        ax.plot(grp["los_categories"].astype(str), grp[metric],
                marker = "o", linewidth = 1.5, label = label)
    ax.set_xlabel("LOS category (days)")
    ax.set_title(f"MNR FFS — {title}")
    ax.legend(fontsize = 9)
    ax.tick_params(axis = "x", rotation = 30)

plt.tight_layout()
plt.show()

# %%
# long-stay flag: > 10 days — which hospital groups have the highest share of long stays?
top_los.sort_values("pct_long_stay", ascending = False)[
    ["hospital_group", "case_count", "median_los", "pct_long_stay", "adr_rate"]
].round(3)

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
hosp_los.to_csv(f"{output_dir}\\loc_los_hosp_mnr_202604.csv", index = False)
print(hosp_los.shape)
