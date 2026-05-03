# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
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

# ipa_li_split is cleaner than AHRQ — Medical / Surgical / ILI / COVID-19 / Transplant / LTAC / SNF / AIR
query = """
    select
        hospital_group
        , ipa_li_split
        , sum(case_count) as case_count
        , sum(initial_adr_cnt) as initial_adr_cnt
        , sum(persistent_adr_cnt) as persistent_adr_cnt
    from tmp_1m.kn_loc_notif_04222026_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'IPA'
        and loc_flag = 1
        and hospital_group is not null
        and ipa_li_split not in ('NA', 'MM')
    group by 1, 2
"""

df = pd.read_sql(query, engine)
engine.dispose()
print(df.shape)

# %%
df["ipa_li_split"].value_counts()

# %%
df["hospital_group"].nunique()

# %%
# rates
df["adr_rate"] = df["initial_adr_cnt"] / df["case_count"].replace(0, np.nan)
df["persistency"] = df["persistent_adr_cnt"] / df["initial_adr_cnt"].replace(0, np.nan)

# %%
# case mix by hospital group — share of cases by clinical category
mix = (
    df
    .pivot_table(index = "hospital_group", columns = "ipa_li_split",
                 values = "case_count", aggfunc = "sum", fill_value = 0)
)
mix_pct = mix.div(mix.sum(axis = 1), axis = 0)

# top 30 hospital groups by total volume
top30 = mix.sum(axis = 1).nlargest(30).index
mix_pct_top = mix_pct.loc[top30]

fig, ax = plt.subplots(figsize = (12, 9))
mix_pct_top.plot(kind = "barh", stacked = True, ax = ax,
                 colormap = "tab10", edgecolor = "k", linewidth = 0.2)
ax.set_xlabel("Share of cases")
ax.set_title("MNR FFS — Clinical Mix by Hospital Group (top 30 by volume)\n(ipa_li_split: Medical / Surgical / ILI / ...)")
ax.legend(loc = "lower right", fontsize = 8)
plt.tight_layout()
plt.show()

# %%
# ADR rate heatmap by hospital_group × clinical category
# only include Medical + Surgical (they have enough volume to trust rates)
adr_pivot = (
    df[df["ipa_li_split"].isin(["Medical", "Surgical"])]
    .pivot_table(index = "hospital_group", columns = "ipa_li_split",
                 values = "adr_rate", aggfunc = "mean")
)
adr_pivot = adr_pivot.loc[adr_pivot.index.isin(top30)]

fig, ax = plt.subplots(figsize = (6, 10))
sns.heatmap(adr_pivot, annot = True, fmt = ".2f", cmap = "RdYlGn_r", center = adr_pivot.stack().median(),
            linewidths = 0.4, ax = ax)
ax.set_title("MNR FFS — ADR Rate by Hospital Group × Clinical Category\n(Medical vs Surgical; top 30 groups)")
plt.tight_layout()
plt.show()

# %%
# persistency heatmap same cut
pers_pivot = (
    df[df["ipa_li_split"].isin(["Medical", "Surgical"])]
    .pivot_table(index = "hospital_group", columns = "ipa_li_split",
                 values = "persistency", aggfunc = "mean")
)
pers_pivot = pers_pivot.loc[pers_pivot.index.isin(top30)]

fig, ax = plt.subplots(figsize = (6, 10))
sns.heatmap(pers_pivot, annot = True, fmt = ".2f", cmap = "RdYlGn_r", center = pers_pivot.stack().median(),
            linewidths = 0.4, ax = ax)
ax.set_title("MNR FFS — Persistency Rate by Hospital Group × Clinical Category\n(Medical vs Surgical; top 30 groups)")
plt.tight_layout()
plt.show()

# %%
# overall clinical mix + rates — rolled up across all groups
overall = (
    df
    .groupby("ipa_li_split")
    .agg(
        case_count = ("case_count", "sum"),
        initial_adr_cnt = ("initial_adr_cnt", "sum"),
        persistent_adr_cnt = ("persistent_adr_cnt", "sum")
    )
    .assign(
        adr_rate = lambda x: x["initial_adr_cnt"] / x["case_count"],
        persistency = lambda x: x["persistent_adr_cnt"] / x["initial_adr_cnt"],
        case_pct = lambda x: x["case_count"] / x["case_count"].sum()
    )
    .sort_values("case_count", ascending = False)
    .round(3)
)
overall

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
df.to_csv(f"{output_dir}\\loc_dx_mix_mnr_202604.csv", index = False)
print(df.shape)
