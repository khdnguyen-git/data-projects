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

query = """
    select
        hospital_group
        , par_nonpar
        , hce_admit_month
        , sum(case_count) as case_count
        , sum(initial_adr_cnt) as initial_adr_cnt
        , sum(persistent_adr_cnt) as persistent_adr_cnt
        , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
        , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
        , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
        , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
    from tmp_1m.kn_loc_notif_04222026_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'IPA'
        and loc_flag = 1
        and par_nonpar is not null
        and par_nonpar not in ('MM', '0', '')
    group by 1, 2, 3
"""

df = pd.read_sql(query, engine)
engine.dispose()
print(df.shape)

# %%
df["par_nonpar"].value_counts()

# %%
df["hospital_group"].nunique()

# %%
# overall rates by par status
overall = (
    df
    .groupby("par_nonpar")
    .agg(
        case_count = ("case_count", "sum"),
        initial_adr_cnt = ("initial_adr_cnt", "sum"),
        persistent_adr_cnt = ("persistent_adr_cnt", "sum"),
        appeal_ovrtn_case_cnt = ("appeal_ovrtn_case_cnt", "sum"),
        p2p_ovrtn_case_cnt = ("p2p_ovrtn_case_cnt", "sum"),
        mcr_ovrtn_case_cnt = ("mcr_ovrtn_case_cnt", "sum"),
        member_appeal_ovtn_cnt = ("member_appeal_ovtn_cnt", "sum")
    )
    .assign(
        adr_rate = lambda x: x["initial_adr_cnt"] / x["case_count"],
        persistency = lambda x: x["persistent_adr_cnt"] / x["initial_adr_cnt"].replace(0, np.nan),
        appeal_overturn_rate = lambda x: x["appeal_ovrtn_case_cnt"] / x["initial_adr_cnt"].replace(0, np.nan),
        p2p_overturn_rate = lambda x: x["p2p_ovrtn_case_cnt"] / x["initial_adr_cnt"].replace(0, np.nan),
        mcr_overturn_rate = lambda x: x["mcr_ovrtn_case_cnt"] / x["initial_adr_cnt"].replace(0, np.nan),
        member_appeal_overturn_rate = lambda x: x["member_appeal_ovtn_cnt"] / x["initial_adr_cnt"].replace(0, np.nan)
    )
    .round(3)
)
overall

# %%
# bar chart: key rates by par status
rate_cols = ["adr_rate", "persistency", "appeal_overturn_rate", "p2p_overturn_rate"]
rate_labels = ["ADR Rate", "Persistency", "Appeal Overturn Rate", "P2P Overturn Rate"]

fig, axes = plt.subplots(1, 4, figsize = (14, 4))
for ax, col, label in zip(axes, rate_cols, rate_labels):
    vals = overall[col].dropna()
    bars = ax.bar(vals.index.astype(str), vals, color = "steelblue", edgecolor = "k", linewidth = 0.5)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + vals.max() * 0.01,
                f"{v:.3f}", ha = "center", va = "bottom", fontsize = 8)
    ax.set_title(label, fontsize = 10)
    ax.tick_params(axis = "x", rotation = 30)

plt.suptitle("MNR FFS — Par vs Non-Par: ADR and Overturn Rates", y = 1.02)
plt.tight_layout()
plt.show()

# %%
# hospital_group × par_nonpar — which hospitals have the biggest par/non-par gap in ADR rate?
hosp_par = (
    df
    .groupby(["hospital_group", "par_nonpar"])
    .agg(
        case_count = ("case_count", "sum"),
        initial_adr_cnt = ("initial_adr_cnt", "sum"),
        persistent_adr_cnt = ("persistent_adr_cnt", "sum")
    )
    .assign(
        adr_rate = lambda x: x["initial_adr_cnt"] / x["case_count"].replace(0, np.nan),
        persistency = lambda x: x["persistent_adr_cnt"] / x["initial_adr_cnt"].replace(0, np.nan)
    )
    .reset_index()
)

# pivot to compare par vs non-par side by side; keep groups with >= 20 cases in both
par_pivot = hosp_par.pivot_table(
    index = "hospital_group", columns = "par_nonpar",
    values = ["adr_rate", "persistency", "case_count"], aggfunc = "first"
)
par_pivot.columns = ["_".join(col) for col in par_pivot.columns]
par_pivot = par_pivot.reset_index()

# filter to groups that have volume on both sides
par_pivot = par_pivot.dropna(subset = [c for c in par_pivot.columns if "adr_rate" in c])
print(par_pivot.shape)

# %%
# scatter: par adr_rate vs non-par adr_rate per hospital group
par_cols = [c for c in par_pivot.columns if "adr_rate_" in c]

if len(par_cols) == 2:
    col_a, col_b = sorted(par_cols)
    label_a = col_a.replace("adr_rate_", "")
    label_b = col_b.replace("adr_rate_", "")

    fig, ax = plt.subplots(figsize = (8, 7))
    ax.scatter(par_pivot[col_a], par_pivot[col_b], s = 50, alpha = 0.7,
               color = "steelblue", edgecolors = "k", linewidths = 0.4)
    lims = [0, max(par_pivot[[col_a, col_b]].max())]
    ax.plot(lims, lims, color = "grey", linestyle = "--", linewidth = 0.8)

    for _, row in par_pivot.nlargest(5, col_b).iterrows():
        ax.annotate(row["hospital_group"], (row[col_a], row[col_b]),
                    fontsize = 7, xytext = (4, 2), textcoords = "offset points")

    ax.set_xlabel(f"ADR rate — {label_a}")
    ax.set_ylabel(f"ADR rate — {label_b}")
    ax.set_title("MNR FFS — ADR Rate: Par vs Non-Par by Hospital Group\n(above diagonal = higher non-par ADR)")
    plt.tight_layout()
    plt.show()

# %%
# monthly trend: par vs non-par persistency over time
monthly = (
    df
    .groupby(["hce_admit_month", "par_nonpar"])
    .agg(
        initial_adr_cnt = ("initial_adr_cnt", "sum"),
        persistent_adr_cnt = ("persistent_adr_cnt", "sum")
    )
    .assign(persistency = lambda x: x["persistent_adr_cnt"] / x["initial_adr_cnt"].replace(0, np.nan))
    .reset_index()
)

fig, ax = plt.subplots(figsize = (12, 5))
for label, grp in monthly.groupby("par_nonpar"):
    ax.plot(grp["hce_admit_month"], grp["persistency"],
            marker = "o", markersize = 3, linewidth = 1.5, label = label)

ax.set_ylabel("Persistency rate")
ax.set_xlabel("Admit month")
ax.set_title("MNR FFS — Persistency Rate Over Time: Par vs Non-Par")
ax.legend(fontsize = 9)
ax.tick_params(axis = "x", rotation = 45)
plt.tight_layout()
plt.show()

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
hosp_par.to_csv(f"{output_dir}\\loc_par_nonpar_mnr_202604.csv", index = False)
print(hosp_par.shape)
