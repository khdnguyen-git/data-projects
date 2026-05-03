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

# raw AVTAR — one row per case; fin_mbi_hicn_fnl is the member ID
# mnr_total_ffs_flag logic applied inline: M&R + (COSMOS non-inst FFS OR NICE FFS/PHYSICIAN)
# loc_flag logic: acute IP, place of service 21, medical or surgical admit category
query = """
    select
        fin_mbi_hicn_fnl as mbi
        , concat(lpad(year(admit_dt_act), 4, 0), lpad(month(admit_dt_act), 2, 0)) as hce_admit_month
        , admit_dt_act
        , dschg_dt_act
        , fin_market
        , substr(fa_prov_id, 2, 9) as prov_tin
        , admit_cat_cd
        , initialfulladr_cases as initial_adr
        , persistentfulladr_cases as persistent_adr
        , case when datediff('day', admit_dt_act, coalesce(dschg_dt_act, current_date())) between 0 and 500
            then datediff('day', admit_dt_act, coalesce(dschg_dt_act, current_date())) end as los
    from hce_ops_fnl.hce_adr_avtar_like_25_26_f
    where svc_setting = 'Inpatient'
        and plc_of_svc_cd = '21 - Acute Hospital'
        and admit_cat_cd in ('17 - Medical', '30 - Surgical')
        and fin_brand = 'M&R'
        and admit_dt_act is not null
        and fin_mbi_hicn_fnl != '0'
        and (
            (global_cap = 'NA' and sgr_source_name in ('COSMOS', 'CSP')
             and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
            or (sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
"""

raw = pd.read_sql(query, engine)
engine.dispose()
print(raw.shape)

# %%
raw.describe().round(2)

# %%
raw["hce_admit_month"].value_counts().sort_index()

# %%
# how many admissions per member over the entire period?
member_counts = raw.groupby("mbi").size().rename("admit_count").reset_index()
member_counts["admit_bucket"] = pd.cut(
    member_counts["admit_count"],
    bins = [0, 1, 2, 3, 100],
    labels = ["1", "2", "3", "4+"]
)
member_counts["admit_bucket"].value_counts().sort_index()

# %%
# merge bucket back to case level
raw = raw.merge(member_counts[["mbi", "admit_count", "admit_bucket"]], on = "mbi")

# adr rate and persistency by admit bucket
bucket_stats = (
    raw
    .groupby("admit_bucket")
    .agg(
        members = ("mbi", "nunique"),
        cases = ("mbi", "count"),
        initial_adr = ("initial_adr", "sum"),
        persistent_adr = ("persistent_adr", "sum"),
        median_los = ("los", "median")
    )
    .assign(
        adr_rate = lambda x: x["initial_adr"] / x["cases"],
        persistency = lambda x: x["persistent_adr"] / x["initial_adr"].replace(0, np.nan)
    )
    .round(3)
)
bucket_stats

# %%
fig, axes = plt.subplots(1, 3, figsize = (13, 4))

for ax, col, title in zip(
    axes,
    ["adr_rate", "persistency", "median_los"],
    ["ADR Rate", "Persistency Rate", "Median LOS (days)"]
):
    vals = bucket_stats[col].dropna()
    ax.bar(vals.index.astype(str), vals, color = "steelblue", edgecolor = "k", linewidth = 0.5)
    ax.set_xlabel("Number of admissions (member total)")
    ax.set_title(f"MNR FFS — {title} by Member Admit Count")
    ax.axhline(vals.median(), color = "grey", linestyle = "--", linewidth = 0.8)

plt.suptitle("MNR FFS — Repeat Admitters: Are high-frequency members denied more?", y = 1.02)
plt.tight_layout()
plt.show()

# %%
# members with 3+ admissions — who are they by market?
repeat_members = raw[raw["admit_count"] >= 3]

market_repeats = (
    repeat_members
    .groupby("fin_market")
    .agg(
        repeat_members = ("mbi", "nunique"),
        total_admits = ("mbi", "count"),
        adr_rate = ("initial_adr", "mean"),
        persistency = ("persistent_adr", lambda x: x.sum() / repeat_members.loc[x.index, "initial_adr"].sum()
                       if repeat_members.loc[x.index, "initial_adr"].sum() > 0 else np.nan)
    )
    .sort_values("repeat_members", ascending = False)
)
market_repeats.head(20).round(3)

# %%
# month trend — are repeat admits growing?
monthly_repeats = (
    raw
    .groupby(["hce_admit_month", "admit_bucket"])
    .agg(cases = ("mbi", "count"))
    .reset_index()
)
monthly_repeats_piv = monthly_repeats.pivot(index = "hce_admit_month", columns = "admit_bucket", values = "cases").fillna(0)

fig, ax = plt.subplots(figsize = (12, 5))
monthly_repeats_piv.plot(kind = "bar", stacked = True, ax = ax,
                         colormap = "Blues", edgecolor = "k", linewidth = 0.2)
ax.set_xlabel("Admit month")
ax.set_title("MNR FFS — Admits by Member Frequency Bucket Over Time")
ax.legend(title = "Admit count", fontsize = 8)
ax.tick_params(axis = "x", rotation = 45)
plt.tight_layout()
plt.show()

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
member_counts.to_csv(f"{output_dir}\\loc_repeat_admits_members_mnr_202604.csv", index = False)
bucket_stats.to_csv(f"{output_dir}\\loc_repeat_admits_stats_mnr_202604.csv")
print(raw.shape)
