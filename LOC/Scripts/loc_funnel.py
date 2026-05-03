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
        , sum(initial_adr_cnt) as initial_adr_cnt
        , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
        , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
        , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
        , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
        , sum(persistent_adr_cnt) as persistent_adr_cnt
    from tmp_1m.kn_loc_notif_04222026_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'IPA'
        and loc_flag = 1
    group by 1
    order by 1
"""

monthly = pd.read_sql(query, engine)
engine.dispose()
print(monthly.shape)

# %%
monthly.tail(12)

# %%
# funnel stages: each stage shows how many denials were reversed at that step
# note: overturn counts can overlap across stages — persistent_adr_cnt is the ground truth end state
monthly["after_p2p"] = monthly["initial_adr_cnt"] - monthly["p2p_ovrtn_case_cnt"]
monthly["after_appeal"] = monthly["after_p2p"] - monthly["appeal_ovrtn_case_cnt"]
monthly["after_member_appeal"] = monthly["after_appeal"] - monthly["member_appeal_ovtn_cnt"]
monthly["after_mcr"] = monthly["after_member_appeal"] - monthly["mcr_ovrtn_case_cnt"]

# as % of initial
for col in ["after_p2p", "after_appeal", "after_member_appeal", "after_mcr", "persistent_adr_cnt"]:
    monthly[f"{col}_pct"] = monthly[col] / monthly["initial_adr_cnt"].replace(0, np.nan)

monthly[["hce_admit_month", "initial_adr_cnt", "after_p2p", "after_appeal",
         "after_member_appeal", "after_mcr", "persistent_adr_cnt"]].tail(12)

# %%
# overall funnel — aggregate across all months
total = monthly.sum(numeric_only = True)

stages = ["initial_adr_cnt", "after_p2p", "after_appeal", "after_member_appeal", "after_mcr", "persistent_adr_cnt"]
labels = ["Initial ADR", "After P2P", "After Appeal", "After Member Appeal", "After MCR", "Persistent"]
values = [total[s] for s in stages]
pcts = [v / total["initial_adr_cnt"] * 100 for v in values]

fig, ax = plt.subplots(figsize = (10, 5))
bars = ax.bar(labels, values, color = "steelblue", edgecolor = "k", linewidth = 0.5)
for bar, pct in zip(bars, pcts):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + total["initial_adr_cnt"] * 0.005,
            f"{pct:.1f}%", ha = "center", va = "bottom", fontsize = 9)
ax.set_ylabel("Denial count")
ax.set_title("MNR FFS — Denial Funnel: Initial ADR → Persistent (all months combined)")
plt.tight_layout()
plt.show()

# %%
# funnel over time — persistency rate by cohort month
fig, ax = plt.subplots(figsize = (12, 5))

stage_cols = ["after_p2p_pct", "after_appeal_pct", "after_member_appeal_pct",
              "after_mcr_pct", "persistent_adr_cnt_pct"]
stage_labels = ["After P2P", "After Appeal", "After Member Appeal", "After MCR", "Persistent"]
colors = ["#a8c8e8", "#6aafd6", "#3d8bbf", "#1f6699", "#0d3d5e"]

for col, label, color in zip(stage_cols, stage_labels, colors):
    ax.plot(monthly["hce_admit_month"], monthly[col], label = label, color = color,
            linewidth = 1.5, marker = "o", markersize = 3)

ax.set_ylabel("% of initial ADRs remaining")
ax.set_xlabel("Admit month")
ax.set_title("MNR FFS — Denial Survival by Cohort Month (% of initial ADRs remaining at each stage)")
ax.legend(fontsize = 8, loc = "upper left")
ax.tick_params(axis = "x", rotation = 45)
plt.tight_layout()
plt.show()

# %%
# reversal breakdown by month — stacked bar of what reversed each denial
monthly["reversed_p2p"] = monthly["p2p_ovrtn_case_cnt"]
monthly["reversed_appeal"] = monthly["appeal_ovrtn_case_cnt"]
monthly["reversed_member_appeal"] = monthly["member_appeal_ovtn_cnt"]
monthly["reversed_mcr"] = monthly["mcr_ovrtn_case_cnt"]

reversal_cols = ["reversed_p2p", "reversed_appeal", "reversed_member_appeal", "reversed_mcr"]
reversal_labels = ["P2P", "Appeal", "Member Appeal", "MCR"]
reversal_colors = ["#3d8bbf", "#6aafd6", "#e07b6a", "#a8c8e8"]

fig, ax = plt.subplots(figsize = (12, 5))
bottom = np.zeros(len(monthly))
for col, label, color in zip(reversal_cols, reversal_labels, reversal_colors):
    ax.bar(monthly["hce_admit_month"], monthly[col], bottom = bottom, label = label,
           color = color, edgecolor = "k", linewidth = 0.3)
    bottom += monthly[col].values

ax.set_ylabel("Reversal count")
ax.set_xlabel("Admit month")
ax.set_title("MNR FFS — Reversals by Channel and Month")
ax.legend(fontsize = 8)
ax.tick_params(axis = "x", rotation = 45)
plt.tight_layout()
plt.show()

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
monthly.to_csv(f"{output_dir}\\loc_funnel_mnr_202604.csv", index = False)
print(monthly.shape)
