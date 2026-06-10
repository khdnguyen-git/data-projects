# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
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

# %%
# pull hospital_group aggregation — MNR FFS, 2025 only (202501-202512)
query_core = """
    select
        d.collection as hospital_group
        , count(*) as case_count
        , sum(a.initialfulladr_cases) as initial_adr_cnt
        , sum(a.persistentfulladr_cases) as persistent_adr_cnt
        , sum(a.p2p_full_ovtn) as p2p_ovrtn_cnt
        , sum(a.appeal_ovrtn_ind) as appeal_ovrtn_cnt
        , sum(a.mcr_ovtrn_ind) as mcr_ovrtn_cnt
        , sum(a.icm_md_reviewed_ind) as md_reviewed_cnt
    from tmp_1m.ec_avtar_25_26_3_od as a
    left join tmp_1y.tin_collection as d
        on substr(a.fa_prov_id, 2, 9) = d.tin
    where fin_brand in ('M&R', 'C&S')
        and (
            (ip_type in ('Medical', 'Surgical', 'Transplant')
             and to_varchar(admit_dt_act, 'MM/dd/yyyy') is not null)
            or ip_type in ('LTAC', 'SNF', 'AIR')
        )
        and fin_brand = 'M&R'
        and loc_flag = 1
        and (
            (global_cap = 'NA' and sgr_source_name = 'COSMOS'
             and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
            or (sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
        and hce_admit_month between '202501' and '202512'
    group by 1
    having count(*) >= 30
"""

df = pd.read_sql(query_core, engine)
print(f"Core (2025): {df.shape}")

# %%
# pull member_appeal — 202601+ only (separate time range)
query_ma = """
    select
        d.collection as hospital_group
        , sum(a.initialfulladr_cases) as ma_initial_adr_cnt
        , sum(a.member_appeal_ind) as member_appeal_cnt
        , sum(a.member_appeal_ovtn_ind) as member_appeal_ovtn_cnt
    from tmp_1m.ec_avtar_25_26_3_od as a
    left join tmp_1y.tin_collection as d
        on substr(a.fa_prov_id, 2, 9) = d.tin
    where fin_brand in ('M&R', 'C&S')
        and (
            (ip_type in ('Medical', 'Surgical', 'Transplant')
             and to_varchar(admit_dt_act, 'MM/dd/yyyy') is not null)
            or ip_type in ('LTAC', 'SNF', 'AIR')
        )
        and fin_brand = 'M&R'
        and loc_flag = 1
        and (
            (global_cap = 'NA' and sgr_source_name = 'COSMOS'
             and fin_product_level_3 != 'INSTITUTIONAL' and tfm_include_flag = 1)
            or (sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
        and hce_admit_month >= '202601'
    group by 1
    having sum(a.initialfulladr_cases) >= 10
"""

df_ma = pd.read_sql(query_ma, engine)
engine.dispose()
print(f"Member appeal (202601+): {df_ma.shape}")

# %%
# merge member_appeal onto core
df = df.merge(df_ma, on = "hospital_group", how = "left")
print(f"After merge: {df.shape}")

# %%
# compute rates
df["initial_adr_rate"] = df["initial_adr_cnt"] / df["case_count"]
df["persistent_adr_rate"] = df["persistent_adr_cnt"] / df["case_count"]
df["p2p_overturn_rate"] = df["p2p_ovrtn_cnt"] / df["initial_adr_cnt"].replace(0, np.nan)
df["appeal_overturn_rate"] = df["appeal_ovrtn_cnt"] / df["initial_adr_cnt"].replace(0, np.nan)
df["mcr_overturn_rate"] = df["mcr_ovrtn_cnt"] / df["initial_adr_cnt"].replace(0, np.nan)
df["md_review_rate"] = df["md_reviewed_cnt"] / df["case_count"]
df["member_appeal_rate"] = df["member_appeal_cnt"] / df["ma_initial_adr_cnt"].replace(0, np.nan)

# drop entities missing overturn rates OR member_appeal_rate (need all features for IF)
df = df.dropna(subset = ["p2p_overturn_rate", "member_appeal_rate"])
print(f"Entities after dropping NaN: {df.shape[0]}")

# %%
# EDA
features = ["initial_adr_rate", "persistent_adr_rate", "p2p_overturn_rate",
            "appeal_overturn_rate", "mcr_overturn_rate", "md_review_rate",
            "member_appeal_rate"]

df[features].describe().round(4)

# %%
df[features].hist(bins = 25, figsize = (16, 8), edgecolor = "white")
plt.suptitle("Hospital Group — Feature Distributions (MNR FFS, 2025 + MA 202601+)")
plt.tight_layout()
plt.show()

# %%
# standardize
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df[features])

# fit isolation forest
iso = IsolationForest(
    contamination = 0.05,
    n_estimators = 200,
    random_state = 42
)
iso.fit(X_scaled)

df["anomaly_score"] = iso.decision_function(X_scaled)
df["is_anomaly"] = (iso.predict(X_scaled) == -1).astype(int)

print(f"Total entities: {len(df)}")
print(f"Flagged: {df['is_anomaly'].sum()} ({df['is_anomaly'].mean():.1%})")

# %%
# z-scores with direction — tells you WHY each entity was flagged and whether it's high or low
z_df = pd.DataFrame(X_scaled, columns = [f"z_{f}" for f in features], index = df.index)
df = pd.concat([df, z_df], axis = 1)

# top reason with direction
z_cols = [f"z_{f}" for f in features]
df["top_feature"] = df[z_cols].abs().idxmax(axis = 1).str.replace("z_", "")
df["top_z"] = df[z_cols].apply(lambda row: row[row.abs().idxmax()], axis = 1)
df["top_direction"] = np.where(df["top_z"] > 0, "high", "low")
df["top_reason"] = df["top_feature"] + " (" + df["top_direction"] + ")"

# %%
# inspect flagged entities
flagged = (
    df
    .query("is_anomaly == 1")
    .sort_values("anomaly_score")
    [["hospital_group", "case_count", "initial_adr_rate", "persistent_adr_rate",
      "p2p_overturn_rate", "appeal_overturn_rate", "mcr_overturn_rate",
      "md_review_rate", "member_appeal_rate", "anomaly_score", "top_reason"]]
    .round(4)
)
print(f"Flagged entities: {len(flagged)}")
flagged

# %%
# all z-scores for flagged entities — see full profile
flagged_z = (
    df
    .query("is_anomaly == 1")
    .sort_values("anomaly_score")
    [["hospital_group", "z_initial_adr_rate", "z_persistent_adr_rate",
      "z_p2p_overturn_rate", "z_appeal_overturn_rate",
      "z_mcr_overturn_rate", "z_md_review_rate", "z_member_appeal_rate"]]
    .round(2)
)
flagged_z

# %%
# export
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
df.to_csv(f"{output_dir}\\loc_if_hospital_group.csv", index = False)
flagged.to_csv(f"{output_dir}\\loc_if_hospital_group_flagged.csv", index = False)
print(f"Exported to {output_dir}")
