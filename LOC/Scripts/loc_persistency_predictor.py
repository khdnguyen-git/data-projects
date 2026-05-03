# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
import statsmodels.api as sm
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
        fin_market
        , hospital_group
        , admit_type
        , los_categories
        , ip_type
        , ipa_li_split
        , sum(initial_adr_cnt) as initial_adr_cnt
        , sum(persistent_adr_cnt) as persistent_adr_cnt
        , sum(case_count) as case_count
        , sum(pre_auth_cases) as pre_auth_cases
        , sum(p2p_case_cnt) as p2p_case_cnt
    from tmp_1m.kn_loc_notif_04222026_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'IPA'
        and loc_flag = 1
    group by 1, 2, 3, 4, 5, 6
    having sum(initial_adr_cnt) >= 5
    order by 1, 2
"""

df = pd.read_sql(query, engine)
engine.dispose()
print(df.shape)

# %%
df.describe().round(3)

# %%
df[["fin_market", "admit_type", "los_categories", "ip_type", "ipa_li_split"]].nunique()

# %%
# derived rates
df["pre_auth_rate"] = df["pre_auth_cases"] / df["case_count"].replace(0, np.nan)
df["p2p_rate"] = df["p2p_case_cnt"] / df["initial_adr_cnt"].replace(0, np.nan)
df["persistency"] = df["persistent_adr_cnt"] / df["initial_adr_cnt"].replace(0, np.nan)

df = df.dropna(subset = ["persistency", "pre_auth_rate", "p2p_rate"])
print(df.shape)

# %%
# encode categoricals — drop first to avoid multicollinearity
cats = ["fin_market", "admit_type", "los_categories", "ip_type", "ipa_li_split"]
X_cat = pd.get_dummies(df[cats], drop_first = True)

X_cont = df[["pre_auth_rate", "p2p_rate"]].copy()
X = pd.concat([X_cont, X_cat], axis = 1).astype(float)
X = sm.add_constant(X)

# binomial GLM — success = persistent_adr_cnt, trials = initial_adr_cnt
# this is the proper grouped logistic regression (equivalent to R's glm with binomial family)
endog = np.column_stack([
    df["persistent_adr_cnt"],
    df["initial_adr_cnt"] - df["persistent_adr_cnt"]
])

model = sm.GLM(endog, X, family = sm.families.Binomial())
result = model.fit()
print(result.summary())

# %%
# coefficient plot — only statistically significant predictors (p < 0.05)
coef_df = pd.DataFrame({
    "coef": result.params,
    "lower": result.conf_int()[0],
    "upper": result.conf_int()[1],
    "pval": result.pvalues
}).drop("const").sort_values("coef")

sig = coef_df[coef_df["pval"] < 0.05]

fig, ax = plt.subplots(figsize = (8, max(4, len(sig) * 0.35)))
ax.barh(sig.index, sig["coef"], xerr = [sig["coef"] - sig["lower"], sig["upper"] - sig["coef"]],
        color = sig["coef"].apply(lambda x: "steelblue" if x > 0 else "crimson"),
        height = 0.6, capsize = 3, error_kw = {"linewidth": 0.8})
ax.axvline(0, color = "k", linewidth = 0.8)
ax.set_xlabel("Log-odds coefficient (positive = higher persistency)")
ax.set_title("MNR — Binomial GLM: Predictors of Persistency (p < 0.05 only)")
plt.tight_layout()
plt.show()

# %%
# predicted vs actual persistency
df["predicted_persistency"] = result.predict(X)

fig, ax = plt.subplots(figsize = (7, 6))
ax.scatter(df["persistency"], df["predicted_persistency"],
           s = df["initial_adr_cnt"] / df["initial_adr_cnt"].max() * 100 + 5,
           alpha = 0.5, color = "steelblue", edgecolors = "k", linewidths = 0.3)
ax.plot([0, 1], [0, 1], color = "grey", linestyle = "--", linewidth = 0.8)
ax.set_xlabel("Actual persistency")
ax.set_ylabel("Predicted persistency")
ax.set_title("MNR — Binomial GLM: Predicted vs Actual Persistency (size = initial ADR count)")
plt.tight_layout()
plt.show()

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
coef_df.to_csv(f"{output_dir}\\loc_glm_coef_mnr_202604.csv")
print(coef_df.shape)
