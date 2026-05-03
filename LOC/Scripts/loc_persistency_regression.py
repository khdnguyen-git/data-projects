# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
import statsmodels.api as sm
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

# use the pre-aggregated agg table — hospital_group dimension only
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
df[["persistency", "adr_rate", "appeal_overturn_rate", "p2p_overturn_rate",
    "mcr_overturn_rate", "member_appeal_rate", "pre_auth_rate", "auth_per_k"]].hist(
    bins = 20, figsize = (14, 8), layout = (2, 4)
)
plt.suptitle("MNR hospital_group — Feature Distributions")
plt.tight_layout()
plt.show()

# %%
# correlation matrix
rate_cols = ["persistency", "adr_rate", "appeal_overturn_rate", "p2p_overturn_rate",
             "mcr_overturn_rate", "member_appeal_rate", "pre_auth_rate", "auth_per_k"]

corr = df[rate_cols].corr().round(2)

fig, ax = plt.subplots(figsize = (9, 7))
sns.heatmap(corr, annot = True, fmt = ".2f", cmap = "RdBu_r", center = 0,
            linewidths = 0.5, ax = ax)
ax.set_title("MNR hospital_group — Correlation Matrix")
plt.tight_layout()
plt.show()

# %%
# OLS regression: persistency ~ KPI rates
# weight by case_count so high-volume hospital groups carry more influence
features = ["adr_rate", "appeal_overturn_rate", "p2p_overturn_rate",
            "mcr_overturn_rate", "member_appeal_rate", "pre_auth_rate", "auth_per_k"]

sub = df[["persistency", "case_count"] + features].dropna()

X = sm.add_constant(sub[features])
y = sub["persistency"]
w = sub["case_count"]

model = sm.WLS(y, X, weights = w)
result = model.fit()
print(result.summary())

# %%
# coefficient plot — all predictors with confidence intervals
coef_df = pd.DataFrame({
    "coef": result.params,
    "lower": result.conf_int()[0],
    "upper": result.conf_int()[1],
    "pval": result.pvalues
}).drop("const").sort_values("coef")

colors = coef_df.apply(
    lambda r: "crimson" if r["pval"] < 0.05 and r["coef"] < 0 else
              "steelblue" if r["pval"] < 0.05 and r["coef"] > 0 else "lightgrey",
    axis = 1
)

fig, ax = plt.subplots(figsize = (8, 5))
ax.barh(coef_df.index, coef_df["coef"],
        xerr = [coef_df["coef"] - coef_df["lower"], coef_df["upper"] - coef_df["coef"]],
        color = colors, height = 0.6, capsize = 3, error_kw = {"linewidth": 0.8})
ax.axvline(0, color = "k", linewidth = 0.8)
ax.set_xlabel("OLS coefficient (effect on persistency rate)")
ax.set_title("MNR hospital_group — WLS Regression: Predictors of Persistency\n(colored = p < 0.05; grey = not significant)")
plt.tight_layout()
plt.show()

# %%
# residuals — which hospital groups are over/under-performing vs model prediction
sub = sub.copy()
sub["predicted"] = result.fittedvalues
sub["residual"] = sub["persistency"] - sub["predicted"]
sub["_dim_value"] = df.loc[sub.index, "_dim_value"]

# top outperformers and underperformers
top = pd.concat([
    sub.nlargest(10, "residual")[["_dim_value", "persistency", "predicted", "residual", "case_count"]],
    sub.nsmallest(10, "residual")[["_dim_value", "persistency", "predicted", "residual", "case_count"]]
])
top

# %%
output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
coef_df.to_csv(f"{output_dir}\\loc_regression_coef_mnr_202604.csv")
sub.sort_values("residual").to_csv(f"{output_dir}\\loc_regression_residuals_mnr_202604.csv", index = False)
print(sub.shape)
