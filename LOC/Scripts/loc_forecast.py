# %%
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
from prophet import Prophet
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

kpi_query = """
    select
        admit_week
        , sum(case_count) as case_count
        , sum(initial_adr_cnt) as initial_adr_cnt
        , sum(persistent_adr_cnt) as persistent_adr_cnt
        , sum(p2p_case_cnt) as p2p_case_cnt
        , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
        , sum(member_appeal_cnt) as member_appeal_cnt
        , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
        , sum(appeal_case_cnt) as appeal_case_cnt
        , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
        , sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
        , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
    from tmp_1m.kn_loc_notif_04222026_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'IPA'
        and loc_flag = 1
    group by 1
    order by 1
"""

mm_query = """
    select
        hce_admit_month
        , sum(membership) as membership
    from tmp_1m.kn_loc_notif_04222026_od
    where mnr_total_ffs_flag = 1
        and ipa_pac_flag = 'MM'
        and loc_flag = 1
    group by 1
    order by 1
"""

weekly = pd.read_sql(kpi_query, engine)
membership = pd.read_sql(mm_query, engine)

engine.dispose()
print(weekly.shape, membership.shape)

# %%
# check admit_week format before converting
weekly["admit_week"].dtype, weekly["admit_week"].head(10)

# %%
weekly["ds"] = pd.to_datetime(weekly["admit_week"])

# year-specific dollar rate
rate_map = {2024: 8300, 2025: 8600, 2026: 8900}
weekly["year"] = weekly["ds"].dt.year
weekly["dollar_rate"] = weekly["year"].map(rate_map)
weekly["financial_value"] = weekly["persistent_adr_cnt"] * weekly["dollar_rate"]
weekly["persistency"] = weekly["persistent_adr_cnt"] / weekly["initial_adr_cnt"].replace(0, np.nan)

# merge monthly membership onto weekly — divide by 4.33 to approximate weekly enrollment
membership["ym"] = pd.to_datetime(membership["hce_admit_month"].astype(str), format = "%Y%m").dt.to_period("M").astype(str)
weekly["ym"] = weekly["ds"].dt.to_period("M").astype(str)
weekly = weekly.merge(membership[["ym", "membership"]], on = "ym", how = "left")
weekly["membership_weekly"] = weekly["membership"] / 4.33

weekly.describe().round(1)

# %%
# raw time series — look before fitting
kpis = [
    "case_count", "initial_adr_cnt", "persistent_adr_cnt",
    "p2p_case_cnt", "p2p_ovrtn_case_cnt",
    "member_appeal_cnt", "member_appeal_ovtn_cnt",
    "appeal_case_cnt", "appeal_ovrtn_case_cnt",
    "mcr_reconsideration_case_cnt", "mcr_ovrtn_case_cnt",
    "financial_value"
]

n_cols = 3
n_rows = int(np.ceil(len(kpis) / n_cols))
fig, axes = plt.subplots(n_rows, n_cols, figsize = (16, 4 * n_rows), squeeze = False)
axes = axes.flatten()

for i, kpi in enumerate(kpis):
    axes[i].plot(weekly["ds"], weekly[kpi], linewidth = 1, color = "steelblue")
    axes[i].set_title(kpi, fontsize = 9)

for j in range(len(kpis), len(axes)):
    axes[j].set_visible(False)

plt.suptitle("MNR FFS — Weekly KPI Raw Time Series", fontsize = 12)
plt.tight_layout()
plt.show()


# %% [markdown]
# ## Model 1 — Univariate Prophet (all KPIs independently)

# %%
# fit one Prophet model per KPI — no regressors, just own history
forecasts = {}

for kpi in kpis:
    prophet_df = weekly[["ds", kpi]].rename(columns = {kpi: "y"}).dropna()

    m = Prophet(yearly_seasonality = True, weekly_seasonality = True, daily_seasonality = False)
    m.fit(prophet_df)

    future = m.make_future_dataframe(periods = 12, freq = "W")
    fc = m.predict(future)

    forecasts[kpi] = {"model": m, "forecast": fc, "actuals": prophet_df}

print("models fit:", len(forecasts))

# %%
fig, axes = plt.subplots(n_rows, n_cols, figsize = (16, 5 * n_rows), squeeze = False)
axes = axes.flatten()

for i, kpi in enumerate(kpis):
    fc = forecasts[kpi]["forecast"]
    actuals = forecasts[kpi]["actuals"]
    ax = axes[i]

    ax.fill_between(fc["ds"], fc["yhat_lower"], fc["yhat_upper"], alpha = 0.2, color = "steelblue")
    ax.plot(fc["ds"], fc["yhat"], color = "steelblue", linewidth = 1.2)
    ax.plot(actuals["ds"], actuals["y"], color = "k", linewidth = 0.8, alpha = 0.7)
    ax.axvline(actuals["ds"].max(), color = "grey", linestyle = "--", linewidth = 0.8)
    ax.set_title(kpi, fontsize = 9)

for j in range(len(kpis), len(axes)):
    axes[j].set_visible(False)

plt.suptitle("Model 1 — Univariate: 12-Week Forecast per KPI (shaded = 95% CI)", fontsize = 12)
plt.tight_layout()
plt.show()


# %% [markdown]
# ## Model 2 — Two-stage: persistency rate × initial ADR → financial value
#
# Rationale: persistent_adr_cnt = persistency_rate × initial_adr_cnt
# Forecasting the rate and the count separately then multiplying may be more
# stable than forecasting persistent_adr_cnt directly, since the rate is
# more stationary than the raw count.

# %%
# stage 1a — forecast initial_adr_cnt
prophet_init = weekly[["ds", "initial_adr_cnt"]].rename(columns = {"initial_adr_cnt": "y"}).dropna()

m_init = Prophet(yearly_seasonality = True, weekly_seasonality = True, daily_seasonality = False)
m_init.fit(prophet_init)

future_init = m_init.make_future_dataframe(periods = 12, freq = "W")
fc_init = m_init.predict(future_init)[["ds", "yhat"]].rename(columns = {"yhat": "initial_adr_forecast"})

# %%
# stage 1b — forecast persistency rate
# clip predictions to [0, 1] since Prophet is unconstrained
prophet_pers = weekly[["ds", "persistency"]].rename(columns = {"persistency": "y"}).dropna()

m_pers = Prophet(yearly_seasonality = True, weekly_seasonality = True, daily_seasonality = False)
m_pers.fit(prophet_pers)

future_pers = m_pers.make_future_dataframe(periods = 12, freq = "W")
fc_pers = m_pers.predict(future_pers)[["ds", "yhat"]].rename(columns = {"yhat": "persistency_forecast"})
fc_pers["persistency_forecast"] = fc_pers["persistency_forecast"].clip(0, 1)

# %%
# stage 2 — combine: persistent count = persistency × initial ADR
fc_2stage = fc_init.merge(fc_pers, on = "ds", how = "inner")
fc_2stage["persistent_cnt_forecast"] = fc_2stage["persistency_forecast"] * fc_2stage["initial_adr_forecast"]

fc_2stage["year"] = fc_2stage["ds"].dt.year
fc_2stage["dollar_rate"] = fc_2stage["year"].map(rate_map).fillna(8900)
fc_2stage["financial_value_2stage"] = fc_2stage["persistent_cnt_forecast"] * fc_2stage["dollar_rate"]

# actual financial value for comparison
actuals_fin = weekly[["ds", "financial_value"]].dropna()

fc_2stage_full = fc_2stage.merge(actuals_fin, on = "ds", how = "left")
fc_2stage_full.tail(14)

# %%
fig, ax = plt.subplots(figsize = (12, 5))
ax.plot(fc_2stage_full["ds"], fc_2stage_full["financial_value"], color = "k",
        linewidth = 0.8, alpha = 0.7, label = "actual")
ax.plot(fc_2stage_full["ds"], fc_2stage_full["financial_value_2stage"], color = "steelblue",
        linewidth = 1.5, label = "2-stage forecast")
ax.axvline(actuals_fin["ds"].max(), color = "grey", linestyle = "--", linewidth = 0.8)
ax.set_title("Model 2 — Two-Stage: Financial Value Forecast (persistency rate × initial ADR count)")
ax.set_ylabel("$ value")
ax.legend()
plt.tight_layout()
plt.show()


# %% [markdown]
# ## Model 3 — case_count with membership as regressor
#
# Rationale: raw case_count trends are partly driven by enrollment changes.
# Adding membership as a regressor isolates the utilization signal from
# the enrollment signal, giving a cleaner trend forecast.

# %%
# build Prophet df with membership_weekly as regressor
prophet_cc = (
    weekly[["ds", "case_count", "membership_weekly"]]
    .rename(columns = {"case_count": "y"})
    .dropna()
)

m_cc = Prophet(yearly_seasonality = True, weekly_seasonality = True, daily_seasonality = False)
m_cc.add_regressor("membership_weekly")
m_cc.fit(prophet_cc)

# future membership: flat projection at last known value
future_cc = m_cc.make_future_dataframe(periods = 12, freq = "W")
future_cc["membership_weekly"] = prophet_cc["membership_weekly"].iloc[-1]

fc_cc = m_cc.predict(future_cc)

# %%
fig, ax = plt.subplots(figsize = (12, 5))
ax.fill_between(fc_cc["ds"], fc_cc["yhat_lower"], fc_cc["yhat_upper"], alpha = 0.2, color = "steelblue")
ax.plot(fc_cc["ds"], fc_cc["yhat"], color = "steelblue", linewidth = 1.5, label = "forecast")
ax.plot(prophet_cc["ds"], prophet_cc["y"], color = "k", linewidth = 0.8, alpha = 0.7, label = "actual")
ax.axvline(prophet_cc["ds"].max(), color = "grey", linestyle = "--", linewidth = 0.8)
ax.set_title("Model 3 — case_count Forecast with Membership Regressor (flat enrollment projection)")
ax.set_ylabel("case_count")
ax.legend()
plt.tight_layout()
plt.show()

# %%
# compare Model 1 vs Model 3 for case_count
fc_m1_cc = forecasts["case_count"]["forecast"][["ds", "yhat"]].rename(columns = {"yhat": "m1_forecast"})
fc_m3_cc = fc_cc[["ds", "yhat"]].rename(columns = {"yhat": "m3_forecast"})

compare = (
    fc_m1_cc
    .merge(fc_m3_cc, on = "ds")
    .merge(prophet_cc[["ds", "y"]].rename(columns = {"y": "actual"}), on = "ds", how = "left")
)

fig, ax = plt.subplots(figsize = (12, 5))
ax.plot(compare["ds"], compare["actual"], color = "k", linewidth = 0.8, alpha = 0.7, label = "actual")
ax.plot(compare["ds"], compare["m1_forecast"], color = "steelblue", linewidth = 1.2,
        linestyle = "--", label = "Model 1 (univariate)")
ax.plot(compare["ds"], compare["m3_forecast"], color = "crimson", linewidth = 1.2, label = "Model 3 (membership regressor)")
ax.axvline(prophet_cc["ds"].max(), color = "grey", linestyle = "--", linewidth = 0.8)
ax.set_title("case_count: Model 1 vs Model 3 Forecast Comparison")
ax.legend()
plt.tight_layout()
plt.show()


# %% [markdown]
# ## Export

# %%
# model 1 — all KPIs stacked
m1_frames = []

for kpi, result in forecasts.items():
    fc = result["forecast"][["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    actuals = result["actuals"].rename(columns = {"y": "actual"})
    fc = fc.merge(actuals, on = "ds", how = "left")
    fc["kpi"] = kpi
    fc["model"] = "univariate"
    fc["is_forecast"] = fc["actual"].isna()
    m1_frames.append(fc)

out_m1 = pd.concat(m1_frames, ignore_index = True)

# model 2 — financial value
out_m2 = fc_2stage_full[["ds", "financial_value_2stage", "financial_value",
                           "initial_adr_forecast", "persistency_forecast"]].copy()
out_m2["model"] = "two_stage"

# model 3 — case_count with membership
out_m3 = compare.copy()
out_m3["model"] = "membership_regressor"

output_dir = r"C:\Users\knguy139\Documents\Projects\Data\Output"
out_m1.to_csv(f"{output_dir}\\loc_forecast_m1_mnr_202604.csv", index = False)
out_m2.to_csv(f"{output_dir}\\loc_forecast_m2_mnr_202604.csv", index = False)
out_m3.to_csv(f"{output_dir}\\loc_forecast_m3_mnr_202604.csv", index = False)

print(out_m1.shape, out_m2.shape, out_m3.shape)
