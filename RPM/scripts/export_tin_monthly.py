import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

engine = create_engine(URL(
    account = "UHG-UHGDWAAS",
    user = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database = "VING_PRD_TREND_DB",
    schema = "TMP_1M"
))

df = pd.read_sql("""
    select
        d.prov_tin
        , d.market_fnl as market
        , coalesce(max(s.provider_group_name), max(s.provider_name), max(d.full_nm)) as provider_name
        , max(s.sig_ramp) as sig_ramp
        , d.fst_srvc_month
        , count(distinct d.mbi) as total_mbrs
        , round(sum(d.allw_amt_fnl), 2) as total_allowed
        , count(*) as total_lines
    from tmp_1m.kn_rpm_202608_detail as d
    left join tmp_1m.kn_rpm_202608_fraud_scores as s
        on d.prov_tin = s.prov_tin
    where d.fst_srvc_year >= 2021
    group by
        d.prov_tin
        , d.market_fnl
        , d.fst_srvc_month
    order by
        d.prov_tin
        , d.fst_srvc_month
""", engine)

engine.dispose()

print(f"{len(df):,} rows exported")
df.to_csv(r"c:\Users\knguy139\Documents\Projects\Data\Output\kn_rpm_tin_monthly_summary.csv", index = False)
print("Done.")
