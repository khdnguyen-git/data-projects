import pyreadstat

file_path = r"O:\National\National Trend\Trend Studies\Inpatient Leading Indicator\2. Data Exports\LOC Valuation Exports\loc_ip_5_13_26_od_v2.sas7bdat"

df, meta = pyreadstat.read_sas7bdat(file_path)

result = df.loc[
    (df["admit_week"] == "202616")
    & (df["mnr_total_ffs_flag"] == 1),
    "case_count"
].sum()

print(result)


import pyreadstat

df, meta = pyreadstat.read_sas7bdat(r"O:\National\National Trend\Trend Studies\Inpatient Leading Indicator\2. Data Exports\LOC Valuation Exports\loc_ip_5_13_26_od_v2.sas7bdat")

(df
 .query("ADMIT_WEEK == '202616' and MNR_TOTAL_FFS_FLAG == 1")
 ["CASE_COUNT"].sum()
)

df.head(20)


(
    df
    .query("ADMIT_WEEK == 202617 and MNR_TOTAL_FFS_FLAG == 1")
    ["CASE_COUNT"]
    .sum()
)


from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns

engine = create_engine(URL(
    account = "UHG-UHGDWAAS",
    user = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database = "VING_PRD_TREND_DB",
    schema = "TMP_1M"
))



pd.read_sql("""
select
sum(case_count)
from TMP_1M.EC_IP_DATASET_LOC_05062026_OD
where admit_week = '202617' and mnr_total_ffs_flag = 1
""", engine)