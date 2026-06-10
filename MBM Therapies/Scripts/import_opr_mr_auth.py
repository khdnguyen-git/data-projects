# %%
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# %%
# --- Section 1: Single-file import (pipe-delimited .txt) ---
input_path = r"C:\Users\knguy139\Documents\Projects\Data\Input\OPR_MR_AUTH_2024901_20260505.txt"

col_names = [
    "AUTHNUMBER",
    "DATERECEIVED",
    "DATEREVIEWED",
    "REQSTART",
    "REQEND",
    "AUTHSTART",
    "AUTHEND",
    "HEALTHPLANPATIENTID",
    "PROVIDERSPECIALTY",
    "TINNUMBER",
    "PATIENTSTATE",
    "GROUPNUMBER",
    "VISITREQ",
    "VISITAUTH",
    "REVIEWDECISION",
    "AUTO_APPROVAL",
    "DIAGCODE",
    "DIAGCODEDESC",
]

df = pd.read_csv(input_path, sep = "|", header = None, names = col_names, dtype = str)
print(f"Loaded: {df.shape[0]:,} rows x {df.shape[1]} cols")
df.head()


import pandas as pd

input_path = r"C:\Users\knguy139\Documents\Projects\Data\Input\OPR_MR_AUTH_2024901_20260505.txt"

col_names = [
    "AUTHNUMBER", "DATERECEIVED", "DATEREVIEWED", "REQSTART", "REQEND",
    "AUTHSTART", "AUTHEND", "HEALTHPLANPATIENTID", "PROVIDERSPECIALTY",
    "TINNUMBER", "PATIENTSTATE", "GROUPNUMBER", "VISITREQ", "VISITAUTH",
    "REVIEWDECISION", "AUTO_APPROVAL", "DIAGCODE", "DIAGCODEDESC",
]

df = pd.read_csv(input_path, sep="|", header=None, names=col_names, dtype=str)

df["DATE_RECEIVED_MTH"] = pd.to_datetime(
    df["DATERECEIVED"].str.split(":").str[0], format="%d%b%Y"
).dt.strftime("%Y%m")

result = (
    df.groupby(["DATE_RECEIVED_MTH", "PROVIDERSPECIALTY"])["AUTHNUMBER"]
    .nunique()
    .reset_index(name = "distinct_auth_count")
    .pivot(index = "DATE_RECEIVED_MTH", columns = "PROVIDERSPECIALTY",
values="distinct_auth_count")
    .fillna(0)
    .astype(int)
    .sort_index()
)

result["TOTAL"] = result.sum(axis=1)

print(result)


input_path = r"C:\Users\knguy139\Documents\Projects\Data\Input\OPR_MR_AUTH_2024901_20260505.txt"

col_names = [
    "AUTHNUMBER", "DATERECEIVED", "DATEREVIEWED", "REQSTART", "REQEND",
    "AUTHSTART", "AUTHEND", "HEALTHPLANPATIENTID", "PROVIDERSPECIALTY",
    "TINNUMBER", "PATIENTSTATE", "GROUPNUMBER", "VISITREQ", "VISITAUTH",
    "REVIEWDECISION", "AUTO_APPROVAL", "DIAGCODE", "DIAGCODEDESC",
]

df = pd.read_csv(input_path, sep = "|", header = None, names = col_names, dtype =
str)

df["DATE_RECEIVED_MTH"] = (
    pd.to_datetime(df["DATERECEIVED"].str.split(":").str[0], format = "%d%b%Y")
    .dt.strftime("%Y%m")
)

# monthly distinct auth count by specialty, filtered to AUTO_APPROVAL = 'Y'
result_y = (
    df
    .query("AUTO_APPROVAL == 'Y'")
    .groupby(["DATE_RECEIVED_MTH", "PROVIDERSPECIALTY"])["AUTHNUMBER"]
    .nunique()
    .reset_index(name = "distinct_auth_count")
    .pivot(index = "DATE_RECEIVED_MTH", columns = "PROVIDERSPECIALTY", values =
"distinct_auth_count")
    .fillna(0)
    .astype(int)
    .sort_index()
)
result_y["TOTAL"] = result_y.sum(axis = 1)

# monthly distinct auth count by specialty, filtered to AUTO_APPROVAL = 'N'
result_n = (
    df
    .query("AUTO_APPROVAL == 'N'")
    .groupby(["DATE_RECEIVED_MTH", "PROVIDERSPECIALTY"])["AUTHNUMBER"]
    .nunique()
    .reset_index(name = "distinct_auth_count")
    .pivot(index = "DATE_RECEIVED_MTH", columns = "PROVIDERSPECIALTY", values =
"distinct_auth_count")
    .fillna(0)
    .astype(int)
    .sort_index()
)
result_n["TOTAL"] = result_n.sum(axis = 1)

print("AUTO_APPROVAL = 'Y'")
print(result_y)
print("\nAUTO_APPROVAL = 'N'")
print(result_n)




# %%
conn = snowflake.connector.connect(
    account       = "UHG-UHGDWAAS",
    user          = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role          = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse     = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database      = "VING_PRD_TREND_DB",
    schema        = "TMP_1Q",
)

# %%
table_name = "OPR_MR_AUTH_20240901_20260505"

success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name,
    auto_create_table = True,
    overwrite = True,
)

print(f"Uploaded {nrows:,} rows in {nchunks} chunks to tmp_1q.{table_name}")
conn.close()

# %% [markdown]
# ## Validation — Section 1
# ```sql
# select count(*), count(distinct healthplanpatientid) from tmp_1q.opr_mr_auth_202409_20260505;
# select count(*) from tmp_1q.opr_mr_auth_202409_20260505 where healthplanpatientid is null;
# ```

# %% [markdown]
# ---
# ## Section 2: Stacked import (Sep 2024 – Oct 2025)

# %%
input_dir = r"C:\Users\knguy139\Documents\Projects\Data\Input"

files = [
    "OPR_MR_AUTH_20240901_20241231.csv",
    "OPR_MR_AUTH_20250101_20250430.csv",
    "OPR_MR_AUTH_20250501_20250831.csv",
    "OPR_MR_AUTH_20250901_20251031.csv",
]

# %%
dfs = []
for f in files:
    tmp = pd.read_csv(f"{input_dir}\\{f}", sep = ",", dtype = str)
    tmp.columns = [c.upper() for c in tmp.columns]
    print(f"{f}: {tmp.shape[0]:,} rows")
    dfs.append(tmp)

df = pd.concat(dfs, ignore_index = True)
print(f"\nStacked total: {df.shape[0]:,} rows x {df.shape[1]} cols")
df.head()

# %%
conn = snowflake.connector.connect(
    account       = "UHG-UHGDWAAS",
    user          = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role          = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse     = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database      = "VING_PRD_TREND_DB",
    schema        = "TMP_1Q",
)

# %%
table_name = "OPR_MR_AUTH_20240901_20251031"

success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name,
    auto_create_table = True,
    overwrite = True,
)

print(f"Uploaded {nrows:,} rows in {nchunks} chunks to tmp_1q.{table_name}")
conn.close()

# %% [markdown]
# ## Validation — Section 2
# ```sql
# select count(*), count(distinct healthplanpatientid) from tmp_1q.opr_mr_auth_20240901_20251031;
# select count(*) from tmp_1q.opr_mr_auth_20240901_20251031 where healthplanpatientid is null;
# ```