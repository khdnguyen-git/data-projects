# %%
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import glob
import os

# %%
input_dir = r"C:\Users\knguy139\Documents\Projects\Data\Input"

files = sorted(glob.glob(os.path.join(input_dir, "OPR_MR_AUTH_*.csv")))
# drop the head(200) sample file
files = [f for f in files if "head" not in f]

print(f"Files to stack: {len(files)}")
for f in files:
    print(f"  {os.path.basename(f)}")

# %%
dfs = []
for f in files:
    df = pd.read_csv(f, dtype = str)
    df["source_file"] = os.path.basename(f)
    dfs.append(df)
    print(f"{os.path.basename(f)}: {len(df):,} rows")

stacked = pd.concat(dfs, ignore_index = True)
print(f"\nTotal stacked: {stacked.shape[0]:,} rows x {stacked.shape[1]} cols")

# %%
# quick checks
stacked.shape
stacked.head()
stacked.dtypes

# %%
conn = snowflake.connector.connect(
    account       = "UHG-UHGDWAAS",
    user          = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role          = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse     = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database      = "VING_PRD_TREND_DB",
    schema        = "TMP_1Y"
)

# %%
table_name = "kn_opr_mr_auth_202409_20251031_202511_raw"

# uppercase column names to match Snowflake convention
stacked.columns = [c.upper() for c in stacked.columns]

success, nchunks, nrows, _ = write_pandas(
    conn,
    stacked,
    table_name,
    auto_create_table = True,
    overwrite = True
)

print(f"Uploaded {nrows:,} rows in {nchunks} chunks to tmp_1y.{table_name}")
conn.close()

# %% [markdown]
# ## Validation
# ```sql
# select count(*), count(distinct healthplanpatientid) from tmp_1y.kn_opr_mr_auth_202409_20251031_202511_raw;
# select count(*) from tmp_1y.kn_opr_mr_auth_202409_20251031_202511_raw where healthplanpatientid is null;
# ```
