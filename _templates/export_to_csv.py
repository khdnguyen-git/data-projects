import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# --- CONFIGURE ---
table = "tmp_1m.kn_loc_od_p2p_oah_05132026_ranked"
output_path = r"C:\Users\knguy139\Documents\Projects\Data\Output"
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
filename = f"{table.split('.')[-1]}_{timestamp}.csv"

# --- CONNECT ---
engine = create_engine(URL(
    account       = "UHG-UHGDWAAS",
    user          = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role          = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse     = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database      = "VING_PRD_TREND_DB",
    schema        = "TMP_1M"
))

# --- EXPORT ---
df = pd.read_sql(f"select * from {table}", engine)
df.to_csv(f"{output_path}\\{filename}", index = False)
print(f"Exported {len(df):,} rows to {output_path}\\{filename}")

engine.dispose()
