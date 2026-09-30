import pyreadstat

path = r"O:\National\National Trend\Trend Studies\Inpatient Leading Indicator\2. Data Exports\LOC Valuation Exports\loc_ip_6_24_26_od.sas7bdat"

df, meta = pyreadstat.read_sas7bdat(path)

# check if member_appeal_ovtn_cnt exists
print("Columns matching 'member_appeal':")
print([c for c in df.columns if "member_appeal" in c.lower()])

# sum for admit_week 202618
result = (
    df
    .query("admit_week == 202618")
    ["member_appeal_ovtn_cnt"]
    .sum()
)

print(f"\nSUM(member_appeal_ovtn_cnt) where admit_week == 202618: {result}")
