"""
Excel Automation Script
-----------------------
Automates:
  0. Create a new copy of the destination file with updated date in the name
  1. Copy-paste a specific range from source to destination workbook
  2. Update an OLE DB connection targeting a SAS table
  3. Extend an existing formula down to match new data rows
  4. Expand a chart's data range to include new rows

Requirements:
  - Python 3.x
  - xlwings: pip install xlwings
  - Microsoft Excel installed
"""

import glob
import os
import shutil
from datetime import datetime
from tkinter import Tk, filedialog

import xlwings as xw

# =============================================================================
# USER CONFIGURATION — Edit these variables before running
# =============================================================================

# --- Default folders for file picker dialogs ---
SOURCE_DEFAULT_DIR = r"O:\National\National Trend\Trend Studies\Inpatient Leading Indicator"

DEST_DIRS = {
    "1": ("M&R",      r"O:\National\Clinical\ICM LOC\LOC Performance Tool\4. 2026 Affordability\M&R"),
    "2": ("OAH",      r"O:\National\Clinical\ICM LOC\LOC Performance Tool\4. 2026 Affordability\OAH"),
    "3": ("C&S Duals", r"O:\National\Clinical\ICM LOC\LOC Performance Tool\4. 2026 Affordability\C&S Duals"),
}

DEST_BASE_NAME = "ICM LOC Performance Tracker {date}_{month_year}_Updating.xlsx"

# --- Copy-Paste Settings ---
SOURCE_SHEET = "0. Inputs"
DEST_SHEET = "1. Assumptions & Slicers"

# --- SAS OLE DB Connection Settings ---
SAS_CONNECTION_NAME = "MyConnection"
SAS_EXPORT_DIR = r"O:\National\National Trend\Trend Studies\Inpatient Leading Indicator\2. Data Exports\LOC Valuation Exports"

# --- Chart Update Settings ---
CHART_SHEET = "Sheet1"
CHART_NAME = "Chart 1"
CHART_DATA_RANGE = None  # None = auto-detect from UsedRange


# =============================================================================
# FILE SELECTION — pick source and destination via dialog
# =============================================================================

# population choice first, before any dialogs
print("\nWhich population?")
for key, (label, _) in DEST_DIRS.items():
    print(f"  {key}. {label}")
choice = input("Enter 1/2/3 [default=1 M&R]: ").strip() or "1"

if choice not in DEST_DIRS:
    raise SystemExit(f"Invalid choice '{choice}' — exiting.")

pop_label, dest_default_dir = DEST_DIRS[choice]
print(f"Selected: {pop_label}")

root = Tk()
root.withdraw()
root.attributes("-topmost", True)  # force dialogs to front

SOURCE_FILE = filedialog.askopenfilename(
    parent=root,
    title="Select the SOURCE file",
    initialdir=SOURCE_DEFAULT_DIR,
    filetypes=[("Excel files", "*.xlsx *.xlsm *.xls")],
)
if not SOURCE_FILE:
    raise SystemExit("No source file selected — exiting.")

DEST_FILE = filedialog.askopenfilename(
    parent=root,
    title=f"Select the DESTINATION file ({pop_label})",
    initialdir=dest_default_dir,
    filetypes=[("Excel files", "*.xlsx *.xlsm *.xls")],
)
if not DEST_FILE:
    raise SystemExit("No destination file selected — exiting.")

root.destroy()

print(f"Source:      {SOURCE_FILE}")
print(f"Destination: {DEST_FILE}")


# =============================================================================
# STEP 0: Read date fields from source & create a new copy of the destination
# =============================================================================

app = xw.App(visible=True)

# open both files once — reuse throughout the script
src_wb = app.books.open(SOURCE_FILE, read_only=True)
inputs_sheet = src_wb.sheets["0. Inputs"]

# C2 = date like "4.22.2026", C5 = YYYYMM like "202604"
raw_c2 = inputs_sheet.range("C2").value
raw_c5 = inputs_sheet.range("C5").value

# C2 may come back as a datetime object if Excel stores it as a date
if isinstance(raw_c2, datetime):
    raw_date = f"{raw_c2.month}.{raw_c2.day}.{raw_c2.year}"
else:
    raw_date = str(raw_c2).strip()

# C5 may come back as a float (202604.0) if stored as a number
raw_yyyymm = str(int(raw_c5))

# convert YYYYMM to "Month Year" for the filename
month_year = datetime.strptime(raw_yyyymm[:6], "%Y%m").strftime("%B %Y")

new_name = DEST_BASE_NAME.format(date=raw_date, month_year=month_year)
dest_dir = os.path.dirname(DEST_FILE)
dest_path = os.path.join(dest_dir, new_name)

shutil.copy2(DEST_FILE, dest_path)
print(f"Step 0: Created copy -> {dest_path}")

old_dst_wb = app.books.open(DEST_FILE, read_only=True)
dst_wb = app.books.open(dest_path)


# =============================================================================
# STEP 1: Copy specific cells from source to destination
#         Source "0. Inputs" -> Dest "1. Assumptions & Slicers"
#         C2 -> C8, C3 -> C9, C7 -> C12
# =============================================================================

src_sheet = src_wb.sheets[SOURCE_SHEET]
dst_sheet = dst_wb.sheets[DEST_SHEET]

# cell-by-cell mapping: source -> destination
val_c2 = src_sheet.range("C2").value
val_c3 = src_sheet.range("C3").value
val_c7 = src_sheet.range("C7").value

dst_sheet.range("C8").value = val_c2
dst_sheet.range("C9").value = val_c3
dst_sheet.range("C12").value = val_c7

print(f"Step 1: Copied C2->{val_c2}, C3->{val_c3}, C7->{val_c7}")


# =============================================================================
# STEP 1b: On destination, copy B89:E145 -> H89:K145, then set B89 to new date
# =============================================================================

# shift existing data to the right before overwriting with new values
old_data = dst_sheet.range("B89:E145").value
dst_sheet.range("H89").resize(len(old_data), len(old_data[0])).value = old_data

# B89 gets the new date value (same as what we just pasted into C8)
dst_sheet.range("B89").value = val_c2

# paste new data from source into the destination
src_data = src_sheet.range("C20:E74").value
dst_sheet.range("C91").resize(len(src_data), len(src_data[0])).value = src_data

print(f"Step 1b: Copied B89:E145 -> H89:K145, set B89 = {val_c2}, pasted C20:E74 -> C91:E145")


# =============================================================================
# STEP 2: Update the SAS OLE DB connection command text to the latest export
# =============================================================================

# find the most recently modified .sas7bdat file in the exports folder
sas_files = glob.glob(os.path.join(SAS_EXPORT_DIR, "*.sas7bdat"))
if not sas_files:
    print(f"Step 2: WARNING — no .sas7bdat files found in {SAS_EXPORT_DIR}")
else:
    latest_sas = max(sas_files, key=os.path.getmtime)
    print(f"Step 2: Latest SAS file -> {latest_sas}")

    connections = dst_wb.api.Connections

    conn_found = False
    for i in range(1, connections.Count + 1):
        conn = connections.Item(i)
        if conn.Name == SAS_CONNECTION_NAME:
            oledb = conn.OLEDBConnection
            oledb.CommandText = latest_sas
            oledb.Refresh()
            conn_found = True
            print(f"Step 2: Updated command text and refreshed '{SAS_CONNECTION_NAME}'")
            break

    if not conn_found:
        available = [connections.Item(i).Name for i in range(1, connections.Count + 1)]
        print(f"Step 2: WARNING — connection '{SAS_CONNECTION_NAME}' not found. Available: {available}")


# =============================================================================
# STEP 2b: Print refreshed values from "Monthly Auths" as a sanity check
# =============================================================================

auths_sheet = src_wb.sheets["Monthly Auths"]
col_f_name = auths_sheet.range("F4").value
col_g_name = auths_sheet.range("G4").value

# find first empty row in F starting at row 29, then stop 2 rows before it
row = 29
while auths_sheet.range(f"F{row}").value is not None and auths_sheet.range(f"F{row}").value != "":
    row += 1
last_print_row = row - 2

print(f"\n  --- {col_f_name} ---")
for r in range(29, last_print_row + 1):
    month = auths_sheet.range(f"B{r}").value
    val = auths_sheet.range(f"F{r}").value
    print(f"  {col_f_name} for {month}: {val:.2%}")

print(f"\n  --- {col_g_name} ---")
for r in range(29, last_print_row + 1):
    month = auths_sheet.range(f"B{r}").value
    val = auths_sheet.range(f"G{r}").value
    print(f"  {col_g_name} for {month}: {val:.2%}")


# =============================================================================
# STEP 2c: Print corresponding values from destination "3a. Monthly"
# =============================================================================

monthly_sheet = dst_wb.sheets["3a. Monthly"]
col_e_name = monthly_sheet.range("E7").value
col_f_name_monthly = monthly_sheet.range("F7").value

# find first empty row in E starting at row 44, stop 2 rows before it
row = 44
while monthly_sheet.range(f"E{row}").value is not None and monthly_sheet.range(f"E{row}").value != "":
    row += 1
last_print_row = row - 2

print(f"\n  --- {col_e_name} (3a. Monthly) ---")
for r in range(44, last_print_row + 1):
    month = monthly_sheet.range(f"B{r}").value
    val = monthly_sheet.range(f"E{r}").value
    print(f"  {col_e_name} for {month}: {val:.2%}")

print(f"\n  --- {col_f_name_monthly} (3a. Monthly) ---")
for r in range(44, last_print_row + 1):
    month = monthly_sheet.range(f"B{r}").value
    val = monthly_sheet.range(f"F{r}").value
    print(f"  {col_f_name_monthly} for {month}: {val:.2%}")


# =============================================================================
# STEP 3: Extend "actual" formulas in 4a. Valuation down to the latest month
#         C59:AA59 = actual formulas, extend down until col B matches C11
# =============================================================================

val_sheet = dst_wb.sheets["4a. Valuation"]
assumptions_sheet = dst_wb.sheets["1. Assumptions & Slicers"]
latest_month = int(assumptions_sheet.range("C11").value)  # e.g. 202603

# find the row in column B that matches the latest month
target_row = None
for r in range(59, val_sheet.range("B59").end("down").row + 1):
    cell_val = val_sheet.range(f"B{r}").value
    # handle both float and int/string formats in column B
    if cell_val is not None and int(cell_val) == latest_month:
        target_row = r
        break

if target_row is None:
    print(f"Step 3: WARNING — could not find {latest_month} in 4a. Valuation column B")
elif target_row <= 59:
    print(f"Step 3: Formulas already cover row {target_row}, no extension needed")
else:
    # AutoFill C59:AA59 down to the target row
    source = val_sheet.range(f"C59:AA59")
    fill_range = val_sheet.range(f"C59:AA{target_row}")
    source.api.AutoFill(fill_range.api, 0)  # 0 = xlFillDefault
    print(f"Step 3: Extended C59:AA59 down to row {target_row} (month {latest_month})")


# =============================================================================
# STEP 4: Expand the chart's data range to include all new rows
# =============================================================================

sheet = dst_wb.sheets[CHART_SHEET]

chart_obj = None
for co in sheet.charts:
    if co.name == CHART_NAME:
        chart_obj = co
        break

if chart_obj is None:
    available = [c.name for c in sheet.charts]
    print(f"Step 4: WARNING — chart '{CHART_NAME}' not found. Available: {available}")
else:
    if CHART_DATA_RANGE:
        new_range = sheet.range(CHART_DATA_RANGE)
    else:
        new_range = sheet.used_range
    chart_obj.set_source_data(new_range)
    print(f"Step 4: Chart '{CHART_NAME}' data range updated to {new_range.address}")


# =============================================================================
# SAVE & DONE
# =============================================================================

dst_wb.save()
src_wb.close()
old_dst_wb.close()
print(f"\nSaved: {dest_path}")
print("Done.")
