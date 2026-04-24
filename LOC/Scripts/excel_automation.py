"""
Excel Automation Script
-----------------------
Automates:
  1. Copy-paste a specific range from source to destination workbook
  2. Update an OLE DB connection targeting a SAS table
  3. Extend an existing formula down to match new data rows
  4. Expand a chart's data range to include new rows

Requirements:
  - Python 3.x
  - xlwings: pip install xlwings
  - Microsoft Excel installed
"""

import xlwings as xw

# =============================================================================
# USER CONFIGURATION — Edit these variables before running
# =============================================================================

# --- 1. Copy-Paste Settings ---
SOURCE_FILE = r"C:\path\to\source.xlsx"
SOURCE_SHEET = "Sheet1"
SOURCE_RANGE = "A1:D100"  # Range to copy from

DEST_FILE = r"C:\path\to\destination.xlsx"
DEST_SHEET = "Sheet1"
DEST_CELL = "A1"  # Top-left cell to paste into

# --- 2. SAS OLE DB Connection Settings ---
SAS_CONNECTION_NAME = "MyConnection"  # Name of the existing connection in the workbook
SAS_NEW_CONNECTION_STRING = (
    "Provider=SAS.IOMProvider;Data Source=your_sas_server;"
    "User ID=your_user;Password=your_password;"
)
SAS_NEW_COMMAND_TEXT = "LIBNAME.NEW_TABLE"  # New table/query to point to

# --- 3. Formula Extension Settings ---
FORMULA_SHEET = "Sheet1"
FORMULA_COLUMN = "E"       # Column containing the formula to extend
FORMULA_START_ROW = 2      # First row that has the formula
DATA_COLUMN = "A"          # Reference column to determine last data row

# --- 4. Chart Update Settings ---
CHART_SHEET = "Sheet1"
CHART_NAME = "Chart 1"     # Name of the chart (right-click chart > name box)
CHART_DATA_RANGE = None     # Set to None for auto-detection, or e.g. "A1:D100"


# =============================================================================
# FUNCTIONS
# =============================================================================

def copy_paste_range(app):
    """Copy a range from the source workbook and paste values into the destination."""
    print(f"Opening source: {SOURCE_FILE}")
    src_wb = app.books.open(SOURCE_FILE)
    src_sheet = src_wb.sheets[SOURCE_SHEET]
    data = src_sheet.range(SOURCE_RANGE).value

    print(f"Opening destination: {DEST_FILE}")
    dst_wb = app.books.open(DEST_FILE)
    dst_sheet = dst_wb.sheets[DEST_SHEET]

    # Determine paste area dimensions
    src_range = src_sheet.range(SOURCE_RANGE)
    rows = src_range.rows.count
    cols = src_range.columns.count
    dst_range = dst_sheet.range(DEST_CELL).resize(rows, cols)

    print(f"Pasting {rows} rows x {cols} cols into {DEST_SHEET}!{DEST_CELL}")
    dst_range.value = data

    src_wb.close()
    print("Copy-paste complete.")
    return dst_wb


def update_sas_connection(wb):
    """Update the OLE DB connection string and command text, then refresh."""
    print(f"Updating SAS connection: '{SAS_CONNECTION_NAME}'")
    connections = wb.api.Connections

    conn_found = False
    for i in range(1, connections.Count + 1):
        conn = connections.Item(i)
        if conn.Name == SAS_CONNECTION_NAME:
            oledb = conn.OLEDBConnection
            oledb.Connection = SAS_NEW_CONNECTION_STRING
            oledb.CommandText = SAS_NEW_COMMAND_TEXT
            print(f"  Connection string updated.")
            print(f"  Command text set to: {SAS_NEW_COMMAND_TEXT}")
            print("  Refreshing connection...")
            oledb.Refresh()
            conn_found = True
            break

    if not conn_found:
        available = [connections.Item(i).Name for i in range(1, connections.Count + 1)]
        print(f"  WARNING: Connection '{SAS_CONNECTION_NAME}' not found.")
        print(f"  Available connections: {available}")
        return

    print("SAS connection update complete.")


def extend_formula(wb):
    """Extend an existing formula down to match the last row of data."""
    sheet = wb.sheets[FORMULA_SHEET]

    # Find last row with data in the reference column
    last_data_row = sheet.range(f"{DATA_COLUMN}1").end("down").row
    print(f"Last data row in column {DATA_COLUMN}: {last_data_row}")

    # Current formula cell
    formula_cell = sheet.range(f"{FORMULA_COLUMN}{FORMULA_START_ROW}")
    current_last = sheet.range(f"{FORMULA_COLUMN}{FORMULA_START_ROW}").end("down").row

    if current_last >= last_data_row:
        print(f"Formula already extends to row {current_last}. No extension needed.")
        return

    # Use AutoFill via COM to drag the formula down
    source = sheet.range(f"{FORMULA_COLUMN}{FORMULA_START_ROW}")
    fill_range = sheet.range(
        f"{FORMULA_COLUMN}{FORMULA_START_ROW}:{FORMULA_COLUMN}{last_data_row}"
    )

    print(f"Extending formula from row {FORMULA_START_ROW} to row {last_data_row}")
    source.api.AutoFill(fill_range.api, 0)  # 0 = xlFillDefault
    print("Formula extension complete.")


def expand_chart_range(wb):
    """Expand the chart's data range to include all new rows."""
    sheet = wb.sheets[CHART_SHEET]

    # Find the chart
    chart_obj = None
    for co in sheet.charts:
        if co.name == CHART_NAME:
            chart_obj = co
            break

    if chart_obj is None:
        available = [c.name for c in sheet.charts]
        print(f"WARNING: Chart '{CHART_NAME}' not found on sheet '{CHART_SHEET}'.")
        print(f"Available charts: {available}")
        return

    if CHART_DATA_RANGE:
        # Use the manually specified range
        new_range = sheet.range(CHART_DATA_RANGE)
        print(f"Setting chart data range to: {CHART_DATA_RANGE}")
    else:
        # Auto-detect: use UsedRange as the chart source
        used = sheet.used_range
        new_range = used
        addr = used.address
        print(f"Auto-detected data range: {addr}")

    chart_obj.set_source_data(new_range)
    print("Chart range update complete.")


def main():
    """Run all automation steps in sequence."""
    print("=" * 50)
    print("Excel Automation Script")
    print("=" * 50)

    # Start Excel (visible so you can watch the automation)
    app = xw.App(visible=True)

    try:
        # Step 1: Copy-paste range
        print("\n--- Step 1: Copy-Paste Range ---")
        dst_wb = copy_paste_range(app)

        # Step 2: Update SAS connection
        print("\n--- Step 2: Update SAS Connection ---")
        update_sas_connection(dst_wb)

        # Step 3: Extend formula
        print("\n--- Step 3: Extend Formula ---")
        extend_formula(dst_wb)

        # Step 4: Expand chart data range
        print("\n--- Step 4: Expand Chart Range ---")
        expand_chart_range(dst_wb)

        # Save the destination workbook
        print("\n--- Saving ---")
        dst_wb.save()
        print(f"Saved: {DEST_FILE}")

        print("\n" + "=" * 50)
        print("All steps completed successfully.")
        print("=" * 50)

    except Exception as e:
        print(f"\nERROR: {e}")
        raise
    finally:
        # Leave Excel open so user can inspect results
        # To auto-close, uncomment: app.quit()
        pass


if __name__ == "__main__":
    main()
