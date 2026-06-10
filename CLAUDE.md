# UHC Project — Claude Code Guidelines

## SQL Formatting Rules

1. **Table creation**: always use `create or replace table`, never `drop table if exists` + `create table`
2. **Commas**: leading commas (`, column_name`), never trailing
3. **Aliases**: lowercase single-letter aliases with `as` — e.g., `left join <table> as a`
4. **Keywords**: all SQL syntax in lowercase (`select`, `from`, `left join`, `where`, `group by`, etc.)
5. **`case` statements**: single space before `then`, no column-aligning padding — `when x = 1 then 'y'`, not `when x = 1          then 'y'`
6. **Inequality operator**: use `!=`, never `<>`
7. **Spacing**: spaces around all operators (`=`, `!=`, `>=`, `<=`, `between`) — `col = 1` not `col=1`; space after leading comma — `, col` not `,col`
8. **CTEs over subqueries**: always use `with ... as (` CTEs instead of inline subqueries. Makes queries readable and debuggable.
9. **Indentation**: 1 tab (4 spaces) for all indented lines under `select`, `where`, `group by`, `order by`, and CTE bodies. Keyword goes on its own line, then each column/expression on a new line indented 4 spaces. Example:
   ```sql
   select
       col_a
       , col_b
   from table
   where col_a = 1
       and col_b = 2
   group by
       col_a
       , col_b
   ```

See `_templates/` for canonical examples of these patterns.

## Table Naming Convention

```
<schema>.<initials>_<projectname>_<topic>_<YYYYMM>
```

- **Schema**: `tmp_1m` (preferred write target)
- **Initials**: `kn` for prod, `knd` for dev
- **Example**: `tmp_1m.kn_loc_valuation_202604`

The date suffix uses the run/notification month in `YYYYMM` format.

## Canonical Table Sources

### Read-Only Schemas (NEVER write to these)

The following schemas are **read-only** source data. Never `create`, `replace`, `alter`, `insert`, `update`, `delete`, or `drop` any object in these schemas:

- `fichsrv` — claims fact tables (`glxy_pr_f`, `glxy_op_f`, `dcsp_*`, `nce_*`, `tre_membership`, `tadm_glxy_reason_code`, etc.)
- `hce_ops_archv` — archived membership snapshots (`gl_rstd_gpsgalnce_f_YYYYMM`)
- `hce_ops_stage` — staging tables (LOPA: `pa_trckng_op_evnt_lopa_dtl`, `pa_trckng_pr_evnt_lopa_dtl`, etc.)
- `hce_ops_fnl` — finalized operational tables (`hce_adr_avtar_like_25_26_f`, etc.)

**Write targets**: Only write to `tmp_*` schemas (`tmp_1m`, `tmp_1q`, `tmp_1y`). All created tables must have the `kn_` (prod) or `knd_` (dev) prefix.

### Claims

Pull from `fichsrv.*` tables. Use `union all` across entities as needed:

- `fichsrv.glxy_op_f` — COSMOS outpatient
- `fichsrv.glxy_pr_f` — COSMOS provider
- `fichsrv.dcsp_op_f` — CSP outpatient
- `fichsrv.dcsp_pr_f` — CSP provider
- `fichsrv.nce_op_f`  — NICE outpatient
- `fichsrv.nce_pr_f`  — NICE provider

See `_templates/claims_template.sql`.

### Membership

Pull from `fichsrv.tre_membership`. See `_templates/membership_template.sql`.

### Authorizations

Pull from `hce_ops_fnl.hce_adr_avtar_like_25_26_f` (HCE ADR AVTAR-like table).

See `_templates/auth_template.sql`.

## Population Segmentation — Default Rules

- **Default population**: `M&R FFS` — this includes DSNP members. Do NOT exclude DSNP unless explicitly told to.
- **DSNP exclusion**: Only applies to **Therapies** projects (MBM Therapies). In Therapies, the default population is `M&R FFS (excl. DSNP)`.
- When building population segments (OAH, M&R ISNP, M&R FFS, C&S DSNP, M&R DSNP, N/A), the `M&R FFS` bucket should NOT have a `fin_product_level_3 not in ('DUAL')` filter unless working on Therapies.

---

## Python / Data Science

### Snowflake connection

Always use SQLAlchemy `create_engine` with `snowflake-sqlalchemy`. Standard block:

```python
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

engine = create_engine(URL(
    account       = "UHG-UHGDWAAS",
    user          = "KHANG.NGUYEN@UHC.COM",
    authenticator = "externalbrowser",
    role          = "AZU_SDRP_VING_PRD_DEVELOPER_ROLE",
    warehouse     = "VING_PRD_MNR_HCE_DATAINFRA_WH",
    database      = "VING_PRD_TREND_DB",
    schema        = "TMP_1M"
))
```

Query with `pd.read_sql("select ...", engine)`. Dispose with `engine.dispose()` when done.

### CSV Export

Default output directory: `C:\Users\knguy139\Documents\Projects\Data\Output`. Filename = table name (without schema) + `.csv`. See `_templates/export_to_csv.py` for the standard pattern.

### Notebook workflow structure

Write notebooks sequentially, one block per step — mirrors R/SAS workflow:

1. **Load** — imports, connection, raw data pull
2. **EDA** — shape, dtypes, missing values, distributions
3. **Transform** — filters, derived columns, merges, reshaping
4. **Analyze** — aggregations, stats, model if applicable
5. **Visualize / Report** — charts, tables, exports

### Code style

- Flat, linear, sequential — **no functions unless explicitly asked**
- Where a function would be natural, write the code inline and add a comment: `# could be a function`
- No classes, modules, logging setup, argparse, or `if __name__ == "__main__"` boilerplate
- Keep it simple and readable; only reach for complex patterns when genuinely required
- Comments explain *why* or *what we're seeing*, not what the code does
- Sparse print statements — only when output is meaningful to inspect
- **No unnecessary intermediate variables** — write values inline where they're used:
  - SQL strings go directly inside `pd.read_sql("""...""", engine)`, not assigned to a `query_xxx` variable first
  - Column lists go directly inside the function call (e.g., `df[["col_a", "col_b"]].describe()`), not stored in a separate `features = [...]` variable unless reused 3+ times
  - Same principle for any string/list/dict that's only used once — just inline it
  - The goal: each cell is self-contained and readable top-to-bottom without scrolling up to find what a variable holds
- **Spacing**: spaces around ALL `=` signs — assignments AND keyword arguments. This overrides PEP 8. Examples:
  - `x = 1` not `x=1`
  - `func(arg = 'value')` not `func(arg='value')`
  - `df.sort_values('col', ascending = False)` not `ascending=False`
  - `.rolling(window = 4, min_periods = 3)` not `window=4, min_periods=3`
  - `.agg(n = ('col', 'count'))` not `n=('col', 'count')`
  - Space after commas: `a, b` not `a,b` or `a ,b`

### Piping / method chaining

Pandas method chaining is strongly preferred — chain as much as naturally flows:

```python
result = (
    df
    .query("year == 2024")
    .assign(pmpm = lambda x: x["paid"] / x["members"])
    .groupby("population")["pmpm"].mean()
    .reset_index()
)
```

Break into an intermediate variable only when the chain becomes hard to read. A plain `df2 = df[df["col"] > 0]` is better than a tortured lambda.

### Creating multiple columns — use `.assign()`, not line-by-line

When computing a batch of new columns (like R `mutate()`), use a single `.assign()` chain — NOT repeated `df["x"] = ...` lines:

```python
# good — one block, like mutate()
df = (
    df
    .assign(
        rate_a = lambda x: x.num_a / x.denom,
        rate_b = lambda x: x.num_b / x.denom.replace(0, np.nan),
        flag = lambda x: np.where(x.rate_a > 0.5, 1, 0)
    )
)

# bad — repetitive, hard to scan
df["rate_a"] = df["num_a"] / df["denom"]
df["rate_b"] = df["num_b"] / df["denom"].replace(0, np.nan)
df["flag"] = np.where(df["rate_a"] > 0.5, 1, 0)
```

Can chain `.dropna()` or other steps right after `.assign()` in the same block.

### Background

Coming from R and SAS — frame explanations in R/SAS terms when helpful. Lead with the R equivalent before explaining the Python approach.

---

## Validation Checkpoints

### After writing SQL

Always suggest these checks before moving to the next step:

```sql
-- row count + distinct member count
select count(*), count(distinct mbi) from <table>;

-- null check on key columns
select count(*) from <table> where mbi is null or prov_tin is null;
```

**Domain-specific monthly validation** — run the appropriate query depending on pull type:

```sql
-- Claims: monthly allowed amount totals
select <month_col>, sum(allowed) from <table> group by 1 order by 1;

-- Authorizations: monthly case counts
select <month_col>, count(case_id) from <table> group by 1 order by 1;

-- Membership: monthly distinct member counts
select <month_col>, count(distinct mbi) from <table> group by 1 order by 1;
-- (use fin_mbi_hicn_fnl if mbi is not available)
```

Flag unexpected row counts or null IDs before continuing.

### After a major Python transform block

Suggest: `df.shape`, `df.head()`, and `df[col].value_counts()` on any new categorical column. Flag unexpected nulls or row count drops.

---

## Shortcuts & Triggers

- **"just do it"** — skip plan mode, execute directly
- **"start fresh"** — start a new context window (equivalent to `/clear`)
- **"claims template"** — read `_templates/claims_template.sql` and scaffold a new claims pull; ask table name, entities, month range, extra filters
- **"membership template"** — read `_templates/membership_template.sql` and scaffold; ask table name, month(s), population filters
- **"auth template"** — read `_templates/auth_template.sql` and scaffold; ask table name, date range, population/setting filters
- **"export to csv"** — read `_templates/export_to_csv.py` and scaffold; ask table name, then export to `Data/Output/<table_name>.csv`

---

## Session Variables

These values change weekly with each notification cycle. State them at the start of a session
or when they change. Claude substitutes them into all generated SQL (table names, date filters, etc.).

| Variable | Format | Example | Usage |
|---|---|---|---|
| `notifications_date` | `MMDDYYYY` | `05062026` | Table name suffix: `kn_*_<notifications_date>_od` |
| `membership_month` | `YYYYMM` | `202603` | Membership archive table + filter: `gl_rstd_gpsgalnce_f_<membership_month>` |
| `claims_month` | `YYYYMM` | `202603` | Claims date filter upper bound |

**How to set**: Just tell Claude, e.g.:
- "notifications_date is 05202026, membership_month is 202604, claims_month is 202604"
- "same as last week but bump notifications_date to 05272026"

Claude will ask for these if not provided when scaffolding from a template that needs them.