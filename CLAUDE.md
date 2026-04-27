# UHC Project — Claude Code Guidelines

## SQL Formatting Rules

1. **Table creation**: always use `create or replace table`, never `drop table if exists` + `create table`
2. **Commas**: leading commas (`, column_name`), never trailing
3. **Aliases**: lowercase single-letter aliases with `as` — e.g., `left join <table> as a`
4. **Keywords**: all SQL syntax in lowercase (`select`, `from`, `left join`, `where`, `group by`, etc.)
5. **`case` statements**: single space before `then`, no column-aligning padding — `when x = 1 then 'y'`, not `when x = 1          then 'y'`
6. **Inequality operator**: use `!=`, never `<>`
7. **Spacing**: spaces around all operators (`=`, `!=`, `>=`, `<=`, `between`) — `col = 1` not `col=1`; space after leading comma — `, col` not `,col`

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
- **Spacing**: spaces around `=` (`x = 1`, not `x=1`); space after commas (`a, b`, not `a,b` or `a ,b`)

### Piping / method chaining

Pandas method chaining is strongly preferred — chain as much as naturally flows:

```python
result = (
    df
    .query("year == 2024")
    .assign(pmpm=lambda x: x["paid"] / x["members"])
    .groupby("population")["pmpm"].mean()
    .reset_index()
)
```

Break into an intermediate variable only when the chain becomes hard to read. A plain `df2 = df[df["col"] > 0]` is better than a tortured lambda.

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

---

## Outlier / Anomaly Detection

When asked to run outlier or anomaly detection:

**1. Clarify before writing any code:**
- What entity to analyze (TIN, MBI, market, etc.)?
- What metrics/features are available — and which should be *excluded* as irrelevant noise?
- What's the actual question: "who is high/low on a specific metric" vs. "who has an unusual combination"?

**2. Suggest the appropriate method — do not default:**

Before writing any code, reason about what statistical or ML approach best fits the question and data. Consider: number of features, data distribution, whether the result needs to be stakeholder-explainable, and what the question is actually asking. Present 2–3 options with trade-offs and let the user choose.

**3. Output format — standard stats only:**
Columns: metric value, z-score or IQR fence position, percentile rank, `is_outlier` flag. No narrative text, no "reasons for flagging", no plain-English summaries. Sort by most extreme first.

**4. Do NOT use Tier 1 / Tier 2 as the default pattern.** That pipeline was built for LOC's 14-feature anomaly detection and is not a general template.
