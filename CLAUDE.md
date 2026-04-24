# UHC Project — Claude Code Guidelines



## SQL Formatting Rules



1. **Table creation**: always use
`create or replace table`, never
`drop table if exists` +
`create table`

2. **Commas**: leading commas (`, column_name`), never trailing

3. **Aliases**: lowercase single-letter aliases with
`as` — e.g., 
`left join <table> as a`

4. **Keywords**: all SQL syntax in lowercase (`select`,
`from`, `left join`,
`where`, `group by`, etc.)

5. **`case` statements**: single space before
`then`, no column-aligning padding —
`when x = 1 then 'y'`, not
`when x = 1          then 'y'`

6. **Inequality operator**: use
`!=`, never `<>`



See `_templates/` for canonical examples of these patterns.



## Table Naming Convention



```

<schema>.<initials>_<projectname>_<topic>_<YYYYMM>

```



- **Schema**:
`tmp_1m` (preferred write target)

- **Initials**:
`kn` for prod, 
`knd` for dev

- **Example**:
`tmp_1m.kn_loc_valuation_202604`



The date suffix uses the run/notification month in `YYYYMM` format.



## Canonical Table Sources



### Claims

Pull from `fichsrv.*` tables. Use
`union all` across entities as needed:

- `fichsrv.glxy_op_f`  — COSMOS outpatient

- `fichsrv.glxy_pr_f`  — COSMOS provider

- `fichsrv.dcsp_op_f`  — CSP outpatient

- `fichsrv.dcsp_pr_f`  — CSP provider

- `fichsrv.nce_op_f`   — NICE outpatient

- `fichsrv.nce_pr_f`   — NICE provider



See `_templates/claims_template.sql`.



### Membership

Pull from `fichsrv.tre_membership`.

See `_templates/membership_template.sql`.



---



## Python / Data Science Coding Style



### General philosophy: data analyst, not software engineer



- Write flat, linear, sequential code by default — no wrapping things into functions unless explicitly asked or logic truly repeats 3+ times

- No classes, no modules, no software architecture patterns

- Minimal functions — only when the same logic genuinely repeats

- No excessive print statements narrating progress — use them sparingly and only when output is actually meaningful to inspect

- No logging setup, no argparse, no
`if __name__ == "__main__"` boilerplate

- Comments should explain
*why* or *what we're seeing*, not just restate what the code does

- Notebook-friendly style — code should read top to bottom like a coherent analysis, not like a library someone would import

- Prefer readability over cleverness — avoid chained one-liners or list comprehensions when a simple loop or intermediate variable is clearer

- When in doubt, break steps into separate blocks with a short comment header, not a function



### Piping and chaining



- Pandas method chaining is welcome — use
`df = (df\n    .method()\n    .method()\n)` style when it flows naturally

- Don't force it — if a lambda or
`pipe()` makes it convoluted just to avoid a separate line, write the separate line

- A plain `df2 = df[df['col'] > 0]` is better than a tortured chain

- Intermediate named variables are fine and often preferred for complex steps — clarity over style points



### Background



Experienced in R, SAS, SQL — translate concepts to Python in ways that map to that mental model when helpful. When explaining something, leading with the R/SAS equivalent first is useful.
