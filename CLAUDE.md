# UHC Project — Claude Code Guidelines

## SQL Formatting Rules

1. **Table creation**: always use `create or replace table`, never `drop table if exists` + `create table`
2. **Commas**: leading commas (`, column_name`), never trailing
3. **Aliases**: lowercase single-letter aliases with `as` — e.g., `left join <table> as a`
4. **Keywords**: all SQL syntax in lowercase (`select`, `from`, `left join`, `where`, `group by`, etc.)
5. **`case` statements**: single space before `then`, no column-aligning padding — `when x = 1 then 'y'`, not `when x = 1          then 'y'`

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
- `fichsrv.glxy_op_f`  — COSMOS outpatient
- `fichsrv.glxy_pr_f`  — COSMOS professional
- `fichsrv.dcsp_op_f`  — CSP outpatient
- `fichsrv.dcsp_pr_f`  — CSP professional
- `fichsrv.nce_op_f`   — NICE outpatient
- `fichsrv.nce_pr_f`   — NICE professional

See `_templates/claims_template.sql`.

### Membership
Pull from `fichsrv.tre_membership`.
See `_templates/membership_template.sql`.
