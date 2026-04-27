# Cohort Analysis — Context

## What This Project Does

Multi-year revenue and membership cohort analysis for C&S DUAL product — compares 2024 vs 2025 YTD performance with RAF adjustments and CMS payment tracking. Members are segmented by HCE cohort assignments (e.g., competitor closure events).

## Key Variables (update each run)

| Variable | Notes |
|---|---|
| Analysis month | YTD end month for the current run |
| Cohort table | `tmp_1y.2024_2025_HCE_COHORT_6` — confirm with team if refreshed |

## Source Tables

- Revenue/financials: `tmp_1m.ab_mml_union` — member-month-level Part C payments, RAF, premiums
- Membership: `fichsrv.tre_membership`
- Cohort assignments: `tmp_1y.2024_2025_HCE_COHORT_6`

## Output Table Pattern

```
tmp_1m.kn_cnsd_<topic>_<YYYYMM>
```

## Key Filters

- `sgr_source_name in ('COSMOS', 'NICE', 'CSP')`
- `fin_brand = 'C&S'`, `product_level_3 = 'DUAL'`
- `migration_source != 'OAH'`

## Metrics

Member counts, RAF adjustments, Part C CMS payments, member premiums, retro adjustments — grouped by RAF bucket and cohort.
