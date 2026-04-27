# Product Transition — Context

## What This Project Does

Tracks M&R member product transitions between December 2024 (baseline) and 2025 YTD — identifies cohort members moving between HMO, PPO, DUAL, CHRONIC, and other products. Used for HCE cohort reporting.

## Key Variables (update each run)

| Variable | Notes |
|---|---|
| Baseline month | `202412` (Dec 2024 enrollment) |
| Transition window | `202401–202501` (or extend as YTD grows) |
| Cohort table | `tmp_1y.2024_2025_HCE_COHORT_6` — filter to `hce_cohort = 'Product_Transition'` |

## Source Tables

- Membership: `fichsrv.tre_membership`, `tadm_tre_cpy.gl_rstd_gpsgalnce_f_<YYYYMM>`
- Claims (for product mix): `tadm_tre_cpy.glxy_op_f_<YYYYMM>`, `tadm_tre_cpy.glxy_pr_f_<YYYYMM>`
- Cohort assignments: `tmp_1y.2024_2025_HCE_COHORT_6`

## Output Table Pattern

```
tmp_1m.kn_prtr_<topic>_<YYYYMM>
```

## Population Filter

- `sgr_source_name = 'COSMOS'`, `fin_brand = 'M&R'`
- `migration_source not in ('OAH', 'CSP')`
- `product_level_3 != 'Institutional'`

## Product Categories

`HMO`, `PPO`, `NPPO`, `DUAL` (product_level_3 = 'DUAL'), `CHRONIC`, `OTHER`
