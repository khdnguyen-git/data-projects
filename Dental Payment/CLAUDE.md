# Dental Payment — Context

## What This Project Does

Dental claims and payment analysis using FACETS data — aggregates allowed/paid amounts by provider (TIN), market, and service category with quarterly breakouts. Includes membership enrollment for PMPM-style calculations.

## Source Tables

- Claims: `tmp_1y.HCE_DENTAL_CLMS_FNL` (FACETS source — not the standard fichsrv claims tables)
- Rollup: `tmp_1y.HCE_DENTAL_FACETS_CLM_MBR_ROLLUP`

> Note: Dental uses FACETS, not COSMOS/NICE/CSP. Do not use the standard fichsrv union here.

## Output Table Pattern

```
tmp_1m.kn_dental_<topic>_<YYYYMM>
```

Examples: `kn_dental_claims_unit`, `kn_dental_mbr`, `kn_dental_aggregated`, `kn_dental_pmt`

## Key Logic

- Quarterly aggregations derived from monthly dates (`fin_inc_month`, `svc_month`, `paid_month`)
- Provider PAR status (`temp_provparstatus`) tracked by TIN
- Claim unit: distinct `MBI | service_date | proc_cd`
- Filter: `dental_clm_source = 'FACETS'`, `fin_inc_month between 202301 and 202412`
