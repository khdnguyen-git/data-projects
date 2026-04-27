# CGM — Context

## What This Project Does

Continuous Glucose Monitoring (CGM) claims analysis — tracks utilization and claim counts by service month, entity, and product type. Monitors CGM device spending across M&R and C&S populations.

## Key Procedure Codes

- Devices: `K0553`, `K0554`, `E2101`, `E2102`, `E2103`
- Supplies: `A9276`, `A9277`, `A9278`, `A4238`, `A4239`

## Source Tables

- Claims: standard `fichsrv.*` union (see root CLAUDE.md)
- Membership: `fichsrv.tre_membership`

## Output Table Pattern

```
tmp_1m.kn_cgm_<topic>_<YYYYMM>
```

## Common Filters

- Service month: `fst_srvc_month >= 202301`
- Non-denied: `clm_dnl_f = 'N'`
- Provider participation: `PAR` vs `Non-PAR` from `prov_prtcp_sts_cd`
