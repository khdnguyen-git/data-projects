# Waterjet — Context

## What This Project Does

Waterjet ablation (BPH treatment) procedure tracking — monitors utilization and cost of waterjet procedures across entities, comparing approval mechanisms and claim outcomes by population segment.

## Key Procedure & Diagnosis Codes

- Procedures: `C2596`, `0421T` (waterjet ablation)
- Diagnosis: `N401` (benign prostatic hyperplasia) — checked across all 25 ICD fields (`primary_diag_cd` through `icd_25`)

## Source Tables

- Claims: standard `fichsrv.*` union (see root CLAUDE.md)
- Service month range: `fst_srvc_month between 202301 and 202512`

## Output Table Pattern

```
tmp_1m.kn_waterjet_<entity>_claims_<YYYYMM>
```

## Population Segments

M&R FFS, C&S DSNP, OAH (standard flags — see root CLAUDE.md)

## Claim Unit

Distinct `MBI | provider | service_date | proc_cd` combination.

## Common Filters

- Non-denied: `clm_dnl_f = 'N'`
- Provider PAR status from `prov_prtcp_sts_cd`
