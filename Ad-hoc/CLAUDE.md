# Ad-hoc — Context

## What This Project Does

One-off analysis scripts for authorization, clinical conditions (NEMT, Heart Failure, CGM), and claims verification. No single pipeline — each script is a standalone query for a specific request.

## Common Source Tables

- Claims: `fichsrv.glxy_op_f`, `fichsrv.glxy_pr_f`, `fichsrv.nce_op_f`, `fichsrv.nce_pr_f`, `fichsrv.dcsp_op_f`, `fichsrv.dcsp_pr_f`
- Membership: `fichsrv.tre_membership`
- Authorizations: `hce_ops_fnl.hce_adr_avtar_like_25_26_f` (or `hce_proj_bd.hce_adr_avtar_like_24_25_f` for prior years)

## Common Patterns

- Population flags: M&R FFS, OAH, C&S DSNP, M&R DUAL, ISNP
- Non-denied claims filter: `clm_dnl_f = 'N'`
- FFS filter: `global_cap = 'NA'`
- Auth determination flags: `partially_adverse`, `fully_adverse`

## Output Tables

Follow root naming convention: `tmp_1m.knd_adhoc_<topic>_<YYYYMM>` for dev, `tmp_1m.kn_adhoc_<topic>_<YYYYMM>` for prod.
