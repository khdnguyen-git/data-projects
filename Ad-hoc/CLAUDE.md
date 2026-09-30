# Ad-hoc — Quick Reference
- One-liner: One-off analyses for auth, clinical conditions (NEMT, Heart Failure, CGM), and claims verification
- Output prefix: `tmp_1m.kn_adhoc_<topic>_<YYYYMM>` (prod), `knd_` for dev
- Session variables: notifications_date, membership_month, claims_month (set per request)

## Source Tables
- Claims: `fichsrv.glxy_op_f`, `fichsrv.glxy_pr_f`, `fichsrv.nce_op_f`, `fichsrv.nce_pr_f`, `fichsrv.dcsp_op_f`, `fichsrv.dcsp_pr_f`
- Membership: `fichsrv.tre_membership`
- Authorizations: `hce_ops_fnl.hce_adr_avtar_like_25_26_f` (prior years: `hce_proj_bd.hce_adr_avtar_like_24_25_f`)

## Common Patterns
- Population flags: M&R FFS, OAH, C&S DSNP, M&R DUAL, ISNP
- Non-denied claims: `clm_dnl_f = 'N'`
- FFS filter: `global_cap = 'NA'`
- Auth determination: `partially_adverse`, `fully_adverse`

## On-demand context
To get current status and recent work, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\Ad-Hoc\04 - Progress.md`

For methodology and data sources, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\Ad-Hoc\01 - Context and Methodology.md`

## Update trigger
When asked to "update my work vault doc" or "update project notes", edit `04 - Progress.md` directly.
