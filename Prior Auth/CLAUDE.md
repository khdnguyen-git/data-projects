# Prior Auth — Quick Reference
- One-liner: Prior authorization analysis using ADR/AVTAR data (same sources as LOC, different filters)
- Output prefix: `tmp_1m.kn_pa_*` (prod), `knd_` for dev
- Session variables: notifications_date, membership_month

## Source Tables
- Authorizations: `hce_ops_fnl.hce_adr_avtar_like_25_26_f`
- Membership: `fichsrv.tre_membership`
- Auth determination flags: `partially_adverse`, `fully_adverse`

## On-demand context
To get current status and recent work, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\Prior Auth\04 - Progress.md`

For methodology and data sources, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\Prior Auth\01 - Context and Methodology.md`

## Update trigger
When asked to "update my work vault doc" or "update project notes", edit `04 - Progress.md` directly.
