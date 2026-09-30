# MBM Therapies — Quick Reference
- One-liner: Affordability reporting, Visits per Episode (VpE), therapy PMPM for MBM team
- Main script: `Therapy_PMPM+VpE_<YYYYMM>_prod.sql` (run first)
- Ad-hoc script: `therapy_VpE_Episodes_adhoc.sql` (optional, run after main)
- Output prefix: `tmp_1m.kn_mbm_*`
- Session variables: notifications_date, membership_month, claims_month
- Population: M&R FFS **excluding** DSNP (Therapies exception — `fin_product_level_3 not in ('DUAL', 'INSTITUTIONAL')`)

## Key Details
- Denial filter column: `clm_dnl_f`
- Membership source: `fichsrv.tre_membership` (flag-based population)
- Therapy categories: chiro, PT-OT, speech therapy

## On-demand context
To get current status and recent work, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\MBM Therapies\04 - Progress.md`

For methodology and script layout, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\MBM Therapies\01 - Context and Methodology.md`

## Update trigger
When asked to "update my work vault doc" or "update project notes", edit `04 - Progress.md` directly.
