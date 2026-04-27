# MCR — Context

## What This Project Does

Medicare Care Review (MCR) work item routing and claims matching — links MCR decisions from ServiceNow to claims data (COSMOS, NICE, CSP) to track dispositions, payment details, and resolution timelines.

## Source Tables

- MCR work items: ServiceNow export (work_item_id, member_id, claim_id, resolved_at, mcr_disposition_code)
- Claims: `fichsrv.glxy_op_f / glxy_pr_f`, `fichsrv.nce_op_f / nce_pr_f`
- Membership: `fichsrv.tre_membership`

## Output Table Pattern

```
tmp_1m.kn_mcr_<topic>_<YYYYMM>
```

## Key Logic

- Work item ID parsing: regex to extract base ID before the `-` suffix
- Deduplication: latest claim payment date per subscriber-claim combination using `dense_rank`
- Claim status: `mnr_clm_dnl_status`
- Join keys: subscriber number, claim audit number, MBI — note TIN padding may be needed
- Monthly/yearly extraction from `resolved_at` datetime field
