# LOC Project — Context & Objectives

## What This Project Does

Produces the **LOC (Level of Care) Valuation** output used for leading indicator reporting. The table tracks acute inpatient medical/surgical authorization decisions and member months across M&R and C&S populations.

## Key Variables (update each run)

| Variable | Example | Notes |
|---|---|---|
| `notifications_date` | `04222026` | drives all `kn_` table names |
| `membership_month` | `202603` | confirm with Pradeepa that enrollment table is updated |
| `claims_month` | `202603` | update to match current claims run |

## Table Naming

Output tables use `kn_` initials (prod) or `knd_` (dev), not `ec_` (IPA's prefix).

```
tmp_1m.kn_loc_*_${notifications_date}_od
```

## Pipeline Overview

**Script**: `Scripts/LOC_202604_snowflake.sql`

| Step | Table | Source |
|---|---|---|
| 1 | `kn_ip_dataset_${notifications_date}_4_od` | `ec_ip_dataset_${notifications_date}_3_od` (IPA's upstream) |
| 2 | `kn_loc_mm_${notifications_date}_od` | `hce_ops_archv.gl_rstd_gpsgalnce_f_${membership_month}` |
| 3 | `kn_loc_notif_${notifications_date}_od` | union of steps 1 + 2 |
| 4 | `kn_ip_dataset_loc_${notifications_date}_od` | step 3, filtered to `loc_flag = 1` |

## Raw Auth Source

`tmp_1m.ec_avtar_25_26_3_od` — case-level authorization table (IPA's AVTAR). When pulling from raw auth, always include:

```sql
where fin_brand in ('M&R', 'C&S')
    and (
        (ip_type in ('Medical', 'Surgical', 'Transplant')
         and to_varchar(admit_dt_act, 'MM/dd/yyyy') is not null)
        or ip_type in ('LTAC', 'SNF', 'AIR')
    )
```

## loc_flag Definition

`loc_flag = 1` means the case is **acute inpatient, place of service 21 (Acute Hospital), medical or surgical admit category**. Defined in IPA's `ec_avtar_23_1_od`. Member month rows are hardcoded to `loc_flag = 1`.

The LOC table filters to `ipa_pac_flag in ('IPA', 'MM')` — claims are excluded (source is `_notif_`, not `_all_`).

## Population Segmentation (priority order)

```
M&R FFS           → mnr_total_ffs_flag = 1
C&S DSNP          → cns_dual_flag = 1
OAH               → total_oah_flag = 'OAH'
M&R DSNP          → mnr_dual_flag = 1
M&R Institutional → institutional_flag = 'Institutional'
```

---

## Rate Features

| Feature | Denominator | Description |
|---|---|---|
| `initial_adr_rate` | case_count | Initial ADR rate |
| `persistent_adr_rate` | case_count | Persistent ADR rate |
| `persistency` | initial_adr_cnt | Persistent / initial ADR |
| `md_review_rate` | case_count | MD review rate |
| `appeal_rate` | initial_adr_cnt | Appeal rate (% of ADRs) |
| `appeal_overturn_rate` | initial_adr_cnt | Appeal overturn rate |
| `p2p_rate` | initial_adr_cnt | P2P rate |
| `p2p_overturn_rate` | initial_adr_cnt | P2P overturn rate |
| `mcr_reconsideration_rate` | initial_adr_cnt | MCR reconsideration rate |
| `mcr_overturn_rate` | initial_adr_cnt | MCR overturn rate |
| `member_appeal_rate` | initial_adr_cnt | Member appeal rate |
| `member_appeal_overturn_rate` | initial_adr_cnt | Member appeal overturn rate |
| `auth_per_k` | membership | Auth per 1,000 members |

---

## Known Data Issues

- **202403–202404**: Prior auth was turned off → ADR artificially low. Exclude from trend analyses.
- **`member_appeal_ind`**: Only available from **202601** onward. No member appeal data before that.
- **Data maturity lag**: Recent months (~last 3-4 months) have incomplete appeal/overturn outcomes — cases still developing through the cycle. Pre-completion data are acceptable for member appeal analysis.
- **Usable time range**: 202301 is the earliest meaningful month.
