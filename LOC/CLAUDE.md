# LOC Project — Context & Objectives

## What This Project Does

Produces the **LOC (Level of Care) Valuation** output used for leading indicator reporting. The table tracks acute inpatient medical/surgical authorization decisions and member months across M&R and C&S populations.

## Key Variables (update each run)

| Variable | Example | Notes |
|---|---|---|
| `notifications_date` | `04152026` | drives all `kn_` table names |
| `membership_month` | `202603` | confirm with Pradeepa that enrollment table is updated |
| `claims_month` | `202603` | update to match current claims run |

## Table Naming

Output tables use `kn_` initials (prod) or `knd_` (dev), not `ec_` (IPA's prefix).

```
tmp_1m.kn_loc_*_${notifications_date}_od
```

## Pipeline Overview

| Step | Table | Source |
|---|---|---|
| 1 | `kn_ip_dataset_${notifications_date}_4_od` | `ec_ip_dataset_${notifications_date}_3_od` (IPA's upstream) |
| 2 | `kn_loc_mm_${notifications_date}_od` | `hce_ops_archv.gl_rstd_gpsgalnce_f_${membership_month}` |
| 3 | `kn_loc_notif_${notifications_date}_od` | union of steps 1 + 2 |
| 4 | `kn_ip_dataset_loc_${notifications_date}_od` | step 3, filtered to `loc_flag = 1` |

## loc_flag Definition

`loc_flag = 1` means the case is **acute inpatient, place of service 21 (Acute Hospital), medical or surgical admit category**. Defined in IPA's `ec_avtar_23_1_od`. Member month rows are hardcoded to `loc_flag = 1`.

The LOC table filters to `ipa_pac_flag in ('IPA', 'MM')` — claims are excluded (source is `_notif_`, not `_all_`).

## Population Segmentation (priority order)

```
OAH               → total_oah_flag = 'OAH'
M&R Institutional → institutional_flag = 'Institutional'
M&R FFS           → mnr_total_ffs_flag = 1
C&S DSNP          → cns_dual_flag = 1
M&R DSNP          → mnr_dual_flag = 1
```
