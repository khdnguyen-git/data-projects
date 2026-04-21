# MBM Therapies Project — Context & Objectives

## Script Types

### `therapy_savings_yyyymm_Snowflake_stable.sql`
- **Purpose**: Affordability reporting
- **Cadence**: Monthly
- **Canonical source**: `fichsrv` claims tables (all 6 entities via `union all`) — see root `_templates/claims_template.sql`

### `therapy_PMPM+VpE_yyyymm_prod.sql`
- **Purpose**: General ad-hoc therapies pull — more comprehensive than Affordability
- **Cadence**: Ad-hoc
- **Includes**: Denial claims filter (`clm_dnl_f` / `dnl_f not in ('D', 'Y')`) and a `population` variable
- **Shared logic**: Visits per Episode (VpE) follows the same `vpe_1 → vpe_3` progression as Affordability

## VpE Logic

Both script types use the same Visits per Episode framework:
- `vpe_1` → `vpe_3` — episode-level visit rollup
- Episodes defined by partitioning on `mbi` + category, ordered by `fst_srvc_dt`; new episode if gap > 30 days or first record for that partition

## Key Filters

| Filter | Affordability | Ad-hoc PMPM+VpE |
|---|---|---|
| Denial claims excluded | yes | yes |
| `clm_dnl_f not in ('D', 'Y')` | yes | yes |
| `population` field | no | yes |

## Therapy Categories

- **Chiro**: proc_cd in (`98940`, `98941`, `98942`)
- **PT-OT**: standard PT/OT proc codes
- **ST**: speech therapy proc codes (`92507`, `92508`, `92526`, etc.)
- **Other**: anything not matching above

## Population Segmentation

Ad-hoc PMPM+VpE scripts include a `population` field. Follow the same priority logic used in LOC:

```
OAH               → total_oah_flag = 'OAH' (or migration_source = 'OAH')
M&R Institutional → institutional_flag = 'Institutional'
M&R FFS           → mnr_total_ffs_flag = 1
C&S DSNP          → cns_dual_flag = 1
M&R DSNP          → mnr_dual_flag = 1
```
