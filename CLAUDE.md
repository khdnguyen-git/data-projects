# Analytics Workspace — Project Rules

## Session Variables

Always ask for these if not provided. Do not assume defaults.

| Variable | Format | Usage |
|---|---|---|
| `notifications_date` | `MMDDYYYY` | Table suffix: `kn_*_<notifications_date>_od` |
| `membership_month` | `YYYYMM` | Archive table: `gl_rstd_gpsgalnce_f_<YYYYMM>` |
| `claims_month` | `YYYYMM` | Claims date filter upper bound |

**How to set**: "notifications_date is 05202026, membership_month is 202604, claims_month is 202604"

## SQL Rules

- **Write queries into active SQL file** — don't just execute ad-hoc.
- **Ask before executing** if creating/replacing a prod table (`kn_` prefix). Dev tables (`knd_`) are fine.
- **Always validate** after writing SQL (row counts, null checks, monthly distribution).
- **Prefer CTEs** over subqueries, always.
- **`mbi` always means `fin_mbi_hicn_fnl`** — use `fin_mbi_hicn_fnl` in all SQL.
- **When in doubt**, reference the relevant skill for project-specific logic.

## Environment

- **Role**: `AZU_SDRP_VING_PRD_DEVELOPER_ROLE`
- **Warehouse**: `VING_PRD_MNR_HCE_DATAINFRA_WH`
- **Database**: `VING_PRD_TREND_DB`
- **Write schema**: `TMP_1M` (always prefix with `kn_` or `knd_`)
- **Read-only schemas**: `fichsrv`, `hce_ops_archv`, `hce_ops_stage`, `hce_ops_fnl`

## Naming Convention

Initials: **kn**

```
<schema>.<initials>_<project>_<topic>_<YYYYMM or notifications_date>
```

- `kn_` = prod, `knd_` = dev
- Examples: `tmp_1m.kn_loc_valuation_202604`, `tmp_1m.knd_therapies_vpe_202605`

## Population Segmentation — Default Rules

- **Default population**: `M&R FFS` includes DSNP. Do NOT exclude DSNP unless explicitly told to.
- **DSNP exclusion**: Only for **Therapies** projects. In Therapies, M&R FFS excludes DSNP (`fin_product_level_3 not in ('DUAL', 'INSTITUTIONAL')`).
- Population buckets: OAH, M&R ISNP, M&R FFS, C&S DSNP, M&R DSNP, N/A.

## Validation Checkpoints

### After SQL

```sql
select count(*), count(distinct fin_mbi_hicn_fnl) from <table>;
select count(*) from <table> where fin_mbi_hicn_fnl is null;
```

Domain-specific monthly validation:
- **Claims**: `select <month_col>, sum(allw_amt_fnl) from <table> group by 1 order by 1;`
- **Auths**: `select <month_col>, count(distinct case_id) from <table> group by 1 order by 1;`
- **Membership**: `select <month_col>, count(distinct fin_mbi_hicn_fnl) from <table> group by 1 order by 1;`

### After Python transform

`df.shape`, `df.head()`, `df[col].value_counts()` on new categorical columns. Flag unexpected nulls or row count drops.

## Skill Map & Triggers

| Skill | When it fires | Trigger phrases |
|-------|---------------|------------------|
| `sql-standards` | **ALWAYS** — invoke before writing ANY SQL | — |
| `python-standards` | Any Python/pandas/notebook task | — |
| `claims-pull` | Pulling claims data from fichsrv | "pull claims for YYYYMM", "claims template" |
| `membership-pull` | Pulling enrollment/member months | "pull membership for YYYYMM", "membership template" |
| `auth-pull` | Pulling authorization/ADR/AVTAR data | "auth template" |
| `loc-project` | LOC valuation pipeline | "refresh LOC for MMDDYYYY" |
| `therapies-project` | MBM Therapies / Affordability / VpE | "refresh Affordability for YYYYMM", "get VpE for YYYYMM" |
| `ds-review` | Review analytical methodology or statistical rigor | "review my approach", "is this method sound" |

- **"just do it"** — skip plan mode, execute directly
- **Templates** (claims, membership, auth) — scaffold a new pull; ask table name, filters, date range
