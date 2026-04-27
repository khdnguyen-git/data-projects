# Synapse — Context

## What This Project Does

Capitated rate buildup analysis for ACO/alternative provider networks — aggregates membership by brand, market, product, ACO network, and group (hpbp). Includes market expansion logic for CSNP/DSNP products.

## Source Tables

- Membership: `tadm_tre_cpy.gl_rstd_gpsgalnce_f_<YYYYMM>` (Galaxy membership with ACO/network fields)
- Claims: standard `fichsrv.*` tables as needed

## Output Table Pattern

```
tmp_1m.kn_synapse_<topic>_<YYYYMM>
```

## Key Logic

- Market expansion: appends `-CSNP` or `-DSNP` suffix to market names for specialized products
- ACO grouping:
  - NICE HMO: `nce_purchaser_id || '-' || substr(nce_src_sys_mdcl_pln_id, 3, 3)`
  - Others: `substr(gal_cust_seg_nbr, 5, 5)`
- Group ID (`hpbp`) logic varies by `tfm_product`
- Aggregation dimensions: `fin_brand`, `contractpbp`, `market_expansion`, `product`, `tfm_product`, `source`, `migration_source`, `hierarchy`, `aco_network`

## Common Filters

- `global_cap = 'NA'` (FFS only)
- `fin_inc_year > 2020`
