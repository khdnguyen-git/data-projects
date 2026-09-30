# LOC Changepoint Detection — Notes

**Date**: 2026-07-13

## DS Review Summary

### Critical

1. **Concordance rate of 6% means methods disagree, not confirm.** PELT found 1,125 CPs; BOCD found 81; 68 overlap. Calibrate against known operational changes before using concordance as a filter.

2. **BOCD MAP run-length criterion is too conservative for n=18.** Posterior can't build confidence in long runs with short series. Use cumulative `P(r < 3)` threshold or lower `hazard_lambda` to 6 for n < 30.

3. **Small-denominator rates produce false changepoints.** p2p_overturn_rate and member_appeal_rate have months with denominators < 5, causing extreme variance flagged as regime shifts. Add denominator threshold (initial_adr_cnt >= 5) to suppress noise.

### Moderate

4. **PELT penalty is too low.** BIC-derived penalty assumes Gaussian segments; rate data [0,1] violates this. Hospital-level produces ~30 CPs per hospital. Increase pen_factor to 3-5 or switch to `"l2"` cost model.

5. **Pre/post mean unreliable at edges.** Changepoints at index 2 of 18 give pre_mean on 2 points. Add min segment length filter (min(cp, n-cp) >= 4).

6. **Index-to-month mapping bug.** `months_sorted[cp]` uses global month list. If a market has missing months, index doesn't align. Use `mkt_df["admit_act_month"].iloc[cp]` instead.

### Minor

- `RuntimeWarning: Mean of empty slice` — some breakpoints at index 0. Filter or guard.
- NIG prior `kappa0=1` is informative for rate data near 0. Consider `kappa0=0.1`.

## Action Items

- [ ] Add denominator threshold for rate KPIs
- [ ] Calibrate PELT penalty (try pen_factor=3-5 or l2 model)
- [ ] Lower hazard_lambda or use cumulative P(r<3) for short series
- [ ] Fix month-index mapping to use market-specific array
- [ ] Add min segment length filter for magnitude calculation
- [ ] Validate against known operational events in 3-4 markets
