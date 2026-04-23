# LOC Anomaly Detection — How It Works (A Guide for R Users Learning Python + ML)

This document walks through `loc_anomaly.py` step by step, explaining **what each section does**, **why it's structured the way it is**, and **how it maps to things you already know from R tidyverse**.

---

## The Big Picture First

Before diving into steps, here's the whole workflow in one sentence:

> Pull raw LOC data from Snowflake → convert raw counts to rates → group by each dimension → run Isolation Forest to find unusual values → explain *why* each flagged value is unusual → save results.

As a real data scientist would think about it:

```
Raw data → Feature engineering → Aggregation → Modeling → Interpretation → Export
```

Every step in the script maps directly to one of these stages. Nothing is decoration.

---

## Why Is Everything Inside Functions?

If you come from R, you're used to writing a script top-to-bottom — define a variable, use it, define another, use it. Python scripts often work the same way, **but** this script wraps everything in functions instead.

**Why?** Three reasons:

1. **Reusability.** The `build_rates()` function is called in three different places (per-dimension aggregation, global aggregation). If it weren't a function, you'd copy-paste the same 14 lines three times.

2. **Readability.** When you see `agg.pipe(build_rates)`, you know exactly what's happening without reading 14 lines of math inline.

3. **Testability.** You can test `build_rates()` in isolation by passing it a tiny fake dataframe. You can't do that with code embedded in a loop.

In R, you do the same thing when you write `my_function <- function(df) { ... }`. Same concept, different syntax.

---

## Why Is There a `main()` Function at the Bottom?

In R, when you `source("script.R")`, everything at the top level runs immediately. Python works the same way **unless** you put your code inside a function.

The `main()` function is the conductor: it calls all the other functions in the right order. Nothing actually runs until `main()` is called.

The last two lines:

```python
if __name__ == "__main__":
    main()
```

This says: *"Only run main() if this file is being executed directly (e.g., `python loc_anomaly.py`). If another script imports this file, don't run main() automatically."*

Think of it like putting your R code inside `if (interactive()) { ... }` — it only runs when you're running the script yourself, not when someone `source()`s your file to use your functions.

---

## Why So Much Printing?

```python
print("STEP 1: Connecting to Snowflake...")
print(f"  Loaded {len(raw):,} rows  |  {SOURCE_TABLE}")
print(f"  Total cases:      {raw['case_count'].sum():>12,.0f}")
```

This is called **logging** — a running commentary printed to the console as the script executes.

**Why bother?** Three reasons:

1. **Feedback.** Snowflake queries can take 30–90 seconds. Without a print statement, you sit there staring at a blank terminal wondering if it crashed. The print confirms it's working.

2. **Sanity checks.** `Total cases: 1,847,302` is a quick gut check. If that number is `0` or `84`, something is wrong with the filters before you waste 5 minutes waiting for the model to run on bad data.

3. **Audit trail.** When this runs in production and someone asks "what happened on April 15th?", you can look at the console output and see exactly what ran, what was loaded, and what was flagged.

In R you'd use `message()` or `cat()` for the same purpose.

---

## Step 0: Setup — Configuration at the Top

```python
NOTIFICATIONS_DATE = "04152026"
CONTAMINATION      = 0.05
MIN_CASE_COUNT     = 30
```

All the "knobs you turn each run" live at the very top of the file, not buried inside functions. This is a best practice — you should never have to hunt through 300 lines of code to change the run date.

**R equivalent:** It's like putting your `run_month <- "202604"` and `threshold <- 0.05` at the top of your `.R` script before anything else.

The lists — `DIMENSIONS`, `RAW_METRICS`, `RATE_FEATURES`, `FEATURE_META` — are the column schemas. They define what gets pulled, what gets computed, and what gets shown in narratives. Changing what the model sees is as simple as adding or removing a string from a list.

---

## Step 1: Load Data from Snowflake

```python
def load_raw(conn):
    ...
    return (
        pd.read_sql(query, conn)
        .rename(columns = str.lower)
    )
```

This is a straightforward database pull. Two things to notice:

**`.rename(columns = str.lower)`** — Snowflake sometimes returns column names in uppercase (`CASE_COUNT`). This one-liner lowercases all of them so the rest of the script doesn't have to guess the casing. In R this would be `names(df) <- tolower(names(df))`.

**Why pull all 13 dimensions at once?** The query groups by all 13 dimensions simultaneously. This gives Python one row per unique combination of all dimensions. Then, when we later group by just `prov_tin`, we're doing a cheap re-aggregation in memory rather than going back to Snowflake 13 times.

---

## Step 2: Feature Engineering — Converting Counts to Rates

```python
def build_rates(df):
    adr = df["initial_adr_cnt"]
    cc  = df["case_count"]
    mm  = df["membership"]

    return df.assign(
        adr_rate     = safe_rate(df["initial_adr_cnt"], cc),
        appeal_rate  = safe_rate(df["appeal_case_cnt"], adr),
        ...
    )
```

### Why rates instead of raw counts?

A provider with 200 appeal cases sounds alarming. But if they handle 10,000 auths, that's a 2% appeal rate — totally normal. A provider with 40 appeal cases out of 150 auths is at 27% — very unusual. **Raw counts can't be compared across providers. Rates can.**

### `.assign()` is dplyr's `mutate()`

| R (tidyverse) | Python (pandas) |
|---|---|
| `df %>% mutate(adr_rate = initial_adr_cnt / case_count)` | `df.assign(adr_rate = safe_rate(df["initial_adr_cnt"], cc))` |

`.assign()` adds new columns and returns a **new** dataframe — it never modifies `df` in place. This is the same immutable philosophy as tidyverse's `mutate()`.

### Why is `safe_rate()` its own function?

```python
def safe_rate(num, denom):
    return num / denom.replace(0, np.nan)
```

When the denominator is zero (e.g., a provider had zero ADRs so we can't compute appeal rate per ADR), dividing gives either a crash or `Inf`. `.replace(0, np.nan)` turns those zeros into missing values before dividing, so the result is `NaN` instead of an error. The model handles `NaN` gracefully later.

In R this would be `ifelse(denom == 0, NA, num / denom)`.

### Denominator logic

| Rate group | Denominator | Reasoning |
|---|---|---|
| `adr_rate`, `md_review_rate`, `pre_auth_rate`, `auth_per_k` | `case_count` | Every case is eligible for these |
| `appeal_rate`, `p2p_rate`, `mcr_*`, `member_appeal_*` | `initial_adr_cnt` | You can only appeal an adverse determination |
| `persistency` | `initial_adr_cnt` | Persistent is a subset of initial ADRs |

---

## Step 3: Aggregation — The Tidyverse Heart of the Script

```python
result = (
    raw
    .groupby(dim, dropna = False)[RAW_METRICS]
    .sum()
    .reset_index()                              # like ungroup()
    .query("case_count >= @MIN_CASE_COUNT")     # like filter()
    .pipe(build_rates)                          # .pipe() is like %>%
    .assign(
        _dimension = dim,
        _dim_value = lambda x: x[dim].astype(str),
    )
)
```

### `.pipe()` is `%>%` for custom functions

This is the key to tidyverse-style chaining in Python. When you write `.pipe(build_rates)`, pandas passes the current dataframe as the first argument to `build_rates()`. It's exactly `%>%` piping.

| R | Python |
|---|---|
| `df %>% build_rates()` | `df.pipe(build_rates)` |
| `df %>% filter(case_count >= 30)` | `df.query("case_count >= @MIN_CASE_COUNT")` |
| `df %>% group_by(dim) %>% summarise(across(..., sum)) %>% ungroup()` | `df.groupby(dim)[cols].sum().reset_index()` |

The `@` in `.query("case_count >= @MIN_CASE_COUNT")` tells pandas to look up `MIN_CASE_COUNT` as a Python variable, not a column name.

### Why filter to `case_count >= 30`?

If a provider TIN only has 5 cases, a single unusual case pushes their `adr_rate` to 20% — that's noise, not signal. 30 cases is the minimum needed for rates to be statistically stable. It's like filtering to `n >= 30` before computing proportions in R.

### Two aggregation modes

| Function | What it produces | What it detects |
|---|---|---|
| `aggregate_dimension(raw, "prov_tin")` | One row per provider TIN | Which TINs are outliers across all their cases |
| `aggregate_global(raw, DIMENSIONS)` | One row per unique combo of all 13 dims | Anomalies that only appear at the intersection of dims |

The global model catches things like: *"TIN 123 looks normal overall, but within M&R FFS + Acute + respiratory cases specifically, their persistency rate is 4σ above average."*

---

## Step 4: Isolation Forest — The Anomaly Detection Model

```python
X_scaled = StandardScaler().fit_transform(X)

model = IsolationForest(
    n_estimators  = 200,
    contamination = CONTAMINATION,
    random_state  = 42,
    n_jobs        = -1,
)
model.fit(X_scaled)
```

### What is Isolation Forest?

Isolation Forest is an **unsupervised** algorithm — meaning there's no labeled training data. You don't tell it which providers are anomalous. It figures that out on its own.

The intuition: imagine randomly drawing lines through your data to split it into regions. Normal points (clustered together with similar peers) take many splits to isolate. Unusual points (sitting far from the cluster) get isolated after just a few splits. The algorithm measures how quickly each point gets isolated — quick = anomalous.

**R equivalent:** There's no base-R equivalent, but `isotree::isolation.forest()` does the same thing.

### Why `StandardScaler` first?

```python
X_scaled = StandardScaler().fit_transform(X)
```

`appeal_rate` might range 0.0–0.3. `auth_per_k` might range 0–500. Without scaling, the large-valued feature would dominate the model — tiny differences in `auth_per_k` would matter more than large differences in `appeal_rate`, purely because of units.

`StandardScaler` transforms each feature to mean = 0, standard deviation = 1 — the same scale. In R: `scale(df)`.

### What is `contamination`?

```python
contamination = 0.05
```

This tells the model to expect ~5% of rows to be anomalous. It's **not** a statistical derivation — it's a business decision. If your team thinks 1% of providers are truly problematic, set it to 0.01. If you want to cast a wider net, use 0.10.

### What does `raw_score` mean?

- **Negative score:** flagged as anomalous
- **More negative:** more anomalous (lower is worse)
- **Positive score:** normal

The scores are only comparable within the same model run (e.g., within the `prov_tin` dimension). A `prov_tin` score of `-0.3` and a `fin_market` score of `-0.3` are not directly comparable — they came from different models trained on different sets of rows.

---

## Step 5: Explain Why — Z-Scores

Isolation Forest flags rows but doesn't explain itself. It's a black box that says "this is weird" without saying which feature makes it weird. Z-scores fill that gap.

```python
z_cols = {
    f"{feat}_z": (df[feat] - df[feat].mean()) / df[feat].std()
    for feat in RATE_FEATURES
    if feat in df.columns and df[feat].std() > 0
}
return df.assign(**z_cols)
```

### What is a z-score?

A z-score answers: *"How many standard deviations is this value from the average?"*

- `appeal_rate_z = +3.2` means this row's appeal rate is 3.2 standard deviations above the mean for its dimension group. That's unusual.
- `appeal_rate_z = -0.4` means slightly below average. Normal.

**R equivalent:** `scale()` computes z-scores. Or manually: `(x - mean(x)) / sd(x)`.

### `top_reasons()` — the most informative z-scores per row

```python
top3 = sorted(z_vals, key = z_vals.get, reverse = True)[:3]
```

For each flagged row, this finds the 3 features with the largest absolute z-score. These are the clearest, most human-readable explanation of why the model flagged it.

Output looks like: `"appeal_rate (+3.2σ above peer mean)"`.

---

## Step 6: Narratives — Plain-English Output

```python
def generate_narrative(row, peer_medians, dim):
    ...
    lines.append(
        f"  • {label}: {fmt.format(row[feat])} "
        f"vs. peer median {fmt.format(peer_medians.get(feat, np.nan))} "
        f"({z:+.1f}σ {direction})"
    )
```

This translates the machine-readable scores into bullet points a stakeholder can read in a meeting without looking at any data.

**Why peer median instead of mean?** Medians are resistant to outliers. If one provider has an appeal rate of 95% (likely a data issue), it would pull the mean up and make everyone else look artificially low. The median isn't affected.

Only features with `|z| >= 1.5` appear in the narrative. Below that threshold, the difference isn't large enough to be worth mentioning.

---

## Step 7: Main — The Conductor

```python
def main():
    # 7a: Load
    conn = get_connection()
    raw  = load_raw(conn)
    conn.close()

    # 7b: Per-dimension models
    for dim in DIMENSIONS:
        agg = aggregate_dimension(raw, dim)
        scored = agg.pipe(run_model, label = dim).pipe(add_z_scores)
        ...

    # 7c: Global model
    global_scored = aggregate_global(raw, DIMENSIONS).pipe(run_model, label = "global")...

    # 7d: Export
    final.to_csv(OUTPUT_CSV, ...)

    # 7e: Narratives
    pd.DataFrame(narrative_rows).to_csv(NARRATIVE_CSV, ...)
```

`main()` is a table of contents in code form. It shows the full flow in ~50 lines without any implementation detail. The detail is in the individual functions. This separation — *what happens* (main) vs *how it happens* (individual functions) — is what makes the script readable.

### `.pipe()` chaining in main

```python
scored = (
    agg
    .pipe(run_model, label = dim)
    .pipe(add_z_scores)
)
```

This is the tidyverse pipe in full action. The dataframe flows through `run_model()`, then the result flows into `add_z_scores()`. In R:

```r
scored <- agg %>%
  run_model(label = dim) %>%
  add_z_scores()
```

---

## Cheat Sheet: Python ↔ R Tidyverse

| What you want to do | R (tidyverse) | Python (pandas) |
|---|---|---|
| Pipe a dataframe into a function | `df %>% f()` | `df.pipe(f)` |
| Add new columns | `mutate(col = expr)` | `.assign(col = expr)` |
| Filter rows | `filter(col >= 30)` | `.query("col >= 30")` |
| Group and summarize | `group_by(x) %>% summarise(sum(y))` | `.groupby(x)[y].sum()` |
| Ungroup | `ungroup()` | `.reset_index()` |
| Sort rows | `arrange(desc(score))` | `.sort_values("score", ascending=False)` |
| Select columns | `select(col1, col2)` | `[["col1", "col2"]]` |
| Scale features | `scale(df)` | `StandardScaler().fit_transform(df)` |
| Z-score | `(x - mean(x)) / sd(x)` | `(x - x.mean()) / x.std()` |
| Fill missing | `replace_na(list(col = 0))` | `.fillna(0)` |
| Stack dataframes | `bind_rows(df1, df2)` | `pd.concat([df1, df2])` |
| Apply function row-wise | `rowwise() %>% mutate(...)` | `.apply(f, axis=1)` |

---

## Output Files

| File | What it contains |
|---|---|
| `loc_anomaly_202604.csv` | One row per dimension value. Columns: rates, anomaly score, top 3 reasons. |
| `loc_anomaly_narratives_202604.csv` | One row per top-20 anomaly per dimension. `narrative` column has the plain-English paragraph. |

### How to read the scored output

1. Filter `_dimension = "prov_tin"` to see provider-level anomalies.
2. Sort by `raw_score` ascending — most anomalous at the top.
3. Read `top_reason_1`, `top_reason_2`, `top_reason_3` to understand why.
4. Cross-reference with `_dimension = "global"` to see if the same provider appears there too — that's a stronger signal.

### How to interpret `raw_score`

| Score | Interpretation |
|---|---|
| `< -0.2` | Strongly anomalous — investigate |
| `-0.2` to `0` | Mildly unusual — worth watching |
| `> 0` | Normal — scores above 0 are within expected behavior |
