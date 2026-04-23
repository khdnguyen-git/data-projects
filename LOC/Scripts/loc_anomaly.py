"""
LOC Anomaly Detection
Reads pre-aggregated rates table built by loc_agg.sql, then runs
Isolation Forest per dimension. Steps 2 & 3 (feature engineering
and aggregation) have moved to SQL.

Workflow:
  1. Run loc_agg.sql in Snowflake  → creates kn_loc_agg_${notifications_date}
  2. Run this script               → reads that table, models, exports CSVs
"""

# ============================================================
# STEP 0: SETUP
# ============================================================

import os
import warnings
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import snowflake.connector

warnings.filterwarnings("ignore", category = RuntimeWarning)


# --- Update each run (must match loc_agg.sql) ---
NOTIFICATIONS_DATE = "04152026"
OUTPUT_DATE        = "202604"

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "Output")

# Table name → output file prefix, one entry per population
POPULATIONS = [
    (f"tmp_1m.kn_loc_mnr_agg_{NOTIFICATIONS_DATE}", "mnr"),
    (f"tmp_1m.kn_loc_cns_agg_{NOTIFICATIONS_DATE}", "cns"),
    (f"tmp_1m.kn_loc_oah_agg_{NOTIFICATIONS_DATE}", "oah"),
]

# --- Model tuning ---
CONTAMINATION = 0.05   # expected share of anomalies — business decision, not statistical
TOP_N         = 20     # narratives written per dimension

# --- Rate features the model sees (column names from loc_agg.sql) ---
RATE_FEATURES = [
    "adr_rate",
    "persistent_adr_rate",
    "persistency",
    "md_review_rate",
    "appeal_rate",
    "appeal_overturn_rate",
    "p2p_rate",
    "p2p_overturn_rate",
    "mcr_reconsideration_rate",
    "mcr_overturn_rate",
    "member_appeal_rate",
    "member_appeal_overturn_rate",
    "pre_auth_rate",
    "auth_per_k",
]

# --- Labels and formats for plain-English narratives ---
FEATURE_META = {
    "adr_rate":                    ("ADR rate",                                "{:.1%}"),
    "persistent_adr_rate":         ("Persistent ADR rate",                     "{:.1%}"),
    "persistency":                 ("Persistency (persistent / initial ADR)",  "{:.1%}"),
    "md_review_rate":              ("MD review rate",                          "{:.1%}"),
    "appeal_rate":                 ("Appeal rate (% of ADRs)",                 "{:.1%}"),
    "appeal_overturn_rate":        ("Appeal overturn rate (% of ADRs)",        "{:.1%}"),
    "p2p_rate":                    ("P2P rate (% of ADRs)",                    "{:.1%}"),
    "p2p_overturn_rate":           ("P2P overturn rate (% of ADRs)",           "{:.1%}"),
    "mcr_reconsideration_rate":    ("MCR reconsideration rate (% of ADRs)",    "{:.1%}"),
    "mcr_overturn_rate":           ("MCR overturn rate (% of ADRs)",           "{:.1%}"),
    "member_appeal_rate":          ("Member appeal rate (% of ADRs)",          "{:.1%}"),
    "member_appeal_overturn_rate": ("Member appeal overturn rate (% of ADRs)", "{:.1%}"),
    "pre_auth_rate":               ("Pre-auth rate",                           "{:.1%}"),
    "auth_per_k":                  ("Auth per 1,000 members",                  "{:.1f}"),
}


# ============================================================
# STEP 1: CONNECT TO SNOWFLAKE & LOAD PRE-AGGREGATED TABLE
# loc_agg.sql must be run first to create this table.
# ============================================================

def get_connection():
    return snowflake.connector.connect(
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE", "PROD"),
        schema    = "tmp_1m",
    )


def load_aggregated(conn, table):
    return (
        pd.read_sql(f"select * from {table}", conn)
        .rename(columns = str.lower)
    )


# ============================================================
# STEP 2: FIT ISOLATION FOREST
# Unsupervised anomaly detection — no labeled examples needed.
# Scores each row: lower raw_score = more anomalous, negative = flagged.
# Features are scaled first so no single rate dominates the model.
# ============================================================

def run_model(df, label):
    available = [f for f in RATE_FEATURES if f in df.columns and df[f].notna().any()]
    if not available:
        return df

    X = df[available].fillna(df[available].median())

    X_scaled = StandardScaler().fit_transform(X)

    model = IsolationForest(
        n_estimators  = 200,
        contamination = CONTAMINATION,
        random_state  = 42,
        n_jobs        = -1,
    )
    model.fit(X_scaled)

    flagged = (model.decision_function(X_scaled) < 0).sum()
    print(f"  [{label}]  n = {len(df):,}  |  flagged = {flagged:,}")

    return df.assign(
        raw_score      = model.decision_function(X_scaled),
        _features_used = ",".join(available),
    )


# ============================================================
# STEP 3: EXPLAIN WHY — Z-SCORES & TOP REASONS
# IF is a black box. Z-scores add interpretability after the fact:
# which features are most extreme relative to the dimension's mean?
# ============================================================

def add_z_scores(df):
    z_cols = {
        f"{feat}_z": (df[feat] - df[feat].mean()) / df[feat].std()
        for feat in RATE_FEATURES
        if feat in df.columns and df[feat].std() > 0
    }
    return df.assign(**z_cols)


def top_reasons(row):
    z_vals = {
        feat: abs(row[f"{feat}_z"])
        for feat in RATE_FEATURES
        if f"{feat}_z" in row.index and pd.notna(row[f"{feat}_z"])
    }
    top3 = sorted(z_vals, key = z_vals.get, reverse = True)[:3]

    reasons = {}
    for i, feat in enumerate(top3, start = 1):
        z         = row[f"{feat}_z"]
        direction = "above" if z > 0 else "below"
        reasons[f"top_reason_{i}"] = f"{feat} ({z:+.1f}σ {direction} peer mean)"
    for i in range(len(top3) + 1, 4):
        reasons[f"top_reason_{i}"] = ""

    return pd.Series(reasons)


# ============================================================
# STEP 4: PLAIN-ENGLISH NARRATIVES
# ============================================================

def generate_narrative(row, peer_medians, dim):
    lines = [
        f"{dim} = {row['_dim_value']}  |  "
        f"cases = {int(row['case_count']):,}  |  "
        f"anomaly score = {row['raw_score']:.3f}"
    ]

    for feat, (label, fmt) in FEATURE_META.items():
        z_col = f"{feat}_z"
        if feat not in row.index or pd.isna(row.get(feat)):
            continue
        z = row.get(z_col, np.nan)
        if pd.isna(z) or abs(z) < 1.5:
            continue
        direction = "above" if z > 0 else "below"
        lines.append(
            f"  • {label}: {fmt.format(row[feat])} "
            f"vs. peer median {fmt.format(peer_medians.get(feat, np.nan))} "
            f"({z:+.1f}σ {direction})"
        )

    if len(lines) == 1:
        lines.append("  • No individual feature stands out; flagged on combined pattern.")

    return "\n".join(lines)


# ============================================================
# STEP 5: MAIN — ORCHESTRATE ALL STEPS
# ============================================================

def run_population(conn, table, prefix):
    """Load one population table, model all dimensions, export scored CSV and narratives."""
    output_csv    = os.path.join(OUTPUT_DIR, f"loc_anomaly_{prefix}_{OUTPUT_DATE}.csv")
    narrative_csv = os.path.join(OUTPUT_DIR, f"loc_anomaly_{prefix}_narratives_{OUTPUT_DATE}.csv")

    print(f"\n{'=' * 60}")
    print(f"  Population: {prefix.upper()}  |  {table}")

    df         = load_aggregated(conn, table)
    dimensions = df["_dimension"].unique()
    print(f"  {len(df):,} rows  |  dimensions: {list(dimensions)}")

    # Model each dimension
    all_scored = []
    for dim, group in df.groupby("_dimension"):
        scored = (
            group
            .pipe(run_model, label = dim)
            .pipe(add_z_scores)
        )
        scored[["top_reason_1", "top_reason_2", "top_reason_3"]] = scored.apply(
            top_reasons, axis = 1
        )
        all_scored.append(scored)

    if not all_scored:
        print(f"  No results for {prefix} — skipping.")
        return

    # Export scores
    id_cols    = ["_dimension", "_dim_value", "case_count", "membership"]
    rate_cols  = [f for f in RATE_FEATURES if f in all_scored[0].columns]
    score_cols = ["raw_score", "_features_used", "top_reason_1", "top_reason_2", "top_reason_3"]

    final = (
        pd.concat(all_scored, ignore_index = True)
        .sort_values(["_dimension", "raw_score"], ascending = [True, True])
    )
    final[[c for c in id_cols + rate_cols + score_cols if c in final.columns]].to_csv(
        output_csv, index = False
    )
    print(f"  Scored output → {output_csv}")

    # Narratives
    narrative_rows = []
    for dim, group in final.groupby("_dimension"):
        top_n        = group.nsmallest(TOP_N, "raw_score")
        peer_medians = group[[f for f in RATE_FEATURES if f in group.columns]].median()
        for _, row in top_n.iterrows():
            narrative_rows.append({
                "dimension":  dim,
                "dim_value":  row["_dim_value"],
                "case_count": int(row["case_count"]),
                "raw_score":  row["raw_score"],
                "narrative":  generate_narrative(row, peer_medians, dim),
            })

    pd.DataFrame(narrative_rows).to_csv(narrative_csv, index = False)
    print(f"  Narratives    → {narrative_csv}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok = True)

    print("=" * 60)
    print("LOC Anomaly Detection — connecting to Snowflake...")
    conn = get_connection()

    for table, prefix in POPULATIONS:
        run_population(conn, table, prefix)

    conn.close()
    print("\n" + "=" * 60)
    print("Done.")


if __name__ == "__main__":
    main()
