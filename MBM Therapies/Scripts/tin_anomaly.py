"""
Isolation Forest — Therapy Provider Anomaly Detection
Unit: prov_tin x category_1 x market_fnl
Two separate models: optum_tin_flag=1 and optum_tin_flag=0
"""

import os
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FEATURE_TABLE = "tmp_1m.knd_mbm_tin_features_202604"
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "..", "Output", "tin_anomaly_202604.csv")
NARRATIVE_CSV = os.path.join(os.path.dirname(__file__), "..", "Output", "tin_anomaly_narratives_202604.csv")

FEATURES = [
    "avg_visits_per_ep",
    "avg_allowed_per_visit",
    "avg_allowed_per_ep",
    "allowed_per_member",
    "denial_rate",
    "market_count",
    "diag_diversity",
]

IDENTIFIERS = ["prov_tin", "category_1", "market_fnl", "optum_tin_flag"]

CONTAMINATION = 0.05
TOP_N_NARRATIVE = 20


# ---------------------------------------------------------------------------
# Snowflake connection — edit credentials or set as env vars
# ---------------------------------------------------------------------------

def get_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "PROD"),
        schema="tmp_1m",
    )


def load_data(conn) -> pd.DataFrame:
    query = f"select * from {FEATURE_TABLE}"
    return pd.read_sql(query, conn)


# ---------------------------------------------------------------------------
# Modeling
# ---------------------------------------------------------------------------

def run_model(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Fit Isolation Forest on df, return df with raw_score appended."""
    X = df[FEATURES].copy()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=200,
        contamination=CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    df = df.copy()
    df["raw_score"] = model.decision_function(X_scaled)  # lower = more anomalous

    print(f"[{label}] n={len(df):,}  flagged={( df['raw_score'] < 0).sum():,}")
    return df


def add_z_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-feature z-scores within each category_1 x market_fnl peer group.
    Adds columns: <feature>_z for each feature, plus top_reason_1/2/3.
    """
    df = df.copy()
    z_cols = []

    for feat in FEATURES:
        z_col = f"{feat}_z"
        z_cols.append(z_col)
        group_mean = df.groupby(["category_1", "market_fnl"])[feat].transform("mean")
        group_std = df.groupby(["category_1", "market_fnl"])[feat].transform("std")
        df[z_col] = (df[feat] - group_mean) / group_std.replace(0, np.nan)

    def top_reasons(row):
        zs = {feat: abs(row[f"{feat}_z"]) for feat in FEATURES if pd.notna(row[f"{feat}_z"])}
        sorted_feats = sorted(zs, key=zs.get, reverse=True)
        results = {}
        for i, feat in enumerate(sorted_feats[:3], 1):
            z = row[f"{feat}_z"]
            direction = "above" if z > 0 else "below"
            results[f"top_reason_{i}"] = f"{feat} ({z:+.1f}σ {direction} peer mean)"
        for i in range(len(sorted_feats) + 1, 4):
            results[f"top_reason_{i}"] = ""
        return pd.Series(results)

    df[["top_reason_1", "top_reason_2", "top_reason_3"]] = df.apply(top_reasons, axis=1)
    return df


# ---------------------------------------------------------------------------
# Narrative generation
# ---------------------------------------------------------------------------

def peer_stats(df: pd.DataFrame, row: pd.Series) -> dict:
    """Median of each feature for the row's category_1 x market_fnl peer group."""
    peers = df[
        (df["category_1"] == row["category_1"])
        & (df["market_fnl"] == row["market_fnl"])
    ]
    return peers[FEATURES].median().to_dict()


def generate_narrative(row: pd.Series, peers: dict) -> str:
    lines = []
    lines.append(
        f"TIN {row['prov_tin']} ({row['category_1']}, {row['market_fnl']} market, "
        f"{'Optum' if row['optum_tin_flag'] == 1 else 'UHC'} network) "
        f"— anomaly score: {row['raw_score']:.3f}"
    )

    def fmt_usd(v):
        return f"${v:,.2f}"

    def fmt_pct(v):
        return f"{v * 100:.1f}%"

    comparisons = [
        ("avg_allowed_per_visit",  fmt_usd,  "average allowed per visit"),
        ("avg_visits_per_ep",      lambda v: f"{v:.1f}", "average visits per episode"),
        ("avg_allowed_per_ep",     fmt_usd,  "average allowed per episode"),
        ("allowed_per_member",     fmt_usd,  "allowed per member"),
        ("denial_rate",            fmt_pct,  "denial rate"),
        ("market_count",           lambda v: f"{int(v)}", "number of markets served"),
        ("diag_diversity",         lambda v: f"{v:.3f}", "diagnosis diversity ratio"),
    ]

    for feat, fmt, label in comparisons:
        val = row[feat]
        peer_val = peers.get(feat)
        z_col = f"{feat}_z"
        z = row.get(z_col, np.nan)
        if pd.notna(z) and abs(z) >= 1.5:
            direction = "above" if z > 0 else "below"
            lines.append(
                f"  • {label.capitalize()}: {fmt(val)} "
                f"vs. peer median {fmt(peer_val)} ({z:+.1f}σ {direction} peer group)"
            )

    if len(lines) == 1:
        lines.append("  • No individual feature stands out strongly; flagged based on combined pattern.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    print("Connecting to Snowflake...")
    conn = get_connection()
    df = load_data(conn)
    conn.close()

    df.columns = [c.lower() for c in df.columns]
    print(f"Loaded {len(df):,} rows from {FEATURE_TABLE}")

    results = []
    for flag, label in [(1, "Optum"), (0, "UHC")]:
        subset = df[df["optum_tin_flag"] == flag].copy()
        if len(subset) < 20:
            print(f"[{label}] Skipping — too few rows ({len(subset)})")
            continue
        scored = run_model(subset, label)
        scored = add_z_scores(scored)
        results.append(scored)

    if not results:
        print("No results — check data filters.")
        return

    final = pd.concat(results, ignore_index=True)
    final = final.sort_values(["optum_tin_flag", "raw_score"], ascending=[False, True])

    output_cols = IDENTIFIERS + ["episode_count", "member_count"] + FEATURES + [
        "raw_score",
        "top_reason_1", "top_reason_2", "top_reason_3",
    ]
    final[output_cols].to_csv(OUTPUT_CSV, index=False)
    print(f"Scored output written to {OUTPUT_CSV}")

    # Narratives for top 20 per model
    narrative_rows = []
    for flag, label in [(1, "Optum"), (0, "UHC")]:
        subset = final[final["optum_tin_flag"] == flag]
        top20 = subset.nsmallest(TOP_N_NARRATIVE, "raw_score")
        for _, row in top20.iterrows():
            peers = peer_stats(final, row)
            text = generate_narrative(row, peers)
            narrative_rows.append({
                "optum_tin_flag": flag,
                "model": label,
                "prov_tin": row["prov_tin"],
                "category_1": row["category_1"],
                "market_fnl": row["market_fnl"],
                "raw_score": row["raw_score"],
                "narrative": text,
            })

    pd.DataFrame(narrative_rows).to_csv(NARRATIVE_CSV, index=False)
    print(f"Narratives written to {NARRATIVE_CSV}")


if __name__ == "__main__":
    main()
