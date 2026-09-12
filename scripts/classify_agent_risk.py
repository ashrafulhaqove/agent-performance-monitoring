"""
classify_agent_risk.py

Reads fact_daily_metrics, trains a Random Forest classifier to score each
agent's risk of underperformance, writes results to fact_agent_risk.

Risk label definition (used as training target):
  high_risk = 1 if agent is in bottom 20% of resolution_rate AND csat_score
              for their channel on that day, else 0

Run:
  set -a && source .env && set +a
  python3 -u scripts/classify_agent_risk.py
"""

import os
from datetime import date
import pandas as pd
import numpy as np
import pymssql
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

SQL_HOST     = os.environ["SQL_HOST"]
SQL_DATABASE = os.environ["SQL_DATABASE"]
SQL_ADMIN    = os.environ["SQL_ADMIN"]
SQL_PASSWORD = os.environ["SQL_PASSWORD"]

FEATURE_COLS = [
    "items_handled",
    "resolution_rate",
    "csat_score",
    "aht_seconds",
    "first_call_resolution",
    "concurrent_chats_avg",
    "avg_response_hours",
    "sla_compliance",
    "avg_days_to_close",
    "reopen_rate",
]

FEATURE_IMPORTANCES = [
    "items_handled",
    "resolution_rate",
    "csat_score",
    "aht_seconds",
    "first_call_resolution",
    "concurrent_chats_avg",
    "avg_response_hours",
    "sla_compliance",
    "avg_days_to_close",
    "reopen_rate",
]


def get_conn():
    return pymssql.connect(
        server=SQL_HOST,
        user=SQL_ADMIN,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
    )


def load_metrics(conn):
    query = """
        SELECT
            f.agent_id, f.channel_id, f.date_id,
            f.items_handled, f.resolution_rate, f.csat_score,
            f.aht_seconds, f.first_call_resolution, f.concurrent_chats_avg,
            f.avg_response_hours, f.sla_compliance,
            f.avg_days_to_close, f.reopen_rate
        FROM fact_daily_metrics f
    """
    return pd.read_sql(query, conn)


def build_risk_labels(df):
    """Bottom 20% of resolution_rate AND csat_score within their channel = at risk."""
    df = df.copy()
    p20_res  = df.groupby("channel_id")["resolution_rate"].transform(lambda x: x.quantile(0.20))
    p20_csat = df.groupby("channel_id")["csat_score"].transform(lambda x: x.quantile(0.20))
    df["risk_label"] = (
        (df["resolution_rate"] <= p20_res) & (df["csat_score"] <= p20_csat)
    ).astype(int)
    return df


def train_and_score(df):
    df = build_risk_labels(df)
    X = df[FEATURE_COLS].fillna(df[FEATURE_COLS].median())
    y = df["risk_label"]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X_scaled, y)

    risk_proba = model.predict_proba(X_scaled)[:, 1]
    importances = model.feature_importances_

    df = df.copy()
    df["risk_score"] = risk_proba
    df["risk_flag"]  = (risk_proba >= 0.5).astype(int)

    # Top risk factor = feature with highest importance that is not null for this row
    top_factor_idx = int(np.argmax(importances))
    df["top_risk_factor"] = FEATURE_IMPORTANCES[top_factor_idx]

    return df[["agent_id", "risk_score", "risk_flag", "top_risk_factor"]]


def aggregate_to_agent(scored_df):
    """One risk score per agent = average across all their daily rows."""
    agg = (
        scored_df
        .groupby("agent_id")
        .agg(
            risk_score=("risk_score", "mean"),
            risk_flag=("risk_flag", lambda x: int(x.mean() >= 0.5)),
            top_risk_factor=("top_risk_factor", "first"),
        )
        .reset_index()
    )
    agg["scored_date"] = date.today().isoformat()
    return agg


def write_risk(conn, df):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE fact_agent_risk")

    batch_size = 100
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        rows = [
            (
                int(r["agent_id"]),
                r["scored_date"],
                round(float(r["risk_score"]), 4),
                int(r["risk_flag"]),
                r["top_risk_factor"],
            )
            for _, r in batch.iterrows()
        ]
        cursor.executemany(
            """
            INSERT INTO fact_agent_risk
                (agent_id, scored_date, risk_score, risk_flag, top_risk_factor)
            VALUES (%d, %s, %f, %d, %s)
            """,
            rows,
        )
        conn.commit()
        print(f"  {min(start + batch_size, len(df))}/{len(df)} agent risk rows written ...")

    cursor.close()


def main():
    print("=== classify_agent_risk.py ===\n")

    conn = get_conn()
    print("Loading fact_daily_metrics ...")
    df = load_metrics(conn)
    print(f"  {len(df)} rows loaded ({df['agent_id'].nunique()} agents)")

    print("Training Random Forest classifier ...")
    scored = train_and_score(df)
    agent_scores = aggregate_to_agent(scored)
    flagged = agent_scores["risk_flag"].sum()
    print(f"  {len(agent_scores)} agents scored, {flagged} flagged as at-risk")

    print("Writing to fact_agent_risk ...")
    write_risk(conn, agent_scores)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
