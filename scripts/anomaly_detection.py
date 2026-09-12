"""
anomaly_detection.py

Reads agg_team_daily, runs Isolation Forest on team KPIs, writes anomaly
flags to fact_kpi_anomalies.

Run:
  set -a && source .env && set +a
  python3 -u scripts/anomaly_detection.py
"""

import os
import pandas as pd
import pymssql
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

SQL_HOST     = os.environ["SQL_HOST"]
SQL_DATABASE = os.environ["SQL_DATABASE"]
SQL_ADMIN    = os.environ["SQL_ADMIN"]
SQL_PASSWORD = os.environ["SQL_PASSWORD"]

KPI_COLS = [
    "total_items_handled",
    "avg_aht",
    "avg_csat",
    "avg_resolution_rate",
    "avg_fcr",
    "avg_concurrent_chats",
    "avg_response_hours",
    "avg_sla_compliance",
    "avg_days_to_close",
    "avg_reopen_rate",
]


def get_conn():
    return pymssql.connect(
        server=SQL_HOST,
        user=SQL_ADMIN,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
    )


def load_agg_team_daily(conn):
    query = """
        SELECT team_id, date_id,
               total_items_handled, avg_aht, avg_csat, avg_resolution_rate,
               avg_fcr, avg_concurrent_chats, avg_response_hours,
               avg_sla_compliance, avg_days_to_close, avg_reopen_rate
        FROM agg_team_daily
    """
    return pd.read_sql(query, conn)


def detect_anomalies(df):
    records = []

    for team_id, group in df.groupby("team_id"):
        available = [c for c in KPI_COLS if group[c].notna().sum() >= 10]
        if not available:
            continue

        X = group[available].fillna(group[available].median())
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model = IsolationForest(contamination=0.05, random_state=42)
        scores = model.fit_predict(X_scaled)
        raw_scores = model.score_samples(X_scaled)

        for i, (_, row) in enumerate(group.iterrows()):
            for kpi in available:
                records.append({
                    "team_id":      int(team_id),
                    "date_id":      int(row["date_id"]),
                    "kpi_name":     kpi,
                    "anomaly_score": round(float(-raw_scores[i]), 6),
                    "is_anomaly":   1 if scores[i] == -1 else 0,
                })

    return pd.DataFrame(records)


def write_anomalies(conn, df):
    if df.empty:
        print("No anomaly records to write.")
        return

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE fact_kpi_anomalies")

    batch_size = 200
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        rows = [
            (
                int(r["team_id"]),
                int(r["date_id"]),
                r["kpi_name"],
                float(r["anomaly_score"]),
                int(r["is_anomaly"]),
            )
            for _, r in batch.iterrows()
        ]
        cursor.executemany(
            """
            INSERT INTO fact_kpi_anomalies
                (team_id, date_id, kpi_name, anomaly_score, is_anomaly)
            VALUES (%d, %d, %s, %f, %d)
            """,
            rows,
        )
        conn.commit()
        print(f"  {min(start + batch_size, len(df))}/{len(df)} anomaly rows written ...")

    cursor.close()


def main():
    print("=== anomaly_detection.py ===\n")

    conn = get_conn()
    print("Loading agg_team_daily ...")
    df = load_agg_team_daily(conn)
    print(f"  {len(df)} rows loaded ({df['team_id'].nunique()} teams)")

    print("Running Isolation Forest ...")
    anomalies = detect_anomalies(df)
    flagged = anomalies["is_anomaly"].sum()
    print(f"  {len(anomalies)} KPI-day records scored, {flagged} flagged as anomalies")

    print("Writing to fact_kpi_anomalies ...")
    write_anomalies(conn, anomalies)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
