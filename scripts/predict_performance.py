"""
predict_performance.py

Reads agg_team_daily, trains a Linear Regression model per team per KPI,
forecasts the next 7 days, writes to agg_team_predictions.

Run:
  set -a && source .env && set +a
  python3 -u scripts/predict_performance.py
"""

import os
from datetime import date, timedelta
import pandas as pd
import numpy as np
import pymssql
from sklearn.linear_model import LinearRegression

SQL_HOST     = os.environ["SQL_HOST"]
SQL_DATABASE = os.environ["SQL_DATABASE"]
SQL_ADMIN    = os.environ["SQL_ADMIN"]
SQL_PASSWORD = os.environ["SQL_PASSWORD"]

# KPIs to forecast — skip if a team has fewer than 14 non-null rows for that KPI
FORECAST_KPIS = [
    "total_items_handled",
    "avg_csat",
    "avg_resolution_rate",
    "avg_aht",
    "avg_sla_compliance",
]

FORECAST_DAYS = 7


def get_conn():
    return pymssql.connect(
        server=SQL_HOST,
        user=SQL_ADMIN,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
    )


def load_agg(conn):
    query = """
        SELECT t.team_id, d.full_date,
               a.total_items_handled, a.avg_aht, a.avg_csat,
               a.avg_resolution_rate, a.avg_sla_compliance
        FROM agg_team_daily a
        JOIN dim_date d ON d.date_id = a.date_id
        JOIN dim_teams t ON t.team_id = a.team_id
        ORDER BY t.team_id, d.full_date
    """
    df = pd.read_sql(query, conn)
    df["full_date"] = pd.to_datetime(df["full_date"])
    return df


def forecast_team_kpi(group, kpi, last_date):
    series = group[["full_date", kpi]].dropna()
    if len(series) < 14:
        return []

    series = series.sort_values("full_date").reset_index(drop=True)
    # day index as feature (0, 1, 2 ... N)
    series["day_idx"] = (series["full_date"] - series["full_date"].min()).dt.days

    X = series[["day_idx"]].values
    y = series[kpi].values

    model = LinearRegression()
    model.fit(X, y)

    last_idx = int(series["day_idx"].max())
    std_resid = float(np.std(y - model.predict(X)))

    records = []
    for i in range(1, FORECAST_DAYS + 1):
        pred_idx  = last_idx + i
        pred_date = last_date + timedelta(days=i)
        pred_val  = float(model.predict([[pred_idx]])[0])
        records.append({
            "prediction_date": pred_date.date().isoformat(),
            "predicted_kpi":   kpi,
            "predicted_value": round(pred_val, 4),
            "lower_bound":     round(pred_val - 1.96 * std_resid, 4),
            "upper_bound":     round(pred_val + 1.96 * std_resid, 4),
        })
    return records


def forecast_all(df):
    last_date = df["full_date"].max()
    records = []

    for team_id, group in df.groupby("team_id"):
        for kpi in FORECAST_KPIS:
            preds = forecast_team_kpi(group, kpi, last_date)
            for p in preds:
                p["team_id"] = int(team_id)
            records.extend(preds)

    return pd.DataFrame(records)


def write_predictions(conn, df):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE agg_team_predictions")

    batch_size = 200
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        rows = [
            (
                int(r["team_id"]),
                r["prediction_date"],
                r["predicted_kpi"],
                float(r["predicted_value"]),
                float(r["lower_bound"]),
                float(r["upper_bound"]),
            )
            for _, r in batch.iterrows()
        ]
        cursor.executemany(
            """
            INSERT INTO agg_team_predictions
                (team_id, prediction_date, predicted_kpi,
                 predicted_value, lower_bound, upper_bound)
            VALUES (%d, %s, %s, %f, %f, %f)
            """,
            rows,
        )
        conn.commit()
        print(f"  {min(start + batch_size, len(df))}/{len(df)} prediction rows written ...")

    cursor.close()


def main():
    print("=== predict_performance.py ===\n")

    conn = get_conn()
    print("Loading agg_team_daily ...")
    df = load_agg(conn)
    print(f"  {len(df)} rows loaded ({df['team_id'].nunique()} teams)")

    print(f"Forecasting next {FORECAST_DAYS} days per team per KPI ...")
    predictions = forecast_all(df)
    print(f"  {len(predictions)} prediction rows generated")

    print("Writing to agg_team_predictions ...")
    write_predictions(conn, predictions)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
