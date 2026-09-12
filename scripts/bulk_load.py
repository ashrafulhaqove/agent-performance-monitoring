"""
bulk_load.py — one-time script to load all 90 days into SQL and run aggregation.
Not committed to the repo — scratch use only.
"""
import os, io, time
import pandas as pd
import pymssql
from azure.storage.blob import BlobServiceClient
from datetime import date, timedelta

SQL_HOST     = os.environ["SQL_HOST"]
SQL_DATABASE = os.environ["SQL_DATABASE"]
SQL_ADMIN    = os.environ["SQL_ADMIN"]
SQL_PASSWORD = os.environ["SQL_PASSWORD"]
STORAGE_CONN = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

CHANNELS   = ["call", "chat", "email", "issue_resolution"]
START_DATE = date(2024, 1, 1)
DAYS       = 90

def get_conn():
    return pymssql.connect(server=SQL_HOST, user=SQL_ADMIN,
                           password=SQL_PASSWORD, database=SQL_DATABASE)

def load_all():
    blob_client = BlobServiceClient.from_connection_string(STORAGE_CONN)
    container   = blob_client.get_container_client("bronze")
    conn = get_conn()
    cur  = conn.cursor()

    # Clear existing data
    cur.execute("TRUNCATE TABLE agg_team_daily")
    cur.execute("TRUNCATE TABLE fact_daily_metrics")
    conn.commit()
    print("Tables cleared.")

    total = 0
    for day_idx in range(DAYS):
        d = START_DATE + timedelta(days=day_idx)
        date_str = d.strftime("%Y-%m-%d")
        date_id  = int(d.strftime("%Y%m%d"))

        for ch in CHANNELS:
            blob_name = f"{ch}/{date_str}.csv"
            try:
                data = container.get_blob_client(blob_name).download_blob().readall()
            except Exception:
                continue

            df = pd.read_csv(io.BytesIO(data))
            rows = []
            for _, r in df.iterrows():
                def nv(val):
                    return None if pd.isna(val) else float(val)
                rows.append((
                    int(r["agent_id"]), int(r["team_id"]), int(r["channel_id"]),
                    int(r["date_id"]),
                    int(r["items_handled"]),
                    float(r["resolution_rate"]),
                    float(r["csat_score"]),
                    None if pd.isna(r["aht_seconds"])            else int(r["aht_seconds"]),
                    nv(r["first_call_resolution"]),
                    nv(r["concurrent_chats_avg"]),
                    nv(r["avg_response_hours"]),
                    nv(r["sla_compliance"]),
                    nv(r["avg_days_to_close"]),
                    nv(r["reopen_rate"]),
                ))

            cur.executemany(
                """
                INSERT INTO fact_daily_metrics
                    (agent_id, team_id, channel_id, date_id, items_handled,
                     resolution_rate, csat_score, aht_seconds, first_call_resolution,
                     concurrent_chats_avg, avg_response_hours, sla_compliance,
                     avg_days_to_close, reopen_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
            conn.commit()
            total += len(rows)

        # Run aggregation SP for this date
        cur.execute(f"EXEC sp_aggregate_team_metrics @ExecutionDate = '{date_str}'")
        conn.commit()

        if (day_idx + 1) % 10 == 0:
            print(f"  {day_idx+1}/90 days loaded, {total:,} rows total ...")
            time.sleep(2)  # DTU relief

    conn.close()
    print(f"\nDone. {total:,} fact rows loaded, aggregation run for all 90 days.")

if __name__ == "__main__":
    print("=== bulk_load.py ===\n")
    load_all()
