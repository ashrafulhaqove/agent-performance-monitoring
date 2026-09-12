import os
import random
import io
import time
from datetime import date, timedelta
import pandas as pd
import pymssql
from azure.storage.blob import BlobServiceClient

AGENT_BATCH_SIZE = 50    # inserts per batch before pausing
AGENT_BATCH_PAUSE = 2    # seconds between agent insert batches (DTU relief)
UPLOAD_BATCH_SIZE = 30   # blob uploads before pausing
UPLOAD_BATCH_PAUSE = 1   # seconds between upload batches

# ── connection config ─────────────────────────────────────────────────────────
SQL_CONN = dict(
    server=os.environ["SQL_HOST"],
    database=os.environ["SQL_DATABASE"],
    user=os.environ["SQL_ADMIN"],
    password=os.environ["SQL_PASSWORD"],
    tds_version="7.4",
)
STORAGE_CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER = "bronze"

# ── simulation parameters ─────────────────────────────────────────────────────
START_DATE   = date(2024, 1, 1)
DAYS         = 90
N_AGENTS     = 500
random.seed(42)

SERVICE_LINES = ["Customer Service", "Seller Service"]
CHANNELS      = ["Call", "Chat", "Email", "Issue Resolution"]
DIRECTIONS    = ["Inbound", "Outbound"]

CHANNEL_SLUG = {
    "Call":             "call",
    "Chat":             "chat",
    "Email":            "email",
    "Issue Resolution": "issue_resolution",
}

FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
    "William","Barbara","David","Elizabeth","Richard","Susan","Joseph","Jessica",
    "Thomas","Sarah","Charles","Karen","Christopher","Lisa","Daniel","Nancy",
    "Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley",
    "Steven","Dorothy","Paul","Kimberly","Andrew","Emily","Kenneth","Donna",
    "Joshua","Michelle","Kevin","Carol","Brian","Amanda","George","Melissa",
    "Timothy","Deborah","Ronald","Stephanie",
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson",
    "White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker",
    "Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
    "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell",
    "Carter","Roberts",
]


# ── helpers ───────────────────────────────────────────────────────────────────
def rnd(lo, hi, decimals=2):
    return round(random.uniform(lo, hi), decimals)

def get_conn():
    return pymssql.connect(**SQL_CONN)


# ── dimension seeding ─────────────────────────────────────────────────────────
def seed_dims(conn):
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM dim_service_lines")
    if cur.fetchone()[0] > 0:
        print("Dimensions already seeded — reading IDs.")
        return _read_dims(cur)

    print("Seeding dimensions...")

    # dim_service_lines
    for sl in SERVICE_LINES:
        cur.execute("INSERT INTO dim_service_lines (service_line_name) VALUES (%s)", (sl,))
    conn.commit()
    cur.execute("SELECT service_line_id, service_line_name FROM dim_service_lines")
    sl_map = {name: sid for sid, name in cur.fetchall()}

    # dim_channels (4 channels × 2 directions = 8 rows)
    for ch in CHANNELS:
        for direction in DIRECTIONS:
            cur.execute(
                "INSERT INTO dim_channels (channel_name, direction) VALUES (%s, %s)",
                (ch, direction),
            )
    conn.commit()
    cur.execute("SELECT channel_id, channel_name, direction FROM dim_channels")
    ch_map = {(name, d): cid for cid, name, d in cur.fetchall()}

    # dim_teams (16 teams)
    team_rows = []
    for sl_name in SERVICE_LINES:
        for ch_name in CHANNELS:
            for direction in DIRECTIONS:
                team_name = f"{sl_name} - {ch_name} {direction}"
                cur.execute(
                    "INSERT INTO dim_teams (service_line_id, channel_id, team_name) "
                    "VALUES (%d, %d, %s)",
                    (sl_map[sl_name], ch_map[(ch_name, direction)], team_name),
                )
                conn.commit()
                cur.execute("SELECT @@IDENTITY")
                team_id = int(cur.fetchone()[0])
                team_rows.append({
                    "team_id":      team_id,
                    "channel_id":   ch_map[(ch_name, direction)],
                    "channel_name": ch_name,
                })
    print(f"  {len(team_rows)} teams created")

    # dim_agents (500 agents spread across 16 teams)
    agent_rows = []
    for idx, team in enumerate(team_rows):
        count = N_AGENTS // len(team_rows) + (1 if idx < N_AGENTS % len(team_rows) else 0)
        names_batch = [
            f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
            for _ in range(count)
        ]
        hire_dates = [
            (START_DATE - timedelta(days=random.randint(30, 1825))).strftime("%Y-%m-%d")
            for _ in range(count)
        ]
        for i, (name, hd) in enumerate(zip(names_batch, hire_dates)):
            cur.execute(
                "INSERT INTO dim_agents (team_id, agent_name, hire_date) VALUES (%d, %s, %s)",
                (team["team_id"], name, hd),
            )
            conn.commit()
            cur.execute("SELECT @@IDENTITY")
            agent_rows.append({
                "agent_id":    int(cur.fetchone()[0]),
                "team_id":     team["team_id"],
                "channel_id":  team["channel_id"],
                "channel_name": team["channel_name"],
            })
            total_inserted = len(agent_rows)
            if total_inserted % AGENT_BATCH_SIZE == 0:
                print(f"  {total_inserted} agents inserted — pausing {AGENT_BATCH_PAUSE}s...")
                time.sleep(AGENT_BATCH_PAUSE)
    print(f"  {len(agent_rows)} agents created")

    # dim_date (90 rows)
    for i in range(DAYS):
        d = START_DATE + timedelta(days=i)
        cur.execute(
            "INSERT INTO dim_date (date_id, full_date, day_of_week, week_num, month_num, quarter) "
            "VALUES (%d, %s, %d, %d, %d, %d)",
            (
                int(d.strftime("%Y%m%d")),
                d.strftime("%Y-%m-%d"),
                d.isoweekday(),
                d.isocalendar()[1],
                d.month,
                (d.month - 1) // 3 + 1,
            ),
        )
    conn.commit()
    print(f"  {DAYS} date rows created")

    return agent_rows


def _read_dims(cur):
    cur.execute("""
        SELECT a.agent_id, a.team_id, t.channel_id, c.channel_name
        FROM   dim_agents a
        JOIN   dim_teams   t ON t.team_id    = a.team_id
        JOIN   dim_channels c ON c.channel_id = t.channel_id
    """)
    return [
        {"agent_id": r[0], "team_id": r[1], "channel_id": r[2], "channel_name": r[3]}
        for r in cur.fetchall()
    ]


# ── KPI generation ────────────────────────────────────────────────────────────
def make_row(agent, date_id):
    ch = agent["channel_name"]
    base = {
        "agent_id":           agent["agent_id"],
        "team_id":            agent["team_id"],
        "channel_id":         agent["channel_id"],
        "date_id":            date_id,
        "channel_name":       ch,
        "items_handled":      None,
        "resolution_rate":    round(rnd(0.60, 0.99), 2),
        "csat_score":         round(rnd(1.00, 5.00), 2),
        "aht_seconds":        None,
        "first_call_resolution": None,
        "concurrent_chats_avg":  None,
        "avg_response_hours":    None,
        "sla_compliance":        None,
        "avg_days_to_close":     None,
        "reopen_rate":           None,
    }
    if ch == "Call":
        base["items_handled"]          = random.randint(30, 80)
        base["aht_seconds"]            = random.randint(180, 600)
        base["first_call_resolution"]  = rnd(0.50, 0.95)
    elif ch == "Chat":
        base["items_handled"]          = random.randint(20, 60)
        base["aht_seconds"]            = random.randint(120, 480)
        base["concurrent_chats_avg"]   = rnd(1.5, 4.0)
    elif ch == "Email":
        base["items_handled"]          = random.randint(15, 50)
        base["avg_response_hours"]     = rnd(1.0, 48.0)
        base["sla_compliance"]         = rnd(0.70, 1.00)
    elif ch == "Issue Resolution":
        base["items_handled"]          = random.randint(5, 25)
        base["sla_compliance"]         = rnd(0.65, 0.99)
        base["avg_days_to_close"]      = rnd(0.5, 10.0)
        base["reopen_rate"]            = rnd(0.02, 0.20)
    return base


# ── upload ────────────────────────────────────────────────────────────────────
def upload_csvs(rows):
    blob_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    df = pd.DataFrame(rows)

    total = 0
    for ch_name, ch_slug in CHANNEL_SLUG.items():
        ch_df = df[df["channel_name"] == ch_name].drop(columns=["channel_name"])
        ch_total = 0
        for i in range(DAYS):
            d = START_DATE + timedelta(days=i)
            date_id = int(d.strftime("%Y%m%d"))
            day_df = ch_df[ch_df["date_id"] == date_id]
            if day_df.empty:
                continue

            blob_name = f"{ch_slug}/{d.strftime('%Y-%m-%d')}.csv"
            buf = io.StringIO()
            day_df.to_csv(buf, index=False)

            blob = blob_client.get_blob_client(container=CONTAINER, blob=blob_name)
            blob.upload_blob(buf.getvalue(), overwrite=True)
            total += 1
            ch_total += 1

            if total % UPLOAD_BATCH_SIZE == 0:
                print(f"  {total} files uploaded so far — pausing {UPLOAD_BATCH_PAUSE}s...")
                time.sleep(UPLOAD_BATCH_PAUSE)

        print(f"  {ch_name}: {ch_total} files uploaded → bronze/{ch_slug}/")

    print(f"\nTotal files uploaded: {total}")


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    print("=== generate_data.py ===\n")

    conn = get_conn()
    agents = seed_dims(conn)
    conn.close()
    print(f"\nGenerating {DAYS} days × {len(agents)} agents = {DAYS * len(agents):,} rows...")

    rows = []
    for i in range(DAYS):
        d = START_DATE + timedelta(days=i)
        date_id = int(d.strftime("%Y%m%d"))
        for agent in agents:
            rows.append(make_row(agent, date_id))

    print(f"Generated {len(rows):,} rows. Uploading to ADLS...")
    upload_csvs(rows)
    print("\nDone. Bronze container populated.")


if __name__ == "__main__":
    main()
