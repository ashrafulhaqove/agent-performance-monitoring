import pymssql, os, re
from pathlib import Path

root = Path(__file__).parent.parent

server   = os.environ["SQL_HOST"]
database = os.environ["SQL_DATABASE"]
user     = os.environ["SQL_ADMIN"]
password = os.environ["SQL_PASSWORD"]

conn   = pymssql.connect(server=server, database=database, user=user, password=password, tds_version="7.4")
cursor = conn.cursor()

files = [
    root / "sql" / "01_schema.sql",
    root / "sql" / "02_stored_procedure.sql",
]

for fpath in files:
    print(f"\n--- {fpath.name} ---")
    sql = fpath.read_text()
    # Split on GO (batch separator) if present, else on semicolons
    if re.search(r"^\s*GO\s*$", sql, re.MULTILINE | re.IGNORECASE):
        batches = re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)
    else:
        batches = re.split(r";(?=\s*\n|$)", sql)
    statements = [s.strip() for s in batches if s.strip()]
    for stmt in statements:
        if not stmt:
            continue
        try:
            cursor.execute(stmt)
            conn.commit()
            print(f"  OK  {stmt.splitlines()[0][:80]}")
        except Exception as e:
            print(f"  ERR {e}\n      {stmt[:80]}")

conn.close()
print("\nDone.")
