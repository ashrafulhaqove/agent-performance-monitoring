import pymssql, os, re
from pathlib import Path

root = Path(__file__).parent.parent

conn = pymssql.connect(
    server=os.environ["SQL_HOST"],
    database=os.environ["SQL_DATABASE"],
    user=os.environ["SQL_ADMIN"],
    password=os.environ["SQL_PASSWORD"],
    tds_version="7.4",
)
cursor = conn.cursor()

# SQL Server error numbers that mean "object already exists" — safe to skip on re-runs
ALREADY_EXISTS = {2714, 1913, 2705}

files = [
    root / "sql" / "01_schema.sql",
    root / "sql" / "02_stored_procedure.sql",
]

for fpath in files:
    print(f"\n--- {fpath.name} ---")
    sql = fpath.read_text()
    if re.search(r"^\s*GO\s*$", sql, re.MULTILINE | re.IGNORECASE):
        batches = re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)
    else:
        batches = re.split(r";(?=\s*\n|$)", sql)
    statements = [s.strip() for s in batches if s.strip()]
    for stmt in statements:
        try:
            cursor.execute(stmt)
            conn.commit()
            print(f"  OK    {stmt.splitlines()[0][:80]}")
        except pymssql.OperationalError as e:
            errno = e.args[0]
            if errno in ALREADY_EXISTS:
                print(f"  SKIP  {stmt.splitlines()[0][:80]}")
            else:
                print(f"  ERR   [{errno}] {e.args[1][:120]}")
        except Exception as e:
            print(f"  ERR   {str(e)[:120]}")

conn.close()
print("\nDone.")
