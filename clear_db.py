

import os
from pathlib import Path

import pymysql
from dotenv import load_dotenv


def _load_backend_env() -> None:
    project_root = Path(__file__).resolve().parent
    backend_env = project_root / "backend" / ".env"
    load_dotenv(dotenv_path=str(backend_env), override=True)


_load_backend_env()

DB_HOST = os.getenv("DB_HOST", "").strip()
if not DB_HOST:
    raise SystemExit("ERROR: DB_HOST must be set in backend/.env")

db_config = {
    "host": DB_HOST,
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "torroforexcel"),
    "connect_timeout": 60,
    "read_timeout": 300,
    "write_timeout": 300,
}

conn = pymysql.connect(**db_config)
cursor = conn.cursor()

try:
    print("Clearing database tables...")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("SET SESSION wait_timeout = 600")
    cursor.execute("SET SESSION interactive_timeout = 600")
    

    print("  Deleting from lineage_relationships...")
    cursor.execute("DELETE FROM lineage_relationships")
    print("  Deleting from sql_queries...")
    cursor.execute("DELETE FROM sql_queries")
    print("  Deleting from data_discovery...")
    cursor.execute("DELETE FROM data_discovery")
    print("  Deleting from assets...")
    cursor.execute("DELETE FROM assets")
    print("  Deleting from connections...")
    cursor.execute("DELETE FROM connections")
    

    print("  Resetting AUTO_INCREMENT...")
    cursor.execute("ALTER TABLE data_discovery AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE connections AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE assets AUTO_INCREMENT = 1")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    

    cursor.execute("SELECT COUNT(*) FROM assets")
    asset_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM connections")
    conn_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM data_discovery")
    disc_count = cursor.fetchone()[0]
    
    print("✅ Database cleared successfully")
    print(f"✅ Verification: Assets={asset_count}, Connections={conn_count}, Discoveries={disc_count}")
except Exception as e:
    conn.rollback()
    print(f"❌ Error: {e}")
finally:
    cursor.close()
    conn.close()

