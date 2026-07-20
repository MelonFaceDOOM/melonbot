"""
swap from sftp to local file storage
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import PSQL_CREDENTIALS
import psycopg2


def apply_migration():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()

    cur.execute("""ALTER TABLE stream_tracker.saved_streams DROP sftp_url""")
    cur.execute("""ALTER TABLE stream_tracker.saved_streams ADD location TEXT""")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    apply_migration()
    print("Migration applied.")
