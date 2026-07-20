"""
Adds display-name cache columns on guilds / users for Nitwitch (and bot).

Idempotent (ADD COLUMN IF NOT EXISTS). Safe if columns already exist.
"""

import sys
from pathlib import Path

# Allow `python migrations/this_file.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import PSQL_CREDENTIALS
import psycopg2


def apply_migration():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()

    cur.execute(
        """
        ALTER TABLE guilds
          ADD COLUMN IF NOT EXISTS name VARCHAR(128),
          ADD COLUMN IF NOT EXISTS icon_url TEXT,
          ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
        """
    )
    cur.execute(
        """
        ALTER TABLE users
          ADD COLUMN IF NOT EXISTS username VARCHAR(64),
          ADD COLUMN IF NOT EXISTS global_name VARCHAR(64),
          ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
        """
    )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    apply_migration()
    print("Migration applied: guilds/users display-name columns.")
