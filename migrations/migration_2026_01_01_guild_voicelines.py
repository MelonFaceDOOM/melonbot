"""
Adds tables for persistent, per-guild keyword -> saved voicelines.
"""

from config import PSQL_CREDENTIALS
import psycopg2


def apply_migration():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()

    cur.execute("""CREATE EXTENSION IF NOT EXISTS citext;""")
    # 2) A saved voiceline instance (guild-scoped) that points to an audio blob
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_voicelines (
            id                 BIGSERIAL PRIMARY KEY,
            guild_id           BIGINT NOT NULL,
            created_by_user_id BIGINT,
            created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            name               CITEXT NOT NULL,
            storage_path       TEXT NOT NULL,                 -- e.g. relative path: 'blobs/ab/<hash>.ogg'
            format             VARCHAR(32) NOT NULL,          -- e.g. 'ogg_opus'
            byte_size          INTEGER NOT NULL,
            duration_ms        INTEGER,
            FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
            FOREIGN KEY (created_by_user_id) REFERENCES users (id) ON DELETE SET NULL,
            UNIQUE (guild_id, name)
        );
        """
    )

    # Indexes for fast lookups and basic admin queries
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS guild_voicelines_guild_idx
            ON guild_voicelines (guild_id);
        """
    )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    apply_migration()
    print("Migration applied.")
