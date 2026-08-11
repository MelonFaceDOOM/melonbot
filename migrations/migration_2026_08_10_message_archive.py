"""
Adds message_archive schema for stealth Discord message/reaction archival.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import PSQL_CREDENTIALS
import psycopg2


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS message_archive;

CREATE TABLE IF NOT EXISTS message_archive.channels (
    id                  BIGINT PRIMARY KEY,
    guild_id            BIGINT NOT NULL,
    name                VARCHAR(128),
    channel_type        SMALLINT,
    parent_id           BIGINT,
    skip                BOOLEAN NOT NULL DEFAULT FALSE,
    backfill_status     TEXT NOT NULL DEFAULT 'pending',
    oldest_message_id   BIGINT,
    newest_message_id   BIGINT,
    last_backfill_at    TIMESTAMPTZ,
    last_error          TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
    CONSTRAINT channels_backfill_status_chk
        CHECK (backfill_status IN (
            'pending', 'in_progress', 'complete', 'failed', 'skipped'
        ))
);

CREATE TABLE IF NOT EXISTS message_archive.messages (
    id                  BIGINT PRIMARY KEY,
    guild_id            BIGINT NOT NULL,
    channel_id          BIGINT NOT NULL,
    author_id           BIGINT NOT NULL,
    content             TEXT,
    created_at          TIMESTAMPTZ NOT NULL,
    edited_at           TIMESTAMPTZ,
    deleted_at          TIMESTAMPTZ,
    message_type        SMALLINT,
    is_bot              BOOLEAN NOT NULL DEFAULT FALSE,
    webhook_id          BIGINT,
    reference_id        BIGINT,
    thread_id           BIGINT,
    has_attachments     BOOLEAN NOT NULL DEFAULT FALSE,
    raw                 JSONB,
    FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
    FOREIGN KEY (channel_id) REFERENCES message_archive.channels (id) ON DELETE CASCADE,
    FOREIGN KEY (author_id) REFERENCES users (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS message_archive.attachments (
    id                  BIGINT PRIMARY KEY,
    message_id          BIGINT NOT NULL,
    filename            TEXT,
    content_type        TEXT,
    size                BIGINT,
    url                 TEXT,
    FOREIGN KEY (message_id) REFERENCES message_archive.messages (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS message_archive.reactions (
    message_id          BIGINT NOT NULL,
    emoji_key           TEXT NOT NULL,
    emoji_id            BIGINT,
    emoji_name          TEXT,
    is_custom           BOOLEAN NOT NULL DEFAULT FALSE,
    count               INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (message_id, emoji_key),
    FOREIGN KEY (message_id) REFERENCES message_archive.messages (id) ON DELETE CASCADE,
    CONSTRAINT reactions_count_nonneg_chk CHECK (count >= 0)
);

CREATE INDEX IF NOT EXISTS messages_guild_author_created_idx
    ON message_archive.messages (guild_id, author_id, created_at);

CREATE INDEX IF NOT EXISTS messages_channel_id_idx
    ON message_archive.messages (channel_id, id);

CREATE INDEX IF NOT EXISTS messages_guild_created_idx
    ON message_archive.messages (guild_id, created_at);

CREATE INDEX IF NOT EXISTS channels_guild_backfill_idx
    ON message_archive.channels (guild_id, backfill_status);

CREATE INDEX IF NOT EXISTS attachments_message_idx
    ON message_archive.attachments (message_id);
"""


def apply_migration():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    apply_migration()
    print("Migration applied.")
