"""
Multi-provider narration (Fish / Gemini / Google):
- narrate_prefs.voice_label, narrate_prefs.style
- tts_usage: month-to-date estimated spend per paid provider (budget cap)
- tts_user_usage: month-to-date estimated spend per user (one user may use at most half the budget)
- tts_guild_usage: month-to-date estimated spend per server (shown by !narrate quota)
- narrate_voice_history: every voice a user picks (shown by !narrate recent), seeded with current voices
- tts_designed_voices: voices users created with `!narrate design`
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import PSQL_CREDENTIALS
import psycopg2

DDL = [
    """
    ALTER TABLE narrate_prefs
      ADD COLUMN IF NOT EXISTS voice_label VARCHAR(256),
      ADD COLUMN IF NOT EXISTS style       VARCHAR(200),
      ADD COLUMN IF NOT EXISTS volume      SMALLINT
    """,
    """
    CREATE TABLE IF NOT EXISTS tts_usage (
        month     DATE NOT NULL,
        provider  VARCHAR(16) NOT NULL,
        requests  INTEGER NOT NULL DEFAULT 0,
        chars     BIGINT NOT NULL DEFAULT 0,
        audio_ms  BIGINT NOT NULL DEFAULT 0,
        est_usd   NUMERIC(12,6) NOT NULL DEFAULT 0,
        PRIMARY KEY (month, provider)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tts_user_usage (
        month    DATE NOT NULL,
        user_id  BIGINT NOT NULL,
        est_usd  NUMERIC(12,6) NOT NULL DEFAULT 0,
        PRIMARY KEY (month, user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tts_guild_usage (
        month    DATE NOT NULL,
        guild_id BIGINT NOT NULL,
        est_usd  NUMERIC(12,6) NOT NULL DEFAULT 0,
        PRIMARY KEY (month, guild_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS narrate_voice_history (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        voice       VARCHAR(160) NOT NULL,
        voice_label VARCHAR(256),
        set_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
        FOREIGN KEY (user_id)  REFERENCES users  (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS narrate_voice_history_user_idx
        ON narrate_voice_history (guild_id, user_id, set_at DESC)
    """,
    """
    INSERT INTO narrate_voice_history (guild_id, user_id, voice, voice_label, set_at)
    SELECT p.guild_id, p.user_id, p.voice, p.voice_label, p.updated_at
    FROM narrate_prefs p
    WHERE p.voice IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM narrate_voice_history h
                      WHERE h.guild_id = p.guild_id AND h.user_id = p.user_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS tts_designed_voices (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT,
        user_id     BIGINT,
        provider    VARCHAR(16) NOT NULL,
        voice_id    VARCHAR(128) NOT NULL,
        title       VARCHAR(128) NOT NULL,
        instruction TEXT,
        sample_path TEXT,          -- set when kept locally (voice_id 'ref:<key>'), relative to data/audio
        sample_text TEXT,          -- transcript of the sample, sent with it
        created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (guild_id) REFERENCES guilds (id) ON DELETE CASCADE,
        FOREIGN KEY (user_id)  REFERENCES users  (id) ON DELETE SET NULL,
        UNIQUE (provider, voice_id)
    )
    """,
    """
    ALTER TABLE tts_designed_voices
      ADD COLUMN IF NOT EXISTS sample_path TEXT,
      ADD COLUMN IF NOT EXISTS sample_text TEXT
    """,
    # Voices designed in a DM belong to the user, not a server
    """
    ALTER TABLE tts_designed_voices ALTER COLUMN guild_id DROP NOT NULL
    """,
    # 'design' or 'clone' (made from the creator's own recording); both are named and usable by anyone
    """
    ALTER TABLE tts_designed_voices ADD COLUMN IF NOT EXISTS kind VARCHAR(16) NOT NULL DEFAULT 'design'
    """,
    # Clones are named and a person can have several (an earlier draft allowed only one)
    """
    DROP INDEX IF EXISTS tts_designed_voices_one_clone_ux
    """,
    # Per-voice defaults set by the creator with `!narrate tune`: built-in style and Fish temperature
    """
    ALTER TABLE tts_designed_voices
      ADD COLUMN IF NOT EXISTS default_style VARCHAR(200),
      ADD COLUMN IF NOT EXISTS temperature   REAL
    """,
    """
    CREATE INDEX IF NOT EXISTS tts_designed_voices_user_idx
        ON tts_designed_voices (guild_id, user_id)
    """,
]


def apply_migration():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    apply_migration()
    print("Migration applied.")
