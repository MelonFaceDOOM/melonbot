"""
Multi-provider narration (Fish / Gemini / Google):
- narrate_prefs.voice_label, narrate_prefs.style
- tts_usage: month-to-date estimated spend per paid provider (budget cap)
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
      ADD COLUMN IF NOT EXISTS style       VARCHAR(200)
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
    CREATE TABLE IF NOT EXISTS tts_designed_voices (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
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
