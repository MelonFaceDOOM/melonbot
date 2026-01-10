"""
Adds tables for tracking and saving stream vods.
"""

from config import PSQL_CREDENTIALS
import psycopg2


def apply_migration():
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    cur = conn.cursor()
    
    cur.execute("""CREATE EXTENSION IF NOT EXISTS citext;""")
    
    # should have done this for other tables honestly
    cur.execute("""CREATE SCHEMA IF NOT EXISTS stream_tracker;""")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_tracker.twitch_oauth (
            singleton_id      SMALLINT PRIMARY KEY DEFAULT 1 CHECK (singleton_id = 1),
        
            user_refresh_token TEXT NOT NULL,
        
            -- Optional metadata (nice for debugging / audits)
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_rotated_at   TIMESTAMPTZ
        );    
        """
    )
    
    # 1) Store twitch channels
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_tracker.channels (
            id                 BIGINT PRIMARY KEY,
            login              CITEXT NOT NULL UNIQUE,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            tracked            BOOL NOT NULL DEFAULT FALSE, -- True if any guilds track it
            total_size         BIGINT -- Total size estimate that will be periodically updated
        );
        """
    )
    
    # 2) Store a guild's twitch channels
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_tracker.guild_channels (
            id                 BIGSERIAL PRIMARY KEY,
            guild_id           BIGINT NOT NULL,
            created_by_user_id BIGINT,
            channel_id         BIGINT NOT NULL,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            tracked            BOOL NOT NULL DEFAULT TRUE,
            total_size         BIGINT, -- Guild's total size estimate
            FOREIGN KEY (channel_id) REFERENCES stream_tracker.channels (id) ON DELETE CASCADE,
            FOREIGN KEY (created_by_user_id) REFERENCES users (id) ON DELETE SET NULL,
            UNIQUE (guild_id, channel_id)
        );
        """
    )

    # 3) Store data about saved file
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_tracker.saved_streams (
            id               BIGSERIAL PRIMARY KEY,
            twitch_stream_id TEXT NOT NULL,
            segment_idx INT NOT NULL DEFAULT 1, -- long videos will chunk into multiple segments
            channel_id       BIGINT NOT NULL,
            sftp_url         TEXT,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status           TEXT NOT NULL DEFAULT 'pending', -- pending/partial/complete/failed
            size             BIGINT, -- bytes
            FOREIGN KEY (channel_id) REFERENCES stream_tracker.channels (id) ON DELETE CASCADE,
            UNIQUE (channel_id, twitch_stream_id, segment_idx),
            CONSTRAINT saved_streams_status_chk
                CHECK (status IN ('pending', 'partial', 'complete','failed'))
        );
        """
    )
    
    ### Trigger to set channels.tracked to true                 ###
    #   if any guild_channels.tracked are true for that channel   #
    ### and false if all guild_channels.tracked are false       ###
    
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION stream_tracker.fn_set_channel_tracked() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          -- Update for the NEW channel_id (INSERT / UPDATE)
          IF (TG_OP = 'INSERT') THEN
            UPDATE stream_tracker.channels c
            -- True if ANY there are guild_channel rows where this channel is tracked
            SET tracked = EXISTS (
                SELECT 1
                FROM stream_tracker.guild_channels gc
                WHERE gc.channel_id = NEW.channel_id
                  AND gc.tracked = TRUE
            )
            WHERE c.id = NEW.channel_id;

            RETURN NEW;

          ELSIF (TG_OP = 'UPDATE') THEN
            -- If the channel_id changed, recompute for BOTH old and new ids.
            IF (NEW.channel_id IS DISTINCT FROM OLD.channel_id) THEN
              UPDATE stream_tracker.channels c
              SET tracked = EXISTS (
                  SELECT 1
                  FROM stream_tracker.guild_channels gc
                  WHERE gc.channel_id = OLD.channel_id
                    AND gc.tracked = TRUE
              )
              WHERE c.id = OLD.channel_id;

              UPDATE stream_tracker.channels c
              SET tracked = EXISTS (
                  SELECT 1
                  FROM stream_tracker.guild_channels gc
                  WHERE gc.channel_id = NEW.channel_id
                    AND gc.tracked = TRUE
              )
              WHERE c.id = NEW.channel_id;
            ELSE
              -- Same channel_id; recompute once (covers tracked flag flips)
              UPDATE stream_tracker.channels c
              SET tracked = EXISTS (
                  SELECT 1
                  FROM stream_tracker.guild_channels gc
                  WHERE gc.channel_id = NEW.channel_id
                    AND gc.tracked = TRUE
              )
              WHERE c.id = NEW.channel_id;
            END IF;

            RETURN NEW;

          ELSIF (TG_OP = 'DELETE') THEN
            UPDATE stream_tracker.channels c
            SET tracked = EXISTS (
                SELECT 1
                FROM stream_tracker.guild_channels gc
                WHERE gc.channel_id = OLD.channel_id
                  AND gc.tracked = TRUE
            )
            WHERE c.id = OLD.channel_id;

            RETURN OLD;
          END IF;

          -- Should never get here, but keep PL/pgSQL happy.
          RETURN NULL;
        END $$;
        """
    )

    cur.execute(
        """
        DROP TRIGGER IF EXISTS set_channel_tracked ON stream_tracker.guild_channels;
        CREATE TRIGGER set_channel_tracked
            AFTER INSERT OR UPDATE OR DELETE ON stream_tracker.guild_channels
            FOR EACH ROW
            EXECUTE FUNCTION stream_tracker.fn_set_channel_tracked();
        """
    )
    
    ### INDEXES ###

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS channels_tracked_true_idx
            ON stream_tracker.channels (id)
            WHERE tracked = TRUE;
        """
    )
        
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS guild_channels_guild_tracked_true_idx
            ON stream_tracker.guild_channels (guild_id)
            WHERE tracked = TRUE;
        """
    )
    
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS guild_channels_channel_tracked_true_idx
            ON stream_tracker.guild_channels (channel_id)
            WHERE tracked = TRUE;
        """
    )

    # get n latest streams from a given channel
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS saved_streams_channel_created_at_idx
            ON stream_tracker.saved_streams (channel_id, created_at DESC);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS saved_streams_channel_stream_segment_idx
            ON stream_tracker.saved_streams (channel_id, twitch_stream_id, segment_idx DESC);

        """
    )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    apply_migration()
    print("Migration applied.")
