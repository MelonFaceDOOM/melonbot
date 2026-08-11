# Melonbot migrations

One-off schema changes. Melonbot uses raw SQL (psycopg2 here, asyncpg at runtime) — not Django/Alembic.

## Environments (dev / prod + SSH tunnel)

Flip `MELONBOT_DB` and `USE_SSH_TUNNEL` at the **top** of `.env`. Config is env-driven ([`config.py`](../config.py) + [`.env.example`](../.env.example)):

| Flag | Meaning |
|------|---------|
| `MELONBOT_DB=dev` | Uses `melonbot_dev` + `DEV_BOT_TOKEN` |
| `MELONBOT_DB=prod` | Uses `melonbot` + `PROD_BOT_TOKEN` |
| `USE_SSH_TUNNEL=0` | Bot is **on the DB server** — connect to `REMOTE_DB_HOST` (always `127.0.0.1`) |
| `USE_SSH_TUNNEL=1` | Bot is **elsewhere** (laptop, even on LAN) — SSH to `SSH_HOST`, then forward to server loopback Postgres |

Postgres is never opened directly on a LAN IP. On LAN you still set `USE_SSH_TUNNEL=1` and put the LAN address in `SSH_HOST`; keep `REMOTE_DB_HOST=127.0.0.1` (Postgres as seen on the server).

## How to run a migration

```text
# from the melonbot repo root, with .env loaded
MELONBOT_DB=dev python migrations/migration_<name>.py
# later:
MELONBOT_DB=prod python migrations/migration_<name>.py
```

On Windows PowerShell you can set `$env:MELONBOT_DB='dev'` before running, or put `MELONBOT_DB=dev` in `.env`.

Scripts are idempotent where possible (`IF NOT EXISTS`).

---

## Display-name cache (`migration_2026_07_18_display_names.py`)

Adds nullable columns Nitwitch reads for human-readable guild/user names:

- `guilds.name`, `guilds.icon_url`, `guilds.updated_at`
- `users.username`, `users.global_name`, `users.updated_at`

### How names get populated

| Path | When |
|------|------|
| **Interaction** | Any command that calls `get_user_id` / `get_guild_id` upserts the author + guild names automatically. |
| **Events** | `on_guild_join` / `on_guild_update`, `on_user_update` / `on_member_update` refresh cache on renames / joins. |
| **Backfill** | Bot owner runs `!sync_names` once after migrate so existing id-only rows get names without waiting for every user to type a command. |

No nightly cron in v1. Users do **not** need a special “register” command for normal use.

Fresh installs also get these columns from `make_melonbot_db.make_db()` (CREATE + `ALTER … IF NOT EXISTS` on bot start).

### Migrate runbook: first dev, then test, then prod

#### A. Dev

1. `.env`: `MELONBOT_DB=dev` (tunnel on if not on the DB server). Confirm Nitwitch `MOVIENIGHT_DB_NAME=melonbot_dev` when browsing locally.
2. Run:

```text
python migrations/migration_2026_07_18_display_names.py
```

3. Verify columns:

```sql
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_name IN ('guilds', 'users')
  AND column_name IN ('name', 'icon_url', 'username', 'global_name', 'updated_at')
ORDER BY table_name, column_name;
```

4. Restart the **dev** bot. Run any movie command (`!add` / `!rate`) in a test guild; confirm that guild/user row has names.
5. Reload Nitwitch `/movienights/` against the same DB — names should appear when filled.

#### B. Test (verification gate)

1. As bot owner: `!sync_names`.
2. Spot-check DB: former id-only rows have `username` / `global_name`; guilds have `name`.
3. Rename a Discord user or the server; confirm events update `updated_at` and the new string.
4. On Nitwitch, set viewer by name search and confirm a synced user resolves.
5. Only after this passes, migrate prod.

#### C. Prod

1. Deploy melonbot code that includes upserts, events, and `!sync_names`.
2. On the server (or any machine that can reach prod Postgres): `.env` with `MELONBOT_DB=prod`, `USE_SSH_TUNNEL` unset if Postgres is local.
3. Run the **same** migration script (no-op if columns already exist).
4. Restart the **prod** bot (`MELONBOT_DB=prod` + `PROD_BOT_TOKEN`).
5. Run `!sync_names` once (it iterates all guilds the bot is in).
6. Confirm Nitwitch prod `MOVIENIGHT_DB_*` shows names.

**Rollback:** columns are nullable and additive. Do not drop them while Nitwitch models still SELECT them.

---

## Message archive (`migration_2026_08_10_message_archive.py`)

Adds schema `message_archive` (channels, messages, attachments, reactions) for stealth Discord history archival. No Discord commands — enable via env (`MESSAGE_ARCHIVE_ENABLED`, `MESSAGE_ARCHIVE_GUILD_IDS`); progress is `data/message_archive_status.json`.

### Migrate runbook: first dev, then prod

#### A. Dev

1. `.env`: `MELONBOT_DB=dev` (tunnel on if needed).
2. Run:

```text
python migrations/migration_2026_08_10_message_archive.py
```

3. Verify:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'message_archive'
ORDER BY table_name;
```

4. Set `MESSAGE_ARCHIVE_ENABLED=1` and `MESSAGE_ARCHIVE_GUILD_IDS=<test_guild_id>`, restart the **dev** bot.
5. Confirm `data/message_archive_status.json` updates and rows appear in `message_archive.messages`.

#### B. Prod

1. Deploy melonbot code that includes the archive cog.
2. Run the same migration with `MELONBOT_DB=prod`.
3. Set allowlist + enable on the prod host; restart the **prod** bot.
4. Watch the status JSON / SQL counts — nothing is announced in Discord.

**Rollback:** drop schema `message_archive` only if you are sure nothing depends on it (`DROP SCHEMA message_archive CASCADE`).
