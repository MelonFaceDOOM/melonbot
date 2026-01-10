from bot_helpers import get_user_id, get_guild_id, send_goodly
from discord.ext import commands
from db_mixin import DbMixin
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Sequence, Tuple
import time
import aiohttp
import asyncio
from config import twitch_client_id, twitch_client_secret, admin_discord_ids, LOCAL_STREAM_FILE_LOCATION, WEB_STREAM_FILE_LOCATION


TOKEN_URL = "https://id.twitch.tv/oauth2/token"
HELIX_USERS_URL = "https://api.twitch.tv/helix/users"


_TWITCH_LOGIN_URL_RE = re.compile(
    r"^(?:https?://)?(?:www\.)?twitch\.tv/([A-Za-z0-9_]{1,50})(?:/.*)?$",
    re.IGNORECASE,
)

def _normalize_twitch_login(s: str) -> str:
    """
    Normalize user input into a Twitch login suitable for storing/querying.

    Accepts:
      - "somechannel"
      - "@somechannel"
      - "twitch.tv/somechannel"
      - "https://www.twitch.tv/somechannel"

    Returns a lowercase login (Twitch logins are case-insensitive).
    Raises ValueError if empty or contains invalid characters.
    """
    raw = (s or "").strip()
    if not raw:
        raise ValueError("login empty")

    m = _TWITCH_LOGIN_URL_RE.match(raw)
    if m:
        raw = m.group(1)

    if raw.startswith("@"):
        raw = raw[1:].strip()

    login = raw.strip().lower()

    if not login:
        raise ValueError("login empty")
    if not re.fullmatch(r"[a-z0-9_]+", login):
        raise ValueError("login has invalid characters")

    return login


def _coerce_twitch_user_id(x: Any) -> int:
    """
    Twitch user/channel ids come as numeric strings from Helix.
    """
    try:
        v = int(x)
    except Exception as e:
        raise ValueError("channel_id must be an integer") from e
    if v <= 0:
        raise ValueError("channel_id must be > 0")
    return v

# ==========================
# lil file server helpers
# ==========================
async def delete_local_files(file_locations: Sequence[str]) -> Tuple[int, int]:
    """
    Delete local files under LOCAL_STREAM_FILE_LOCATION.

    `file_locations` must be relative to base dir. Today it's usually just a filename,
    but it may later be like "channel/filename".

    Returns: (ok_count, failed_count)

      - Missing files count as ok (already gone).
      - Any path traversal / absolute paths are rejected as failed.
    """
    base = Path(LOCAL_STREAM_FILE_LOCATION).resolve()

    def _delete_one(rel: str) -> bool:
        rel = (rel or "").strip()
        if not rel:
            return False

        # Prevent absolute paths; normalize leading slash away just in case.
        rel = rel.lstrip("/")

        # Resolve and enforce "must be under base"
        try:
            target = (base / rel).resolve()
        except Exception:
            return False

        # Must be inside base dir (prevents ../../etc/passwd)
        try:
            target.relative_to(base)
        except ValueError:
            return False

        # If it doesn't exist, treat as success
        if not target.exists():
            return True

        # Only delete files (not dirs / symlinked dirs). Symlinks to files are ok.
        if not target.is_file():
            return False

        try:
            target.unlink()
            return True
        except FileNotFoundError:
            return True
        except Exception as e:
            return False

    ok = 0
    failed = 0
    for rel in file_locations:
        success = await asyncio.to_thread(_delete_one, rel)
        if success:
            ok += 1
        else:
            failed += 1

    return ok, failed

# ==========================
# Cog Formatting Helpers
# ==========================

def _fmt_bytes(n: Optional[int]) -> str:
    if n is None:
        return "?"
    # readable-ish but still technical
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.2f}{unit}"
        n /= 1024
    return "?"

def _fmt_dt(dt: Any) -> str:
    "Jan 2, 2026"
    if dt is None:
        return "?"
    try:
        if not isinstance(dt, datetime):
            # fallback if it's an ISO string or similar
            dt = datetime.fromisoformat(str(dt))
        s = dt.strftime("%b %d, %Y")
        s = s.replace(" 0", " ")  # "Jan 02, 2026" -> "Jan 2, 2026"
        return s
    except Exception:
        return str(dt)

def _line2_link_or_inferred_status(r: Dict[str, Any]) -> str:
    """
    If `location` exists (relative path under streams dir), return a download link.
    Otherwise infer: failed vs recording.
    """
    loc = (r.get("location") or "").strip()
    if loc:
        loc = loc.lstrip("/")
        loc_enc = "/".join(quote(seg) for seg in loc.split("/"))
        return f"  link: {WEB_STREAM_FILE_LOCATION.rstrip('/')}/{loc_enc}"
    status = str(r.get("status") or "").lower()
    inferred = "failed" if status == "failed" else "recording"
    return f"  status: {inferred}"

# ==========================
# Cog
# ==========================

class StreamTrackerCog(DbMixin, commands.Cog, name="StreamTracker"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # One shared session for this cog
        self._http: aiohttp.ClientSession = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

        # App token cache
        self._app_access_token: str | None = None
        self._app_expires_at: float = 0.0  # epoch seconds

    async def aclose(self) -> None:
        """Async cleanup hook. Call this from the bot's shutdown path."""
        if getattr(self, "_http", None) is not None and not self._http.closed:
            try:
                await self._http.close()
            except Exception:
                print("Failed closing aiohttp session for StreamTrackerCog")

    def _app_token_valid(self, skew_s: int = 60) -> bool:
        return bool(self._app_access_token) and time.time() < (self._app_expires_at - skew_s)

    async def _get_app_access_token(self) -> str:
        """
        Client Credentials token (app token). Works for Helix Get Users.
        """
        if self._app_token_valid():
            return self._app_access_token  # type: ignore[return-value]

        payload = {
            "client_id": twitch_client_id,
            "client_secret": twitch_client_secret,
            "grant_type": "client_credentials",
        }
        async with self._http.post(TOKEN_URL, data=payload) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"App token request failed: {resp.status} {str(data)[:300]}")

        tok = data.get("access_token")
        exp = int(data.get("expires_in", 0) or 0)
        if not tok or exp <= 0:
            raise RuntimeError(f"Invalid app token response: {str(data)[:300]}")

        self._app_access_token = str(tok)
        self._app_expires_at = time.time() + exp
        return self._app_access_token

    async def _resolve_twitch_user(self, login_or_url: str) -> tuple[int, str] | None:
        """
        Resolve user input -> (twitch_user_id, canonical_login) using Helix Get Users (by login),
        authenticated with an APP access token (client_credentials).
        """
        login = _normalize_twitch_login(login_or_url)

        # We'll retry once on 401 by forcing a new token
        retried_401 = False

        while True:
            token = await self._get_app_access_token()
            headers = {
                "Client-Id": str(twitch_client_id),
                "Authorization": f"Bearer {token}",
            }
            params = {"login": login}

            async with self._http.get(HELIX_USERS_URL, headers=headers, params=params) as resp:
                if resp.status == 401 and not retried_401:
                    # Token invalid/expired early: clear cache and retry once
                    self._app_access_token = None
                    self._app_expires_at = 0.0
                    retried_401 = True
                    continue

                if resp.status == 429:
                    # Rate limited: wait until reset if provided, else short backoff
                    reset = resp.headers.get("Ratelimit-Reset")
                    if reset and reset.isdigit():
                        sleep_for = max(0.0, float(reset) - time.time()) + 0.5
                    else:
                        sleep_for = 1.5
                    await asyncio.sleep(sleep_for)
                    continue

                data = await resp.json(content_type=None)
                if resp.status >= 400:
                    raise RuntimeError(f"Helix Get Users failed: {resp.status} {str(data)[:300]}")

            users = data.get("data") or []
            if not users:
                raise ValueError(f"Twitch user not found: {login}")

            u = users[0]
            user_id = _coerce_twitch_user_id(u.get("id"))
            canonical_login = str(u.get("login") or login).lower()
            return user_id, canonical_login

    async def _get_channel_by_login(self, login: str) -> Optional[Dict[str, Any]]:
        login_norm = _normalize_twitch_login(login)
        row = await self.db.fetchrow(
            """
            SELECT id, login, created_at, tracked, total_size
            FROM stream_tracker.channels
            WHERE login=$1
            """,
            login_norm,
        )
        return dict(row) if row else None
        
    async def _set_channel_total_size(self, channel_id: Any, total_size_bytes: Optional[int]) -> bool:
        cid = _coerce_twitch_user_id(channel_id)
        if total_size_bytes is not None and total_size_bytes < 0:
            raise ValueError("total_size_bytes must be >= 0")

        updated = await self.db.execute(
            """
            UPDATE stream_tracker.channels
            SET total_size=$2
            WHERE id=$1
            """,
            cid,
            total_size_bytes,
        )
        return "1" in str(updated)

    async def _list_guild_tracked_channels(self, guild_id: Any) -> List[Dict[str, Any]]:
        gid = int(guild_id)
        rows = await self.db.fetch(
            """
            SELECT
                gc.guild_id,
                gc.channel_id,
                c.login,
                gc.created_at,
                gc.created_by_user_id,
                gc.tracked,
                gc.total_size
            FROM stream_tracker.guild_channels gc
            JOIN stream_tracker.channels c ON c.id = gc.channel_id
            WHERE gc.guild_id=$1 AND gc.tracked=TRUE
            ORDER BY c.login
            """,
            gid,
        )
        return [dict(r) for r in rows]

    async def _set_guild_channel_total_size(
        self, guild_id: Any, channel_id: Any, total_size_bytes: Optional[int]
    ) -> bool:
        gid = int(guild_id)
        cid = _coerce_twitch_user_id(channel_id)
        if total_size_bytes is not None and total_size_bytes < 0:
            raise ValueError("total_size_bytes must be >= 0")

        updated = await self.db.execute(
            """
            UPDATE stream_tracker.guild_channels
            SET total_size=$3
            WHERE guild_id=$1 AND channel_id=$2
            """,
            gid,
            cid,
            total_size_bytes,
        )
        return "1" in str(updated)
        
    async def _track_channel_for_guild(
        self,
        guild_id: Any,
        created_by_user_id: Any,
        channel_id: Any,
        login: str,
    ) -> bool:
        """
        Ensure channel exists in `channels`, then upsert `guild_channels` to tracked=TRUE.

        Returns:
          True  -> guild is now tracking (inserted or changed from false->true)
          False -> was already tracked=true for that guild/channel
        """
        gid = int(guild_id)
        uid = int(created_by_user_id) if created_by_user_id is not None else None
        cid = _coerce_twitch_user_id(channel_id)
        login_norm = _normalize_twitch_login(login)

        # Fast path: already tracking?
        existing = await self.db.fetchrow(
            """
            SELECT tracked
            FROM stream_tracker.guild_channels
            WHERE guild_id=$1 AND channel_id=$2
            """,
            gid,
            cid,
        )
        if existing and bool(existing["tracked"]) is True:
            return False

        # Single-statement: guarantees FK satisfaction
        await self.db.execute(
            """
            WITH upsert_channel AS (
                INSERT INTO stream_tracker.channels (id, login, tracked)
                VALUES ($1, $2, FALSE)
                ON CONFLICT (id) DO UPDATE SET login=EXCLUDED.login
                RETURNING id
            )
            INSERT INTO stream_tracker.guild_channels (guild_id, created_by_user_id, channel_id, tracked)
            VALUES ($3, $4, (SELECT id FROM upsert_channel), TRUE)
            ON CONFLICT (guild_id, channel_id) DO UPDATE
              SET tracked=TRUE,
                  created_by_user_id=EXCLUDED.created_by_user_id
            """,
            cid,
            login_norm,
            gid,
            uid,
        )
        return True
        
    async def _untrack_channel_for_guild(self, guild_id: Any, channel_id: Any) -> bool:
        """
        Set guild_channels.tracked = FALSE (does not delete the row).

        Returns:
          True  -> row existed and is now untracked (or was already false but row exists)
          False -> no such guild/channel row
        """
        gid = int(guild_id)
        cid = _coerce_twitch_user_id(channel_id)

        row = await self.db.fetchrow(
            """
            SELECT tracked
            FROM stream_tracker.guild_channels
            WHERE guild_id=$1 AND channel_id=$2
            """,
            gid,
            cid,
        )
        if not row:
            return False

        # If already false, treat as success.
        if bool(row["tracked"]) is False:
            return True

        updated = await self.db.execute(
            """
            UPDATE stream_tracker.guild_channels
            SET tracked=FALSE
            WHERE guild_id=$1 AND channel_id=$2
            """,
            gid,
            cid,
        )
        return "1" in str(updated)

    async def _list_saved_streams(self, guild_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        gid = int(guild_id)
        lim = int(limit)
        if lim <= 0 or lim > 500:
            raise ValueError("limit must be in 1..500")

        rows = await self.db.fetch(
            """
            SELECT
                ss.id,
                ss.channel_id,
                c.login,
                ss.location,
                ss.status,
                ss.segment_idx,
                ss.created_at,
                ss.size
            FROM stream_tracker.saved_streams ss
            JOIN stream_tracker.guild_channels gc
              ON gc.channel_id = ss.channel_id
            JOIN stream_tracker.channels c
              ON c.id = ss.channel_id
            WHERE gc.guild_id = $1
              AND gc.tracked = TRUE
            ORDER BY ss.created_at DESC
            LIMIT $2
            """,
            gid,
            lim,
        )
        return [dict(r) for r in rows]

    async def _list_saved_streams_for_channel(self, channel_id: Any, limit: int = 50) -> List[Dict[str, Any]]:
        cid = _coerce_twitch_user_id(channel_id)
        lim = int(limit)
        if lim <= 0 or lim > 500:
            raise ValueError("limit must be in 1..500")

        rows = await self.db.fetch(
            """
            SELECT
                ss.id,
                ss.channel_id,
                ss.twitch_stream_id,
                ss.segment_idx,
                ss.status,
                ss.location,
                ss.created_at,
                ss.size
            FROM stream_tracker.saved_streams ss
            WHERE ss.channel_id=$1
            ORDER BY ss.created_at DESC
            LIMIT $2
            """,
            cid,
            lim,
        )
        return [dict(r) for r in rows]


    async def _delete_saved_streams_by_ids(self, stream_ids: Sequence[int]) -> List[Dict[str, Any]]:
        """
        Deletes saved_streams rows by id (global admin action).
        Returns deleted rows with enough info to delete on disk.
        """
        ids = [int(x) for x in stream_ids if str(x).strip()]
        if not ids:
            return []

        rows = await self.db.fetch(
            """
            DELETE FROM stream_tracker.saved_streams ss
            USING stream_tracker.channels c
            WHERE ss.id = ANY($1::bigint[])
              AND c.id = ss.channel_id
            RETURNING
                ss.id,
                ss.channel_id,
                c.login,
                ss.twitch_stream_id,
                ss.segment_idx,
                ss.location,
                ss.created_at,
                ss.status,
                ss.size
            """,
            ids,
        )
        return [dict(r) for r in rows]

    async def _purge_channel_from_db(self, channel_id: int) -> Dict[str, Any]:
        """
        Deletes a channel globally:
          - stream_tracker.channels row (cascades to guild_channels and saved_streams)
          - returns the saved_streams rows (with location) for follow-up deletion

        Returns:
          {
            "channel": {"id": ..., "login": ...} | None,
            "streams": [ ...deleted stream rows... ],
          }
        """
        cid = _coerce_twitch_user_id(channel_id)

        async with self.db.acquire() as conn:
            async with conn.transaction():
                channel = await conn.fetchrow(
                    "SELECT id, login FROM stream_tracker.channels WHERE id=$1",
                    cid,
                )
                if not channel:
                    return {"channel": None, "streams": []}

                # Grab stream rows BEFORE deleting channel (because delete cascades).
                streams = await conn.fetch(
                    """
                    SELECT
                        ss.id,
                        ss.channel_id,
                        c.login,
                        ss.twitch_stream_id,
                        ss.segment_idx,
                        ss.location,
                        ss.created_at,
                        ss.status,
                        ss.size
                    FROM stream_tracker.saved_streams ss
                    JOIN stream_tracker.channels c ON c.id = ss.channel_id
                    WHERE ss.channel_id = $1
                        AND ss.status <> 'pending'
                    ORDER BY ss.created_at DESC
                    """,
                    cid,
                )

                # This cascades:
                # - saved_streams (FK ON DELETE CASCADE)
                # - guild_channels (FK ON DELETE CASCADE)
                deleted_channel = await conn.fetchrow(
                    """
                    DELETE FROM stream_tracker.channels
                    WHERE id = $1
                    RETURNING id, login, created_at, tracked, total_size
                    """,
                    cid,
                )

                return {
                    "channel": dict(deleted_channel) if deleted_channel else None,
                    "streams": [dict(r) for r in streams],
                }
        
    @commands.group(name="stream", invoke_without_command=True)
    @commands.guild_only()  # this disables use in dms. prob should have used it elsewhere too but w.e.
    async def stream_root(self, ctx: commands.Context):
        """
        Stream tracking commands.
        """
        await ctx.send("Use: stream track|untrack|channels|vods|channel_vods|delete|purge")

    # 1) track channel
    @stream_root.command(name="track")
    @commands.guild_only()
    async def cmd_track_channel(self, ctx: commands.Context, *, login_or_url: str):
        gid = await get_guild_id(ctx, self.db)
        uid = await get_user_id(ctx, self.db)

        try:
            # Resolve Twitch login -> (id, canonical login)
            channel_id, canonical_login = await self._resolve_twitch_user(login_or_url)

            changed = await self._track_channel_for_guild(
                guild_id=gid,
                created_by_user_id=uid,
                channel_id=channel_id,
                login=canonical_login,
            )
        except ValueError as e:
            await ctx.send(f"Invalid input: {e}")
            return
        except Exception as e:
            await ctx.send(f"Failed to track channel: {e}")
            return

        if changed:
            await ctx.send(f"Tracking enabled for **{canonical_login}**.")
        else:
            await ctx.send(f"Already tracking **{canonical_login}**.")

    # 2) untrack channel
    @stream_root.command(name="untrack")
    @commands.guild_only()
    async def cmd_untrack_channel(self, ctx: commands.Context, *, login_or_url: str):
        gid = await get_guild_id(ctx, self.db)

        try:
            login = _normalize_twitch_login(login_or_url)
        except ValueError as e:
            await ctx.send(f"Invalid input: {e}")
            return

        row = await self._get_channel_by_login(login)
        if row:
            ok = await self._untrack_channel_for_guild(gid, row["id"])
            if ok:
                await ctx.send(f"Tracking disabled for **{row['login']}**.")
            else:
                await ctx.send(f"This guild is not tracking **{login}**.")
            return
        else:
            await ctx.send(f"This guild is not tracking **{login}**.")
            return

    @stream_root.command(name="channels")
    @commands.guild_only()
    async def cmd_list_tracked_channels(self, ctx: commands.Context):
        gid = await get_guild_id(ctx, self.db)
        rows = await self._list_guild_tracked_channels(gid)
        if not rows:
            await ctx.send("No tracked channels in this guild.")
            return

        lines = [f"- **{r['login']}** (id `{r['channel_id']}`)" for r in rows]
        msg = "Tracked channels:\n" + "\n".join(lines[:50])
        if len(lines) > 50:
            msg += f"\n…and {len(lines) - 50} more."
        await ctx.send(msg)

    # 4) list streams (latest streams for channels tracked by this guild)
    @stream_root.command(name="vods")
    @commands.guild_only()
    async def cmd_list_streams(self, ctx: commands.Context, limit: int = 5):
        gid = await get_guild_id(ctx, self.db)
        try:
            rows = await self._list_saved_streams(gid, limit=limit)
        except ValueError as e:
            await ctx.send(str(e))
            return

        if not rows:
            await ctx.send("No saved streams found for tracked channels in this guild.")
            return

        lines: List[str] = ["Latest saved streams:"]
        for r in rows:
            created_s = _fmt_dt(r.get("created_at"))
            login = r.get("login") or "?"
            seg = r.get("segment_idx")
            st = r.get("status") or "?"

            lines.append(f"- `#{r['id']}` **{login}** — {created_s} — seg={seg} — {st}")
            lines.append(_line2_link_or_inferred_status(r))

        await ctx.send("\n".join(lines))

    # 5) list streams from channel (scoped to guild-tracked channels)
    @stream_root.command(name="channel_vods")
    @commands.guild_only()
    async def cmd_list_streams_for_channel(self, ctx: commands.Context, login_or_url: str, limit: int = 5):
        gid = await get_guild_id(ctx, self.db)

        try:
            login = _normalize_twitch_login(login_or_url)
        except ValueError as e:
            await ctx.send(f"Invalid input: {e}")
            return

        ch = await self._get_channel_by_login(login)
        if not ch:
            await ctx.send(f"Channel **{login}** not found in DB (track it first).")
            return

        tracked_rows = await self.db.fetchrow(
            """
            SELECT 1
            FROM stream_tracker.guild_channels
            WHERE guild_id=$1 AND channel_id=$2 AND tracked=TRUE
            """,
            gid,
            int(ch["id"]),
        )
        if not tracked_rows:
            await ctx.send(f"This guild is not tracking **{ch['login']}**.")
            return

        try:
            rows = await self._list_saved_streams_for_channel(ch["id"], limit=limit)
        except ValueError as e:
            await ctx.send(str(e))
            return

        if not rows:
            await ctx.send(f"No saved streams for **{ch['login']}**.")
            return

        lines: List[str] = [f"Saved streams for **{ch['login']}**:"]
        for r in rows:
            size_s = _fmt_bytes(r.get("size"))
            created_s = _fmt_dt(r.get("created_at"))
            seg = r.get("segment_idx")
            st = r.get("status") or "?"

            lines.append(f"- `#{r['id']}` — {created_s} — seg={seg} — {st}")
            lines.append(_line2_link_or_inferred_status(r))

        await ctx.send("\n".join(lines))

    @stream_root.command(name="delete")
    async def cmd_delete_streams(self, ctx: commands.Context, *, stream_ids: str):
        uid = await get_user_id(ctx, self.db)
        if uid not in admin_discord_ids:
            await ctx.send(f"Only admins can delete streams.")
            return

        stream_ids = stream_ids.split(",")
        stream_ids = [int(i.strip()) for i in stream_ids]
            
        if not stream_ids:
            await ctx.send("Usage: `stream delete <stream_id>[, stream_id, ...]`")
            return

        try:
            deleted = await self._delete_saved_streams_by_ids(stream_ids)
        except Exception as e:
            await ctx.send(f"Delete failed: {e}")
            return

        if not deleted:
            await ctx.send("No matching streams found.")
            return

        file_locations = [r.get("location") for r in deleted if r.get("location")]
        ok, failed = await delete_local_files(file_locations)
        await ctx.send(f"DB deleted {len(deleted)} row(s). File deleted {ok}, failed {failed}.")

    @stream_root.command(name="purge")
    async def cmd_purge_channel(self, ctx: commands.Context, *, login_or_url: str):
        uid = await get_user_id(ctx, self.db)
        if uid not in admin_discord_ids:
            await ctx.send(f"Only melon can purge channels.")
            return

        try:
            login = _normalize_twitch_login(login_or_url)
        except ValueError as e:
            await ctx.send(f"Invalid input: {e}")
            return

        ch = await self._get_channel_by_login(login)
        if not ch:
            await ctx.send(f"Channel **{login}** not found in DB.")
            return

        await ctx.send(f"The purge has begun...")

        try:
            _ = await self._purge_channel_from_db(int(ch["id"]))
            deleted_channel = _['channel']
            deleted_streams = _['streams']
        except Exception as e:
            await ctx.send(f"Delete failed: {e}")
            return

        if not deleted_channel:
            await ctx.send(f"Delete failed: Unable to find channel for **{login}**.")
            return

        file_locations = [r.get("location") for r in deleted_streams if r.get("location")]
        ok, failed = await delete_local_files(file_locations)
        await ctx.send(f"Untracked/deleted channel **{login}**.")
        await ctx.send(f"Removed metadata for {len(deleted_streams)} streams.")
        await ctx.send(f"Deleted {ok} files from server.")
        if failed > 0:
            await ctx.send(f"Failed to delete {failed} files from server.")

    @stream_root.command(name="reset")
    async def reset(self, ctx: commands.Context):
        uid = await get_user_id(ctx, self.db)
        if uid not in admin_discord_ids:
            await ctx.send(f"Only melon can reset bot.")
            return
        try:
            await self.db.execute(
                """
                UPDATE stream_tracker.saved_streams
                SET status='failed'
                WHERE status='pending'
                """
            )
        except Exception as e:
            await ctx.send(f"Reset failed: {e}")
            return
        await ctx.send(f"Reset complete.")
