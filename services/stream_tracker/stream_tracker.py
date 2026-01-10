from __future__ import annotations
import asyncio
import aiohttp
import signal
from config import twitch_client_id, twitch_client_secret, PSQL_CREDENTIALS, LOCAL_STREAM_FILE_LOCATION
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from pathlib import Path
import os
import logging
import sys

import asyncpg

# ---------------------------------------------------------------------
# logging setup
# ---------------------------------------------------------------------

DEBUG = True
LOG_LEVEL = "DEBUG" if DEBUG else "INFO"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("stream_tracker")

# ---------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------

db_pool: asyncpg.Pool | None = None

async def init_db_pool() -> asyncpg.Pool:
    global db_pool
    if db_pool is not None:
        return db_pool

    try:
        db_pool = await asyncpg.create_pool(**PSQL_CREDENTIALS)
        print("[stream_tracker] Database connection pool created successfully.")
        return db_pool
    except Exception as e:
        print(f"[stream_tracker] Failed to connect to the database: {e}")
        db_pool = None
        raise


def _pool() -> asyncpg.Pool:
    if db_pool is None:
        raise RuntimeError("db_pool is not initialized. Call await init_db_pool() first.")
    return db_pool


async def close_db_pool() -> None:
    """
    Close the pool on shutdown.
    """
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None
        print("[stream_tracker] Database connection pool closed.")

# ----------------------------
# Data objects
# ----------------------------

@dataclass(frozen=True)
class LiveStream:
    channel_id: int
    login: str
    twitch_stream_id: str
    started_at: Optional[str] = None

# -----------------------0---------------------------------------------
# DB Interface
# ---------------------------------------------------------------------

async def load_tracked_streams() -> List[Dict[str, Any]]:
    """
    Fetch tracked channels from DB.

    Returns:
      [{"channel_id": 123, "login": "somechannel"}, ...]
    """
    rows = await _pool().fetch(
        """
        SELECT id AS channel_id, login
        FROM stream_tracker.channels
        WHERE tracked = TRUE
        ORDER BY login
        """
    )
    return [{"channel_id": int(r["channel_id"]), "login": str(r["login"])} for r in rows]


async def db_try_claim_next_segment(live: LiveStream) -> Optional[Tuple[int, int]]:
    """
    Returns (saved_id, segment_idx) if a new segment row was created, else None.
    """
    cid = int(live.channel_id)
    sid = str(live.twitch_stream_id)
    logger.debug("db_claim start channel_id=%s stream_id=%s", cid, sid)

    async with _pool().acquire() as conn:
        async with conn.transaction():
            # If any active-ish row exists, ignore.
            active = await conn.fetchval(
                """
                SELECT 1
                FROM stream_tracker.saved_streams
                WHERE channel_id=$1 AND twitch_stream_id=$2
                  AND status IN ('pending','complete')
                LIMIT 1
                """,
                cid, sid
            )
            if active:
                logger.info("db_claim ignore: pending/complete exists channel_id=%s stream_id=%s", cid, sid)
                return None

            # Look at latest segment (if any)
            latest = await conn.fetchrow(
                """
                SELECT id, segment_idx, status
                FROM stream_tracker.saved_streams
                WHERE channel_id=$1 AND twitch_stream_id=$2
                ORDER BY segment_idx DESC
                LIMIT 1
                """,
                cid, sid
            )

            if latest is None:
                next_idx = 1
            else:
                if str(latest["status"]) not in ("partial", "failed"):
                    logger.info("db_claim ignore: latest status=%s (not partial) channel_id=%s stream_id=%s",
                                latest["status"], cid, sid)
                    return None
                next_idx = int(latest["segment_idx"]) + 1

            row = await conn.fetchrow(
                """
                INSERT INTO stream_tracker.saved_streams (channel_id, twitch_stream_id, segment_idx, status)
                VALUES ($1, $2, $3, 'pending')
                ON CONFLICT (channel_id, twitch_stream_id, segment_idx) DO NOTHING
                RETURNING id
                """,
                cid, sid, next_idx
            )
            if not row:
                logger.info("Tried to insert but failed (maybe someone else did it first): channel_id=%s stream_id=%s",
                            cid, sid)
                # Another worker/process raced us; treat as "already handled"
                return None
            logger.info("db_claim ok saved_id=%s seg=%s channel_id=%s stream_id=%s", row["id"], next_idx, cid, sid)
            return int(row["id"]), next_idx


async def db_mark_done(saved_id: int, *, status: str, file_location: str, size_bytes: int) -> None:
    if status not in ("complete", "partial"):
        logger.info("db_mark_done id=%s status=%s size=%s location=%s", saved_id, status, size_bytes, file_location)
        raise ValueError("status must be complete|partial")

    await _pool().execute(
        """
        UPDATE stream_tracker.saved_streams
        SET status=$2,
            location=$3,
            size=$4
        WHERE id=$1
        """,
        int(saved_id),
        status,
        str(file_location),
        int(size_bytes),
    )


async def db_mark_failed(saved_id: int, *, error: str) -> None:
    await _pool().execute(
        """
        UPDATE stream_tracker.saved_streams
        SET status='failed'
        WHERE id=$1
        """,
        int(saved_id),
    )
    # Schema has no error column; log it for now.
    logger.warning("db_mark_failed id=%s error=%s", saved_id, error)


async def db_fail_all_pending() -> None:
    """meant to be called to clear stale jobs.
    all streams with 'pending' will be converted to 'failed'"""
    await _pool().execute(
        """
        UPDATE stream_tracker.saved_streams
        SET status='failed'
        WHERE status='pending'
        """
    )

async def db_set_location_pending(saved_id: int, *, file_location: str) -> None:
    # Only set if still pending, and don't overwrite if it's already set.
    await _pool().execute(
        """
        UPDATE stream_tracker.saved_streams
        SET location = COALESCE(location, $2)
        WHERE id=$1
          AND status='pending'
        """,
        int(saved_id),
        str(file_location),
    )


# ----------------------------
# stream status checking
# ----------------------------

_APP_ACCESS_TOKEN: Optional[str] = None
_APP_EXPIRES_AT: float = 0.0  # epoch seconds

async def _get_app_access_token(session: aiohttp.ClientSession) -> str:
    """
    Client Credentials token (app token). Good enough for Helix Get Streams.
    """
    global _APP_ACCESS_TOKEN, _APP_EXPIRES_AT

    # refresh 60s early
    if _APP_ACCESS_TOKEN and time.time() < (_APP_EXPIRES_AT - 60):
        return _APP_ACCESS_TOKEN

    url = "https://id.twitch.tv/oauth2/token"
    data = {
        "client_id": twitch_client_id,
        "client_secret": twitch_client_secret,
        "grant_type": "client_credentials",
    }
    async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        payload = await resp.json(content_type=None)
        if resp.status >= 400:
            raise RuntimeError(f"twitch token error {resp.status}: {str(payload)[:300]}")

    tok = payload.get("access_token")
    exp = int(payload.get("expires_in", 0))
    if not tok or exp <= 0:
        raise RuntimeError(f"invalid token response: {str(payload)[:300]}")

    _APP_ACCESS_TOKEN = str(tok)
    _APP_EXPIRES_AT = time.time() + exp
    return _APP_ACCESS_TOKEN


def _chunks(seq: List[int], n: int) -> List[List[int]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


async def _helix_get_streams(
    session: aiohttp.ClientSession,
    headers: Dict[str, str],
    user_ids: List[int],
) -> Dict[str, Any]:
    url = "https://api.twitch.tv/helix/streams"
    params: List[Tuple[str, str]] = [("user_id", str(cid)) for cid in user_ids]

    backoff = 1.0
    retried_401 = False

    while True:
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 401 and not retried_401:
                # Token invalid/expired early: clear cache and retry once with a fresh token
                global _APP_ACCESS_TOKEN, _APP_EXPIRES_AT
                _APP_ACCESS_TOKEN = None
                _APP_EXPIRES_AT = 0.0

                new_tok = await _get_app_access_token(session)
                headers = {
                    **headers,
                    "Authorization": f"Bearer {new_tok}",
                }
                retried_401 = True
                continue

            if resp.status == 429:
                reset = resp.headers.get("Ratelimit-Reset")
                if reset and reset.isdigit():
                    sleep_for = max(0.0, float(reset) - time.time()) + 0.5
                else:
                    sleep_for = backoff
                    backoff = min(backoff * 2.0, 30.0)
                await asyncio.sleep(sleep_for)
                continue

            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"helix get streams error {resp.status}: {str(data)[:300]}")
            return data


async def check_if_streams_are_live(
    session: aiohttp.ClientSession,
    tracked: Sequence[Dict[str, Any]],
) -> List["LiveStream"]:
    """
    Call Helix Get Streams and return only those live.
    Must populate twitch_stream_id (Helix stream id).
    """
    id_to_login: Dict[int, str] = {}
    ids: List[int] = []

    for t in tracked:
        try:
            cid = int(t["channel_id"])
            login = str(t.get("login") or "").lower()
            if cid > 0:
                ids.append(cid)
                if login:
                    id_to_login[cid] = login
        except Exception:
            continue

    if not ids:
        return []

    token = await _get_app_access_token(session)
    headers = {
        "Client-Id": str(twitch_client_id),
        "Authorization": f"Bearer {token}",
    }

    live_streams: List["LiveStream"] = []
    for chunk in _chunks(ids, 100):
        data = await _helix_get_streams(session, headers, chunk)

        for s in data.get("data", []) or []:
            try:
                channel_id = int(s["user_id"])
                twitch_stream_id = str(s["id"])
                started_at = s.get("started_at")
                login = id_to_login.get(channel_id) or str(s.get("user_login") or s.get("user_name") or "").lower()

                if not twitch_stream_id or channel_id <= 0:
                    continue

                live_streams.append(
                    LiveStream(
                        channel_id=channel_id,
                        login=login,
                        twitch_stream_id=twitch_stream_id,
                        started_at=str(started_at) if started_at else None,
                    )
                )
            except Exception:
                continue

    return live_streams

# ----------------------------
# Streamlink DL + Paramiko UL
# ----------------------------

async def record_stream_with_streamlink(
    live: LiveStream,
    *,
    segment_idx: int,
    filename: str,
) -> Tuple[str, int, str]:
    """
    Record a live stream with streamlink to a local file
    then return (file_location, size_bytes, status).
    """
    # ---- config knobs ----
    quality = os.environ.get("STREAMLINK_QUALITY", "best")
    out_dir = Path(LOCAL_STREAM_FILE_LOCATION).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    local_path = out_dir / filename

    # ---- 0) Ensure we have write perms ----
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(b"")  # touch file
    except PermissionError as e:
        logger.exception("NO WRITE PERMISSION out_dir=%s local_path=%s", out_dir, local_path)
        raise
    except OSError as e:
        logger.exception("FAILED TO CREATE OUTPUT FILE out_dir=%s local_path=%s", out_dir, local_path)
        raise
    else:
        try:
            local_path.unlink()  # cleanup the touched file; streamlink will create it again
        except Exception:
            pass

    # ---- 1) record locally with streamlink ----
    url = f"https://twitch.tv/{live.login}"

    logger.info(
        "--- RECORDING STARTED --- login=%s channel_id=%s stream_id=%s seg=%s out=%s",
        live.login, live.channel_id, live.twitch_stream_id, segment_idx, local_path
    )

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "streamlink",
        url, quality, "-o", str(local_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    #proc = await asyncio.create_subprocess_exec(
    #    streamlink_bin,
    #    url,
    #    quality,
    #    "-o",
    #    str(local_path),
    #    stdout=asyncio.subprocess.PIPE,
    #    stderr=asyncio.subprocess.PIPE,
    #)

    logger.info("streamlink spawned pid=%s", proc.pid)
    segment_timeout_s = float(os.environ.get("STREAMLINK_SEGMENT_SECONDS", str(10 * 3600)))
    if segment_timeout_s < 60: 
        raise ValueError("STREAMLINK_SEGMENT_SECONDS should be at least 60 seconds")

    was_timeout = False
    stdout_b: bytes = b""
    stderr_b: bytes = b""

    try:
        # Normal path: wait until stream ends OR timeout hits
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=segment_timeout_s)
    except asyncio.TimeoutError:
        # Timeout path (i.e. our video chunk time length was hit):
        # stop the process and mark segment as partial.
        was_timeout = True

        # Try graceful terminate first
        try:
            proc.terminate()
        except ProcessLookupError:
            pass

        # Give it a moment to exit, then hard kill if needed
        try:
            await asyncio.wait_for(proc.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()

        stdout_b, stderr_b = b"", b""

    # Non-timeout error handling:
    if not was_timeout and proc.returncode != 0:
        stderr_s = (stderr_b or b"").decode("utf-8", errors="replace")[-2000:]
        stdout_s = (stdout_b or b"").decode("utf-8", errors="replace")[-2000:]
        raise RuntimeError(
            f"streamlink failed rc={proc.returncode}\nSTDERR:\n{stderr_s}\nSTDOUT:\n{stdout_s}"
        )

    # Even on timeout, we expect a non-empty file (otherwise treat as failure)
    try:
        st = local_path.stat()
    except FileNotFoundError:
        logger.error("streamlink failure rc=%s login=%s stream_id=%s", proc.returncode, live.login,
                     live.twitch_stream_id)
        raise RuntimeError("streamlink finished but output file is missing")

    if st.st_size <= 0:
        logger.error("streamlink failure rc=%s login=%s stream_id=%s", proc.returncode, live.login,
                     live.twitch_stream_id)
        raise RuntimeError("streamlink finished but output file is empty")

    size_bytes = int(local_path.stat().st_size)

    # logging on dl attempt
    logger.info("streamlink exit rc=%s timed_out=%s size=%s path=%s", proc.returncode, was_timeout, size_bytes,
                local_path)

    # Return filename (relative “location”); caller can store this in DB
    location = filename
    final_status = "partial" if was_timeout else "complete"
    return location, size_bytes, final_status


class StreamTrackerService:
    def __init__(
        self,
        *,
        sleep_timer: float = 5.0,
        max_concurrent_streams: int = 10,
        seen_ttl_seconds: int = 7 * 24 * 3600,  # prune after 7 days
        http_timeout_seconds: float = 15.0,
    ):
        self.sleep_timer = float(sleep_timer)
        self.max_concurrent_streams = int(max_concurrent_streams)
        self.seen_ttl_seconds = int(seen_ttl_seconds)

        self._cooldown_until: Dict[str, float] = {}
        self.claim_none_cooldown_s: float = 60.0

        self.queue: asyncio.Queue[LiveStream] = asyncio.Queue()
        self._stop = asyncio.Event()

        # stream_ids either in queue or being recorded
        self._queued_stream_ids: Set[str] = set()

        self.http_timeout = aiohttp.ClientTimeout(total=float(http_timeout_seconds))
        self.http_session: aiohttp.ClientSession | None = None


    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        # Create ONE session for the lifetime of the service (fixes per-loop session creation)
        self.http_session = aiohttp.ClientSession(timeout=self.http_timeout)

        workers = [
            asyncio.create_task(self._worker_loop(i), name=f"worker_{i}")
            for i in range(self.max_concurrent_streams)
        ]
        try:
            await self._poll_loop()
        finally:
            # Stop workers
            for _ in workers:
                await self.queue.put(_Sentinel())  # type: ignore[arg-type]
            await asyncio.gather(*workers, return_exceptions=True)

            if self.http_session is not None:
                await self.http_session.close()
                self.http_session = None

    async def _poll_loop(self) -> None:
        assert self.http_session is not None

        while not self._stop.is_set():
            try:
                tracked = await load_tracked_streams()
                live_streams = await check_if_streams_are_live(self.http_session, tracked)
                if DEBUG:
                    logger.debug("ping: tracked=%s live=%s queued=%s cooldown=%s",
                                 len(tracked), len(live_streams), len(self._queued_stream_ids),
                                 len(self._cooldown_until))

            except Exception as e:
                logging.exception("[poll] error.")
                await asyncio.sleep(self.sleep_timer)
                continue

            enqueued = 0
            now = time.time()
            for live in live_streams:
                sid = live.twitch_stream_id
                if not sid:
                    continue

                # Check if this sid is on cooldown
                until = self._cooldown_until.get(sid)
                if until is not None:
                    if now < until:
                        continue
                    else:
                        self._cooldown_until.pop(sid, None)

                # already waiting in queue
                if sid in self._queued_stream_ids:
                    continue

                self._queued_stream_ids.add(sid)
                if DEBUG:
                    logger.debug("poll enqueue stream_id=%s login=%s", sid, live.login)
                await self.queue.put(live)
                enqueued += 1

            if enqueued:
                logger.info("poll enqueued=%s queue_size=%s", enqueued, self.queue.qsize())

            await asyncio.sleep(self.sleep_timer)

    async def _worker_loop(self, worker_idx: int) -> None:
        while True:
            item = await self.queue.get()
            sid: Optional[str] = None
            try:
                if isinstance(item, _Sentinel):
                    return

                live: LiveStream = item
                sid = live.twitch_stream_id
                logger.info("worker[%s] dequeued stream_id=%s login=%s", worker_idx, sid, live.login)

                try:
                    claimed = await db_try_claim_next_segment(live)
                except Exception as e:
                    logger.exception(f"[worker {worker_idx}] db claim error for {sid}: {e}")
                    continue

                if not claimed:
                    logger.info("worker[%s] claim_none stream_id=%s cooldown=%ss", worker_idx, sid,
                                self.claim_none_cooldown_s)
                    # next poll worker will see this cooldown and wait
                    self._cooldown_until[sid] = time.time() + self.claim_none_cooldown_s
                    continue

                saved_id, segment_idx = claimed

                safe_login = "".join(ch for ch in (live.login or "") if ch.isalnum() or ch == "_").strip(
                    "_") or f"chan_{live.channel_id}"
                ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
                filename = f"{safe_login}_{live.twitch_stream_id}_seg{int(segment_idx)}_{ts}.ts"

                # Write location immediately so listings can show the link while recording
                try:
                    await db_set_location_pending(saved_id, file_location=filename)
                except Exception:
                    logger.exception("Failed to set pending location saved_id=%s filename=%s", saved_id, filename)
                    # keep going; not fatal

                try:
                    logger.info("worker[%s] recording stream_id=%s seg=%s location=%s", worker_idx, sid, segment_idx,
                                filename)
                    file_location, size_bytes, final_status = await record_stream_with_streamlink(
                        live,
                        segment_idx=segment_idx,
                        filename=filename,
                    )
                    await db_mark_done(saved_id, status=final_status, file_location=file_location, size_bytes=size_bytes)

                    print(f"[worker {worker_idx}] {final_status}: {live.login} ({sid}) seg={segment_idx}")
                except Exception as e:
                    err = str(e)
                    logger.exception("worker[%s] failed login=%s stream_id=%s seg=%s", worker_idx, live.login, sid,
                                     segment_idx)
                    try:
                        await db_mark_failed(saved_id, error=err)
                    except Exception:
                        logger.exception("db_mark_failed failed saved_id=%s (original error=%r)", saved_id, err)
            finally:
                if sid:
                    self._queued_stream_ids.discard(sid)
                self.queue.task_done()


class _Sentinel:
    # pass to worker to kill it
    pass


async def main() -> None:
    await init_db_pool()
    await db_fail_all_pending()  # clear pre-existing stale recording jobs
    svc = StreamTrackerService(sleep_timer=5, max_concurrent_streams=10)

    # Clean shutdown on SIGINT/SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, svc.stop)
        except NotImplementedError:
            # e.g. some Windows environments
            pass

    try:
        await svc.run()
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(main())
