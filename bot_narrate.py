import asyncio
import time
import io
import base64
import subprocess
from typing import Optional, Dict, Tuple, List, Deque
from collections import deque, Counter
import unicodedata
import aiohttp
import discord
from discord.ext import commands
from config import google_narrate_key
from bot_helpers import get_user_id, get_guild_id


# ==========================
# Tuning knobs & feature flags
# ==========================
MAX_CHARS_PER_CHUNK = 180                  # small chunks → faster time-to-first-audio
CHUNK_COALESCE_WINDOW_MS = 500             # coalesce rapid consecutive messages
CACHE_MAX_ITEMS = 512                      # in-memory audio cache entries
PLAYBACK_IDLE_DISCONNECT_SECS = 1200       # leave VC when idle
DEFAULT_VOICE = "en-US-Wavenet-D"          # must be a full canonical name
DEFAULT_LANG = "en-US"
DEFAULT_RATE = 1.0                         # 0.25–4.0 (classic voices only)
FFMPEG_BIN = "ffmpeg"

# AMIX (overlap) feature flag (off by default)
AMIX_ENABLED = True
AMIX_GROUP_WINDOW = 0.25                   # seconds to wait for additional clips to mix
AMIX_MAX_INPUTS = 4                        # keep low; intelligibility suffers otherwise

# Global TTS concurrency (simple protection for many guilds)
GLOBAL_TTS_CONCURRENCY = 10

# Earcon: add a brief beep to disambiguate when multiple users use the SAME voice
EARCON_ENABLED = True
EARCON_FREQ_HZ = 880
EARCON_DURATION_S = 0.18

GOOGLE_TTS_ENDPOINT = "https://texttospeech.googleapis.com/v1/text:synthesize?key={api_key}"


# ==========================
# Helpers
# ==========================
def _strip_zero_width(s: str) -> str:
    # Removes zero-width “Cf” characters that sometimes sneak in when copying mentions
    return "".join(ch for ch in s if unicodedata.category(ch) != "Cf")

def _message_is_narrate_command(bot: commands.Bot, content: str) -> bool:
    """Return True if the message looks like a narrate command with the bot's prefix."""
    if not content:
        return False
    content = content.strip()
    prefixes = bot.command_prefix
    if callable(prefixes):
        # If you ever switch to a callable prefix, resolve it to a list; for now assume string
        prefixes = "!"
    if isinstance(prefixes, str):
        prefixes = [prefixes]
    for p in prefixes:
        if content.startswith(p):
            rest = content[len(p):].lstrip().lower()
            return rest.startswith("narrate")
    return False

def _now_ms() -> int:
    return int(time.time() * 1000)

def chunk_text(text: str, limit: int = MAX_CHARS_PER_CHUNK) -> List[str]:
    text = text.strip()
    if len(text) <= limit:
        return [text] if text else []
    chunks, cur = [], []
    cur_len = 0
    tokens = text.split()
    for tok in tokens:
        add_len = len(tok) + (1 if cur else 0)
        if cur_len + add_len > limit:
            chunks.append(" ".join(cur))
            cur, cur_len = [tok], len(tok)
        else:
            cur.append(tok)
            cur_len += add_len
    if cur:
        chunks.append(" ".join(cur))
    return chunks


class LRUCache:
    def __init__(self, max_items: int):
        self.max = max_items
        self.store: Dict[Tuple[str, str, float], Tuple[bytes, int]] = {}

    def get(self, key: Tuple[str, str, float]) -> Optional[bytes]:
        val = self.store.get(key)
        if not val:
            return None
        audio, _ = val
        self.store[key] = (audio, _now_ms())
        return audio

    def put(self, key: Tuple[str, str, float], audio: bytes) -> None:
        if key in self.store:
            self.store[key] = (audio, _now_ms())
            return
        if len(self.store) >= self.max:
            oldest_key = min(self.store.items(), key=lambda kv: kv[1][1])[0]
            self.store.pop(oldest_key, None)
        self.store[key] = (audio, _now_ms())


# ==========================
# TTS Provider (Google)
# ==========================
class GoogleTTSProvider:
    """
    Google Cloud Text-to-Speech v1 REST.
    - Returns compressed (OGG_OPUS) bytes; we transcode to PCM via ffmpeg pipe.
    - Uses a tiny in-memory cache and a global concurrency cap.
    - Automatically adapts payload for Chirp/Journey voices (omit classic knobs).
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache = LRUCache(CACHE_MAX_ITEMS)
        self._sem = asyncio.Semaphore(GLOBAL_TTS_CONCURRENCY)

    async def start(self):
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def synth(
        self,
        text: str,
        voice_name: str = DEFAULT_VOICE,
        language_code: str = DEFAULT_LANG,
        speaking_rate: float = DEFAULT_RATE,
        audio_encoding: str = "OGG_OPUS"
    ) -> bytes:
        # Voice-family detection (string heuristic, no canonicalization)
        vlow = (voice_name or "").lower()
        is_chirp_or_journey = ("-chirp" in vlow) or ("-journey" in vlow)

        # Cache key should reflect whether we included rate or not
        eff_rate = 1.0 if is_chirp_or_journey else float(speaking_rate)
        key = (f"{language_code}:{voice_name}:{text}", audio_encoding, eff_rate)
        cached = self._cache.get((key[0], key[1], key[2]))
        if cached is not None:
            return cached

        if not self._session:
            await self.start()

        # Build payload; omit speakingRate for Chirp/Journey
        audio_cfg = {"audioEncoding": audio_encoding}
        if not is_chirp_or_journey:
            audio_cfg["speakingRate"] = eff_rate

        payload = {
            "input": {"text": text},
            "voice": {"languageCode": language_code, "name": voice_name},
            "audioConfig": audio_cfg
        }

        url = GOOGLE_TTS_ENDPOINT.format(api_key=self.api_key)
        async with self._sem:
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(f"Google TTS error {resp.status}: {body[:500]}")
                data = await resp.json()

        audio_b64 = data.get("audioContent")
        if not audio_b64:
            raise RuntimeError("Google TTS response missing audioContent")
        audio_bytes = base64.b64decode(audio_b64)
        self._cache.put((key[0], key[1], key[2]), audio_bytes)
        return audio_bytes


# ==========================
# Guild voice management
# ==========================
class GuildVoiceSession:
    """
    One voice connection + playback pipeline per guild.
    Policy = MOST-RECENT-WINS:
      - Any eligible user action (VC join/move or text in chosen channel) claims control.
      - The bot moves to that user's channel and enqueues their audio.
    Earcon Rules:
      - If multiple users share the SAME voice in recent activity → insert earcon before each utterance.
      - If different voices → no earcon.
    AMIX (optional):
      - If AMIX_ENABLED, group clips that arrive within AMIX_GROUP_WINDOW and mix (up to AMIX_MAX_INPUTS).
    """
    def __init__(self, bot: commands.Bot, guild_id: int, tts: GoogleTTSProvider):
        self.bot = bot
        self.guild_id = guild_id
        self.tts = tts

        self.voice_client: Optional[discord.VoiceClient] = None
        # queue entries are tuples:
        #   (audio_bytes, user_id, voice_name, label, is_earcon)
        self.queue: "asyncio.Queue[Tuple[bytes, int, str, str, bool]]" = asyncio.Queue()

        self.player_task: Optional[asyncio.Task] = None
        self.idle_task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()
        self.last_activity = time.time()

        # “most recent wins”
        self.active_user_id: Optional[int] = None

        # recent speakers (sliding few seconds)
        self.recent_speakers: Deque[Tuple[int, str, float]] = deque(maxlen=50)
        self.RECENT_WINDOW = 6.0  # seconds

        # cached earcon bytes
        self._earcon_bytes: Optional[bytes] = None

    # ---------- connection & lifecycle ----------
    async def ensure_connected(self, channel: discord.VoiceChannel) -> None:
        async with self.lock:
            if self.voice_client and self.voice_client.channel and self.voice_client.channel.id == channel.id:
                self._ensure_player()
                self._ensure_idle_timer()
                return
            if self.voice_client and self.voice_client.is_connected():
                await self.voice_client.move_to(channel)
            else:
                self.voice_client = await channel.connect(reconnect=False, self_deaf=True)
            self._ensure_player()
            self._ensure_idle_timer()

    def _ensure_player(self):
        if self.player_task and not self.player_task.done():
            return
        self.player_task = asyncio.create_task(self._player_loop(), name=f"player:{self.guild_id}")

    def _ensure_idle_timer(self):
        if self.idle_task and not self.idle_task.done():
            return
        self.idle_task = asyncio.create_task(self._idle_loop(), name=f"idle:{self.guild_id}")

    async def _idle_loop(self):
        try:
            while True:
                await asyncio.sleep(5)
                vc = self.voice_client
                if not vc or not vc.is_connected():
                    return

                if time.time() - self.last_activity > PLAYBACK_IDLE_DISCONNECT_SECS:
                    # --- FULL TEARDOWN on idle ---
                    try:
                        # Stop any residual playback
                        try:
                            if vc.is_playing():
                                vc.stop()
                        except Exception:
                            pass

                        # Disconnect (we connect with reconnect=False elsewhere)
                        await vc.disconnect(force=True)
                    finally:
                        # Disable narration for ALL users in this guild
                        try:
                            pool = getattr(self.bot, "db_pool", None)
                            if pool:
                                async with pool.acquire() as conn:
                                    await conn.execute(
                                        """UPDATE narrate_prefs
                                           SET enabled=FALSE, updated_at=CURRENT_TIMESTAMP
                                           WHERE guild_id=$1""",
                                        self.guild_id
                                    )
                        except Exception as e:
                            print(f"[narrate] failed to disable prefs on idle: {e}")

                        # Clear local state & background tasks
                        self.voice_client = None
                        self.active_user_id = None

                        if self.player_task and not self.player_task.done():
                            self.player_task.cancel()
                        self.player_task = None

                        # cancel *this* idle task by returning
                        if self.idle_task and not self.idle_task.done():
                            self.idle_task.cancel()
                        self.idle_task = None

                        # Drain any queued audio so nothing nudges the VC
                        try:
                            while True:
                                self.queue.get_nowait()
                                self.queue.task_done()
                        except asyncio.QueueEmpty:
                            pass

                        return  # exit the idle loop after teardown
        except asyncio.CancelledError:
            return


    # ---------- earcon generation ----------
    async def _get_earcon_bytes(self) -> bytes:
        if self._earcon_bytes is not None:
            return self._earcon_bytes
        # Generate a tiny beep via ffmpeg synth (lavfi sine) → ogg/opus bytes
        cmd = [
            FFMPEG_BIN,
            "-v", "error",
            "-f", "lavfi",
            "-i", f"sine=frequency={EARCON_FREQ_HZ}:duration={EARCON_DURATION_S}:sample_rate=48000",
            "-c:a", "libopus",
            "-b:a", "32k",
            "-f", "ogg",
            "pipe:1",
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        audio_bytes, _ = proc.communicate()
        if not audio_bytes:
            audio_bytes = b""
        self._earcon_bytes = audio_bytes
        return self._earcon_bytes

    # ---------- enqueue ----------
    async def enqueue(self, audio_bytes: bytes, user_id: int, voice_name: str, label: str, is_earcon: bool = False):
        await self.queue.put((audio_bytes, user_id, voice_name, label, is_earcon))
        self.last_activity = time.time()

    def _voices_in_recent_window(self) -> Counter:
        now = time.time()
        while self.recent_speakers and (now - self.recent_speakers[0][2] > self.RECENT_WINDOW):
            self.recent_speakers.popleft()
        seen_by_voice: Dict[str, set] = {}
        for uid, vname, ts in self.recent_speakers:
            seen_by_voice.setdefault(vname, set()).add(uid)
        vc = Counter({v: len(uids) for v, uids in seen_by_voice.items()})
        return vc

    # ---------- playback ----------
    async def _player_loop(self):
        try:
            while True:
                item = await self.queue.get()
                batch = [item]

                if AMIX_ENABLED:
                    try:
                        while len(batch) < AMIX_MAX_INPUTS:
                            nxt = await asyncio.wait_for(self.queue.get(), timeout=AMIX_GROUP_WINDOW)
                            batch.append(nxt)
                    except asyncio.TimeoutError:
                        pass

                if AMIX_ENABLED:
                    non_earcons = [b for b in batch if not b[4]]
                    earcons = [b for b in batch if b[4]]
                    if len(non_earcons) > 1:
                        # (AMIX is placeholder; see comments in _mix_bytes_amix in earlier versions)
                        # For now, just play sequentially as the multi-pipe approach is platform-heavy.
                        for e in earcons:
                            await self._play_one(e[0]); self.queue.task_done()
                        for b in non_earcons:
                            await self._play_one(b[0]); self.queue.task_done()
                        self.last_activity = time.time()
                        continue

                for b in batch:
                    await self._play_one(b[0])
                    self.queue.task_done()
                self.last_activity = time.time()
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"[narrate] player loop error: {e}")
            return

    async def _play_one(self, audio_bytes: bytes):
        if not self.voice_client or not self.voice_client.is_connected():
            return
        source = discord.FFmpegPCMAudio(
            io.BytesIO(audio_bytes),
            pipe=True,
            executable=FFMPEG_BIN,
        )
        done = asyncio.Event()
        def _after(_err):
            done.set()
        self.voice_client.play(source, after=_after)
        try:
            await done.wait()
        finally:
            try: source.cleanup()
            except Exception: pass

    async def cancel_playback(self):
        """Stop current audio and clear the queue for this guild."""
        if self.voice_client and self.voice_client.is_connected() and self.voice_client.is_playing():
            try:
                self.voice_client.stop()
            except Exception:
                pass
        try:
            while True:
                self.queue.get_nowait()
                self.queue.task_done()
        except asyncio.QueueEmpty:
            pass
        self.last_activity = time.time()


# ==========================
# Cog
# ==========================
class NarrationCog(commands.Cog, name="Narrate"):
    """
    Commands:
      !narrate on #text-channel [voice] [rate]
      !narrate off
      !narrate status
      !narrate cancel | !narrate x
      !narrate voices
      !narrate shutoff

    Behavior:
      - Most-Recent-Wins: the latest eligible user action takes control; the bot moves to their VC.
      - Earcon only when multiple users share the SAME voice in recent activity; otherwise none.
      - Optional AMIX (overlap) behind a flag.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.tts = GoogleTTSProvider(google_narrate_key)
        self.guild_sessions: Dict[int, GuildVoiceSession] = {}
        # coalescing state per user
        self._pending_user_text: Dict[Tuple[int, int], Tuple[int, List[str]]] = {}  # (guild_id,user_id) -> (deadline_ms, parts)
        self._coalesce_task = asyncio.create_task(self._coalesce_loop())

    def cog_unload(self):
        if self._coalesce_task and not self._coalesce_task.done():
            self._coalesce_task.cancel()
        coro = self.tts.close()
        try:
            asyncio.create_task(coro)
        except RuntimeError:
            pass

    def _get_session(self, guild_id: int) -> GuildVoiceSession:
        sess = self.guild_sessions.get(guild_id)
        if not sess:
            sess = GuildVoiceSession(self.bot, guild_id, self.tts)
            self.guild_sessions[guild_id] = sess
        return sess

    # ---------- DB Helpers ----------
    async def _get_pref(self, guild_id: int, user_id: int) -> Optional[dict]:
        pool = getattr(self.bot, "db_pool", None)
        if not pool:
            return None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT guild_id, user_id, text_channel_id, voice, rate, enabled
                   FROM narrate_prefs
                   WHERE guild_id=$1 AND user_id=$2""",
                guild_id, user_id
            )
            return dict(row) if row else None

    async def _upsert_pref(
        self, guild_id: int, user_id: int, text_channel_id: int,
        voice: Optional[str], rate: Optional[float], enabled: bool
    ) -> None:
        pool = getattr(self.bot, "db_pool", None)
        if not pool:
            raise RuntimeError("db_pool not available")
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO narrate_prefs (guild_id, user_id, text_channel_id, voice, rate, enabled)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (guild_id, user_id)
                DO UPDATE SET text_channel_id=EXCLUDED.text_channel_id,
                              voice=EXCLUDED.voice,
                              rate=EXCLUDED.rate,
                              enabled=EXCLUDED.enabled,
                              updated_at=CURRENT_TIMESTAMP
                """,
                guild_id, user_id, text_channel_id, voice, rate, enabled
            )

    async def _upsert_pref_ctx(self, ctx, text_channel_id, voice, rate, enabled):
        pool = getattr(self.bot, "db_pool", None)
        if not pool:
            raise RuntimeError("db_pool not available")
        guild_id = await get_guild_id(ctx, pool)
        user_id  = await get_user_id(ctx, pool)
        if guild_id is None or user_id is None:
            return  # helpers already messaged on DB error
        await self._upsert_pref(guild_id, user_id, text_channel_id, voice, rate, enabled)

    async def _set_enabled(self, guild_id: int, user_id: int, enabled: bool) -> None:
        pool = getattr(self.bot, "db_pool", None)
        if not pool:
            raise RuntimeError("db_pool not available")
        async with pool.acquire() as conn:
            await conn.execute(
                """UPDATE narrate_prefs
                   SET enabled=$3, updated_at=CURRENT_TIMESTAMP
                   WHERE guild_id=$1 AND user_id=$2""",
                guild_id, user_id, enabled
            )
            

    # ---------- Channel monitoring helpers ---------
    async def _enabled_user_ids(self, guild_id: int) -> List[int]:
        pool = getattr(self.bot, "db_pool", None)
        if not pool:
            return []
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT user_id FROM narrate_prefs
                   WHERE guild_id=$1 AND enabled=TRUE""",
                guild_id
            )
        return [r["user_id"] for r in rows]
    
    async def _any_enabled_in_channel(self, guild: discord.Guild, channel: discord.VoiceChannel) -> bool:
        member_ids = [m.id for m in channel.members if not m.bot]
        if not member_ids:
            return False
        pool = getattr(self.bot, "db_pool", None)
        if not pool:
            return False
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT 1
                   FROM narrate_prefs
                   WHERE guild_id=$1 AND enabled=TRUE AND user_id = ANY($2::bigint[])
                   LIMIT 1""",
                guild.id, member_ids
            )
            return row is not None

    async def _disconnect_if_no_enabled_in_channel(self, guild: discord.Guild, channel: discord.VoiceChannel, session: "GuildVoiceSession"):
        has_enabled = await self._any_enabled_in_channel(guild, channel)
        vc = session.voice_client

        if not has_enabled and vc and vc.is_connected():
            try:
                try:
                    if vc.is_playing():
                        vc.stop()
                except Exception:
                    pass
                await vc.disconnect(force=True)
            finally:
                session.voice_client = None
                session.active_user_id = None

                if session.player_task and not session.player_task.done():
                    session.player_task.cancel()
                session.player_task = None

                # don't cancel the idle task here; it will end naturally since vc is None
                # Clear queue so nothing triggers playback
                try:
                    while True:
                        session.queue.get_nowait()
                        session.queue.task_done()
                except asyncio.QueueEmpty:
                    pass


    # ---------- Commands ----------
    @commands.group(name="narrate", invoke_without_command=True)
    async def narrate_root(self, ctx: commands.Context):
        await ctx.send(
            "Usage:\n"
            "  !narrate on [#text-channel] [voice]\n"
            "  !narrate off\n"
            "  !narrate status\n"
            "  !narrate cancel   (alias: !narrate x)\n"
            "  !narrate channel #text-channel\n"
            "  !narrate voice <full-voice-name>\n"
            "  !narrate voices\n"
            "  !narrate shutoff",
            suppress_embeds=True,
        )

    @narrate_root.command(name="on", aliases=["start"])
    async def narrate_on(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
        voice: Optional[str] = None,
        rate: Optional[float] = None,
    ):
        # Load existing prefs to use as defaults if args omitted
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        v = voice or (pref.get("voice") if pref else None) or DEFAULT_VOICE
        r = float(rate if rate is not None else (pref.get("rate") if (pref and pref.get("rate") is not None) else DEFAULT_RATE))
        ch = channel or (ctx.guild.get_channel(pref["text_channel_id"]) if pref and pref.get("text_channel_id") else None)
        if ch is None:
            return await ctx.send(
                "Choose a text channel first: `!narrate on #your-text-channel [voice]` "
                "or set it with `!narrate channel #your-text-channel`.",
                suppress_embeds=True,
            )

        perms = ch.permissions_for(ctx.guild.me)
        if not (perms.view_channel and perms.send_messages and perms.read_message_history):
            return await ctx.send(f"I need view/send/read-history access in {ch.mention}.", suppress_embeds=True)

        await self._upsert_pref_ctx(ctx, ch.id, v, r, True)
        await self.tts.start()

        # Auto-join current VC if user is in one (most-recent wins)
        session = self._get_session(ctx.guild.id)
        session.active_user_id = ctx.author.id
        if ctx.author.voice and ctx.author.voice.channel:
            await session.ensure_connected(ctx.author.voice.channel)

        await ctx.send(
            f"Narration enabled for you in {ch.mention}.\n"
            f"Voice={v} | Rate={r}. Join a voice channel and type in {ch.mention} to hear narration.",
            suppress_embeds=True,
        )

        if session.voice_client and session.voice_client.is_connected():
            await self._disconnect_if_no_enabled_in_channel(ctx.guild, session.voice_client.channel, session)

    @narrate_root.command(name="off", aliases=["stop"])
    async def narrate_off(self, ctx: commands.Context):
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        if not pref or not pref.get("enabled"):
            return await ctx.send("Narration is already disabled for you.")
        await self._set_enabled(ctx.guild.id, ctx.author.id, False)
        await ctx.send("Narration disabled for you.")
        session = self._get_session(ctx.guild.id)
        vc = session.voice_client
        if vc and vc.is_connected():
            await self._disconnect_if_no_enabled_in_channel(ctx.guild, vc.channel, session)

    @narrate_root.command(name="cancel", aliases=["x"])
    async def narrate_cancel(self, ctx: commands.Context):
        session = self._get_session(ctx.guild.id)
        await session.cancel_playback()
        await ctx.send("⏹️ Stopped current narration and cleared the queue.")

    @narrate_root.command(name="voices")
    async def narrate_voices(self, ctx: commands.Context):
        # Per plan: only provide a link; no listing/filtering in Discord.
        return await ctx.send(
            "Choose a voice here (use the exact **Name** as your voice):\n"
            "https://cloud.google.com/text-to-speech/docs/voices",
            suppress_embeds=True,
        )

    @narrate_root.command(name="status")
    async def narrate_status(self, ctx: commands.Context):
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        session = self._get_session(ctx.guild.id)
        vc = session.voice_client

        ch_disp = "—"
        if pref and pref.get("text_channel_id"):
            ch = ctx.guild.get_channel(pref["text_channel_id"])
            ch_disp = ch.mention if ch else f"<#{pref['text_channel_id']}>"

        who = "—"
        if session.active_user_id:
            m = ctx.guild.get_member(session.active_user_id)
            who = m.mention if m else f"<@{session.active_user_id}>"

        vc_disp = "not connected"
        if vc and vc.is_connected():
            vc_disp = f"{vc.channel.name} (id={vc.channel.id})"

        enabled = pref['enabled'] if pref else False
        vname = (pref.get('voice') if pref else None) or DEFAULT_VOICE
        rate = (pref.get('rate') if pref else None) or DEFAULT_RATE

        # NEW: list everyone enabled in this guild
        enabled_ids = await self._enabled_user_ids(ctx.guild.id)
        if enabled_ids:
            enabled_mentions = []
            for uid in enabled_ids:
                m = ctx.guild.get_member(uid)
                enabled_mentions.append(m.mention if m else f"<@{uid}>")
            enabled_line = "• Enabled users in this guild: " + ", ".join(enabled_mentions)
        else:
            enabled_line = "• Enabled users in this guild: nobody has narrate enabled"

        await ctx.send(
            f"**Narration status**\n"
            f"• You: Enabled={enabled} | Channel={ch_disp} | Voice={vname} | Rate={rate}\n"
            f"• Bot VC: {vc_disp}\n"
            f"• Active narrator (most-recent): {who}\n"
            f"{enabled_line}\n"
            f"Commands: !narrate on/off, channel, status, cancel, voice, voices, shutoff",
            suppress_embeds=True,
        )

        
    # Set (and optionally live-apply) the text channel
    @narrate_root.command(name="channel")
    async def narrate_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        v = (pref.get("voice") if pref else None) or DEFAULT_VOICE
        r = (pref.get("rate") if pref and pref.get("rate") is not None else DEFAULT_RATE)
        perms = channel.permissions_for(ctx.guild.me)
        if not (perms.view_channel and perms.send_messages and perms.read_message_history):
            return await ctx.send(f"I need view/send/read-history access in {channel.mention}.", suppress_embeds=True)

        await self._upsert_pref_ctx(ctx, channel.id, v, r, pref.get("enabled") if pref else True)
        await ctx.send(f"Default narration channel set to {channel.mention}.", suppress_embeds=True)

        # Live-apply: nothing to move in VC; we just start reading from the new text channel for this user.
        # If they are enabled and already in VC, nothing else to do.

    # Set (and optionally live-apply) the voice
    @narrate_root.command(name="voice")
    async def narrate_voice(self, ctx: commands.Context, *, voice: str):
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        if not pref:
            # Create a row with defaults for channel=required later
            await self._upsert_pref_ctx(ctx, ctx.channel.id, voice, DEFAULT_RATE, True)
            return await ctx.send(
                f"Default voice set to `{voice}`. Use `!narrate on #text-channel` to start.",
                suppress_embeds=True,
            )
        await self._upsert_pref_ctx(ctx, pref["text_channel_id"], voice, pref.get("rate") or DEFAULT_RATE, pref.get("enabled"))
        await ctx.send(f"Default voice set to `{voice}`.", suppress_embeds=True)
        # Live-apply: the next narration will use this voice automatically.

    # ---------- Events ----------
    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        if member.bot or not member.guild:
            return
        pref = await self._get_pref(member.guild.id, member.id)
        session = self._get_session(member.guild.id)

        # MOST-RECENT-WINS: user joining/moving claims the session
        if pref and pref.get("enabled") and after and after.channel:
            session.active_user_id = member.id
            await session.ensure_connected(after.channel)

        # Regardless of who moved, if we're connected, ensure at least one enabled user remains in *that* channel
        vc = session.voice_client
        if vc and vc.is_connected():
            await self._disconnect_if_no_enabled_in_channel(member.guild, vc.channel, session)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return

        # Ignore our own command messages (e.g., "!narrate on …")
        if _message_is_narrate_command(self.bot, message.content):
            return

        pref = await self._get_pref(message.guild.id, message.author.id)
        if not pref or not pref.get("enabled"):
            return
        target = pref["text_channel_id"]
        chan = message.channel
        parent_id = getattr(chan, "parent_id", None)
        if not (chan.id == target or parent_id == target):
            return

        # Claim session (MOST-RECENT-WINS) and ensure connection
        session = self._get_session(message.guild.id)
        session.active_user_id = message.author.id

        member_vs = message.author.voice
        if member_vs and member_vs.channel:
            await session.ensure_connected(member_vs.channel)
        else:
            return

        # Coalesce rapid messages
        key = (message.guild.id, message.author.id)
        deadline = _now_ms() + CHUNK_COALESCE_WINDOW_MS
        prev = self._pending_user_text.get(key)
        if prev:
            _, parts = prev
            parts.append(message.content)
            self._pending_user_text[key] = (deadline, parts)
        else:
            self._pending_user_text[key] = (deadline, [message.content])
            
            
    @narrate_root.command(name="shutoff")
    @commands.has_permissions(manage_guild=True)  # optional: gate it; remove if you want anyone to run it
    async def narrate_shutoff(self, ctx: commands.Context):
        pool = getattr(self.bot, "db_pool", None)
        if not pool:
            return await ctx.send("DB not available.", suppress_embeds=True)

        # Disable everyone for this guild
        async with pool.acquire() as conn:
            status = await conn.execute(
                """UPDATE narrate_prefs
                   SET enabled=FALSE, updated_at=CURRENT_TIMESTAMP
                   WHERE guild_id=$1 AND enabled=TRUE""",
                ctx.guild.id
            )
        # asyncpg returns strings like "UPDATE 5"
        try:
            updated = int(status.split()[-1])
        except Exception:
            updated = 0

        # Attempt disconnect if we're in VC
        session = self._get_session(ctx.guild.id)
        vc = session.voice_client
        if vc and vc.is_connected():
            await self._disconnect_if_no_enabled_in_channel(ctx.guild, vc.channel, session)

        await ctx.send(f"Shutoff complete. Disabled narrate for {updated} user(s).", suppress_embeds=True)


    # ---------- Coalescer & enqueuer ----------
    async def _coalesce_loop(self):
        try:
            while True:
                await asyncio.sleep(0.05)
                now = _now_ms()
                to_emit = []
                for key, (deadline, parts) in list(self._pending_user_text.items()):
                    if now >= deadline:
                        combined = " ".join(p.strip() for p in parts if p.strip())
                        to_emit.append((key, combined))
                        del self._pending_user_text[key]

                for (guild_id, user_id), text in to_emit:
                    if not text:
                        continue

                    pref = await self._get_pref(guild_id, user_id)
                    if not pref or not pref.get("enabled"):
                        continue

                    voice = pref.get("voice") or DEFAULT_VOICE
                    language_code = "-".join(voice.split("-")[0:2]) if "-" in voice else DEFAULT_LANG
                    rate = float(pref.get("rate") or DEFAULT_RATE)

                    session = self._get_session(guild_id)

                    # record recent speaker for earcon decision
                    session.recent_speakers.append((user_id, voice, time.time()))
                    vcount = session._voices_in_recent_window()
                    same_voice_shared = vcount.get(voice, 0) >= 2  # >=2 distinct users used same voice recently
                    use_earcon = EARCON_ENABLED and same_voice_shared

                    chunks = chunk_text(text, MAX_CHARS_PER_CHUNK)
                    if not chunks:
                        continue

                    async def synth_chunk(t: str) -> bytes:
                        return await self.tts.synth(
                            t,
                            voice_name=voice,
                            language_code=language_code,
                            speaking_rate=rate,  # provider adapts for Chirp/Journey automatically
                        )

                    # Optional earcon
                    if use_earcon:
                        try:
                            ear_b = await session._get_earcon_bytes()
                            if ear_b:
                                await session.enqueue(ear_b, user_id=user_id, voice_name=voice, label="earcon", is_earcon=True)
                        except Exception as e:
                            print(f"[narrate] earcon error: {e}")

                    # Synthesize first chunk now (minimize TTFB) with robust user-facing errors
                    try:
                        first_bytes = await synth_chunk(chunks[0])
                        await session.enqueue(first_bytes, user_id=user_id, voice_name=voice, label="first")
                    except Exception as e:
                        # Loud, friendly error to the user's text channel
                        guild = self.bot.get_guild(guild_id)
                        ch = guild.get_channel(pref["text_channel_id"]) if guild else None
                        short = str(e)
                        msg = (
                            "TTS failed. "
                            "Make sure you used the exact **Name** from Google's voice list "
                            "(e.g., `en-US-Wavenet-D` or `en-US-Chirp3-HD-Gacrux`).\n"
                            "See: https://cloud.google.com/text-to-speech/docs/voices"
                        )
                        # Special hint for common Chirp/Journey errors
                        if "requires a model name" in short.lower():
                            msg = (
                                "TTS failed: that voice requires a full model-qualified **Name** "
                                "(e.g., `en-US-Chirp3-HD-<Voice>`). "
                                "Use the exact **Name** from: https://cloud.google.com/text-to-speech/docs/voices"
                            )
                        try:
                            if ch:
                                await ch.send(msg, suppress_embeds=True,)
                        except Exception:
                            pass
                        print(f"[narrate] synth error: {e}")
                        continue

                    # Prefetch remaining
                    if len(chunks) > 1:
                        async def prefetch_and_enqueue(rest: List[str]):
                            for part in rest:
                                try:
                                    data = await synth_chunk(part)
                                    await session.enqueue(data, user_id=user_id, voice_name=voice, label="chunk")
                                except Exception as e:
                                    # One failure shouldn't spam; log and stop fetching the rest
                                    print(f"[narrate] synth error (rest): {e}")
                                    break

                        asyncio.create_task(prefetch_and_enqueue(chunks[1:]))

        except asyncio.CancelledError:
            return
