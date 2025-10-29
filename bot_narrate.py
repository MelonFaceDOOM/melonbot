import asyncio
import time
import io
import base64
import subprocess
from typing import Optional, Dict, Tuple, List, Deque, Union
from collections import deque, Counter
import unicodedata
import aiohttp
import discord
from discord.ext import commands
from config import google_narrate_key
from bot_helpers import get_user_id, get_guild_id
from db_mixin import DbMixin
import re


# ==========================
# Tuning knobs & feature flags
# ==========================
MAX_CHARS_PER_CHUNK = 180                  # small chunks → faster time-to-first-audio
NARRATE_WORKERS = 4                        # will reduce lag if multiple guilds are narrating
CACHE_MAX_ITEMS = 512                      # in-memory audio cache entries
PLAYBACK_IDLE_DISCONNECT_SECS = 3600       # leave VC when idle
DEFAULT_VOICE = "en-US-Wavenet-D"          # must be a full canonical name
DEFAULT_LANG = "en-US"
DEFAULT_RATE = 1.0                         # 0.25–4.0 (classic voices only)
FFMPEG_BIN = "ffmpeg"
PLAY_TIMEOUT = 120                          # max seconds per clip

# Global TTS concurrency (simple protection for many guilds)
GLOBAL_TTS_CONCURRENCY = 10

GOOGLE_TTS_ENDPOINT = "https://texttospeech.googleapis.com/v1/text:synthesize?key={api_key}"


# ==========================
# Helpers
# ==========================

_URL_RE = re.compile(r'^(https?://\S+|www\.\S+)$', re.I)
_CUSTOM_EMOJI_RE = re.compile(r'^<a?:\w+:\d+>$')         # <:name:id> or <a:name:id>
_USER_MENTION_RE = re.compile(r'^<@!?\d+>$')             # <@123> / <@!123>
_ROLE_MENTION_RE = re.compile(r'^<@&\d+>$')              # <@&123>
_CHANNEL_MENTION_RE = re.compile(r'^<#\d+>$')            # <#123>

def _is_noise_token(t: str) -> bool:
    return (
        _URL_RE.match(t)
        or _CUSTOM_EMOJI_RE.match(t)
        or _USER_MENTION_RE.match(t)
        or _ROLE_MENTION_RE.match(t)
        or _CHANNEL_MENTION_RE.match(t)
    )

def _is_link_emoji_or_mention_only(s: str) -> bool:
    if not s or not s.strip():
        return True
    toks = s.strip().split()
    return all(_is_noise_token(t) for t in toks)

def _clean_content(s: str) -> str:
    """Remove links, custom/unicode emoji, and mentions; keep the rest."""
    toks = s.split()
    kept = [t for t in toks if not _is_noise_token(t)]
    return " ".join(kept).strip()

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
    
def _norm_gender(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    g = s.strip().upper()
    if g in {"M", "MALE"}:
        return "MALE"
    if g in {"F", "FEMALE"}:
        return "FEMALE"
    if g in {"N", "NEUTRAL"}:
        return "NEUTRAL"
    return None  # invalid -> no filter

def _collapse_ws(s: str) -> str:
    return " ".join(s.split())
    
def _short_voice_name(voice_name: str) -> str:
    # Strip the leading "<lang>-<REGION>-" prefix, keep the rest.
    # Examples:
    #   en-US-Wavenet-D            -> Wavenet-D
    #   ar-XA-Chirp3-HD-Achernar   -> Chirp3-HD-Achernar
    parts = voice_name.split("-", 2)
    return parts[2] if len(parts) >= 3 else voice_name

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
    """
    Caches requests to & responses from the TTS service
    
    Keys (Dict[Tuple[str, str, float):
        f"{language}:{voice}:{text}", audio_encoding, speaking_rate

    Values (Tuple[bytes, int]):
        bytes = the compressed audio returned by Google TTS (e.g., OGG/Opus).
        int = the last-access timestamp (ms) used for LRU eviction
    """
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
class GuildVoiceSession():
    """
    One voice connection + playback pipeline per guild.
    Policy = MOST-RECENT-WINS:
      - Any eligible user action (VC join/move or text in chosen channel) claims control.
      - The bot moves to that user's channel and enqueues their audio.
    """
    def __init__(self, bot: commands.Bot, guild_id: int, tts: GoogleTTSProvider):
        self.bot = bot
        self.guild_id = guild_id
        self.tts = tts

        self.voice_client: Optional[discord.VoiceClient] = None
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=200)

        self.player_task: Optional[asyncio.Task] = None
        self.idle_task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()
        self.last_activity = time.time()

    # ---------- enqueue ----------
    async def enqueue(self, audio_bytes: bytes):
        await self.queue.put(audio_bytes)
        self.last_activity = time.time()

    # ---------- connection & lifecycle ----------
    async def ensure_connected(self, channel: discord.VoiceChannel) -> None:
        async with self.lock:
            if self.voice_client and self.voice_client.is_connected():
                if self.voice_client.channel.id != channel.id:
                    await self.voice_client.move_to(channel)
            else:
                self.voice_client = await channel.connect(reconnect=True, self_deaf=True)
            self._ensure_player()
            self._ensure_idle_timer()
            
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

    def _ensure_player(self):
        if self.player_task and not self.player_task.done():
            return
        self.player_task = asyncio.create_task(self._player_loop(), name=f"player:{self.guild_id}")

    def _ensure_idle_timer(self):
        if self.idle_task and not self.idle_task.done():
            return
        self.idle_task = asyncio.create_task(self._idle_loop(), name=f"idle:{self.guild_id}")

    async def _player_loop(self):
        try:
            while True:
                audio_bytes = await self.queue.get()
                try:
                    try:
                        await self._play_one(audio_bytes)
                    finally:
                        self.queue.task_done()
                    self.last_activity = time.time()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[narrate] player error: {e}")
                    self.last_activity = time.time()
        except asyncio.CancelledError:
            return
            
    async def _play_one(self, audio_bytes: bytes):
        vc = self.voice_client
        if not vc or not vc.is_connected():
            return

        source = None
        done = asyncio.Event()

        def _after(_err):
            # discord.py calls this on natural end or on stop()
            done.set()

        try:
            source = discord.FFmpegPCMAudio(
                io.BytesIO(audio_bytes),
                pipe=True,
                executable=FFMPEG_BIN,
                before_options='-nostdin',   # avoid ffmpeg reading from stdin
            )
            vc.play(source, after=_after)

            try:
                await asyncio.wait_for(done.wait(), timeout=PLAY_TIMEOUT)
            except asyncio.TimeoutError:
                # Playback wedged: force stop and rebuild the voice connection
                try:
                    vc.stop()
                except Exception:
                    pass
                await self._recycle_voice_connection(reason="playback timeout")
        except discord.ClientException as e:
            # e.g. "Not connected." race
            print(f"[narrate] play race: {e}")
            await self._recycle_voice_connection(reason="client exception")
        except Exception as e:
            print(f"[narrate] _play_one error: {e}")
        finally:
            if source:
                try:
                    source.cleanup()
                except Exception:
                    pass
                    
    async def _recycle_voice_connection(self, reason: str):
        print(f"[narrate] recycling voice connection: {reason}")
        vc = self.voice_client
        try:
            if vc and vc.is_connected():
                try:
                    vc.stop()
                except Exception:
                    pass
                await vc.disconnect(force=True)
        except Exception:
            pass
        finally:
            self.voice_client = None

    async def _idle_loop(self):
        try:
            while True:
                await asyncio.sleep(5)
                now = time.time()
                if (now - self.last_activity) <= PLAYBACK_IDLE_DISCONNECT_SECS:
                    # If not connected, nothing to do—exit quietly
                    if not self.voice_client or not self.voice_client.is_connected():
                        return
                    continue
                await self.teardown()
                return
        except asyncio.CancelledError:
            raise

    async def teardown(self):
        vc = self.voice_client
        try:
            try:
                if vc and vc.is_connected() and vc.is_playing():
                    vc.stop()
            except Exception:
                pass
            if vc and vc.is_connected():
                await vc.disconnect(force=True)
        finally:
            self.voice_client = None

            if self.player_task and not self.player_task.done():
                self.player_task.cancel()
            self.player_task = None

            if self.idle_task and not self.idle_task.done():
                self.idle_task.cancel()
            self.idle_task = None

            try:
                while True:
                    self.queue.get_nowait()
                    self.queue.task_done()
            except asyncio.QueueEmpty:
                pass

# ==========================
# Cog
# ==========================
class NarrationCog(DbMixin, commands.Cog, name="Narrate"):
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
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.tts = GoogleTTSProvider(google_narrate_key)
        self.guild_sessions: Dict[int, GuildVoiceSession] = {}
        # guild_id, user_id, text_to_narrate, voice, language_code, rate, channel
        self._narrate_queue: asyncio.Queue[tuple[int, int, str, str, str, float, int]] = asyncio.Queue()
        self._guild_locks: Dict[int, asyncio.Lock] = {}
        self._workers: list[asyncio.Task] = [
            asyncio.create_task(self._narrate_worker(), name=f"narrate:{i}")
            for i in range(NARRATE_WORKERS)
        ]
    def cog_unload(self):
        # Cancel background narrate workers immediately.
        for t in getattr(self, "_workers", []):
            try:
                if not t.done():
                    t.cancel()
            except Exception:
                pass

        async def _cleanup():
            # Wait for workers to finish cancelling.
            try:
                await asyncio.gather(*[t for t in getattr(self, "_workers", [])], return_exceptions=True)
            except Exception:
                pass
            self._workers = []
            try:
                await asyncio.gather(
                    *[sess.teardown() for sess in list(self.guild_sessions.values())],
                    return_exceptions=True,
                )
            finally:
                self.guild_sessions.clear()
            q = getattr(self, "_narrate_queue", None)
            if q is not None:
                try:
                    while True:
                        q.get_nowait()
                        q.task_done()
                except asyncio.QueueEmpty:
                    pass
            try:
                await self.tts.close()
            except Exception:
                pass
                
        try:
            self.bot.loop.create_task(_cleanup())
        except Exception:
            asyncio.create_task(_cleanup())

    def _guild_lock(self, guild_id: int) -> asyncio.Lock:
        lock = self._guild_locks.get(guild_id)
        if lock is None:
            lock = self._guild_locks[guild_id] = asyncio.Lock()
        return lock

    def _get_session(self, guild_id: int) -> GuildVoiceSession:
        sess = self.guild_sessions.get(guild_id)
        if not sess:
            sess = GuildVoiceSession(self.bot, guild_id, self.tts)
            self.guild_sessions[guild_id] = sess
        return sess

    # ---------- DB Helpers ----------
    async def _get_pref(self, guild_id: int, user_id: int) -> Optional[dict]:
        row = await self.db.fetchrow("""
            SELECT guild_id, user_id, text_channel_id, voice, rate, enabled
            FROM narrate_prefs
            WHERE guild_id=$1 AND user_id=$2""",
            guild_id, user_id
        )
        return dict(row) if row else None

    async def _upsert_pref(self, ctx, text_channel_id, voice, rate, enabled):
        guild_id = await get_guild_id(ctx, self.db)
        user_id  = await get_user_id(ctx, self.db)
        if guild_id is None or user_id is None:
            return  # helpers already messaged on DB error
            
        await self.db.execute("""
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
        
    async def _set_enabled(self, guild_id: int, user_id: int, enabled: bool) -> None:
        await self.db.execute("""
            UPDATE narrate_prefs
            SET enabled=$3, updated_at=CURRENT_TIMESTAMP
            WHERE guild_id=$1 AND user_id=$2""",
            guild_id, user_id, enabled
        )
    async def _set_all_prefs_disabled(self, guild_id: int) -> None:
        await self.db.execute("""
            UPDATE narrate_prefs
            SET enabled=FALSE, updated_at=CURRENT_TIMESTAMP
            WHERE guild_id=$1""",
        guild_id
        )
        
    async def _set_channel_pref(
        self,
        ctx: commands.Context,
        channel: Union[discord.TextChannel, discord.Thread],
        *,
        enable: Optional[bool] = None,  # None = preserve existing enabled; True/False = force
    ) -> bool:
        # Only allow text channels or threads
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            await ctx.send("Please choose a **text channel or thread**.", suppress_embeds=True)
            return False

        # Permission checks (threads have a distinct flag)
        perms = channel.permissions_for(ctx.guild.me)
        can_send = getattr(perms, "send_messages_in_threads", None) if isinstance(channel, discord.Thread) else perms.send_messages
        if not (perms.view_channel and perms.read_message_history and (can_send or perms.send_messages)):
            what = "thread" if isinstance(channel, discord.Thread) else "channel"
            await ctx.send(f"I need **view / send / read-history** permissions in that {what}: {channel.mention}", suppress_embeds=True)
            return False

        # Pull existing prefs to keep voice/rate (and enabled if enable=None)
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        voice   = (pref.get("voice") if pref else None) or DEFAULT_VOICE
        rate    = float((pref.get("rate") if pref else None) or DEFAULT_RATE)
        enabled = (bool(pref.get("enabled")) if pref else False) if (enable is None) else bool(enable)

        await self._upsert_pref(ctx, channel.id, voice, rate, enabled)
        return True
        
    async def _any_enabled_in_channel(self, guild: discord.Guild, channel: discord.VoiceChannel) -> bool:
        member_ids = [m.id for m in channel.members if not m.bot]
        if not member_ids:
            return False
        row = await self.db.fetchrow(
            """
            SELECT 1
            FROM narrate_prefs
            WHERE guild_id=$1 AND enabled=TRUE AND user_id = ANY($2::bigint[])
            LIMIT 1
            """,
            guild.id, member_ids
        )
        return row is not None

    async def _disable_enabled_users_not_in_channel(self, guild: discord.Guild, channel: discord.VoiceChannel) -> None:
        # Disable everyone with enabled=TRUE who is not currently in `channel`
        member_ids = {m.id for m in channel.members if not m.bot}
        if not member_ids:
            # nobody in channel → disable all
            await self._set_all_prefs_disabled(guild.id)
            return
        # Bulk UPDATE except the ones inside this channel
        await self.db.execute(
            """
            UPDATE narrate_prefs
            SET enabled=FALSE, updated_at=CURRENT_TIMESTAMP
            WHERE guild_id=$1 AND enabled=TRUE AND NOT (user_id = ANY($2::bigint[]))
            """,
            guild.id, list(member_ids)
        )

    # ---------- Channel monitoring helpers ---------
    async def _enabled_user_ids(self, guild_id: int) -> List[int]:
        rows = await self.db.fetch(
            """SELECT user_id FROM narrate_prefs
               WHERE guild_id=$1 AND enabled=TRUE""",
            guild_id
        )
        return [r["user_id"] for r in rows]
    
    async def _any_enabled_in_channel(self, guild_id: int, channel: discord.VoiceChannel) -> bool:
        member_ids = [m.id for m in channel.members if not m.bot]
        if not member_ids:
            return False
        row = await self.db.fetchrow("""
            SELECT 1
            FROM narrate_prefs
            WHERE guild_id=$1 AND enabled=TRUE AND user_id = ANY($2::bigint[])
            LIMIT 1""",
            guild_id, member_ids
        )
        return row is not None

    async def _disconnect_if_no_enabled_in_channel(self, guild_id: int, channel: discord.VoiceChannel, session: "GuildVoiceSession") -> None:
        """Disconnect from voice if nobody in channel has it enabled.
        Others outside the channel may have it enabled,
        but it would likely be jarring to jump to their channel,
        so simply disable prefs for all and disconnect"""
        
        has_enabled = await self._any_enabled_in_channel(guild_id, channel)
        vc = session.voice_client

        if not has_enabled:
            await session.teardown()
            await self._set_all_prefs_disabled(guild_id)


    # ---------- Commands ----------
    @commands.group(name="narrate", invoke_without_command=True)
    async def narrate_root(self, ctx: commands.Context):
        usage = (
            "Usage:\n"
            "  !narrate on [#text-channel]\n"
            "  !narrate off\n"
            "  !narrate status\n"
            "  !narrate cancel   (alias: !narrate x)\n"
            "  !narrate channel [#text-channel|thread]\n"
            "  !narrate voice <voice-name|short-name>\n"
            "  !narrate voices [language] [gender]\n"
            "  !narrate rate <float>\n"
            "  !narrate shutoff"
        )
        await ctx.send(usage, suppress_embeds=True)

    @narrate_root.command(name="on", aliases=["start"])
    async def narrate_on(self, ctx: commands.Context):
        ch = ctx.channel  # could be a TextChannel or a Thread
        if not await self._set_channel_pref(ctx, ch, enable=True):
            return
        await self.tts.start()
        session = self._get_session(ctx.guild.id)
        if ctx.author.voice and ctx.author.voice.channel:
            await session.ensure_connected(ctx.author.voice.channel)
        await ctx.send(f"Narration **enabled** for you in {ch.mention}.", suppress_embeds=True)

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
            await self._disconnect_if_no_enabled_in_channel(ctx.guild.id, vc.channel, session)

    @narrate_root.command(name="cancel", aliases=["x"])
    async def narrate_cancel(self, ctx: commands.Context):
        session = self._get_session(ctx.guild.id)
        await session.cancel_playback()
        await ctx.send("⏹️ Stopped current narration.")

    @narrate_root.command(name="voices")
    async def narrate_voices(self, ctx: commands.Context, language: Optional[str] = None, gender: Optional[str] = None):
        url = "https://cloud.google.com/text-to-speech/docs/voices"

        # No args: show link + languages (comma-separated)
        if not language:
            rows = await self.db.fetch(
                "SELECT DISTINCT language FROM google_tts_voices ORDER BY language"
            )
            langs = ", ".join(r["language"] for r in rows) if rows else "(no data)"
            return await ctx.send(
                f"This command helps you pick a voice.\n"
                f"Once you pick a voice, you can set it with `!narrate voice voice-name`\n"
                f"You can either get a voice-name by copying a full voice name from this website:\n {url}\n Or you can get a list of available voices by calling\n"
                f"`!narrate voices language male/female`\n Valid language options: {langs}", suppress_embeds=True)

        # With filters
        lang_in = _collapse_ws(language)
        gender_in = _norm_gender(gender)
        params = [f"%{lang_in}%"]

        sql = """
          SELECT nickname, language, gender
          FROM google_tts_voices
          WHERE language ILIKE $1
        """
        if gender_in:
            sql += " AND gender = $2"
            params.append(gender_in)

        sql += " ORDER BY nickname"

        rows = await self.db.fetch(sql, *params)
        if not rows:
            return await ctx.send("No voices matched that language/gender.", suppress_embeds=True)

        # Nicknames only, comma-separated; chunk if near Discord limit
        names = [r["nickname"] for r in rows if r["nickname"]]
        if not names:
            return await ctx.send("No nicknames found for that filter.", suppress_embeds=True)
            
        out = f"{rows[0]["gender"]+" - " if gender_in else ""}{rows[0]["language"]} Names:\n"
        out += ", ".join(names)
        if len(out) <= 1800:
            return await ctx.send(out, suppress_embeds=True)

        # Chunk safely
        buf, acc = [], 0
        for n in names:
            seg = (", " if buf else "") + n
            if acc + len(seg) > 1800:
                await ctx.send("".join(buf), suppress_embeds=True)
                buf, acc = [n], len(n)
            else:
                buf.append(seg if buf else n)
                acc += len(seg)
        if buf:
            await ctx.send("".join(buf), suppress_embeds=True)

    @narrate_root.command(name="status")
    async def narrate_status(self, ctx: commands.Context):
        rows = await self.db.fetch(
            """
            SELECT user_id, voice, rate
            FROM narrate_prefs
            WHERE guild_id=$1 AND enabled=TRUE
            ORDER BY user_id
            """,
            ctx.guild.id,
        )
        bot_enabled = bool(rows)

        session = self._get_session(ctx.guild.id)
        vc = session.voice_client
        ch_disp = ""
        if vc and vc.is_connected() and vc.channel:
            ch = vc.channel
            ch_disp = f" ({getattr(ch, 'mention', ch.name)})"

        lines = [f"STATUS: {'Enabled' if bot_enabled else 'Disabled'}{ch_disp}"]
        if bot_enabled:
            lines.append("ACTIVE USERS:")
            for r in rows:
                uid = r["user_id"]
                member = ctx.guild.get_member(uid)
                username = (member.display_name if member else f"<@{uid}>")
                v = r["voice"] or DEFAULT_VOICE
                rate_val = r["rate"]
                rate_str = f"{DEFAULT_RATE if rate_val is None else rate_val}"
                lines.append(f"- {username} | {v} | {rate_str}")

        await ctx.send("\n".join(lines), suppress_embeds=True)

    @narrate_root.command(name="channel")
    async def narrate_channel(self, ctx: commands.Context, channel: Optional[Union[discord.TextChannel, discord.Thread]] = None):
        ch = channel or ctx.channel  # allow calling it inside a thread to select that thread
        if not await self._set_channel_pref(ctx, ch, enable=None):
            return
        await ctx.send(f"Narration channel set to {ch.mention}.", suppress_embeds=True)

    @narrate_root.command(name="voice")
    async def narrate_voice(self, ctx: commands.Context, *, voice: str):
        q = _collapse_ws(voice)

        # 1) Try exact (case-insensitive) full voice_name
        row = await self.db.fetchrow(
            """
            SELECT language, voice_name, gender, nickname
            FROM google_tts_voices
            WHERE voice_name ILIKE $1
            LIMIT 1
            """,
            q,
        )

        # 2) If not found, try exact (case-insensitive) nickname
        if not row:
            row = await self.db.fetchrow(
                """
                SELECT language, voice_name, gender, nickname
                FROM google_tts_voices
                WHERE nickname ILIKE $1
                LIMIT 1
                """,
                q,
            )

        if not row:
            return await ctx.send(
                "Unknown voice. Try `!narrate voices [language] [gender]` or use the full list:\n"
                "https://cloud.google.com/text-to-speech/docs/voices",
                suppress_embeds=True,
            )

        language   = row["language"]
        voice_name = row["voice_name"]       # full canonical
        nickname   = row["nickname"] or ""   # should exist; safe-guard

        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        channel_id = (pref.get("text_channel_id") if pref else None) or ctx.channel.id
        rate       = float((pref.get("rate") if pref else None) or DEFAULT_RATE)
        enabled    = bool(pref.get("enabled")) if pref else False

        # Persist the full name in prefs (what TTS needs)
        await self._upsert_pref(ctx, channel_id, voice_name, rate, enabled)

        # Friendly confirmation shows both full name and nickname
        await ctx.send(
            f"Voice set to `{voice_name}` (`{nickname}`) - {language}.",
            suppress_embeds=True,
        )

    @narrate_root.command(name="rate")
    async def narrate_rate(self, ctx: commands.Context, *, rate: float):
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)

        channel_id = (pref.get("text_channel_id") if pref else None) or ctx.channel.id
        voice      = (pref.get("voice") if pref else None) or DEFAULT_VOICE
        enabled    = bool(pref.get("enabled")) if pref else False
        rate_val   = float(rate)

        await self._upsert_pref(ctx, channel_id, voice, rate_val, enabled)
        await ctx.send(f"Speaking rate set to `{rate_val}`.", suppress_embeds=True)
            
    @narrate_root.command(name="shutoff")
    @commands.has_permissions(manage_guild=True)
    async def narrate_shutoff(self, ctx: commands.Context):
        await self._set_all_prefs_disabled(ctx.guild.id)
        session = self._get_session(ctx.guild.id)
        await session.teardown()
        await ctx.send("Narrate has been shut off.", suppress_embeds=True)

    # ---------- Events ----------
    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        """what to do when a user joins/leaves a voice channel"""
        guild = member.guild
        if not guild:
            return
            
        # --- Handle the BOT being disconnected/kicked from VC ---
        if member.id == guild.me.id:
            bot_was_in_vc = before.channel is not None
            bot_now_out   = after.channel is None
            if bot_was_in_vc and bot_now_out:
                try:
                    await self._set_all_prefs_disabled(guild.id)
                except Exception as e:
                    print(f"[narrate] failed to disable all on bot kick: {e}")
                try:
                    session = self._get_session(guild.id)
                    await session.teardown()
                except Exception as e:
                    print(f"[narrate] teardown error on bot kick: {e}")
            return
                
        # --- Handle user voice channel movement---
        if member.bot or not member.guild:
            return

        session = self._get_session(guild.id)
        bot_vc = session.voice_client
        bot_chan = bot_vc.channel if (bot_vc and bot_vc.is_connected()) else None

        prev_chan = before.channel   # None if previously not in VC
        new_chan  = after.channel    # None if now not in VC

        user_leaving_bot_channel = (
            bot_chan is not None
            and prev_chan is not None
            and prev_chan.id == bot_chan.id
            and (new_chan is None or new_chan.id != bot_chan.id)
        )
        user_joining_voice = (prev_chan is None and new_chan is not None)

        if user_leaving_bot_channel:
            try:
                await self._set_enabled(guild.id, member.id, False)
            except Exception as e:
                print(f"[narrate] failed to disable user {member.id} on leave: {e}")

            # If nobody remaining in the bot VC has narrate enabled → shut down
            try:
                still_has_enabled = await self._any_enabled_in_channel(guild, bot_chan) if bot_chan else False
            except Exception as e:
                print(f"[narrate] enabled check error: {e}")
                still_has_enabled = True  # be conservative

            if not still_has_enabled:
                # Do the underlying actions directly (don’t call the command since we have no ctx)
                await self._set_all_prefs_disabled(guild.id)
                await session.teardown()
            return

        # --- Case 2: user joins a voice channel from nothing
        if user_joining_voice:
            pref = await self._get_pref(guild.id, member.id)
            if pref and pref.get("enabled") and new_chan:
                # Move/ensure the bot in that VC
                try:
                    await session.ensure_connected(new_chan)
                except Exception as e:
                    print(f"[narrate] ensure_connected error (join): {e}")
                    return
                # Disable everyone NOT in this VC
                # this will reduce likelihood of stale states.
                # you're only enabled if you're in the bot's channel.
                try:
                    await self._disable_enabled_users_not_in_channel(guild, new_chan)
                except Exception as e:
                    print(f"[narrate] bulk disable error: {e}")
            return  # If user doesn't have narrate enabled, do nothing

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Narrate message into voice chat if appropriate. Bot commands are ignored here."""
        if message.author.bot or not message.guild:
            return
        if _message_is_narrate_command(self.bot, message.content):
            return
        if _is_link_emoji_or_mention_only(message.content or ""):
            return
        pref = await self._get_pref(message.guild.id, message.author.id)
        if not pref or not pref.get("enabled"):
            return
        if message.channel.id != pref.get("text_channel_id"):
            return
        if not (message.author.voice and message.author.voice.channel):
            return

        cleaned = _clean_content(message.content or "")
        if not cleaned:
            return

        voice = (pref.get("voice") or DEFAULT_VOICE)
        language_code = "-".join(voice.split("-")[0:2]) if "-" in voice else DEFAULT_LANG
        try:
            rate = float(pref.get("rate") if pref.get("rate") is not None else DEFAULT_RATE)
        except Exception:
            rate = DEFAULT_RATE
        await self._narrate_queue.put((message.guild.id, message.author.id, cleaned, voice, language_code, rate, message.channel.id))

    async def _narrate_worker(self):
        try:
            while True:
                guild_id, user_id, text, voice, language_code, rate, channel_id = await self._narrate_queue.get()
                try:
                    if not text:
                        continue

                    guild = self.bot.get_guild(guild_id)
                    if not guild:
                        continue
                    member = guild.get_member(user_id)
                    if not member or not (member.voice and member.voice.channel):
                        continue

                    async with self._guild_lock(guild_id):
                        session = self._get_session(guild_id)
                        try:
                            await session.ensure_connected(member.voice.channel)
                        except Exception as e:
                            print(f"[narrate] ensure_connected error: {e}")
                            continue

                        chunks = chunk_text(text, MAX_CHARS_PER_CHUNK)
                        if not chunks:
                            continue

                        async def synth_chunk(t: str) -> bytes:
                            return await self.tts.synth(
                                t, voice_name=voice, language_code=language_code, speaking_rate=rate
                            )

                        try:
                            for part in chunks:
                                data = await synth_chunk(part)
                                await session.enqueue(data)
                        except Exception as e:
                            ch = guild.get_channel(channel_id)
                            if ch:
                                try:
                                    await ch.send(
                                        "TTS failed. Ensure you used the exact **Name** from Google’s voice list "
                                        "(e.g., `en-US-Wavenet-D` or `en-US-Chirp3-HD-Gacrux`).\n"
                                        "See: https://cloud.google.com/text-to-speech/docs/voices",
                                        suppress_embeds=True,
                                    )
                                except Exception:
                                    pass
                            print(f"[narrate] synth error: {e}")
                            continue
                finally:
                    self._narrate_queue.task_done()
        except asyncio.CancelledError:
            return
