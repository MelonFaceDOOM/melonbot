import asyncio
import asyncpg
from pathlib import Path
import time
import io
import math
import traceback
import subprocess
from datetime import date, timedelta
from typing import Awaitable, Callable, Optional, Dict, Tuple, List, Union
import discord
from discord.ext import commands
from config import (
    google_narrate_key, fish_api_key, gemini_api_key, FISH_DEFAULT_VOICE_ID, FISH_MODEL,
    GEMINI_MODEL, GEMINI_FALLBACK_MODELS, GEMINI_RPM, TTS_MONTHLY_BUDGET_USD,
)
from bot_helpers import get_user_id, get_guild_id, paginate, parse_user_input_for_mention, send_goodly
from db_mixin import DbMixin
# LRUCache / GoogleTTSProvider / DEFAULT_LANG lived here before tts_providers; still importable from bot_narrate.
from tts_providers import (
    BudgetTracker, FishTTSProvider, GeminiTTSProvider, GoogleTTSProvider, LRUCache, TTSRouter,
    DEFAULT_GOOGLE_VOICE, DEFAULT_LANG, DEFAULT_RATE, FISH_REF_PREFIX, FISH_VOICE_DESIGN_USD, ProviderError,
    fish_cost,
    GEMINI_MODEL_ALIASES, GEMINI_MODEL_LABELS, audio_format, extract_fish_id, parse_voice_ref,
    resolve_gemini_model, split_gemini_voice,
)
from tts_text import chunk_for_speech, split_leading_style
import re
import uuid
import os
import tempfile

# ==========================
# Tuning knobs & feature flags
# ==========================
GUILD_BACKLOG = 20                         # queued messages per guild; more are dropped
# Limits only exist to catch abuse; normal chat shouldn't come near any of them.
MAX_NARRATE_CHARS = 1500                   # longer messages are cut off (~1m50s of speech)
USER_BURST_CHARS = 4000                    # per-user token bucket: burst size...
USER_REFILL_CHARS_PER_SEC = 2000 / 60      # ...and refill: 2000 chars/min, ~2.4x faster than speech plays
USER_BUDGET_SHARE = 0.5                    # one user may spend at most this share of the monthly budget
SELF_DISCONNECT_GRACE_SECS = 15            # voice-state events this soon after our own disconnect aren't kicks
ALONE_DISCONNECT_SECS = 10                 # safety net (checked every 5s): leave if no humans have been in the bot's channel this long
RATE_MIN, RATE_MAX = 0.25, 4.0
PLAYBACK_IDLE_DISCONNECT_SECS = 3600       # leave VC when idle
# Voice ref for users who haven't picked one; existing Google picks are left alone.
DEFAULT_VOICE = f"fish:{FISH_DEFAULT_VOICE_ID.lower()}" if (FISH_DEFAULT_VOICE_ID and fish_api_key) else DEFAULT_GOOGLE_VOICE
FFMPEG_BIN = "ffmpeg"
PLAY_TIMEOUT = 120                          # max seconds per clip
DESIGN_COOLDOWN_SECS = 60                   # per user, for !narrate design
# Also the transcript Fish clones from for locally kept designs, so use most of the 150-char limit
DESIGN_PREVIEW_LINE = ("Hey everyone, it's me. I have some news, and honestly, you are not going to believe it. "
                       "Pull up a chair, because this one is a long story.")
DESIGN_VIEW_TIMEOUT = 600
# narrate subcommands that also work in a DM with the bot (designing a voice privately)
DM_COMMANDS = frozenset({"narrate", "design", "designs", "clone"})

# Voice cloning: the user reads this (consent sentence first) in a voice message sent to the bot by DM
CLONE_CONSENT = "This is my own voice, and I agree to melonbot letting people use it to read messages."
CLONE_SCRIPT = (CLONE_CONSENT + " The quick brown fox jumps over the lazy dog, then naps in the warm afternoon sun. "
                "I'd like a large coffee, two sugars, and maybe a blueberry muffin if they have any left. "
                "Honestly, the best part of any trip is the snacks, and nobody can convince me otherwise.")
CLONE_MIN_SECS = 10
CLONE_MAX_SECS = 180
CLONE_MAX_BYTES = 15 * 1024 * 1024
CLONE_WAIT_SECS = 600            # how long the bot waits for the recording
CLONE_COOLDOWN_SECS = 60
CLONE_MAX_ATTEMPTS = 8           # recordings per clone session
CLONE_PREVIEW_LINE = "Hey everyone, it's me. This is what I sound like now, so let me know what you think."
WHISPER_MODEL = "base.en"

FISH_LIBRARY_URL = "https://fish.audio/voice-library/"
GEMINI_VOICES_URL = "https://cloud.google.com/text-to-speech/docs/gemini-tts"
GOOGLE_VOICES_URL = "https://cloud.google.com/text-to-speech/docs/voices"
VOICES_EMBED_COLOR = 0x77C66E  # melon green

# fallback_reason -> (notice, cooldown seconds; None = once per month)
FALLBACK_NOTICES = {
    "budget": ("This month's TTS budget is used up, so narration is using the free Google voice until next month.", None),
    "balance": ("The Fish Audio credit has run out, so narration is using the free Google voice until it's topped up.", None),
    "not_configured": ("That voice's provider isn't set up on this bot, so narration is using the Google voice.", None),
    "rate_limited": ("The voice service is rate limiting right now, so some messages used the Google voice.", 3600),
    "gemini_limit": ("Gemini voices have hit their usage limit for now, so they're using the default Fish voice "
                     "(or Google) until it resets.", 6 * 3600),
    "error": ("The selected voice failed, so that message used the Google voice instead.", 3600),
    "user_quota": ("Someone here has used their share of this month's premium-voice budget, so their messages use the Google voice until next month.", 3600),
}


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

def _message_is_bot_command(bot: commands.Bot, content: str) -> bool:
    """True if the message invokes one of this bot's commands (!narrate, !add, ...); those aren't narrated.
    '!wow' or '! lol' aren't commands, so they're still read out."""
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
        if p and content.startswith(p):
            words = content[len(p):].split(maxsplit=1)
            # all_commands is case-insensitive here (bot is built with case_insensitive=True)
            return bool(words) and bot.get_command(words[0]) is not None
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

def _strip_parenthetical_for_tts(s: str) -> str:
    """Remove (...) segments (including nested) before TTS; collapse whitespace."""
    s = s.strip()
    while True:
        new_s = re.sub(r"\([^()]*\)", "", s)
        if new_s == s:
            break
        s = new_s
    s = re.sub(r"\s+([.,;:!?])", r"\1", s)
    return _collapse_ws(s).strip()

def _short_voice_name(voice_name: str) -> str:
    # Strip the leading "<lang>-<REGION>-" prefix, keep the rest.
    # Examples:
    #   en-US-Wavenet-D            -> Wavenet-D
    #   ar-XA-Chirp3-HD-Achernar   -> Chirp3-HD-Achernar
    parts = voice_name.split("-", 2)
    return parts[2] if len(parts) >= 3 else voice_name

# (heading, [(command, what it does)]) for `!narrate` with no subcommand; commands get the prefix + "narrate "
HELP_SECTIONS = [
    ("▶️ Getting started", [
        ("on [#channel]", "read your messages from this channel (or #channel) into your voice channel"),
        ("off", "stop reading your messages (leaving voice does this too)"),
        ("cancel", "stop what's playing right now (also `x`)"),
        ("channel [#channel|thread]", "change which channel gets read"),
        ("status", "who has narration on, and with which voice"),
    ]),
    ("🎙️ Voices", [
        ("voices", "where to find Fish, Gemini and Google voices"),
        ("voice <link | ID | gemini:Name | google name>", "pick your voice. Add `@2.5` etc. to a Gemini voice to prefer a model"),
        ("recent [n,p] [@someone]", "voices someone has used, newest first, with commands to switch back"),
        ("rate <0.25–4>", "speaking speed (1 is normal)"),
    ]),
    ("🎭 Delivery (Fish & Gemini voices)", [
        ("style <text | off>", "default delivery for your messages, e.g. `dry and sarcastic`"),
    ]),
    ("🎨 Make your own", [
        ("design <description>", "invent a voice. I DM you 3 takes to pick from, privately"),
        ("designs", "your designed voices, usable in any server"),
        ("clone", "turn a recording of YOUR voice into a named, public voice (by DM, with a preview first). "
                  "`voice clone <name>` uses one, `clone delete <name>` removes it"),
    ]),
    ("💾 Voicelines", [
        ("save <name>", "save your last narrated message; typing `<name>` later plays it back"),
        ("list", "this server's saved voicelines"),
        ("rename <old> <new>", "rename a voiceline (whoever saved it, or a server manager)"),
        ("delete <name>", "delete a voiceline (same rule)"),
    ]),
    ("📊 Spending", [
        ("quota", "your and this server's premium-voice spend this month"),
        ("usage", "spend per provider this month (server managers)"),
    ]),
    ("🛑 Server managers", [
        ("shutoff", "turn narration off for everyone and leave voice"),
    ]),
]
HELP_DELIVERY_TIPS = ("Start a message with `[whispering]`, `[excited]`, `[very sad]`… to set how it's read.\n"
                      "Drop `[laugh]` `[sigh]` `[gasp]` `[pause]` anywhere in it.")


def _consent_heard(transcript: str) -> bool:
    """Did the recording include the consent sentence? Whisper mangles the made-up name ('Malambut'), so the
    name can be a close match or be backed by the rest of the sentence."""
    import difflib
    t = " ".join(re.sub(r"[^a-z0-9 ]+", " ", (transcript or "").lower()).split())
    if "my own voice" not in t or "agree" not in t:
        return False
    words = t.split()
    names = words + [a + b for a, b in zip(words, words[1:])]  # 'melon bot' -> 'melonbot'
    name_heard = any(difflib.SequenceMatcher(None, w, "melonbot").ratio() >= 0.7 for w in names)
    return name_heard or "read messages" in t or "read my messages" in t


def _decode_16k_mono(audio: bytes):
    """Any audio -> float32 16 kHz mono samples (what Whisper wants), via ffmpeg."""
    import numpy as np
    pcm = _ffmpeg_convert(audio, ["-f", "f32le", "-ac", "1", "-ar", "16000"])
    return np.frombuffer(pcm, dtype=np.float32)


def _help_embed(prefix: str) -> discord.Embed:
    p = f"{prefix}narrate"
    embed = discord.Embed(
        title="📖 Narrate",
        description="Reads your messages aloud in voice chat, in the voice you pick.",
        color=VOICES_EMBED_COLOR,
    )
    for heading, cmds in HELP_SECTIONS:
        value = "\n".join(f"`{p} {cmd}` · {what}" for cmd, what in cmds)
        if heading.startswith("🎭"):
            value += "\n" + HELP_DELIVERY_TIPS
        embed.add_field(name=heading, value=value, inline=False)
    embed.set_footer(text=f"New here? Try {p} voices, then {p} on.")
    return embed

def _gemini_model_choices() -> str:
    """'`3.8` `3.8-lite` `3.1` `2.5` `2.5-pro`': one short name per model, in chain order."""
    seen, names = set(), []
    for alias, model in GEMINI_MODEL_ALIASES.items():
        if model not in seen:
            seen.add(model)
            names.append(f"`{alias}`")
    return " ".join(names)

def _usd_bar(spent: float, cap: float, width: int = 12) -> str:
    """Text progress bar like '███░░░░░░░░░ 24%'."""
    frac = 0.0 if cap <= 0 else min(1.0, max(0.0, spent / cap))
    filled = round(frac * width)
    return f"`{'█' * filled}{'░' * (width - filled)}` {frac:.0%}"

def _voice_display(voice_ref: str, label: Optional[str]) -> str:
    """Human-readable voice for status lines: 'Label (Fish)' / 'Label (Gemini)' / Google name."""
    provider, _ = parse_voice_ref(voice_ref)
    if provider == "google":
        return f"{voice_ref} ({label})" if label else voice_ref
    if label and "Gemini" in label:  # 'Kore · Gemini 2.5 Flash' already says it
        return label
    return f"{label or voice_ref} ({'Fish' if provider == 'fish' else 'Gemini'})"


### VOICELINE SAVE/LOAD HELPERS ###

def _normalize_voiceline(text: str) -> str:
    text = text.strip()
    text = " ".join(text.split())
    text = text.casefold()
    return text

def _atomic_write_bytes(dest_path: Path, data: bytes) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=str(dest_path.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, dest_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

def _join_to_ogg_opus(clips: List[bytes]) -> bytes:
    """Concatenate clips (any mix of OGG/WAV, sample rates) into one OGG/Opus file."""
    with tempfile.TemporaryDirectory() as tmp:
        args = [FFMPEG_BIN, "-nostdin", "-loglevel", "error"]
        for i, clip in enumerate(clips):
            path = os.path.join(tmp, f"{i}.bin")
            with open(path, "wb") as f:
                f.write(clip)
            args += ["-i", path]
        n = len(clips)
        graph = "".join(f"[{i}:a]aresample=48000,aformat=channel_layouts=mono[a{i}];" for i in range(n))
        graph += "".join(f"[a{i}]" for i in range(n)) + f"concat=n={n}:v=0:a=1[out]"
        args += ["-filter_complex", graph, "-map", "[out]", "-c:a", "libopus", "-b:a", "48k", "-f", "ogg", "pipe:1"]
        return subprocess.run(args, capture_output=True, check=True).stdout


def _ffmpeg_convert(audio: bytes, out_args: List[str]) -> bytes:
    """Run ffmpeg on audio bytes and return its stdout. The input goes through a temp file, not a pipe:
    .m4a/.mp4 (e.g. Windows Sound Recorder) keep their index at the end and can't be decoded from a pipe."""
    fd, path = tempfile.mkstemp(prefix="narrate_in_")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(audio)
        return subprocess.run([FFMPEG_BIN, "-nostdin", "-loglevel", "error", "-i", path, *out_args, "pipe:1"],
                              capture_output=True, check=True).stdout
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def _transcode_to_ogg_opus(audio: bytes) -> bytes:
    """Gemini WAV, uploaded recordings, ... -> OGG/Opus, which voicelines and clone samples are stored as."""
    return _ffmpeg_convert(audio, ["-c:a", "libopus", "-b:a", "48k", "-f", "ogg"])


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
        self.last_activity = time.monotonic()
        # Bumped by cancel/teardown; queued work stamped with an older value is dropped.
        self.generation = 0
        # When we last disconnected on purpose (recycle), so the voice-state echo isn't treated as a kick.
        self.self_disconnect_at = 0.0
        # Set by the cog: called when the bot has been alone in its channel too long (turns narration off + leaves)
        self.on_alone: Optional[Callable[[], Awaitable[None]]] = None
        self.alone_since: Optional[float] = None

    def _alone_too_long(self, vc, now: float) -> bool:
        members = getattr(getattr(vc, "channel", None), "members", None)
        if not isinstance(members, (list, tuple)):
            return False
        if any(not getattr(m, "bot", False) for m in members):
            self.alone_since = None
            return False
        if self.alone_since is None:
            self.alone_since = now
        return now - self.alone_since >= ALONE_DISCONNECT_SECS

    def recently_self_disconnected(self) -> bool:
        return time.monotonic() - self.self_disconnect_at < SELF_DISCONNECT_GRACE_SECS

    # ---------- enqueue ----------
    async def enqueue(self, audio_bytes: bytes):
        try:
            self.queue.put_nowait(audio_bytes)
        except asyncio.QueueFull:
            print(f"[narrate] playback queue full, dropping clip: guild={self.guild_id}")
            return
        self.last_activity = time.monotonic()

    # ---------- connection & lifecycle ----------
    async def ensure_connected(self, channel: discord.VoiceChannel) -> None:
        async with self.lock:
            # Sessions survive disconnects and can exist long before the first join.
            # Claiming a channel is activity, even if no narration follows it.
            self.last_activity = time.monotonic()
            self.alone_since = None
            if not (self.voice_client and self.voice_client.is_connected()):
                # discord.py may still hold a connection we lost track of (or is reconnecting it);
                # connect() would raise "Already connected", so adopt it instead.
                existing = getattr(channel.guild, "voice_client", None)
                if isinstance(existing, discord.VoiceClient):
                    self.voice_client = existing
            if self.voice_client and self.voice_client.is_connected():
                if self.voice_client.channel.id != channel.id:
                    await self.voice_client.move_to(channel)
            else:
                self.voice_client = await channel.connect(reconnect=True, self_deaf=True)
            # Give the connection a full idle interval after the handshake/move.
            self.last_activity = time.monotonic()
            self._ensure_player()
            self._ensure_idle_timer()
            
    async def cancel_playback(self):
        """Stop current audio, clear the queue, and drop narration already queued for synthesis."""
        self.generation += 1
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
        self.last_activity = time.monotonic()

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
                    self.last_activity = time.monotonic()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[narrate] player error: {e}")
                    self.last_activity = time.monotonic()
        except asyncio.CancelledError:
            return
            
    async def _play_one(self, audio_bytes: bytes):
        vc = self.voice_client
        if not vc or not vc.is_connected():
            return

        source = None
        done = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _after(_err):
            # discord.py calls this from its audio thread on natural end or on stop()
            loop.call_soon_threadsafe(done.set)

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
        self.self_disconnect_at = time.monotonic()
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
                now = time.monotonic()
                vc = self.voice_client
                connected = bool(vc and vc.is_connected())
                # Safety net for missed leave events: never sit alone in a channel
                if connected and self._alone_too_long(vc, now):
                    print(f"[narrate] alone in voice for {ALONE_DISCONNECT_SECS}s, leaving: guild={self.guild_id}")
                    if self.on_alone:
                        await self.on_alone()
                    else:
                        await self.teardown()
                    return
                if (now - self.last_activity) <= PLAYBACK_IDLE_DISCONNECT_SECS:
                    # If not connected, nothing to do—exit quietly
                    if not connected:
                        return
                    continue
                print(f"[narrate] idle disconnect: guild={self.guild_id}, idle={now - self.last_activity:.0f}s")
                await self.teardown()
                return
        except asyncio.CancelledError:
            raise

    async def teardown(self):
        # Same lock as ensure_connected, so a connect can't land in the middle of a teardown.
        async with self.lock:
            await self._teardown_locked()

    async def _teardown_locked(self):
        self.generation += 1
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

class _CloneRejected(Exception):
    """A recording that can't be used for cloning; the message is shown to the user."""


class _ClonePreviewView(discord.ui.View):
    """Keep / record again / cancel under a clone preview. `choice` is set once a button is pressed."""
    def __init__(self, owner_id: int):
        super().__init__(timeout=CLONE_WAIT_SECS)
        self.owner_id = owner_id
        self.choice: Optional[str] = None
        self.message: Optional[discord.Message] = None
        for label, choice, style in (("Keep it", "keep", discord.ButtonStyle.success),
                                     ("Record again", "redo", discord.ButtonStyle.primary),
                                     ("Cancel", "cancel", discord.ButtonStyle.secondary)):
            btn = discord.ui.Button(label=label, style=style)
            btn.callback = self._choose(choice)
            self.add_item(btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.owner_id

    def _choose(self, choice: str):
        async def callback(interaction: discord.Interaction):
            self.choice = choice
            for item in self.children:
                item.disabled = True
            await interaction.response.edit_message(view=self)
            self.stop()
        return callback

    async def on_timeout(self):
        if self.message:
            for item in self.children:
                item.disabled = True
            try:
                await self.message.edit(view=self)
            except Exception:
                pass


# ==========================
# Voice design picker
# ==========================
class _DesignPickView(discord.ui.View):
    """Buttons under a !narrate design result (sent by DM): pick a candidate (designer only) or cancel."""
    def __init__(self, cog: "NarrationCog", owner_id: int, origin_guild_id: Optional[int], prefix: str,
                 description: str, candidates: List[dict]):
        super().__init__(timeout=DESIGN_VIEW_TIMEOUT)
        self.cog = cog
        self.owner_id = owner_id
        self.origin_guild_id = origin_guild_id
        self.prefix = prefix
        self.description = description
        self.candidates = candidates
        self.message: Optional[discord.Message] = None
        for i in range(len(candidates)):
            btn = discord.ui.Button(label=f"Use {i + 1}", style=discord.ButtonStyle.primary)
            btn.callback = self._make_pick(i)
            self.add_item(btn)
        cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
        cancel.callback = self._cancel
        self.add_item(cancel)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the person who designed this voice can pick it.", ephemeral=True)
            return False
        return True

    def _disable_all(self):
        for item in self.children:
            item.disabled = True
        self.stop()

    def _make_pick(self, index: int):
        async def pick(interaction: discord.Interaction):
            self._disable_all()
            await interaction.response.edit_message(view=self)
            try:
                ref = await self.cog._save_designed_voice(self.owner_id, self.origin_guild_id,
                                                          self.description, self.candidates[index])
            except Exception as e:
                print(f"[narrate] saving designed voice failed: {e}")
                await interaction.followup.send("Couldn't save that voice; try designing again.", suppress_embeds=True)
                return
            await interaction.followup.send(
                f"Saved take {index + 1} of **{self.description[:100]}** to your voices. Nobody else has heard it.\n"
                f"When you're ready, use it in any server with `{self.prefix}narrate voice {ref}` "
                f"(`{self.prefix}narrate designs` lists all of yours).",
                suppress_embeds=True,
            )
        return pick

    async def _cancel(self, interaction: discord.Interaction):
        self._disable_all()
        await interaction.response.edit_message(content="Voice design cancelled.", view=self)

    async def on_timeout(self):
        if self.message:
            for item in self.children:
                item.disabled = True
            try:
                await self.message.edit(view=self)
            except Exception:
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
        self.fish = FishTTSProvider(fish_api_key, FISH_MODEL) if fish_api_key else None
        self.gemini = (GeminiTTSProvider(gemini_api_key, GEMINI_MODEL, GEMINI_RPM, GEMINI_FALLBACK_MODELS)
                       if gemini_api_key else None)
        self.budget = BudgetTracker(lambda: self.db, TTS_MONTHLY_BUDGET_USD)
        self.router = TTSRouter(self.tts, self.fish, self.gemini, self.budget,
                                reference_loader=self._load_designed_sample)
        # (guild_id, user_id, voice ref) -> (monotonic time, [(fallback ref, label)]) for Gemini users
        self._fallback_cache: Dict[Tuple[int, int, str], Tuple[float, List[Tuple[str, Optional[str]]]]] = {}
        self.guild_sessions: Dict[int, GuildVoiceSession] = {}
        # One queue + worker per guild, so a busy guild can't hold up the others.
        # Items: (user_id, text, voice ref, style, rate, channel_id, session generation)
        self._guild_queues: Dict[int, asyncio.Queue] = {}
        self._guild_workers: Dict[int, asyncio.Task] = {}
        # user_id -> (tokens, last refill time) for the per-user narration rate limit
        self._user_buckets: Dict[int, Tuple[float, float]] = {}
        # user_id -> (month, est. USD) of paid-voice spend; persisted in tts_user_usage
        self._user_spend: Dict[int, Tuple[date, float]] = {}
        self.guild_voiceline_caches: Dict[int, Dict[str, str]] = {}
        self.persistent_audio_root = "data/audio"
        # (guild_id, user_id) -> clips [(audio bytes, format)] of the most recent message, for !narrate save
        self._last_narrated_audio: Dict[Tuple[int, int], List[Tuple[bytes, str]]] = {}
        # (guild_id, reason) -> (month, monotonic time) of the last fallback notice
        self._fallback_notices: Dict[Tuple[int, str], Tuple[date, float]] = {}
        self._design_last_used: Dict[int, float] = {}
        self._clone_last_used: Dict[int, float] = {}
        self._whisper = None  # faster-whisper model, loaded on first clone

    async def cog_load(self):
        try:
            await self._load_keyword_cache()
        except Exception as e:
            print(f"[narrate] failed to load voiceline cache: {e}")

    def cog_unload(self):
        # Cancel background narrate workers immediately.
        workers = list(getattr(self, "_guild_workers", {}).values())
        for t in workers:
            try:
                if not t.done():
                    t.cancel()
            except Exception:
                pass

        async def _cleanup():
            # Wait for workers to finish cancelling.
            try:
                await asyncio.gather(*workers, return_exceptions=True)
            except Exception:
                pass
            self._guild_workers.clear()
            try:
                await asyncio.gather(
                    *[sess.teardown() for sess in list(self.guild_sessions.values())],
                    return_exceptions=True,
                )
            finally:
                self.guild_sessions.clear()
            self._guild_queues.clear()
            try:
                await self.router.close()
            except Exception:
                pass
                
        try:
            self.bot.loop.create_task(_cleanup())
        except Exception:
            asyncio.create_task(_cleanup())

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.guild is None and (ctx.command is None or ctx.command.name not in DM_COMMANDS):
            raise commands.NoPrivateMessage()
        return True

    async def cog_command_error(self, ctx: commands.Context, error: Exception):
        if isinstance(error, commands.NoPrivateMessage):
            await ctx.send(f"That only works in a server. In a DM with me you can use "
                           f"`{ctx.clean_prefix}narrate design <description>` and `{ctx.clean_prefix}narrate designs`.")
            return
        # Defining this handler stops discord.py's default logging for this cog, so keep printing the rest
        print(f"Ignoring exception in command {ctx.command}:")
        traceback.print_exception(type(error), error, error.__traceback__)

    async def aclose(self):
        """Called by BottyBoy.close() on shutdown: stop workers and close the TTS HTTP sessions."""
        for t in list(self._guild_workers.values()):
            t.cancel()
        await asyncio.gather(*self._guild_workers.values(), return_exceptions=True)
        self._guild_workers.clear()
        await self.router.close()

    def _enqueue_narration(self, guild_id: int, item: tuple) -> bool:
        q = self._guild_queues.get(guild_id)
        if q is None:
            q = self._guild_queues[guild_id] = asyncio.Queue(maxsize=GUILD_BACKLOG)
        worker = self._guild_workers.get(guild_id)
        if worker is None or worker.done():
            self._guild_workers[guild_id] = asyncio.create_task(self._guild_worker(guild_id, q), name=f"narrate:{guild_id}")
        try:
            q.put_nowait(item)
            return True
        except asyncio.QueueFull:
            print(f"[narrate] backlog full, dropping message: guild={guild_id}")
            return False

    def _take_rate_tokens(self, user_id: int, chars: int) -> bool:
        """Per-user token bucket in characters; False = over the limit, drop the message."""
        now = time.monotonic()
        tokens, last = self._user_buckets.get(user_id, (float(USER_BURST_CHARS), now))
        tokens = min(float(USER_BURST_CHARS), tokens + (now - last) * USER_REFILL_CHARS_PER_SEC)
        if tokens < chars:
            self._user_buckets[user_id] = (tokens, now)
            return False
        self._user_buckets[user_id] = (tokens - chars, now)
        return True

    async def _user_spent(self, user_id: int) -> float:
        """This month's estimated paid-voice spend for a user (0 if the DB can't say; the global cap still applies)."""
        month = BudgetTracker.current_month()
        cached = self._user_spend.get(user_id)
        if cached and cached[0] == month:
            return cached[1]
        try:
            row = await self.db.fetchrow(
                "SELECT est_usd FROM tts_user_usage WHERE month=$1 AND user_id=$2", month, user_id)
        except Exception as e:
            print(f"[narrate] failed to load user spend: {e}")
            return 0.0
        usd = float(row["est_usd"]) if row else 0.0
        self._user_spend[user_id] = (month, usd)
        return usd

    async def _add_spend(self, guild_id: int, user_id: int, usd: float) -> None:
        """Charge paid-voice spend to the user (for their share cap) and the server (for !narrate quota)."""
        if usd <= 0:
            return
        month = BudgetTracker.current_month()
        self._user_spend[user_id] = (month, await self._user_spent(user_id) + usd)
        try:
            await self.db.execute(
                """
                INSERT INTO tts_user_usage (month, user_id, est_usd) VALUES ($1, $2, $3)
                ON CONFLICT (month, user_id) DO UPDATE SET est_usd = tts_user_usage.est_usd + EXCLUDED.est_usd
                """,
                month, user_id, round(usd, 6),
            )
            if guild_id is not None:  # designs made in a DM don't belong to any server
                await self.db.execute(
                    """
                    INSERT INTO tts_guild_usage (month, guild_id, est_usd) VALUES ($1, $2, $3)
                    ON CONFLICT (month, guild_id) DO UPDATE SET est_usd = tts_guild_usage.est_usd + EXCLUDED.est_usd
                    """,
                    month, guild_id, round(usd, 6),
                )
        except Exception as e:
            print(f"[narrate] failed to record spend: {e}")

    async def _guild_spent(self, guild_id: int) -> Optional[float]:
        try:
            row = await self.db.fetchrow(
                "SELECT est_usd FROM tts_guild_usage WHERE month=$1 AND guild_id=$2",
                BudgetTracker.current_month(), guild_id)
        except Exception as e:
            print(f"[narrate] failed to load server spend: {e}")
            return None
        return float(row["est_usd"]) if row else 0.0

    async def _over_user_share(self, user_id: int) -> bool:
        return await self._user_spent(user_id) >= self.budget.cap * USER_BUDGET_SHARE

    def _get_session(self, guild_id: int) -> GuildVoiceSession:
        sess = self.guild_sessions.get(guild_id)
        if not sess:
            sess = GuildVoiceSession(self.bot, guild_id, self.tts)
            sess.on_alone = lambda gid=guild_id: self._shut_down_alone(gid)
            self.guild_sessions[guild_id] = sess
        return sess

    # ---------- DB Helpers ----------
    async def _get_pref(self, guild_id: int, user_id: int) -> Optional[dict]:
        row = await self.db.fetchrow("""
            SELECT guild_id, user_id, text_channel_id, voice, voice_label, style, rate, enabled
            FROM narrate_prefs
            WHERE guild_id=$1 AND user_id=$2""",
            guild_id, user_id
        )
        return dict(row) if row else None

    async def _upsert_pref(self, ctx, text_channel_id, voice, rate, enabled, voice_label=None):
        """voice_label=None keeps the stored label unless the voice itself changes."""
        guild_id = await get_guild_id(ctx, self.db)
        user_id  = await get_user_id(ctx, self.db)
        if guild_id is None or user_id is None:
            return  # helpers already messaged on DB error
            
        await self.db.execute("""
            INSERT INTO narrate_prefs (guild_id, user_id, text_channel_id, voice, voice_label, rate, enabled)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (guild_id, user_id)
            DO UPDATE SET text_channel_id=EXCLUDED.text_channel_id,
                          voice=EXCLUDED.voice,
                          voice_label=CASE
                              WHEN narrate_prefs.voice IS DISTINCT FROM EXCLUDED.voice THEN EXCLUDED.voice_label
                              ELSE COALESCE(EXCLUDED.voice_label, narrate_prefs.voice_label) END,
                          rate=EXCLUDED.rate,
                          enabled=EXCLUDED.enabled,
                          updated_at=CURRENT_TIMESTAMP
            """,
            guild_id, user_id, text_channel_id, voice, voice_label, rate, enabled
        )

    async def _set_style(self, guild_id: int, user_id: int, style: Optional[str]) -> None:
        await self.db.execute("""
            UPDATE narrate_prefs
            SET style=$3, updated_at=CURRENT_TIMESTAMP
            WHERE guild_id=$1 AND user_id=$2""",
            guild_id, user_id, style
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

    def _bot_voice_channel(self, guild: discord.Guild, session: "GuildVoiceSession"):
        """Where the bot actually is. The session's own record can be empty after a restart or a voice reconnect
        it didn't see, so fall back to discord.py's voice client and then Discord's voice state."""
        vc = session.voice_client
        if vc and vc.is_connected():
            return vc.channel
        gvc = getattr(guild, "voice_client", None)
        if isinstance(gvc, discord.VoiceClient) and gvc.channel:
            return gvc.channel
        me = getattr(guild, "me", None)
        voice = getattr(me, "voice", None)
        return getattr(voice, "channel", None) if isinstance(voice, discord.VoiceState) else None

    async def _leave_voice(self, guild: discord.Guild, session: "GuildVoiceSession") -> None:
        """Leave voice for sure: the tracked connection, any connection discord.py still holds, and any leftover
        presence (e.g. from before a restart)."""
        await session.teardown()
        gvc = getattr(guild, "voice_client", None)
        if isinstance(gvc, discord.VoiceClient):
            try:
                await gvc.disconnect(force=True)
            except Exception as e:
                print(f"[narrate] leftover voice client disconnect failed: {e}")
        voice = getattr(getattr(guild, "me", None), "voice", None)
        if isinstance(voice, discord.VoiceState) and voice.channel:
            try:
                await guild.change_voice_state(channel=None)
            except Exception as e:
                print(f"[narrate] couldn't clear leftover voice state: {e}")

    async def _shut_down_alone(self, guild_id: int) -> None:
        """Bot was left alone in its channel: nobody is listening, so narration is off for everyone."""
        guild = self.bot.get_guild(guild_id)
        try:
            await self._set_all_prefs_disabled(guild_id)
        except Exception as e:
            print(f"[narrate] failed to disable narration when alone: {e}")
        if guild:
            await self._leave_voice(guild, self._get_session(guild_id))
        else:
            await self._get_session(guild_id).teardown()

    @commands.Cog.listener()
    async def on_ready(self):
        # After a restart Discord can still show the bot in a channel it no longer has a connection to.
        for guild in self.bot.guilds:
            voice = getattr(guild.me, "voice", None)
            if voice and voice.channel and not guild.voice_client:
                print(f"[narrate] clearing leftover voice presence in guild={guild.id}")
                try:
                    await guild.change_voice_state(channel=None)
                except Exception as e:
                    print(f"[narrate] couldn't clear leftover voice state: {e}")

    async def _disconnect_if_no_enabled_in_channel(self, guild_id: int, channel: discord.VoiceChannel, session: "GuildVoiceSession") -> None:
        """Disconnect from voice if nobody in channel has it enabled.
        Others outside the channel may have it enabled,
        but it would likely be jarring to jump to their channel,
        so simply disable prefs for all and disconnect"""
        
        has_enabled = await self._any_enabled_in_channel(guild_id, channel)
        vc = session.voice_client

        if not has_enabled:
            guild = self.bot.get_guild(guild_id)
            if guild:
                await self._leave_voice(guild, session)
            else:
                await session.teardown()
            await self._set_all_prefs_disabled(guild_id)

    async def _load_keyword_cache(self) -> Dict[int, Dict[str, str]]:
        """
        Loads ALL guild keyword -> voiceline filepath mappings into memory.

        Cache shape:
          self.guild_voiceline_caches[guild_id][keyword_norm] = storage_path (str)

        Returns the cache dict.
        """
        rows = await self.db.fetch(
            """
            SELECT guild_id, name, storage_path
            FROM guild_voicelines
            """
        )

        caches: Dict[int, Dict[str, str]] = {}
        for r in rows:
            gid = int(r["guild_id"])
            name_norm = _normalize_voiceline(r["name"])  # should already be normalized but make sure
            path = r["storage_path"]  # expected to be relative to persistent_audio_root
            caches.setdefault(gid, {})[name_norm] = path

        self.guild_voiceline_caches = caches
        return caches

    async def _resolve_voiceline_override(self, guild_id: int, text: str) -> Optional[bytes]:
        """
        If a voiceline exists for this guild and exact text matches a saved voiceline name,
        return its audio bytes (OGG_OPUS). Otherwise None.

        Cache shape:
          self.guild_voiceline_caches[guild_id][name_norm] = storage_path
        """
        name_norm = _normalize_voiceline(text)

        try:
            storage_path = self.guild_voiceline_caches[guild_id][name_norm]
        except Exception:
            return None

        p = Path(storage_path)
        if not p.is_absolute():
            root = Path(getattr(self, "persistent_audio_root", "data/audio"))
            p = root / p

        if not p.exists() or not p.is_file():
            # stale cache entry
            try:
                del self.guild_voiceline_caches[guild_id][name_norm]
            except Exception:
                pass
            return None

        # Avoid blocking the event loop on file IO
        def _read_bytes(path: Path) -> bytes:
            return path.read_bytes()

        try:
            return await asyncio.to_thread(_read_bytes, p)
        except Exception:
            return None

    import asyncio
    from pathlib import Path
    import uuid
    from typing import Optional

    try:
        import asyncpg  # if you're using asyncpg underneath
    except Exception:
        asyncpg = None

    async def _save_voiceline(
            self,
            guild_id: int,
            voiceline_name: str,
            audio_bytes: bytes,
            *,
            created_by_user_id: Optional[int] = None,
            fmt: str = "ogg_opus",
            duration_ms: Optional[int] = None,
    ) -> dict:
        """
        Save a voiceline for (guild_id, name_norm).

        - Name is normalized with _normalize_voiceline (casefold + collapse ws).
        - If the name already exists in this guild, raises ValueError.
        - Saves EXACT OGG_OPUS bytes to disk (unique filename).
        - Inserts a row into guild_voicelines.
        - Updates in-memory cache.

        Returns dict with id, name_norm, storage_path, etc.
        """
        if not audio_bytes:
            raise ValueError("audio_bytes is empty")
        if not voiceline_name or not voiceline_name.strip():
            raise ValueError("voiceline_name is empty")

        name_norm = _normalize_voiceline(voiceline_name)

        # ensure FK rows exist
        await self.db.execute(
            "INSERT INTO guilds (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
            int(guild_id),
        )
        if created_by_user_id is not None:
            await self.db.execute(
                "INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
                int(created_by_user_id),
            )

        # Unique file per save
        file_id = uuid.uuid4().hex
        storage_path = f"voicelines/{guild_id}/{file_id}.ogg"
        root = Path(getattr(self, "persistent_audio_root", "data/audio"))
        abs_path = root / storage_path

        # write file first
        await asyncio.to_thread(_atomic_write_bytes, abs_path, audio_bytes)

        try:
            row = await self.db.fetchrow(
                """
                INSERT INTO guild_voicelines (
                    guild_id, created_by_user_id, name, storage_path, format, byte_size, duration_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                """,
                int(guild_id),
                int(created_by_user_id) if created_by_user_id is not None else None,
                name_norm,  # store normalized name (your policy)
                storage_path,
                fmt,
                int(len(audio_bytes)),
                int(duration_ms) if duration_ms is not None else None,
            )
        except Exception as e:
            # If name conflicts, remove the file we just wrote
            try:
                if abs_path.exists():
                    abs_path.unlink()
            except Exception:
                pass

            # Turn unique violations into a clean error message
            # Works with asyncpg; otherwise just raise a ValueError on any conflict-ish error
            if isinstance(e, asyncpg.UniqueViolationError):
                raise ValueError(f"Voiceline name already exists: '{name_norm}'") from e
            raise

        # update in-memory cache
        if not hasattr(self, "guild_voiceline_caches") or self.guild_voiceline_caches is None:
            self.guild_voiceline_caches = {}
        self.guild_voiceline_caches.setdefault(int(guild_id), {})[name_norm] = storage_path

        return {
            "id": int(row["id"]) if row else None,
            "guild_id": int(guild_id),
            "name": name_norm,
            "name_norm": name_norm,
            "storage_path": storage_path,
            "byte_size": int(len(audio_bytes)),
            "duration_ms": duration_ms,
        }

    async def _list_voicelines(self, guild_id: int, limit: int = 100) -> List[dict]:
        rows = await self.db.fetch(
            """
            SELECT name, created_at, byte_size, storage_path
            FROM guild_voicelines
            WHERE guild_id=$1
            ORDER BY created_at DESC
            LIMIT $2
            """,
            int(guild_id), int(limit)
        )
        return [dict(r) for r in rows]

    async def _rename_voiceline(self, guild_id: int, old_name: str, new_name: str) -> bool:
        """
        Renames a voiceline within a guild.

        - Uses your normalization policy (casefold + collapse whitespace) on both names.
        - Returns False if the old name doesn't exist.
        - Raises ValueError if the new name already exists (unique constraint) or if inputs are empty.
        """
        old_norm = _normalize_voiceline(old_name or "")
        new_norm = _normalize_voiceline(new_name or "")
        if not old_norm or not new_norm:
            raise ValueError("old_name/new_name empty")
        if old_norm == new_norm:
            # No-op rename: treat as success (or return False if you prefer)
            return True

        # Fetch existing row so we can update cache by storage_path
        row = await self.db.fetchrow(
            """
            SELECT storage_path
            FROM guild_voicelines
            WHERE guild_id=$1 AND name=$2
            """,
            int(guild_id), old_norm
        )
        if not row:
            return False

        try:
            updated = await self.db.execute(
                """
                UPDATE guild_voicelines
                SET name=$3
                WHERE guild_id=$1 AND name=$2
                """,
                int(guild_id), old_norm, new_norm
            )
        except Exception as e:
            # Unique conflict: new name already exists in this guild
            msg = str(e).lower()
            if "unique" in msg or "duplicate" in msg:
                raise ValueError(f"Voiceline name already exists: '{new_norm}'") from e
            raise

        # Update cache (best effort)
        try:
            gid_cache = self.guild_voiceline_caches.get(int(guild_id), {})
            storage_path = row["storage_path"]
            gid_cache.pop(old_norm, None)
            gid_cache[new_norm] = storage_path
        except Exception:
            pass
        return "1" in str(updated)

    async def _delete_voiceline(self, guild_id: int, name: str) -> bool:
        name_clean = " ".join((name or "").strip().split())
        if not name_clean:
            raise ValueError("name empty")

        row = await self.db.fetchrow(
            """
            SELECT storage_path
            FROM guild_voicelines
            WHERE guild_id=$1 AND name=$2
            """,
            int(guild_id), name_clean
        )
        if not row:
            return False

        # Delete DB row
        deleted = await self.db.execute(
            """
            DELETE FROM guild_voicelines
            WHERE guild_id=$1 AND name=$2
            """,
            int(guild_id), name_clean
        )

        # Remove from cache
        try:
            gid_cache = self.guild_voiceline_caches.get(int(guild_id), {})
            gid_cache.pop(_normalize_voiceline(name_clean), None)
        except Exception:
            pass

        # Delete file (safe because you use unique per-save filenames)
        try:
            storage_path = row["storage_path"]
            p = Path(storage_path)
            if not p.is_absolute():
                root = Path(getattr(self, "persistent_audio_root", "data/audio"))
                p = root / p
            if p.exists() and p.is_file():
                p.unlink()
        except Exception as e:
            # Not fatal if the DB row is gone
            print(f"[narrate] failed to delete voiceline file for {name_clean}: {e}")

        return "1" in str(deleted)


    # ---------- Commands ----------
    @commands.group(name="narrate", invoke_without_command=True)
    async def narrate_root(self, ctx: commands.Context):
        await ctx.send(embed=_help_embed(ctx.clean_prefix))

    @narrate_root.command(name="on", aliases=["start"])
    async def narrate_on(self, ctx: commands.Context):
        ch = ctx.channel  # could be a TextChannel or a Thread
        if not await self._set_channel_pref(ctx, ch, enable=True):
            return
        await self.router.start()
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
        await session.cancel_playback()  # also drops messages still waiting to be synthesized
        await ctx.send("⏹️ Stopped current narration.")

    async def _voices_embed(self, ctx: commands.Context) -> discord.Embed:
        """Provider guide for `!narrate voices`: what each is good at, where to browse, how to pick one."""
        p = f"{ctx.clean_prefix}narrate"
        try:
            pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        except Exception:
            pref = None
        current = _voice_display((pref or {}).get("voice") or DEFAULT_VOICE, (pref or {}).get("voice_label"))
        embed = discord.Embed(
            title="🎙️ Narration voices",
            description=f"Your voice: **{discord.utils.escape_markdown(current)}**\nChange it with `{p} voice …`",
            color=VOICES_EMBED_COLOR,
        )
        if self.fish:
            embed.add_field(
                name="🐟 Fish Audio · default",
                value=(f"⚡ **Fast** (~1–2 s) · 🎭 expressive · 📚 huge community library: characters, accents, memes\n"
                       f"[Browse voices]({FISH_LIBRARY_URL}), open one, copy its link or ID\n"
                       f"`{p} voice <link or ID>`"),
                inline=False,
            )
        if self.gemini:
            embed.add_field(
                name="✨ Gemini",
                value=(f"🎯 **Best at following `[style]` hints** · 🐢 slower (~3–5 s) · 200+ voices\n"
                       f"[Voice list]({GEMINI_VOICES_URL})\n"
                       f"`{p} voice gemini:Kore`\n"
                       f"Prefer an older model's sound? Add it after `@`: `{p} voice gemini:Kore@2.5`\n"
                       f"Models: {_gemini_model_choices()} (default `3.8`). "
                       f"If yours is out of quota, the others fill in."),
                inline=False,
            )
        embed.add_field(
            name="🔊 Google",
            value=(f"🆓 **Free** · ⚡ fastest (<1 s) · 😐 no emotion, ignores hints · stands in when the budget runs out\n"
                   f"[Voice list]({GOOGLE_VOICES_URL})\n"
                   f"`{p} voice en-US-Wavenet-D`, or `{p} voices en-US female` for short names"),
            inline=False,
        )
        if self.fish:
            embed.add_field(
                name="🎨 Make your own",
                value=(f"`{p} design raspy old pirate, slow and amused`: I DM you 3 takes to pick from, "
                       f"privately. Use it here whenever you're ready."),
                inline=False,
            )
        if self.fish or self.gemini:
            embed.add_field(
                name="💬 Hints (Fish & Gemini)",
                value=(f"Start a message with `[whispering]`, `[excited]`, `[very sad]`…\n"
                       f"Drop `[laugh]` `[sigh]` `[pause]` anywhere\n"
                       f"`{p} style dry and sarcastic` sets a default"),
                inline=False,
            )
            embed.set_footer(text="Fish and Gemini share a monthly budget; past it, Google takes over until next month.")
        return embed

    @narrate_root.command(name="voices")
    async def narrate_voices(self, ctx: commands.Context, language: Optional[str] = None, gender: Optional[str] = None):
        # No args: where to browse each provider's voices
        if not language:
            return await ctx.send(embed=await self._voices_embed(ctx))

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

        out = f"{rows[0]['gender']+' - ' if gender_in else ''}{rows[0]['language']} Names:\n"
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
            SELECT user_id, voice, voice_label, style, rate
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
                v = _voice_display(r["voice"] or DEFAULT_VOICE, r["voice_label"])
                rate_val = r["rate"]
                rate_str = f"{DEFAULT_RATE if rate_val is None else rate_val}"
                style_str = f" | style: {r['style']}" if r["style"] else ""
                lines.append(f"- {username} | {v} | {rate_str}{style_str}")

        # Several users with long styles can pass Discord's 2000-char limit
        buf = ""
        for line in lines:
            if buf and len(buf) + 1 + len(line) > 1900:
                await ctx.send(buf, suppress_embeds=True)
                buf = ""
            buf = f"{buf}\n{line}" if buf else line[:1900]
        if buf:
            await ctx.send(buf, suppress_embeds=True)

    @narrate_root.command(name="channel")
    async def narrate_channel(self, ctx: commands.Context, channel: Optional[Union[discord.TextChannel, discord.Thread]] = None):
        ch = channel or ctx.channel  # allow calling it inside a thread to select that thread
        if not await self._set_channel_pref(ctx, ch, enable=None):
            return
        await ctx.send(f"Narration channel set to {ch.mention}.", suppress_embeds=True)

    @narrate_root.command(name="voice")
    async def narrate_voice(self, ctx: commands.Context, *, voice: str):
        q = _collapse_ws(voice).strip("<>")
        low = q.lower()

        # One of your cloned voices, by name: `voice clone <name>` (or just `voice clone` if you have one)
        if low == "clone" or low.startswith("clone "):
            row, rows = await self._find_own_clone(ctx.author.id, q[6:].strip() or None)
            if not rows:
                return await ctx.send(f"You don't have a cloned voice yet. `{ctx.clean_prefix}narrate clone` makes one.",
                                      suppress_embeds=True)
            if not row:
                names = ", ".join(f"`{r['title']}`" for r in rows)
                return await ctx.send(f"Which one? `{ctx.clean_prefix}narrate voice clone <name>`. Yours: {names}",
                                      suppress_embeds=True)
            return await self._apply_voice(ctx, f"fish:{row['voice_id']}", row["title"], "cloned voice")

        # A locally kept designed voice: fish:ref:<key> (from !narrate designs)
        if low.startswith(f"fish:{FISH_REF_PREFIX}"):
            row = await self.db.fetchrow(
                "SELECT title FROM tts_designed_voices WHERE provider='fish' AND voice_id=$1 "
                "AND (user_id=$2 OR guild_id=$3 OR kind='clone')",  # yours anywhere, server-shared designs, public clones
                low[len("fish:"):], ctx.author.id, ctx.guild.id,
            )
            if not row:
                return await ctx.send(f"No designed voice with that id. `{ctx.clean_prefix}narrate designs` lists yours.", suppress_embeds=True)
            return await self._apply_voice(ctx, low, row["title"], "Fish Audio, designed")

        # Fish: fish:<id>, a fish.audio link, or a bare 32-hex model id
        looks_fish =low.startswith("fish:") or "fish.audio" in low or re.fullmatch(r"[0-9a-f]{32}", low)
        if looks_fish:
            if not self.fish:
                return await ctx.send("Fish Audio isn't set up on this bot.", suppress_embeds=True)
            fish_id = extract_fish_id(q)
            if not fish_id:
                return await ctx.send("That doesn't look like a Fish voice link or id.", suppress_embeds=True)
            try:
                info = await self.fish.get_model(fish_id)
            except Exception as e:
                print(f"[narrate] fish model lookup failed: {e}")
                return await ctx.send("Couldn't reach Fish Audio to check that voice; try again shortly.", suppress_embeds=True)
            if not info:
                return await ctx.send("Fish Audio doesn't have a voice with that id (or it's private).", suppress_embeds=True)
            return await self._apply_voice(ctx, f"fish:{fish_id}", (info.get("title") or fish_id)[:256], "Fish Audio")

        # Gemini: gemini:<name>
        if low.startswith("gemini:"):
            return await self._set_gemini_voice(ctx, q[7:].strip())

        # 1) Try exact (case-insensitive) full voice_name
        row = await self.db.fetchrow(
            """
            SELECT language, voice_name, gender, nickname
            FROM google_tts_voices
            WHERE lower(voice_name) = lower($1)
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
                WHERE lower(nickname) = lower($1)
                LIMIT 1
                """,
                q,
            )

        if not row:
            # A bare Gemini voice name (Google names/nicknames win when both exist)
            if self.gemini:
                try:
                    if await self.gemini.find_voice(q.partition("@")[0]):
                        return await self._set_gemini_voice(ctx, q)
                except Exception as e:
                    print(f"[narrate] gemini voice lookup failed: {e}")
            return await ctx.send(
                f"Unknown voice. `{ctx.clean_prefix}narrate voices` shows where to find Fish, Gemini and Google voices.",
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
        await self._upsert_pref(ctx, channel_id, voice_name, rate, enabled, voice_label=nickname or None)
        await self._record_voice_history(ctx.guild.id, ctx.author.id, voice_name, nickname or None)

        # Friendly confirmation shows both full name and nickname
        await ctx.send(
            f"Voice set to `{voice_name}` (`{nickname}`) - {language}.",
            suppress_embeds=True,
        )

    async def _apply_voice(self, ctx: commands.Context, ref: str, label: str, provider_name: str):
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        channel_id = (pref.get("text_channel_id") if pref else None) or ctx.channel.id
        rate       = float((pref.get("rate") if pref else None) or DEFAULT_RATE)
        enabled    = bool(pref.get("enabled")) if pref else False
        await self._upsert_pref(ctx, channel_id, ref, rate, enabled, voice_label=label)
        await self._record_voice_history(ctx.guild.id, ctx.author.id, ref, label)
        await ctx.send(f"Voice set to **{label}** ({provider_name}).", suppress_embeds=True)

    async def _set_gemini_voice(self, ctx: commands.Context, name: str):
        """`name` is a voice, optionally with a preferred model: 'Kore' or 'Kore@2.5'."""
        if not self.gemini:
            return await ctx.send("Gemini TTS isn't set up on this bot.", suppress_embeds=True)
        voice_name, _, model_name = name.partition("@")
        voice_name = voice_name.strip()
        model = None
        if model_name.strip():
            model = resolve_gemini_model(model_name)
            if not model:
                return await ctx.send(
                    f"Unknown Gemini model `{model_name.strip()}`. Use one of: {_gemini_model_choices()}.",
                    suppress_embeds=True)
        try:
            v = await self.gemini.find_voice(voice_name)
        except Exception as e:
            print(f"[narrate] gemini voice lookup failed: {e}")
            return await ctx.send("Couldn't reach Gemini to check that voice; try again shortly.", suppress_embeds=True)
        if not v:
            return await ctx.send(f"Gemini has no voice called `{voice_name}`. See <{GEMINI_VOICES_URL}>.",
                                  suppress_embeds=True)
        label = v.get("display_name") or v["id"]
        desc = f": {v['description']}" if v.get("description") else ""
        ref = f"gemini:{v['id'].lower()}"
        provider_name = f"Gemini{desc}"
        if model:
            ref += f"@{model}"
            label += f" · {GEMINI_MODEL_LABELS.get(model, model)}"
            provider_name = (f"{GEMINI_MODEL_LABELS.get(model, model)} first, other Gemini models if it's out of quota"
                             f"{desc}")
        await self._apply_voice(ctx, ref, label[:256], provider_name[:300])

    @narrate_root.command(name="style")
    async def narrate_style(self, ctx: commands.Context, *, style: str):
        """Default delivery style for your messages (Fish & Gemini voices). 'off' clears it."""
        style = _collapse_ws(style).strip("[]")
        clear = style.lower() in {"off", "none", "clear", "reset"}
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)
        if not pref:
            # Create the row (with defaults) so there's somewhere to store the style
            await self._upsert_pref(ctx, ctx.channel.id, DEFAULT_VOICE, DEFAULT_RATE, False)
        await self._set_style(ctx.guild.id, ctx.author.id, None if clear else style[:200])
        if clear:
            return await ctx.send("Default style cleared.", suppress_embeds=True)
        provider = parse_voice_ref((pref or {}).get("voice") or DEFAULT_VOICE)[0]
        note = "" if provider != "google" else " (your current voice is a Google voice, which ignores styles)"
        await ctx.send(f"Default style set to **{style[:200]}**{note}. Starting a message with `[...]` overrides it.",
                       suppress_embeds=True)

    @narrate_root.command(name="quota")
    async def narrate_quota(self, ctx: commands.Context):
        """Your and this server's premium-voice (Fish/Gemini) spend this month."""
        if not ctx.guild:
            return await ctx.send("Run this in a server.")
        month = BudgetTracker.current_month()
        user_cap = self.budget.cap * USER_BUDGET_SHARE
        mine = await self._user_spent(ctx.author.id)
        server = await self._guild_spent(ctx.guild.id)
        total = await self.budget.spent()

        embed = discord.Embed(title=f"📊 Premium voice usage · {month:%B %Y}", color=VOICES_EMBED_COLOR)
        embed.add_field(name="You", value=f"{_usd_bar(mine, user_cap)}\n${mine:.2f} of your ${user_cap:.2f} limit",
                        inline=False)
        embed.add_field(name="This server",
                        value="unavailable right now" if server is None else f"${server:.2f} this month",
                        inline=False)
        embed.add_field(name="Shared budget (all servers)",
                        value=("unavailable right now (paid voices paused)" if total == float("inf")
                               else f"{_usd_bar(total, self.budget.cap)}\n${total:.2f} of ${self.budget.cap:.2f}"),
                        inline=False)
        next_month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
        embed.set_footer(text=f"Resets {next_month:%b} 1. Google voices are free and don't count. "
                              f"Past your limit, your messages use the Google voice.")
        await ctx.send(embed=embed)

    @narrate_root.command(name="usage")
    @commands.has_permissions(manage_guild=True)
    async def narrate_usage(self, ctx: commands.Context):
        try:
            totals = await self.budget.by_provider()
        except RuntimeError:
            return await ctx.send("Can't read usage right now (database unreachable); paid voices are paused until it is.",
                                  suppress_embeds=True)
        spent = sum(totals.values())
        lines = [f"**TTS spend this month:** ${spent:.2f} of ${self.budget.cap:.2f} (estimated, all servers)"]
        for prov, usd in sorted(totals.items()):
            lines.append(f"- {prov}: ${usd:.3f}")
        if spent >= self.budget.cap:
            lines.append("Budget reached: paid voices are using the Google voice until next month.")
        await ctx.send("\n".join(lines), suppress_embeds=True)

    @narrate_root.command(name="design")
    async def narrate_design(self, ctx: commands.Context, *, description: str):
        """Create a new Fish voice from a text description; pick one of 3 candidates."""
        if not self.fish:
            return await ctx.send("Voice design needs Fish Audio, which isn't set up on this bot.", suppress_embeds=True)
        description = _collapse_ws(description)
        if len(description) < 5:
            return await ctx.send(f"Describe the voice, e.g. `{ctx.clean_prefix}narrate design raspy old pirate, slow and amused`.",
                                  suppress_embeds=True)
        now = time.monotonic()
        last = self._design_last_used.get(ctx.author.id)
        if last and now - last < DESIGN_COOLDOWN_SECS:
            return await ctx.send(f"Give it {int(DESIGN_COOLDOWN_SECS - (now - last)) + 1}s before designing another voice.",
                                  suppress_embeds=True)
        if await self.budget.exhausted():
            return await ctx.send("This month's TTS budget is used up, so voice design is off until next month.",
                                  suppress_embeds=True)
        if await self._over_user_share(ctx.author.id):
            return await ctx.send("You've used your share of this month's premium-voice budget, so voice design is off "
                                  "for you until next month.", suppress_embeds=True)
        user_id = await get_user_id(ctx, self.db)
        guild_id = await get_guild_id(ctx, self.db) if ctx.guild else None
        if user_id is None or (ctx.guild and guild_id is None):
            return

        # Designing is private: the samples and pick buttons always go to the designer's DMs.
        # Check DMs work before spending anything.
        try:
            dm = await ctx.author.create_dm()
            if ctx.guild:
                await dm.send(f"Designing **{description[:200]}**, samples coming here in a moment…",
                              suppress_embeds=True)
        except discord.Forbidden:
            return await ctx.send(
                f"I couldn't DM you. Allow DMs from server members (Privacy Settings), or DM me "
                f"`{ctx.clean_prefix}narrate design <description>` directly.", suppress_embeds=True)
        if ctx.guild:
            await ctx.send("📬 Check your DMs, I'm sending the samples there.", suppress_embeds=True)
        self._design_last_used[ctx.author.id] = now

        async with dm.typing():
            try:
                candidates = await self.fish.design(description, DESIGN_PREVIEW_LINE, n=3)
            except Exception as e:
                print(f"[narrate] voice design failed: {e}")
                return await dm.send("Voice design failed; try a different description.", suppress_embeds=True)
        await self.budget.record("fish", FISH_VOICE_DESIGN_USD)
        await self._add_spend(guild_id, ctx.author.id, FISH_VOICE_DESIGN_USD)
        candidates = [c for c in candidates if c.get("audio")]
        if not candidates:
            return await dm.send("Fish didn't return any voices for that; try rewording it.", suppress_embeds=True)

        files = []
        for i, c in enumerate(candidates, 1):
            ext = {"wav": "wav", "ogg_opus": "ogg", "mp3": "mp3"}.get(audio_format(c["audio"]), "wav")
            files.append(discord.File(io.BytesIO(c["audio"]), filename=f"voice_{i}.{ext}"))
        view = _DesignPickView(self, ctx.author.id, guild_id, ctx.clean_prefix, description, candidates)
        view.message = await dm.send(
            f"Here are {len(candidates)} takes on **{description[:200]}**. Listen, then pick one to save it. "
            f"It won't change your voice until you choose it.",
            files=files, view=view, suppress_embeds=True,
        )

    async def _save_designed_voice(self, owner_id: int, origin_guild_id: Optional[int], description: str,
                                   candidate: dict) -> str:
        """
        Save a design candidate to the user's designed voices (usable in any server); returns its voice ref.
        It isn't applied: the user picks it with `narrate voice` when they're ready.

        Prefers a private Fish model. Fish caps API model creation by prepaid credit, so when that's
        refused the sample is kept on disk instead and sent inline with each request (fish:ref:<key>).
        """
        title = description[:100]
        sample_text = candidate.get("text") or DESIGN_PREVIEW_LINE
        sample_path = None
        try:
            info = await self.fish.create_model(
                title=title, audio=candidate["audio"], text=sample_text,
                signature=candidate.get("signature") or candidate.get("id"), description=description,
            )
            voice_id = str(info["_id"]).lower()
        except ProviderError as e:
            if e.status not in (402, 403):
                raise
            print(f"[narrate] fish model creation refused, keeping designed voice locally: {e}")
            key = uuid.uuid4().hex
            sample_path = f"designed/user_{owner_id}/{key}.ogg"
            audio = candidate["audio"]
            if audio_format(audio) != "ogg_opus":
                audio = await asyncio.to_thread(_transcode_to_ogg_opus, audio)
            root = Path(getattr(self, "persistent_audio_root", "data/audio"))
            await asyncio.to_thread(_atomic_write_bytes, root / sample_path, audio)
            voice_id = f"{FISH_REF_PREFIX}{key}"

        await self.db.execute(
            """
            INSERT INTO tts_designed_voices (guild_id, user_id, provider, voice_id, title, instruction,
                                             sample_path, sample_text)
            VALUES ($1, $2, 'fish', $3, $4, $5, $6, $7)
            ON CONFLICT (provider, voice_id) DO NOTHING
            """,
            origin_guild_id, owner_id, voice_id, title, description, sample_path, sample_text,
        )
        return f"fish:{voice_id}"

    @narrate_root.command(name="clone")
    async def narrate_clone(self, ctx: commands.Context, action: Optional[str] = None, *, name: Optional[str] = None):
        """Make a named voice from a recording of your own voice (by DM). `clone delete <name>` removes one."""
        if action and action.lower() in ("delete", "remove", "del", "rm"):
            return await self._delete_clone(ctx, name)
        if not self.fish:
            return await ctx.send("Voice cloning needs Fish Audio, which isn't set up on this bot.", suppress_embeds=True)
        now = time.monotonic()
        last = self._clone_last_used.get(ctx.author.id)
        if last and now - last < CLONE_COOLDOWN_SECS:
            return await ctx.send(f"Give it {int(CLONE_COOLDOWN_SECS - (now - last)) + 1}s before starting another.",
                                  suppress_embeds=True)
        if await self.budget.exhausted():
            return await ctx.send("This month's TTS budget is used up, so cloning is off until next month.",
                                  suppress_embeds=True)
        if await self._get_whisper() is None:
            return await ctx.send("Voice cloning isn't available on this bot right now (speech recognition for the "
                                  "consent check isn't installed).", suppress_embeds=True)
        user_id = await get_user_id(ctx, self.db)
        if user_id is None:
            return
        self._clone_last_used[ctx.author.id] = now
        try:
            dm = await ctx.author.create_dm()
            await dm.send(
                "🎤 **Clone your voice**\n"
                f"Record yourself reading this ({CLONE_MIN_SECS}+ seconds, quiet room), then send it here:\n"
                f"> {CLONE_SCRIPT}\n"
                "📱 **Phone:** hold the 🎤 button in this chat to send a voice message.\n"
                "💻 **PC:** Discord desktop can't send voice messages, so record with Windows' **Sound Recorder** "
                "(or any recorder) and drag the file into this chat.\n"
                "The first sentence is your consent. You'll hear a preview before anything is saved, and you can "
                f"redo it as often as you like. Type `cancel` to stop.",
                suppress_embeds=True,
            )
        except discord.Forbidden:
            return await ctx.send(
                f"I couldn't DM you. Allow DMs from server members (Privacy Settings), or DM me "
                f"`{ctx.clean_prefix}narrate clone` directly.", suppress_embeds=True)
        if ctx.guild:
            await ctx.send("📬 Check your DMs.", suppress_embeds=True)
        await self._clone_session(ctx.author, dm, ctx.clean_prefix)

    async def _clone_session(self, user, dm, prefix: str) -> None:
        """Record -> check -> preview -> keep/redo -> name -> save, all in the DM."""
        def from_user(m: discord.Message) -> bool:
            return m.author.id == user.id and m.channel.id == dm.id

        async def next_message() -> Optional[discord.Message]:
            try:
                return await self.bot.wait_for("message", check=from_user, timeout=CLONE_WAIT_SECS)
            except asyncio.TimeoutError:
                await dm.send(f"I didn't hear back, so I stopped. `{prefix}narrate clone` to start again.")
                return None

        attempts = 0
        while True:
            msg = await next_message()
            if msg is None:
                return
            if (msg.content or "").strip().lower() == "cancel":
                return await dm.send("Cancelled, nothing was saved.")
            if not msg.attachments:
                await dm.send("Send a voice message or an audio file of you reading the script (or `cancel`).")
                continue
            attempts += 1
            if attempts > CLONE_MAX_ATTEMPTS:
                return await dm.send(f"That's a lot of tries; let's stop here. `{prefix}narrate clone` to start fresh.")
            att = msg.attachments[0]
            if att.size > CLONE_MAX_BYTES:
                await dm.send("That file is too big; a minute or so of speech is plenty. Send another (or `cancel`).")
                continue
            audio = await att.read()
            try:
                async with dm.typing():
                    sample, transcript = await self._check_recording(audio)
                    preview = await self._clone_preview(user.id, sample, transcript)
            except _CloneRejected as e:
                await dm.send(f"{e} Send another recording (or `cancel`).", suppress_embeds=True)
                continue
            except Exception as e:
                print(f"[narrate] clone preview failed: {e!r}")
                await dm.send("Something went wrong making the preview. Send the recording again (or `cancel`).")
                continue

            view = _ClonePreviewView(user.id)
            view.message = await dm.send(
                "🔊 Here's your cloned voice. Happy with it?",
                file=discord.File(io.BytesIO(preview), filename="preview.ogg"), view=view)
            if await view.wait() or view.choice == "cancel":   # wait() is True on timeout
                return await dm.send("Okay, nothing was saved." if view.choice == "cancel"
                                     else f"I didn't hear back, so nothing was saved. `{prefix}narrate clone` to start again.")
            if view.choice == "redo":
                await dm.send("No problem, send a new recording.")
                continue
            break  # keep

        name = await self._ask_clone_name(user, dm, next_message)
        if name is None:
            return
        try:
            async with dm.typing():
                ref = await self._save_clone(user, sample, transcript, name)
        except Exception as e:
            print(f"[narrate] saving clone failed: {e!r}")
            return await dm.send("Something went wrong saving the voice; try again in a bit.")
        await dm.send(
            f"✅ **{discord.utils.escape_markdown(name)}** is saved. It's public: anyone can use it, and it's yours to "
            "remove.\n"
            f"Use it in a server: `{prefix}narrate voice clone {name}` (or `{prefix}narrate voice {ref}`, which "
            f"anyone can use). Remove it: `{prefix}narrate clone delete {name}`.",
            suppress_embeds=True)

    async def _ask_clone_name(self, user, dm, next_message) -> Optional[str]:
        await dm.send("What should this voice be called? (up to 60 characters, e.g. `Melon, sleepy`)")
        while True:
            msg = await next_message()
            if msg is None:
                return None
            name = " ".join((msg.content or "").split())
            if name.lower() == "cancel":
                await dm.send("Cancelled, nothing was saved.")
                return None
            if not name or len(name) > 60 or "`" in name:
                await dm.send("Give it a name of 1 to 60 characters (no backticks).")
                continue
            taken = await self.db.fetchrow(
                "SELECT 1 FROM tts_designed_voices WHERE user_id=$1 AND lower(title)=lower($2)", user.id, name)
            if taken:
                await dm.send("You already have a voice with that name; pick another.")
                continue
            return name

    async def _get_whisper(self):
        """faster-whisper model for the consent check; None if it isn't installed."""
        if self._whisper is None:
            try:
                from faster_whisper import WhisperModel
            except ImportError:
                return None
            self._whisper = await asyncio.to_thread(WhisperModel, WHISPER_MODEL, device="cpu", compute_type="int8")
        return self._whisper

    async def _transcribe(self, samples) -> str:
        model = await self._get_whisper()

        def run():
            segs, _ = model.transcribe(samples, beam_size=5)
            return " ".join(s.text for s in segs).strip()
        return await asyncio.to_thread(run)

    async def _check_recording(self, audio: bytes) -> Tuple[bytes, str]:
        """Length + consent check. Returns (recording as OGG/Opus, transcript); raises _CloneRejected with a reason."""
        try:
            samples = await asyncio.to_thread(_decode_16k_mono, audio)
        except Exception:
            raise _CloneRejected("I couldn't read that audio file.")
        secs = len(samples) / 16000
        if secs < 0.5:
            raise _CloneRejected("I couldn't read any audio in that file. A voice message, .m4a, .mp3, .wav or .ogg works.")
        if secs < CLONE_MIN_SECS:
            raise _CloneRejected(f"That was only {secs:.0f}s; read the whole script ({CLONE_MIN_SECS}+ seconds).")
        if secs > CLONE_MAX_SECS:
            raise _CloneRejected(f"That's over {CLONE_MAX_SECS // 60} minutes; the script alone is plenty.")
        transcript = await self._transcribe(samples)
        if not _consent_heard(transcript):
            raise _CloneRejected("I couldn't hear the consent sentence at the start "
                                 f"(\"{CLONE_CONSENT}\"). Make sure you read it clearly.")
        ogg = audio if audio_format(audio) == "ogg_opus" else await asyncio.to_thread(_transcode_to_ogg_opus, audio)
        return ogg, transcript

    async def _clone_preview(self, user_id: int, sample: bytes, transcript: str) -> bytes:
        """Speak a line in the would-be clone without saving anything (the recording is sent inline)."""
        audio = await self.fish.synth(CLONE_PREVIEW_LINE, None, reference=(sample, transcript))
        cost = fish_cost(CLONE_PREVIEW_LINE, self.fish.model)
        await self.budget.record("fish", cost, chars=len(CLONE_PREVIEW_LINE))
        await self._add_spend(None, user_id, cost)
        return audio

    async def _save_clone(self, user, sample: bytes, transcript: str, name: str) -> str:
        """Create the (public, named) voice; returns its ref. Falls back to a local sample if Fish refuses."""
        sample_path = None
        try:
            info = await self.fish.create_model(
                title=name[:100], audio=sample, text=transcript,
                description="Cloned from the creator's own recording via !narrate clone.")
            voice_id = str(info["_id"]).lower()
        except ProviderError as e:
            if e.status not in (402, 403):
                raise
            print(f"[narrate] fish model creation refused, keeping clone locally: {e}")
            key = uuid.uuid4().hex
            sample_path = f"cloned/user_{user.id}/{key}.ogg"
            root = Path(getattr(self, "persistent_audio_root", "data/audio"))
            await asyncio.to_thread(_atomic_write_bytes, root / sample_path, sample)
            voice_id = f"{FISH_REF_PREFIX}{key}"
        await self.db.execute(
            """
            INSERT INTO tts_designed_voices (guild_id, user_id, provider, voice_id, title, instruction,
                                             sample_path, sample_text, kind)
            VALUES (NULL, $1, 'fish', $2, $3, 'cloned from own recording', $4, $5, 'clone')
            """,
            user.id, voice_id, name[:128], sample_path, transcript,
        )
        return f"fish:{voice_id}"

    async def _remove_clone_row(self, voice_id: str, sample_path: Optional[str]) -> None:
        if not voice_id.startswith(FISH_REF_PREFIX) and self.fish:
            try:
                await self.fish.delete_model(voice_id)
            except Exception as e:
                print(f"[narrate] couldn't delete cloned Fish model {voice_id}: {e}")
        if sample_path:
            try:
                (Path(getattr(self, "persistent_audio_root", "data/audio")) / sample_path).unlink(missing_ok=True)
            except OSError as e:
                print(f"[narrate] couldn't remove clone sample {sample_path}: {e}")
        await self.db.execute("DELETE FROM tts_designed_voices WHERE kind='clone' AND voice_id=$1", voice_id)
        # It's public, so anyone using it goes back to the default voice
        await self.db.execute(
            "UPDATE narrate_prefs SET voice=NULL, voice_label=NULL, updated_at=CURRENT_TIMESTAMP WHERE voice=$1",
            f"fish:{voice_id}")
        self.router.forget_reference(voice_id)

    async def _find_own_clone(self, user_id: int, name: Optional[str]):
        """(row or None, all of the user's clones) where row is the clone matching `name` (or the only one)."""
        rows = await self.db.fetch(
            "SELECT voice_id, title, sample_path FROM tts_designed_voices WHERE kind='clone' AND user_id=$1 "
            "ORDER BY created_at DESC", user_id)
        rows = list(rows)
        if not name:
            return (rows[0] if len(rows) == 1 else None), rows
        q = " ".join(name.split()).lower()
        exact = [r for r in rows if r["title"].lower() == q]
        partial = [r for r in rows if q in r["title"].lower()]
        match = exact[0] if exact else (partial[0] if len(partial) == 1 else None)
        return match, rows

    async def _delete_clone(self, ctx: commands.Context, name: Optional[str]):
        row, rows = await self._find_own_clone(ctx.author.id, name)
        if not rows:
            return await ctx.send("You don't have any cloned voices.", suppress_embeds=True)
        if not row:
            names = ", ".join(f"`{r['title']}`" for r in rows)
            return await ctx.send(f"Which one? `{ctx.clean_prefix}narrate clone delete <name>`. Yours: {names}",
                                  suppress_embeds=True)
        await self._remove_clone_row(row["voice_id"], row["sample_path"])
        await ctx.send(f"🗑️ Deleted **{discord.utils.escape_markdown(row['title'])}**, here and from the voice "
                       "service. Anyone who was using it is back on the default voice.", suppress_embeds=True)

    async def _load_designed_sample(self, key: str) -> Optional[Tuple[bytes, str]]:
        """Router callback for fish:ref:<key> voices: (ogg sample, transcript), or None."""
        row = await self.db.fetchrow(
            """
            SELECT sample_path, sample_text
            FROM tts_designed_voices
            WHERE provider='fish' AND voice_id=$1 AND sample_path IS NOT NULL
            """,
            f"{FISH_REF_PREFIX}{key}",
        )
        if not row:
            return None
        p = Path(getattr(self, "persistent_audio_root", "data/audio")) / row["sample_path"]
        try:
            audio = await asyncio.to_thread(p.read_bytes)
        except OSError as e:
            print(f"[narrate] designed voice sample missing at {p}: {e}")
            return None
        return audio, row["sample_text"] or DESIGN_PREVIEW_LINE

    async def _record_voice_history(self, guild_id: int, user_id: int, voice: str, label: Optional[str]) -> None:
        """Append a voice pick to the user's history (skipped if it's the same as their last one)."""
        try:
            last = await self.db.fetchrow(
                """
                SELECT voice FROM narrate_voice_history
                WHERE guild_id=$1 AND user_id=$2
                ORDER BY set_at DESC, id DESC LIMIT 1
                """,
                guild_id, user_id,
            )
            if last and last["voice"] == voice:
                return
            await self.db.execute(
                """
                INSERT INTO narrate_voice_history (guild_id, user_id, voice, voice_label)
                VALUES ($1, $2, $3, $4)
                """,
                guild_id, user_id, voice[:160], (label or None) and label[:256],
            )
        except Exception as e:
            print(f"[narrate] failed to record voice history: {e}")

    @narrate_root.command(name="recent")
    async def narrate_recent(self, ctx: commands.Context, *user_input):
        """<name or mention> <[n,p]> — Voices someone has used, newest first (default: you, 15 per page)."""
        guild_id = await get_guild_id(ctx, self.db)
        if guild_id is None:
            return
        try:
            pagination, discord_id, username = await parse_user_input_for_mention(self.db, ctx, user_input)
        except Exception as e:
            print(f"Parsing User Input error: {e}")
            return await ctx.send("Ruh roh user input error")
        if not discord_id:
            if user_input and any(not a.startswith("[") for a in user_input):
                return await ctx.send("Couldn't find that person in this server.", suppress_embeds=True)
            discord_id, username = ctx.author.id, ctx.author.name
        if not pagination:
            pagination = (15, 1)
        title_descriptor = "RECENT" if pagination[0] > 0 else "OLDEST"
        try:
            rows = await self.db.fetch(
                """
                SELECT voice, voice_label, set_at FROM narrate_voice_history
                WHERE guild_id=$1 AND user_id=$2
                ORDER BY set_at DESC, id DESC
                """,
                guild_id, discord_id,
            )
        except asyncpg.exceptions.PostgresError as e:
            print(f"Database error: {e}")
            return await ctx.send("Ruh roh database error")
        if not rows:
            return await ctx.send(f"No voice history for {username} yet.", suppress_embeds=True)
        current = (await self._get_pref(guild_id, discord_id) or {}).get("voice")
        rows = await paginate(list(rows), pagination[0], pagination[1])
        message = f"------ {title_descriptor} VOICES FROM {username.upper()} ------\n"
        for r in rows:
            mark = " *" if r["voice"] == current else ""
            message += f"{r['set_at']:%Y-%m-%d} - {_voice_display(r['voice'], r['voice_label'])}{mark}\n"
            message += f"             {ctx.clean_prefix}narrate voice {r['voice']}\n"
        message += "* = current voice"
        return await send_goodly(ctx, message)

    @narrate_root.command(name="designs")
    async def narrate_designs(self, ctx: commands.Context):
        rows = await self.db.fetch(
            """
            SELECT voice_id, title, created_at, kind
            FROM tts_designed_voices
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 25
            """,
            ctx.author.id,
        )
        if not rows:
            return await ctx.send(f"You haven't made any voices yet. Try `{ctx.clean_prefix}narrate design <description>` "
                                  f"or `{ctx.clean_prefix}narrate clone`.", suppress_embeds=True)
        lines = [f"Your voices, usable in any server with `{ctx.clean_prefix}narrate voice <id>`:"]
        for r in rows:
            tag = " 🎤 *cloned*" if r["kind"] == "clone" else ""
            lines.append(f"- **{discord.utils.escape_markdown(r['title'])}**{tag}: `fish:{r['voice_id']}`")
        await ctx.send("\n".join(lines)[:1900], suppress_embeds=True)

    @narrate_root.command(name="rate")
    async def narrate_rate(self, ctx: commands.Context, *, rate: float):
        pref = await self._get_pref(ctx.guild.id, ctx.author.id)

        channel_id = (pref.get("text_channel_id") if pref else None) or ctx.channel.id
        voice      = (pref.get("voice") if pref else None) or DEFAULT_VOICE
        enabled    = bool(pref.get("enabled")) if pref else False
        rate_val   = float(rate)
        if not math.isfinite(rate_val) or not (RATE_MIN <= rate_val <= RATE_MAX):
            return await ctx.send(f"Rate must be between {RATE_MIN} and {RATE_MAX} (1.0 is normal).", suppress_embeds=True)
        rate_val = round(rate_val, 2)

        await self._upsert_pref(ctx, channel_id, voice, rate_val, enabled)
        await ctx.send(f"Speaking rate set to `{rate_val}`.", suppress_embeds=True)
            
    @narrate_root.command(name="shutoff")
    @commands.has_permissions(manage_guild=True)
    async def narrate_shutoff(self, ctx: commands.Context):
        await self._set_all_prefs_disabled(ctx.guild.id)
        session = self._get_session(ctx.guild.id)
        await self._leave_voice(ctx.guild, session)
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
                session = self._get_session(guild.id)
                if session.recently_self_disconnected():
                    # Our own recycle after a voice error; the next message reconnects. Not a kick.
                    return
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
        bot_chan = self._bot_voice_channel(guild, session)

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

            # Nobody left in the channel (bots don't count), or nobody left with narrate on → shut down
            humans_left = [m for m in getattr(bot_chan, "members", []) if not m.bot]
            if not humans_left:
                still_has_enabled = False
            else:
                try:
                    still_has_enabled = await self._any_enabled_in_channel(guild.id, bot_chan)
                except Exception as e:
                    print(f"[narrate] enabled check error: {e}")
                    still_has_enabled = True  # be conservative while people are still there

            if not still_has_enabled:
                # Do the underlying actions directly (don’t call the command since we have no ctx)
                try:
                    await self._set_all_prefs_disabled(guild.id)
                except Exception as e:
                    print(f"[narrate] failed to disable narration on leave: {e}")
                await self._leave_voice(guild, session)
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
        if _message_is_bot_command(self.bot, message.content):
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

        cleaned = _strip_parenthetical_for_tts(_clean_content(message.content or ""))
        # A leading [style] sets delivery for this message; otherwise the user's default style applies
        msg_style, cleaned = split_leading_style(cleaned)
        if not cleaned:
            return

        if len(cleaned) > MAX_NARRATE_CHARS:
            cut = cleaned[:MAX_NARRATE_CHARS]
            cleaned = cut[:cut.rfind(" ")] if " " in cut[MAX_NARRATE_CHARS // 2:] else cut
        if not self._take_rate_tokens(message.author.id, len(cleaned)):
            return  # talking faster than anyone can listen; drop rather than build a backlog

        voice = (pref.get("voice") or DEFAULT_VOICE)
        style = msg_style or pref.get("style")
        try:
            rate = float(pref.get("rate") if pref.get("rate") is not None else DEFAULT_RATE)
        except Exception:
            rate = DEFAULT_RATE
        if not math.isfinite(rate) or not (RATE_MIN <= rate <= RATE_MAX):
            rate = DEFAULT_RATE
        if parse_voice_ref(voice)[0] != "google" and await self._over_user_share(message.author.id):
            voice = DEFAULT_GOOGLE_VOICE
            await self._notify_fallback(message.guild, message.channel.id, "user_quota")
        gen = self._get_session(message.guild.id).generation
        self._enqueue_narration(message.guild.id, (message.author.id, cleaned, voice, style, rate, message.channel.id, gen))

    @narrate_root.command(name="save")
    async def narrate_save(self, ctx: commands.Context, *, voiceline_name: str):
        guild_id = await get_guild_id(ctx, self.db)
        user_id = await get_user_id(ctx, self.db)
        if guild_id is None or user_id is None:
            return

        last = self._last_narrated_audio.get((guild_id, user_id))
        if not last:
            return await ctx.send(
                "No recent narrated audio found for you in this server.",
                suppress_embeds=True,
            )
        if len(last) == 1 and last[0][1] == "ogg_opus":
            audio = last[0][0]
        else:
            try:
                audio = await asyncio.to_thread(_join_to_ogg_opus, [clip for clip, _ in last])
            except Exception as e:
                print(f"[narrate] voiceline transcode failed: {e}")
                return await ctx.send("Failed to save voiceline: couldn't convert the audio.", suppress_embeds=True)

        try:
            saved = await self._save_voiceline(
                guild_id=guild_id,
                voiceline_name=voiceline_name,
                audio_bytes=audio,
                created_by_user_id=user_id,
                fmt="ogg_opus",
            )
        except Exception as e:
            return await ctx.send(f"Failed to save voiceline: {e}", suppress_embeds=True)

        final_name = saved["name"]
        await ctx.send(f"Saved voiceline as `{final_name}`.", suppress_embeds=True)

    @narrate_root.command(name="list")
    async def narrate_list(self, ctx: commands.Context):
        guild_id = await get_guild_id(ctx, self.db)
        if guild_id is None:
            return

        try:
            rows = await self._list_voicelines(guild_id, limit=100)
        except Exception as e:
            return await ctx.send(f"Failed to list voicelines: {e}", suppress_embeds=True)

        if not rows:
            return await ctx.send("No voicelines saved in this server.", suppress_embeds=True)

        # Format conservatively for Discord limits
        lines = ["Saved voicelines (most recent first):"]
        for r in rows:
            nm = r["name"]
            sz = r["byte_size"]
            lines.append(f"- `{nm}` ({sz} bytes)")
            if len("\n".join(lines)) > 1800:
                lines.append("… (truncated)")
                break

        await ctx.send("\n".join(lines), suppress_embeds=True)

    @narrate_root.command(name="rename")
    async def narrate_rename(self, ctx: commands.Context, old_name: str, *, new_name: str):
        guild_id = await get_guild_id(ctx, self.db)
        user_id = await get_user_id(ctx, self.db)
        if guild_id is None or user_id is None:
            return

        if not await self._may_edit_voiceline(ctx, guild_id, user_id, old_name):
            return await ctx.send("Only whoever saved that voiceline (or a server manager) can rename it.", suppress_embeds=True)
        try:
            ok = await self._rename_voiceline(guild_id, old_name, new_name)
        except Exception as e:
            # If this is a unique conflict, postgres will raise; surface a clear message
            return await ctx.send(f"Rename failed: {e}", suppress_embeds=True)

        if not ok:
            return await ctx.send(f"No voiceline named `{old_name}` found.", suppress_embeds=True)

        await ctx.send(f"Renamed `{old_name}` → `{new_name}`.", suppress_embeds=True)

    @narrate_root.command(name="delete", aliases=["del", "rm"])
    async def narrate_delete(self, ctx: commands.Context, *, name: str):
        guild_id = await get_guild_id(ctx, self.db)
        user_id = await get_user_id(ctx, self.db)
        if guild_id is None or user_id is None:
            return

        if not await self._may_edit_voiceline(ctx, guild_id, user_id, name):
            return await ctx.send("Only whoever saved that voiceline (or a server manager) can delete it.", suppress_embeds=True)
        try:
            ok = await self._delete_voiceline(guild_id, name)
        except Exception as e:
            return await ctx.send(f"Delete failed: {e}", suppress_embeds=True)

        if not ok:
            return await ctx.send(f"No voiceline named `{name}` found.", suppress_embeds=True)

        await ctx.send(f"Deleted voiceline `{name}`.", suppress_embeds=True)

    async def _gemini_fallbacks(self, guild_id: int, user_id: int, voice_ref: str) -> List[Tuple[str, Optional[str]]]:
        """
        Voices to use when a Gemini voice can't speak, in order:
          1. the user's most recent non-Gemini voice (their history)
          2. Google's free Chirp3-HD version of the same voice name (the 30 standard Gemini voices have one)
          3. a free Chirp3-HD voice picked from the user id, so it's always the same one for them
        """
        key = (guild_id, user_id, voice_ref)
        cached = self._fallback_cache.get(key)
        if cached and time.monotonic() - cached[0] < 600:
            return cached[1]
        refs: List[Tuple[str, Optional[str]]] = []
        try:
            row = await self.db.fetchrow(
                """
                SELECT voice, voice_label FROM narrate_voice_history
                WHERE guild_id=$1 AND user_id=$2 AND voice NOT LIKE 'gemini:%'
                ORDER BY set_at DESC, id DESC LIMIT 1
                """,
                guild_id, user_id,
            )
            if row:
                refs.append((row["voice"], row["voice_label"]))
            name = split_gemini_voice(parse_voice_ref(voice_ref)[1])[0]
            same = await self.db.fetchrow(
                "SELECT voice_name, nickname FROM google_tts_voices WHERE lower(voice_name) = lower($1)",
                f"en-US-Chirp3-HD-{name}",
            )
            if same:
                refs.append((same["voice_name"], same["nickname"]))
            else:
                pool = await self.db.fetch(
                    "SELECT voice_name, nickname FROM google_tts_voices "
                    "WHERE voice_name LIKE 'en-US-Chirp3-HD-%' ORDER BY voice_name")
                if pool:
                    pick = pool[user_id % len(pool)]
                    refs.append((pick["voice_name"], pick["nickname"]))
        except Exception as e:
            print(f"[narrate] couldn't work out fallback voices: {e}")
        self._fallback_cache[key] = (time.monotonic(), refs)
        return refs

    async def _switch_off_gemini(self, guild: discord.Guild, user_id: int, channel_id: int,
                                 old_ref: str, new_ref: str, label: Optional[str]) -> None:
        """Save the stand-in as the user's voice and tell them how to switch back."""
        try:
            await self.db.execute(
                """
                UPDATE narrate_prefs SET voice=$3, voice_label=$4, updated_at=CURRENT_TIMESTAMP
                WHERE guild_id=$1 AND user_id=$2 AND voice=$5
                """,
                guild.id, user_id, new_ref, label, old_ref,
            )
            await self._record_voice_history(guild.id, user_id, new_ref, label)
        except Exception as e:
            print(f"[narrate] failed to save the switch off Gemini: {e}")
            return
        self._fallback_cache = {k: v for k, v in self._fallback_cache.items() if k[:2] != (guild.id, user_id)}
        member = guild.get_member(user_id)
        who = discord.utils.escape_markdown(member.display_name) if member else "Someone"
        prefix = self.bot.command_prefix if isinstance(self.bot.command_prefix, str) else "!"
        ch = guild.get_channel(channel_id)
        if ch:
            try:
                await ch.send(
                    f"Gemini voices are out of quota for now, so **{who}** now uses "
                    f"**{discord.utils.escape_markdown(_voice_display(new_ref, label))}**. "
                    f"Switch back later with `{prefix}narrate voice {old_ref}`.",
                    suppress_embeds=True,
                )
            except Exception:
                pass

    async def _may_edit_voiceline(self, ctx: commands.Context, guild_id: int, user_id: int, name: str) -> bool:
        """Creator or manage_guild. Unknown names return True so the command reports 'not found'."""
        if ctx.author.guild_permissions.manage_guild:
            return True
        row = await self.db.fetchrow(
            "SELECT created_by_user_id FROM guild_voicelines WHERE guild_id=$1 AND name=$2",
            int(guild_id), _normalize_voiceline(name or ""),
        )
        return row is None or row["created_by_user_id"] == user_id

    async def _notify_fallback(self, guild: discord.Guild, channel_id: int, reason: str) -> None:
        """Tell the channel a paid voice fell back to Google, without repeating it every message."""
        notice, cooldown = FALLBACK_NOTICES.get(reason, FALLBACK_NOTICES["error"])
        month = date.today().replace(day=1)
        now = time.monotonic()
        prev = self._fallback_notices.get((guild.id, reason))
        if prev:
            prev_month, prev_t = prev
            if cooldown is None and prev_month == month:
                return
            if cooldown is not None and now - prev_t < cooldown:
                return
        self._fallback_notices[(guild.id, reason)] = (month, now)
        ch = guild.get_channel(channel_id)
        if ch:
            try:
                await ch.send(notice, suppress_embeds=True)
            except Exception:
                pass

    async def _guild_worker(self, guild_id: int, q: asyncio.Queue):
        """Synthesize this guild's messages in order; one bad message never kills the worker."""
        try:
            while True:
                item = await q.get()
                try:
                    await self._narrate_one(guild_id, *item)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[narrate] worker error: guild={guild_id}: {e!r}")
                finally:
                    q.task_done()
        except asyncio.CancelledError:
            return

    async def _narrate_one(self, guild_id: int, user_id: int, text: str, voice: str, style: Optional[str],
                           rate: float, channel_id: int, gen: int):
        if not text:
            return
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        session = self._get_session(guild_id)
        if session.generation != gen:
            return  # cancelled / shut off / torn down since this was queued

        # Prefs can change while a message waits (off, shutoff, kick, channel change): re-check them.
        pref = await self._get_pref(guild_id, user_id)
        if not pref or not pref.get("enabled") or pref.get("text_channel_id") != channel_id:
            return
        member = guild.get_member(user_id)
        if not member or not (member.voice and member.voice.channel):
            return

        ### CHECK IF VOICELINE EXISTS (in which case tts api won't be called) ###
        try:
            voiceline_data = await self._resolve_voiceline_override(
                guild_id=guild_id,
                text=text
            )  # returns None if no voiceline found
        except Exception as e:
            ch = guild.get_channel(channel_id)
            if ch:
                try:
                    await ch.send(
                        "Voiceline retrieval failed",
                        suppress_embeds=True,
                    )
                except Exception:
                    pass
            voiceline_data = None
            print(f"[narrate] voiceline retrieval check failed for {text!r}: {e}")

        try:
            await session.ensure_connected(member.voice.channel)
        except Exception as e:
            print(f"[narrate] ensure_connected error: {e}")
            return

        if voiceline_data:
            self._last_narrated_audio[(guild_id, user_id)] = [(voiceline_data, "ogg_opus")]
            await session.enqueue(voiceline_data)
            return

        # NO voiceline data -- do normal tts api call
        first, cap = self.router.chunk_plan(voice)
        chunks = chunk_for_speech(text, first, cap)
        clips: List[Tuple[bytes, str]] = []
        self._last_narrated_audio[(guild_id, user_id)] = clips
        try:
            for part in chunks:
                if session.generation != gen:
                    return  # cancelled mid-message: don't pay for the rest
                is_gemini = parse_voice_ref(voice)[0] == "gemini"
                fallbacks = await self._gemini_fallbacks(guild_id, user_id, voice) if is_gemini else []
                res = await self.router.synth(voice, part, style=style, rate=rate,
                                              fallback_refs=[ref for ref, _ in fallbacks])
                print(f"[narrate] guild={guild_id} user={user_id} voice={voice} -> "
                      f"{res.voice_ref_used or voice} via {res.provider}/{res.model or '-'}"
                      + (f" (fallback: {res.fallback_reason})" if res.fallback_reason else ""))
                if is_gemini and res.fallback_reason == "gemini_limit" and res.voice_ref_used:
                    # Every Gemini model is out: make the stand-in their voice so it stays put instead of
                    # flipping back and forth as quota comes and goes. One notice explains how to go back.
                    label = dict(fallbacks).get(res.voice_ref_used)
                    await self._switch_off_gemini(guild, user_id, channel_id, voice, res.voice_ref_used, label)
                    voice = res.voice_ref_used
                elif res.fallback_reason:
                    await self._notify_fallback(guild, channel_id, res.fallback_reason)
                if session.generation != gen:
                    return
                await self._add_spend(guild_id, user_id, res.cost_usd)
                clips.append((res.audio, res.fmt))
                await session.enqueue(res.audio)
        except Exception as e:
            ch = guild.get_channel(channel_id)
            if ch:
                try:
                    await ch.send(
                        "TTS failed.",
                        suppress_embeds=True,
                    )
                except Exception:
                    pass
            print(f"[narrate] synth error: {e}")
