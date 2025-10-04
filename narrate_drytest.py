# test_narrate_sim.py
import asyncio
import time
from collections import deque, Counter
from types import SimpleNamespace

# Import your cog and constants
from bot_narrate import (
    NarrationCog,
    DEFAULT_VOICE,
    DEFAULT_LANG,
    DEFAULT_RATE,
    MAX_CHARS_PER_CHUNK,
    CHUNK_COALESCE_WINDOW_MS,
    EARCON_ENABLED,
)

# ---------- Tiny stubs for "discord.py" objects we need ----------
class FakeTextChannel:
    def __init__(self, gid, cid, name="#text"):
        self.id = cid
        self.guild = SimpleNamespace(id=gid)
        self.mention = f"<#{cid}>"

class FakeVoiceChannel:
    def __init__(self, gid, cid, name="VC"):
        self.id = cid
        self.guild = SimpleNamespace(id=gid)
        self.name = name

class FakeVoiceState:
    def __init__(self, channel):
        self.channel = channel

class FakeMember:
    def __init__(self, gid, uid, voice_channel=None, name=None):
        self.id = uid
        self.guild = SimpleNamespace(id=gid)
        self.bot = False
        self.name = name or f"user{uid}"
        self.voice = SimpleNamespace(channel=voice_channel) if voice_channel else None

class FakeMessage:
    def __init__(self, gid, author: FakeMember, channel: FakeTextChannel, content: str):
        self.guild = SimpleNamespace(id=gid, get_channel=lambda _id: channel)
        self.author = author
        self.channel = channel
        self.content = content

# ---------- In-memory prefs ----------
# prefs[(guild_id, user_id)] = dict(text_channel_id=..., voice=..., rate=..., enabled=bool)
PREFS = {}

# ---------- Monkeypatch the cog: in-memory DB + stub TTS + sim session ----------
class StubTTS:
    """Very fast, deterministic TTS: returns bytes that include the text and voice markers."""
    async def start(self): pass
    async def close(self): pass
    async def synth(self, text, voice_name=DEFAULT_VOICE, language_code=DEFAULT_LANG, speaking_rate=DEFAULT_RATE, audio_encoding="OGG_OPUS"):
        marker = f"[{voice_name}|{speaking_rate}] {text}".encode("utf-8")
        return marker

def chunk_text(text: str, limit: int = MAX_CHARS_PER_CHUNK):
    text = text.strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    out, cur, cur_len = [], [], 0
    for tok in text.split():
        add = len(tok) + (1 if cur else 0)
        if cur_len + add > limit:
            out.append(" ".join(cur))
            cur, cur_len = [tok], len(tok)
        else:
            cur.append(tok)
            cur_len += add
    if cur:
        out.append(" ".join(cur))
    return out

class SimGuildSession:
    """
    Minimal stand-in for GuildVoiceSession that *logs* what would play.
    - most-recent-wins: `active_user_id` is set by the cog; we just track moves.
    - enqueue(...) logs entries in order; we don't do a real audio queue.
    - recent_speakers supports earcon decisions identical to real cog.
    """
    def __init__(self, guild_id, tts):
        self.guild_id = guild_id
        self.tts = tts
        self.connected_channel_id = None
        self.active_user_id = None
        self.log = []  # list of tuples: ("MOVE", channel_id) or ("EARCON", user_id, voice) or ("PLAY", user_id, voice, text_fragment)
        self.recent_speakers = deque(maxlen=50)
        self.RECENT_WINDOW = 6.0

    async def ensure_connected(self, channel):
        if self.connected_channel_id != channel.id:
            self.connected_channel_id = channel.id
            self.log.append(("MOVE", channel.id))

    def _voices_in_recent_window(self):
        now = time.time()
        while self.recent_speakers and (now - self.recent_speakers[0][2] > self.RECENT_WINDOW):
            self.recent_speakers.popleft()
        distinct = {}
        for uid, vname, ts in self.recent_speakers:
            distinct.setdefault(vname, set()).add(uid)
        c = Counter({v: len(uids) for v, uids in distinct.items()})
        return c

    async def _get_earcon_bytes(self):
        # In sim, we don't need real bytes; we just log the event.
        return b"[EARCON]"

    async def enqueue(self, audio_bytes: bytes, user_id: int, voice_name: str, label: str, is_earcon: bool = False):
        if is_earcon:
            self.log.append(("EARCON", user_id, voice_name))
        else:
            # decode for readability in log
            try:
                frag = audio_bytes.decode("utf-8")
            except Exception:
                frag = str(len(audio_bytes)) + "bytes"
            self.log.append(("PLAY", user_id, voice_name, frag))

class SimNarrationCog(NarrationCog):
    """Overrides DB helpers, TTS provider, and session creation for a pure in-memory run."""
    def __init__(self):
        # Fake bot object with only what's accessed
        fake_bot = SimpleNamespace(db_pool=None)
        super().__init__(fake_bot)
        # Replace TTS with stub
        self.tts = StubTTS()
        # Replace sessions map
        self.guild_sessions = {}
        # Replace background coalescer with our own; cancel the parent task and start anew
        if self._coalesce_task and not self._coalesce_task.done():
            self._coalesce_task.cancel()
        self._coalesce_task = asyncio.create_task(self._coalesce_loop())

    def _get_session(self, guild_id: int):
        s = self.guild_sessions.get(guild_id)
        if not s:
            s = SimGuildSession(guild_id, self.tts)
            self.guild_sessions[guild_id] = s
        return s

    # DB helpers against in-memory dict
    async def _get_pref(self, guild_id: int, user_id: int):
        return PREFS.get((guild_id, user_id))

    async def _upsert_pref(self, guild_id, user_id, text_channel_id, voice, rate, enabled):
        PREFS[(guild_id, user_id)] = {
            "guild_id": guild_id,
            "user_id": user_id,
            "text_channel_id": text_channel_id,
            "voice": voice,
            "rate": rate,
            "enabled": enabled,
        }

    async def _set_enabled(self, guild_id, user_id, enabled):
        p = PREFS.get((guild_id, user_id))
        if p:
            p["enabled"] = enabled

# --------------- Scenario helpers ---------------
async def setup_prefs(cog: SimNarrationCog, guild_id: int, users, text_channel_id: int, voices):
    """users: [uid,...]; voices: dict uid->voice_name"""
    for uid in users:
        await cog._upsert_pref(
            guild_id=guild_id,
            user_id=uid,
            text_channel_id=text_channel_id,
            voice=voices.get(uid, DEFAULT_VOICE),
            rate=1.0,
            enabled=True
        )

async def simulate_message(cog: SimNarrationCog, guild_id: int, author: FakeMember, channel: FakeTextChannel, content: str):
    msg = FakeMessage(gid=guild_id, author=author, channel=channel, content=content)
    await cog.on_message(msg)

async def simulate_voice_join(cog: SimNarrationCog, member: FakeMember, new_channel: FakeVoiceChannel):
    before = FakeVoiceState(channel=None)
    after  = FakeVoiceState(channel=new_channel)
    member.voice = SimpleNamespace(channel=new_channel)
    await cog.on_voice_state_update(member, before, after)

# --------------- Scenarios ---------------
async def scenario_single_user_most_recent():
    print("\n=== Scenario A: Single user; most-recent-wins re-connect ===")
    cog = SimNarrationCog()
    gid = 10
    u1 = FakeMember(gid, 101)
    tc = FakeTextChannel(gid, 201, "#narrate")
    vc1 = FakeVoiceChannel(gid, 301, "VC-1")
    vc2 = FakeVoiceChannel(gid, 302, "VC-2")

    await setup_prefs(cog, gid, [u1.id], tc.id, {u1.id: "en-US-Wavenet-D"})
    # user joins VC-1 (claims session)
    await simulate_voice_join(cog, u1, vc1)
    # user types (moves to VC-1 already ok)
    await simulate_message(cog, gid, u1, tc, "hello from vc1")
    # same user moves to VC-2 (most-recent wins -> move)
    await simulate_voice_join(cog, u1, vc2)
    # types again
    await simulate_message(cog, gid, u1, tc, "now in vc2")

    await asyncio.sleep((CHUNK_COALESCE_WINDOW_MS + 50)/1000)
    log = cog._get_session(gid).log
    for e in log:
        print(e)

async def scenario_two_users_different_voices():
    print("\n=== Scenario B: Two users, DIFFERENT voices → NO earcon ===")
    cog = SimNarrationCog()
    gid = 11
    u1 = FakeMember(gid, 111)
    u2 = FakeMember(gid, 112)
    tc = FakeTextChannel(gid, 211, "#narrate")
    vc = FakeVoiceChannel(gid, 311, "VC-A")

    await setup_prefs(cog, gid, [u1.id, u2.id], tc.id, {
        u1.id: "en-US-Wavenet-D",
        u2.id: "en-GB-Wavenet-B"  # different locale/voice
    })

    await simulate_voice_join(cog, u1, vc)
    await simulate_message(cog, gid, u1, tc, "one")
    # near-concurrent second user (most-recent wins → move stays same channel if same)
    await simulate_voice_join(cog, u2, vc)
    await simulate_message(cog, gid, u2, tc, "two")

    await asyncio.sleep((CHUNK_COALESCE_WINDOW_MS + 50)/1000)
    for e in cog._get_session(gid).log:
        print(e)
    # Expect: PLAY entries only, no ("EARCON", ...)

async def scenario_two_users_same_voice():
    print("\n=== Scenario C: Two users, SAME voice → Earcon enabled ===")
    if not EARCON_ENABLED:
        print("Earcon disabled via flag; enable in bot_narrate.py to test.")
    cog = SimNarrationCog()
    gid = 12
    u1 = FakeMember(gid, 121)
    u2 = FakeMember(gid, 122)
    tc = FakeTextChannel(gid, 221, "#narrate")
    vc = FakeVoiceChannel(gid, 321, "VC-B")

    same_voice = "en-US-Wavenet-D"
    await setup_prefs(cog, gid, [u1.id, u2.id], tc.id, {
        u1.id: same_voice,
        u2.id: same_voice
    })

    # user1 speaks
    await simulate_voice_join(cog, u1, vc)
    await simulate_message(cog, gid, u1, tc, "alpha")
    await asyncio.sleep((CHUNK_COALESCE_WINDOW_MS + 10)/1000)

    # user2 speaks shortly after with SAME voice → expect earcon before user2 chunk
    await simulate_voice_join(cog, u2, vc)
    await simulate_message(cog, gid, u2, tc, "bravo")
    await asyncio.sleep((CHUNK_COALESCE_WINDOW_MS + 50)/1000)

    for e in cog._get_session(gid).log:
        print(e)
    # Expect: no earcon for first user; an ("EARCON", user2, voice) before user2's PLAY

async def scenario_most_recent_wins_moves_between_vcs():
    print("\n=== Scenario D: Most-recent-wins across different VCs ===")
    cog = SimNarrationCog()
    gid = 13
    u1 = FakeMember(gid, 131)
    u2 = FakeMember(gid, 132)
    tc = FakeTextChannel(gid, 231, "#narrate")
    vc1 = FakeVoiceChannel(gid, 331, "VC-1")
    vc2 = FakeVoiceChannel(gid, 332, "VC-2")

    await setup_prefs(cog, gid, [u1.id, u2.id], tc.id, {
        u1.id: "en-US-Wavenet-D",
        u2.id: "en-US-Wavenet-E"
    })

    await simulate_voice_join(cog, u1, vc1)
    await simulate_message(cog, gid, u1, tc, "u1 here")
    await asyncio.sleep((CHUNK_COALESCE_WINDOW_MS + 10)/1000)

    # u2 takes over from another VC → bot should MOVE to vc2
    await simulate_voice_join(cog, u2, vc2)
    await simulate_message(cog, gid, u2, tc, "u2 took control")
    await asyncio.sleep((CHUNK_COALESCE_WINDOW_MS + 10)/1000)

    for e in cog._get_session(gid).log:
        print(e)
    # Expect to see ("MOVE", 331) early, then later ("MOVE", 332)

# (Optional) AMIX: we only assert the grouping *intention* here; actual mixing is tested in integration.
async def scenario_amix_flag_note():
    print("\n=== Scenario E: AMIX flag note ===")
    from bot_narrate import AMIX_ENABLED, AMIX_MAX_INPUTS, AMIX_GROUP_WINDOW
    print(f"AMIX_ENABLED={AMIX_ENABLED}, AMIX_MAX_INPUTS={AMIX_MAX_INPUTS}, AMIX_GROUP_WINDOW={AMIX_GROUP_WINDOW}s")
    print("This offline sim validates enqueue/earcon logic. Real AMIX behavior is exercised in a voice integration test.")

# ---------- Main ----------
async def main():
    await scenario_single_user_most_recent()
    await scenario_two_users_different_voices()
    await scenario_two_users_same_voice()
    await scenario_most_recent_wins_moves_between_vcs()
    await scenario_amix_flag_note()

if __name__ == "__main__":
    asyncio.run(main())
