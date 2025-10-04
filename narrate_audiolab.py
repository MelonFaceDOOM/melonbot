# narrate_audiolab.py
# Live audio tests (no Discord): Google TTS + ffmpeg/ffplay
# Requires: pip install aiohttp; ffmpeg/ffplay in PATH; config.py with google_narrate_key

import asyncio
import base64
import os
import tempfile
import subprocess
import time
from dataclasses import dataclass
from typing import List, Optional

import aiohttp

try:
    from config import google_narrate_key
except Exception:
    google_narrate_key = os.getenv("GOOGLE_NARRATE_KEY") or ""

# ---------- Tunables ----------
FFMPEG = "ffmpeg"
FFPLAY = "ffplay"
GOOGLE_TTS_ENDPOINT = "https://texttospeech.googleapis.com/v1/text:synthesize?key={api_key}"

DEFAULT_LANG = "en-US"
VOICE_A = "en-US-Wavenet-D"
VOICE_B = "en-GB-Wavenet-B"  # different enough to hear contrast
SPEAK_RATE = 1.0

MAX_CHARS_PER_CHUNK = 180
FIRST_CHUNK_PREFETCH = True  # synth first chunk immediately; prefetch rest concurrently

# Earcon
EARCON_FREQ_HZ = 880
EARCON_DURATION_S = 0.18
EARCON_BPS = "32k"

# AMIX
AMIX_DURATION = "longest"
AMIX_DROPOUT = 0

# ---------- Helpers ----------
def chunk_text(text: str, limit: int = MAX_CHARS_PER_CHUNK) -> List[str]:
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

async def ffplay_bytes(audio_bytes: bytes, blocking: bool = True) -> None:
    """
    Play compressed audio bytes (e.g., OGG/OPUS/MP3) via ffplay.
    """
    # -nodisp: no window; -autoexit: ends when stream ends; -i pipe:0 to read stdin
    p = subprocess.Popen(
        [FFPLAY, "-nodisp", "-autoexit", "-hide_banner", "-loglevel", "error", "-i", "pipe:0"],
        stdin=subprocess.PIPE
    )
    try:
        p.stdin.write(audio_bytes)
        p.stdin.close()
    except Exception:
        pass
    if blocking:
        p.wait()

async def ffmpeg_beep_ogg(freq_hz=EARCON_FREQ_HZ, dur_s=EARCON_DURATION_S, bitrate=EARCON_BPS) -> bytes:
    """
    Generate a short sine beep as OGG/Opus bytes.
    """
    cmd = [
        FFMPEG, "-v", "error",
        "-f", "lavfi", "-i", f"sine=frequency={freq_hz}:duration={dur_s}:sample_rate=48000",
        "-c:a", "libopus", "-b:a", bitrate, "-f", "ogg", "pipe:1"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    return out or b""

async def amix_files_to_ogg(input_paths: List[str], bitrate="64k") -> bytes:
    """
    Mix multiple files with ffmpeg amix → OGG/Opus bytes.
    """
    if len(input_paths) == 1:
        with open(input_paths[0], "rb") as f:
            return f.read()

    cmd = [FFMPEG, "-v", "error"]
    for p in input_paths:
        cmd += ["-i", p]
    cmd += [
        "-filter_complex", f"amix=inputs={len(input_paths)}:duration={AMIX_DURATION}:dropout_transition={AMIX_DROPOUT}",
        "-c:a", "libopus", "-b:a", bitrate, "-f", "ogg", "pipe:1"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    return out or b""

# ---------- Google TTS ----------
class GoogleTTS:
    def __init__(self, api_key: str, session: Optional[aiohttp.ClientSession] = None):
        self.api_key = api_key
        self.session = session
        self._own_session = False

    async def __aenter__(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
            self._own_session = True
        return self

    async def __aexit__(self, *_):
        if self._own_session and self.session:
            await self.session.close()
            self.session = None

    async def synth(self, text: str, voice: str = VOICE_A, rate: float = SPEAK_RATE, lang: str = DEFAULT_LANG, encoding="OGG_OPUS") -> bytes:
        url = GOOGLE_TTS_ENDPOINT.format(api_key=self.api_key)
        payload = {
            "input": {"text": text},
            "voice": {"languageCode": lang, "name": voice},
            "audioConfig": {"audioEncoding": encoding, "speakingRate": rate}
        }
        async with self.session.post(url, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Google TTS error {resp.status}: {body[:300]}")
            data = await resp.json()
        b64 = data.get("audioContent")
        if not b64:
            raise RuntimeError("Missing audioContent")
        return base64.b64decode(b64)

# ---------- Scenario drivers ----------
@dataclass
class Utterance:
    who: str
    text: str
    voice: str

async def play_sequential(utterances: List[Utterance], gtts: GoogleTTS, with_earcon: bool = False):
    """
    Sequential playback with optional earcon before each utterance (used when voices are identical across speakers).
    """
    last_voice_by_user = {}
    beep = await ffmpeg_beep_ogg() if with_earcon else b""
    for utt in utterances:
        # Earcon rule: if enabled and multiple users share same voice, play before utterance from *second* speaker onwards
        if with_earcon:
            # Simple heuristic: play earcon when speaker switches (simulates shared-voice confusion)
            if last_voice_by_user.get("prev_voice") == utt.voice and last_voice_by_user.get("prev_speaker") != utt.who:
                await ffplay_bytes(beep, blocking=True)
        # speak
        chunks = chunk_text(utt.text)
        if not chunks:
            continue
        # first chunk now
        audio0 = await gtts.synth(chunks[0], voice=utt.voice)
        await ffplay_bytes(audio0, blocking=True)
        # rest prefetch + play
        for c in chunks[1:]:
            audio = await gtts.synth(c, voice=utt.voice)
            await ffplay_bytes(audio, blocking=True)
        last_voice_by_user["prev_voice"] = utt.voice
        last_voice_by_user["prev_speaker"] = utt.who

async def play_amix(utterances: List[Utterance], gtts: GoogleTTS):
    """
    Overlapped playback via ffmpeg amix. We synth each utterance to a temp OGG, mix to a single OGG, then play.
    """
    tmp_files = []
    try:
        # synth all
        for utt in utterances:
            audio = await gtts.synth(utt.text, voice=utt.voice)
            tf = tempfile.NamedTemporaryFile(delete=False, suffix=".ogg")
            tf.write(audio)
            tf.flush()
            tf.close()
            tmp_files.append(tf.name)
        # mix
        mixed = await amix_files_to_ogg(tmp_files)
        await ffplay_bytes(mixed, blocking=True)
    finally:
        for p in tmp_files:
            try:
                os.unlink(p)
            except Exception:
                pass

# ---------- Pre-canned scenarios ----------
async def scenario_one_user(gtts: GoogleTTS):
    print("\n[Scenario] One user, sequential (chunked)")
    U = "Alice"
    await play_sequential([
        Utterance(U, "Okay, testing one two three. Low latency chunking should feel snappy.", VOICE_A),
        Utterance(U, "Second sentence. This one should start quickly as well.", VOICE_A),
    ], gtts, with_earcon=False)

async def scenario_two_diff(gtts: GoogleTTS):
    print("\n[Scenario] Two users, DIFFERENT voices (no earcon)")
    await play_sequential([
        Utterance("Alice", "Hello there. I am using the American voice.", VOICE_A),
        Utterance("Bob",   "And I am using the British voice. No earcon should play.", VOICE_B),
        Utterance("Alice", "Back to me. Still no earcon, because the voices differ.", VOICE_A),
    ], gtts, with_earcon=False)

async def scenario_two_same(gtts: GoogleTTS):
    print("\n[Scenario] Two users, SAME voice (earcon before second speaker)")
    await play_sequential([
        Utterance("Alice", "This is Alice, with the default voice.", VOICE_A),
        Utterance("Bob",   "Bob here, same voice. You should hear a short beep first.", VOICE_A),
        Utterance("Alice", "Alice again, same voice; earcon should precede Bob only.", VOICE_A),
    ], gtts, with_earcon=True)

async def scenario_two_amix(gtts: GoogleTTS):
    print("\n[Scenario] Two simultaneous (AMIX)")
    await play_amix([
        Utterance("Alice", "Talking at the same time can be hard to understand.", VOICE_A),
        Utterance("Bob",   "Overlapping speech is usually not recommended.", VOICE_B),
    ], gtts)

async def scenario_three_amix(gtts: GoogleTTS):
    print("\n[Scenario] Three simultaneous (AMIX)")
    await play_amix([
        Utterance("Alice", "This is the first overlapping voice.", VOICE_A),
        Utterance("Bob",   "Here is the second overlapping voice.", VOICE_B),
        Utterance("Cara",  "And a third voice overlaps as well.", VOICE_A),  # reuse A to test intelligibility
    ], gtts)

# ---------- CLI ----------
import argparse

async def main():
    if not google_narrate_key:
        raise SystemExit("Missing Google API key. Put google_narrate_key in config.py or set GOOGLE_NARRATE_KEY env var.")

    parser = argparse.ArgumentParser(description="Audio lab for narration blending (Google TTS + ffmpeg/ffplay).")
    parser.add_argument("--scenario", choices=["one", "two_diff", "two_same", "two_amix", "three_amix", "all"], default="all")
    args = parser.parse_args()

    async with GoogleTTS(google_narrate_key) as gtts:
        # quick sanity: generate a tiny beep so ffplay window permissions are primed on some OSes
        _ = await ffmpeg_beep_ogg()

        if args.scenario in ("one", "all"):
            await scenario_one_user(gtts)
        if args.scenario in ("two_diff", "all"):
            await scenario_two_diff(gtts)
        if args.scenario in ("two_same", "all"):
            await scenario_two_same(gtts)
        if args.scenario in ("two_amix", "all"):
            await scenario_two_amix(gtts)
        if args.scenario in ("three_amix", "all"):
            await scenario_three_amix(gtts)

if __name__ == "__main__":
    asyncio.run(main())
