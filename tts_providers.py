"""
TTS providers for narration plus a router that picks one per voice ref.

Voice refs (what narrate_prefs.voice stores):
  fish:<model id>      Fish Audio voice model (https://fish.audio/voice-library/)
  fish:ref:<key>       designed voice stored as a local sample, sent inline with each request
  gemini:<voice id>    Gemini TTS prebuilt voice (e.g. gemini:kore)
  en-US-Wavenet-D      anything else is a Google Cloud TTS voice name (unchanged from before)

Google is free (within our usage) and is the fallback whenever a paid provider is
over budget, rate limited, misconfigured, or erroring.
"""
import asyncio
import base64
import zlib
import io
import json
import re
import time
import wave
from dataclasses import dataclass
from datetime import date
from typing import Awaitable, Callable, Dict, Hashable, List, Optional, Sequence, Tuple

import aiohttp

import tts_text

# ==========================
# Shared knobs
# ==========================
CACHE_MAX_ITEMS = 512                      # in-memory audio cache entries
GLOBAL_TTS_CONCURRENCY = 10                # simple protection for many guilds
DEFAULT_GOOGLE_VOICE = "en-US-Wavenet-D"   # free fallback; must be a full canonical name
DEFAULT_LANG = "en-US"
DEFAULT_RATE = 1.0

GOOGLE_TTS_ENDPOINT = "https://texttospeech.googleapis.com/v1/text:synthesize"
FISH_TTS_ENDPOINT = "https://api.fish.audio/v1/tts"
FISH_MODEL_ENDPOINT = "https://api.fish.audio/model"
FISH_VOICE_DESIGN_ENDPOINT = "https://api.fish.audio/v1/voice-design"
GEMINI_INTERACTIONS_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/interactions"
GEMINI_VOICES_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/voices"

_FISH_ID_RE = re.compile(r"\b([0-9a-f]{32})\b", re.I)
FISH_REF_PREFIX = "ref:"   # fish:ref:<key> = designed voice kept as a local sample, not a Fish model

# ==========================
# Pricing (USD) for the monthly budget estimate
# ==========================
FISH_USD_PER_M_BYTES = {"s2.1-pro": 15.0, "s2-pro": 15.0, "s2.1-pro-free": 0.0, "drama-3": 50.0}
FISH_VOICE_DESIGN_USD = 0.01
# Per-voice "feel" -> Fish sampling temperature (0-1; Fish's default is 0.7). Lower = more consistent delivery.
FISH_FEEL_TEMPERATURE = {"steady": 0.45, "normal": None, "lively": 0.9}
GEMINI_AUDIO_TOKENS_PER_SEC = 25
# model: (text input per 1M tokens, audio output per 1M tokens); introductory prices end 2026-12-31
GEMINI_USD_PER_M_TOKENS = {
    "gemini-3.8-flash-tts": ((0.50, 9.00), (1.00, 18.00)),
    "gemini-3.8-flash-lite-tts": ((0.50, 6.00), (1.00, 12.00)),
    "gemini-3.1-flash-tts-preview": ((1.00, 20.00), (1.00, 20.00)),
    "gemini-2.5-flash-preview-tts": ((0.50, 10.00), (0.50, 10.00)),
    "gemini-2.5-pro-preview-tts": ((1.00, 20.00), (1.00, 20.00)),
}
# Each Gemini model has its own (small, Tier 1) daily quota; when one runs out the next is tried.
DEFAULT_GEMINI_FALLBACK_MODELS = (
    "gemini-3.8-flash-lite-tts", "gemini-3.1-flash-tts-preview",
    "gemini-2.5-flash-preview-tts", "gemini-2.5-pro-preview-tts",
)
GEMINI_PCM_RATE = 24000  # older models return headerless 16-bit mono PCM at this rate
# Only these accept the speech_metadata style annotation; older models reject it ("Speech annotations are
# not supported") and instead take the style as a spoken direction in front of the text, which they don't read aloud.
GEMINI_ANNOTATION_MODELS = frozenset({"gemini-3.8-flash-tts", "gemini-3.8-flash-lite-tts"})
# Switching models changes how a voice sounds, so stay put through short waits:
GEMINI_STICKY_WAIT_SECS = 8.0     # wait this long for the current model's per-minute slot before trying another
GEMINI_SHORT_RETRY_SECS = 10.0    # a 429 asking to retry within this long is waited out on the same model
# A 400 that means "this model can't take a style" (vs. a bad voice etc.): retry the same model without it
_STYLE_REJECTED_RE = re.compile(
    r"(annotation|metadata|style|prompt)\w*\W+(?:\w+\W+){0,4}not supported|not supported\W+(?:\w+\W+){0,4}"
    r"(annotation|metadata|style|prompt)", re.I)
# Short names users can put after a voice (gemini:kore@2.5) to try that model first
GEMINI_MODEL_ALIASES = {
    "3.8": "gemini-3.8-flash-tts", "3.8-flash": "gemini-3.8-flash-tts",
    "3.8-lite": "gemini-3.8-flash-lite-tts", "3.8-flash-lite": "gemini-3.8-flash-lite-tts",
    "3.1": "gemini-3.1-flash-tts-preview", "3.1-flash": "gemini-3.1-flash-tts-preview",
    "2.5": "gemini-2.5-flash-preview-tts", "2.5-flash": "gemini-2.5-flash-preview-tts",
    "2.5-pro": "gemini-2.5-pro-preview-tts",
}
GEMINI_MODEL_LABELS = {
    "gemini-3.8-flash-tts": "Gemini 3.8 Flash",
    "gemini-3.8-flash-lite-tts": "Gemini 3.8 Flash-Lite",
    "gemini-3.1-flash-tts-preview": "Gemini 3.1 Flash",
    "gemini-2.5-flash-preview-tts": "Gemini 2.5 Flash",
    "gemini-2.5-pro-preview-tts": "Gemini 2.5 Pro",
}


def resolve_gemini_model(name: str) -> Optional[str]:
    """'2.5' / '2.5-pro' / a full model id -> full model id, or None if unknown."""
    key = (name or "").strip().lower().removeprefix("gemini-").removeprefix("gemini")
    if key in GEMINI_MODEL_ALIASES:
        return GEMINI_MODEL_ALIASES[key]
    full = (name or "").strip().lower()
    return full if full in GEMINI_USD_PER_M_TOKENS else None


def split_gemini_voice(voice_id: str) -> Tuple[str, Optional[str]]:
    """'kore@gemini-2.5-flash-preview-tts' -> ('kore', 'gemini-2.5-flash-preview-tts'); no '@' -> (id, None)."""
    voice, _, model = (voice_id or "").partition("@")
    return voice, (model or None)
GEMINI_PRICE_CHANGE = date(2027, 1, 1)


def fish_cost(text: str, model: str) -> float:
    return len(text.encode("utf-8")) * FISH_USD_PER_M_BYTES.get(model, 15.0) / 1e6


def gemini_cost(text: str, style: Optional[str], audio_secs: float, model: str, today: Optional[date] = None) -> float:
    # Unknown models are priced like the most expensive one so the budget errs high
    now, later = GEMINI_USD_PER_M_TOKENS.get(model, ((1.00, 20.00), (1.00, 20.00)))
    pin, pout = later if (today or date.today()) >= GEMINI_PRICE_CHANGE else now
    in_tokens = (len(text) + len(style or "")) / 4
    return in_tokens * pin / 1e6 + audio_secs * GEMINI_AUDIO_TOKENS_PER_SEC * pout / 1e6


def wav_duration(data: bytes) -> float:
    try:
        with wave.open(io.BytesIO(data)) as w:
            frames, rate = w.getnframes(), w.getframerate()
            # Streaming-style headers can carry a bogus frame count; fall back to byte length.
            if frames <= 0 or frames > rate * 3600:
                frames = (len(data) - 44) // (w.getsampwidth() * w.getnchannels())
            return frames / rate
    except Exception:
        return 0.0


def pcm_to_wav(pcm: bytes, rate: int = GEMINI_PCM_RATE) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(pcm)
    return buf.getvalue()


_RETRY_RE = re.compile(r"retry in\s+((?:\d+h)?\s*(?:\d+m)?\s*(?:\d+(?:\.\d+)?s)?)", re.I)


def retry_after_secs(body: str) -> Optional[float]:
    """Parse Gemini's 'Please retry in 11h8m36s' / 'retry in 5s' hint."""
    m = _RETRY_RE.search(body or "")
    if not m or not m.group(1).strip():
        return None
    parts = {unit: value for value, unit in re.findall(r"(\d+(?:\.\d+)?)([hms])", m.group(1))}
    return float(parts.get("h", 0)) * 3600 + float(parts.get("m", 0)) * 60 + float(parts.get("s", 0))


def audio_format(data: bytes) -> str:
    if data[:4] == b"OggS":
        return "ogg_opus"
    if data[:4] == b"RIFF":
        return "wav"
    if data[:3] == b"ID3" or data[:2] == b"\xff\xfb":
        return "mp3"
    return "unknown"


def _find_audio_b64(obj) -> Optional[str]:
    """Return the last base64 audio block in a Gemini interactions response."""
    found = None
    if isinstance(obj, dict):
        if obj.get("type") == "audio" and isinstance(obj.get("data"), str):
            found = obj["data"]
        for v in obj.values():
            found = _find_audio_b64(v) or found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_audio_b64(v) or found
    return found


# ==========================
# Errors / helpers
# ==========================
class ProviderError(RuntimeError):
    def __init__(self, provider: str, status: Optional[int], body: str):
        super().__init__(f"{provider} error {status}: {body[:500]}")
        self.provider = provider
        self.status = status


class RateLimited(ProviderError):
    pass


class LRUCache:
    """
    Caches requests to & responses from the TTS services.

    Values (Tuple[bytes, int]): the audio, and the last-access timestamp (ms) used for LRU eviction.
    Bounded by item count and, optionally, total bytes.
    """
    def __init__(self, max_items: int, max_bytes: Optional[int] = None):
        self.max = max_items
        self.max_bytes = max_bytes
        self.bytes = 0
        self.store: Dict[Hashable, Tuple[bytes, int]] = {}

    def get(self, key: Hashable) -> Optional[bytes]:
        val = self.store.get(key)
        if not val:
            return None
        audio, _ = val
        self.store[key] = (audio, _now_ms())
        return audio

    def put(self, key: Hashable, audio: bytes) -> None:
        if self.max_bytes is not None and len(audio) > self.max_bytes:
            return
        if key in self.store:
            self.bytes -= len(self.store[key][0])
        while self.store and (len(self.store) >= self.max or
                              (self.max_bytes is not None and self.bytes + len(audio) > self.max_bytes)):
            oldest_key = min(self.store.items(), key=lambda kv: kv[1][1])[0]
            self.bytes -= len(self.store.pop(oldest_key)[0])
        self.store[key] = (audio, _now_ms())
        self.bytes += len(audio)


def _now_ms() -> int:
    return int(time.time() * 1000)


class RpmLimiter:
    """Allows at most `rpm` request starts in any rolling 60s window."""
    def __init__(self, rpm: int):
        self.rpm = max(1, rpm)
        self.starts: List[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self, max_wait: float) -> bool:
        """Wait for a slot; give up (False) if that would take longer than max_wait seconds in total."""
        deadline = time.monotonic() + max_wait
        while True:
            async with self.lock:
                now = time.monotonic()
                self.starts = [t for t in self.starts if now - t < 60]
                if len(self.starts) < self.rpm:
                    self.starts.append(now)
                    return True
                wait = 60 - (now - self.starts[0]) + 0.05
            if now + wait > deadline:
                return False
            await asyncio.sleep(wait)


class _HttpProvider:
    def __init__(self, timeout: float):
        self._timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout))

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def _sess(self) -> aiohttp.ClientSession:
        if not self._session:
            await self.start()
        return self._session


# ==========================
# Google Cloud TTS
# ==========================
class GoogleTTSProvider(_HttpProvider):
    """
    Google Cloud Text-to-Speech v1 REST.
    - Returns compressed (OGG_OPUS) bytes; we transcode to PCM via ffmpeg pipe.
    - Uses a tiny in-memory cache and a global concurrency cap.
    - Automatically adapts payload for Chirp/Journey voices (omit classic knobs).
    """
    def __init__(self, api_key: str):
        super().__init__(timeout=15)
        self.api_key = api_key
        self._cache = LRUCache(CACHE_MAX_ITEMS)
        self._sem = asyncio.Semaphore(GLOBAL_TTS_CONCURRENCY)

    async def synth(
        self,
        text: str,
        voice_name: str = DEFAULT_GOOGLE_VOICE,
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
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # Build payload; omit speakingRate for Chirp/Journey
        audio_cfg = {"audioEncoding": audio_encoding}
        if not is_chirp_or_journey:
            audio_cfg["speakingRate"] = eff_rate

        payload = {
            "input": {"text": text},
            "voice": {"languageCode": language_code, "name": voice_name},
            "audioConfig": audio_cfg
        }

        session = await self._sess()
        async with self._sem:
            async with session.post(GOOGLE_TTS_ENDPOINT, json=payload,
                                    headers={"x-goog-api-key": self.api_key}) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(f"Google TTS error {resp.status}: {body[:500]}")
                data = await resp.json()

        audio_b64 = data.get("audioContent")
        if not audio_b64:
            raise RuntimeError("Google TTS response missing audioContent")
        audio_bytes = base64.b64decode(audio_b64)
        self._cache.put(key, audio_bytes)
        return audio_bytes


# ==========================
# Fish Audio
# ==========================
class FishTTSProvider(_HttpProvider):
    def __init__(self, api_key: str, model: str):
        super().__init__(timeout=30)
        self.api_key = api_key
        self.model = model
        self._sem = asyncio.Semaphore(5)  # Fish starter tier allows 5 concurrent requests
        self._model_cache: Dict[str, dict] = {}

    def _auth(self) -> dict:
        return {"Authorization": f"Bearer {self.api_key}"}

    async def synth(self, text: str, voice_id: Optional[str], speed: float = 1.0,
                    reference=None, temperature: Optional[float] = None) -> bytes:
        """
        Speak with a voice model (voice_id) or, for unsaved voices, inline sample(s): `reference` is one
        (audio, transcript) pair or a list of them. temperature: 0-1, lower = steadier delivery.
        """
        body = {"text": text, "format": "opus", "latency": "balanced"}
        if abs(speed - 1.0) > 1e-3:
            body["prosody"] = {"speed": min(2.0, max(0.5, float(speed)))}
        if temperature is not None:
            body["temperature"] = min(1.0, max(0.0, float(temperature)))
        headers = {**self._auth(), "model": self.model}
        if reference:
            # Inline references are only accepted as MessagePack
            import msgpack
            refs = reference if isinstance(reference, list) else [reference]
            body["references"] = [{"audio": a, "text": t} for a, t in refs]
            kw = {"data": msgpack.packb(body), "headers": {**headers, "Content-Type": "application/msgpack"}}
        else:
            body["reference_id"] = voice_id
            kw = {"json": body, "headers": headers}
        session = await self._sess()
        async with self._sem:
            async with session.post(FISH_TTS_ENDPOINT, **kw) as resp:
                data = await resp.read()
                if resp.status == 429:
                    raise RateLimited("fish", resp.status, data.decode(errors="replace"))
                if resp.status != 200:
                    raise ProviderError("fish", resp.status, data.decode(errors="replace"))
        if not data:
            raise ProviderError("fish", 200, "empty audio")
        return data

    async def delete_model(self, model_id: str) -> bool:
        """Delete a voice model the bot's Fish account owns. True if it's gone (deleted or already missing)."""
        session = await self._sess()
        async with session.delete(f"{FISH_MODEL_ENDPOINT}/{model_id}", headers=self._auth()) as resp:
            if resp.status in (200, 204, 404):
                self._model_cache.pop(model_id, None)
                return True
            raise ProviderError("fish", resp.status, await resp.text())

    async def get_model(self, model_id: str) -> Optional[dict]:
        """Voice model metadata, or None if it doesn't exist / isn't accessible."""
        if model_id in self._model_cache:
            return self._model_cache[model_id]
        session = await self._sess()
        async with session.get(f"{FISH_MODEL_ENDPOINT}/{model_id}", headers=self._auth()) as resp:
            if resp.status in (400, 404):
                return None
            if resp.status != 200:
                raise ProviderError("fish", resp.status, await resp.text())
            info = await resp.json()
        self._model_cache[model_id] = info
        return info

    async def design(self, instruction: str, reference_text: str, n: int = 4, seed: Optional[int] = None) -> List[dict]:
        """
        Voice design candidates: [{id, index, audio (bytes), duration_ms, ...}]. Billed per request, not per
        candidate, so asking for the max (4) is free. Fish documents `seed` as deterministic, but in testing the
        same seed + instruction still gave different takes, so don't rely on it to reproduce a voice.
        """
        body = {"instruction": instruction[:2000], "reference_text": reference_text[:300], "n": max(1, min(4, n))}
        if seed is not None:
            body["seed"] = int(seed)
        session = await self._sess()
        async with session.post(FISH_VOICE_DESIGN_ENDPOINT, json=body,
                                headers={**self._auth(), "model": "voice-design-1"},
                                timeout=aiohttp.ClientTimeout(total=120)) as resp:
            if resp.status != 200:
                raise ProviderError("fish", resp.status, await resp.text())
            data = await resp.json()
        out = []
        for c in data.get("candidates") or []:
            c = dict(c)
            c["audio"] = base64.b64decode(c.pop("audio_base64", "") or "")
            out.append(c)
        return out

    async def create_model(self, title: str, audio, text, signature: Optional[str] = None,
                           description: Optional[str] = None) -> dict:
        """Create a private voice model from one sample, or several (lists of audio + matching transcripts)."""
        audios = audio if isinstance(audio, list) else [audio]
        texts = text if isinstance(text, list) else [text]

        def form(sig: Optional[str]) -> aiohttp.FormData:
            f = aiohttp.FormData()
            f.add_field("type", "tts")
            f.add_field("title", title[:100])
            f.add_field("train_mode", "fast")
            f.add_field("visibility", "private")
            for t in texts:
                f.add_field("texts", t)
            if description:
                f.add_field("description", description[:500])
            for i, a in enumerate(audios):
                ext = {"wav": "wav", "ogg_opus": "ogg", "mp3": "mp3"}.get(audio_format(a), "wav")
                f.add_field("voices", a, filename=f"sample_{i}.{ext}", content_type="application/octet-stream")
            if sig:
                f.add_field("voice_design_signatures", sig)
            return f

        session = await self._sess()
        for sig in ([signature, None] if signature else [None]):
            async with session.post(FISH_MODEL_ENDPOINT, data=form(sig), headers=self._auth(),
                                    timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status in (200, 201):
                    return await resp.json()
                body = await resp.text()
                # An unrecognised signature rejects the request; retry once as a plain sample upload.
                if sig and resp.status in (400, 422):
                    print(f"[tts] fish rejected voice design signature ({resp.status}): {body[:200]}")
                    continue
                raise ProviderError("fish", resp.status, body)
        raise ProviderError("fish", None, "model creation failed")


# ==========================
# Gemini TTS
# ==========================
class GeminiTTSProvider(_HttpProvider):
    """
    Gemini TTS over a chain of models. Each model has its own quota, so when one is rate limited
    (or out for the day) the next is tried; a model told to wait is skipped until then.
    """
    def __init__(self, api_key: str, model: str, rpm: int, fallback_models: Tuple[str, ...] = ()):
        super().__init__(timeout=45)
        self.api_key = api_key
        self.models: List[str] = [model] + [m for m in fallback_models if m and m != model]
        self.model = model  # primary
        self._rpm = rpm
        self._limiters = {m: RpmLimiter(rpm) for m in self.models}
        self._blocked_until: Dict[str, float] = {}
        # Models that rejected a style: they get the text without one from then on
        self._no_style_models: set = set()
        self._voices: Optional[Dict[str, dict]] = None
        self._voices_lock = asyncio.Lock()

    def _auth(self) -> dict:
        return {"x-goog-api-key": self.api_key}

    def blocked_for(self, model: str) -> float:
        return max(0.0, self._blocked_until.get(model, 0.0) - time.monotonic())

    async def synth(self, text: str, voice: str, style: Optional[str] = None,
                    max_wait: float = 5.0, prefer: Optional[str] = None) -> Tuple[bytes, str]:
        """Returns (wav bytes, model that produced them). `prefer` is tried before the usual chain."""
        order = ([prefer] + [m for m in self.models if m != prefer]) if prefer else self.models
        for m in order:
            self._limiters.setdefault(m, RpmLimiter(self._rpm))
        available = [m for m in order if self.blocked_for(m) <= 0]
        if not available:
            raise RateLimited("gemini", 429, "all Gemini models are at their limits")
        # If any model was skipped or refused for quota, the chain ran out of capacity: report that as a
        # rate limit even when the last model failed for another reason, so callers treat it as one.
        limited = len(available) < len(order)
        last_error: Optional[Exception] = None
        for i, model in enumerate(available):
            # Stay on the first usable model through a short per-minute wait (switching models is audible);
            # later models only get the caller's max_wait on the last option.
            wait_for = max(max_wait, GEMINI_STICKY_WAIT_SECS) if i == 0 else (max_wait if i == len(available) - 1 else 0)
            if not await self._limiters[model].acquire(wait_for):
                limited = True
                last_error = RateLimited("gemini", None, f"{model}: client-side rate limit")
                continue
            # A style the model can't take is dropped (same model, same voice) rather than switching models
            styles = [style, None] if style and model not in self._no_style_models else [None]
            si, waited_out = 0, False
            while si < len(styles):
                attempt_style = styles[si]
                try:
                    return await self._synth_one(model, text, voice, attempt_style), model
                except RateLimited as e:
                    wait = retry_after_secs(str(e)) or 60.0
                    if wait <= GEMINI_SHORT_RETRY_SECS and not waited_out:
                        waited_out = True
                        print(f"[tts] gemini {model} asked to wait {wait:.0f}s; waiting rather than switching")
                        await asyncio.sleep(wait + 0.25)
                        continue
                    self._blocked_until[model] = time.monotonic() + wait
                    print(f"[tts] gemini {model} limited for {wait / 3600:.1f}h; trying next model")
                    limited = True
                    last_error = e
                    break
                except ProviderError as e:
                    last_error = e
                    if attempt_style and e.status == 400 and _STYLE_REJECTED_RE.search(str(e)):
                        print(f"[tts] gemini {model} rejected the style; retrying without it")
                        self._no_style_models.add(model)
                        si += 1
                        continue
                    # e.g. a voice an older model doesn't have; another model may still work
                    print(f"[tts] gemini {model} failed ({e.status}): {str(e)[:200]}; trying next model")
                    break
        if limited and not isinstance(last_error, RateLimited):
            raise RateLimited("gemini", 429, f"Gemini models out of quota (last error: {last_error})")
        raise last_error or RateLimited("gemini", 429, "no Gemini model available")

    @staticmethod
    def voice_seed(voice: str) -> int:
        """Fixed sampling seed per voice: the same voice always samples the same way, which steadies how it
        sounds from message to message (identical text then comes out identical)."""
        return zlib.crc32(voice.lower().encode()) % 2_147_483_647

    @staticmethod
    def build_body(model: str, text: str, voice: str, style: Optional[str]) -> dict:
        if style and model not in GEMINI_ANNOTATION_MODELS:
            text = f"Read aloud in this style ({style}): {text}"
        content = {"type": "text", "text": text}
        if style and model in GEMINI_ANNOTATION_MODELS:
            content["annotations"] = [{"type": "speech_metadata", "style": style}]
        return {
            "model": model,
            "input": [{"type": "user_input", "content": [content]}],
            "response_format": {"type": "audio"},
            "generation_config": {"speech_config": [{"voice": voice}], "seed": GeminiTTSProvider.voice_seed(voice)},
        }

    async def _synth_one(self, model: str, text: str, voice: str, style: Optional[str]) -> bytes:
        body = self.build_body(model, text, voice, style)
        session = await self._sess()
        async with session.post(GEMINI_INTERACTIONS_ENDPOINT, json=body, headers=self._auth()) as resp:
            raw = await resp.read()
            if resp.status == 429:
                raise RateLimited("gemini", resp.status, raw.decode(errors="replace"))
            if resp.status != 200:
                raise ProviderError("gemini", resp.status, raw.decode(errors="replace"))
        b64 = _find_audio_b64(json.loads(raw))
        if not b64:
            raise ProviderError("gemini", 200, f"no audio in response: {raw[:300]!r}")
        audio = base64.b64decode(b64)
        # 3.8 models send a WAV file; older ones send bare PCM, which ffmpeg can't identify
        return audio if audio[:4] == b"RIFF" else pcm_to_wav(audio)

    async def voices(self) -> Dict[str, dict]:
        """All voices keyed by lowercase id (fetched once)."""
        async with self._voices_lock:
            if self._voices is not None:
                return self._voices
            out: Dict[str, dict] = {}
            token = None
            session = await self._sess()
            while True:
                params = {"page_size": "1000"}
                if token:
                    params["page_token"] = token
                async with session.get(GEMINI_VOICES_ENDPOINT, params=params, headers=self._auth()) as resp:
                    if resp.status != 200:
                        raise ProviderError("gemini", resp.status, await resp.text())
                    d = await resp.json()
                for v in d.get("voices", []):
                    if v.get("id"):
                        out[v["id"].lower()] = v
                token = d.get("next_page_token") or d.get("nextPageToken")
                if not token:
                    break
            self._voices = out
            return out

    async def find_voice(self, name: str) -> Optional[dict]:
        q = " ".join(name.split()).lower()
        voices = await self.voices()
        if q in voices:
            return voices[q]
        return next((v for v in voices.values() if (v.get("display_name") or "").lower() == q), None)


# ==========================
# Budget
# ==========================
class BudgetTracker:
    """
    Month-to-date estimated spend across paid providers, persisted in tts_usage.
    Totals are cached in memory; the DB is the source of truth across restarts.
    """
    def __init__(self, db_getter: Callable[[], object], monthly_cap_usd: float):
        self._db_getter = db_getter
        self.cap = float(monthly_cap_usd)
        self._month: Optional[date] = None
        self._totals: Dict[str, float] = {}
        # Spend that couldn't be written yet: provider -> [usd, requests, chars, audio_ms]
        self._unsaved: Dict[str, List[float]] = {}
        self._lock = asyncio.Lock()
        self._load_failed_at: Optional[float] = None

    @staticmethod
    def current_month() -> date:
        return date.today().replace(day=1)

    def _db(self):
        try:
            return self._db_getter()
        except Exception:
            return None

    async def _ensure_loaded(self) -> bool:
        """Load this month's totals. False = unknown (DB unreachable); callers treat that as over budget."""
        month = self.current_month()
        if self._month == month:
            return True
        db = self._db()
        totals: Dict[str, float] = {}
        if db is not None:
            # Don't hammer a down DB on every message
            if self._load_failed_at and time.monotonic() - self._load_failed_at < 30:
                return False
            try:
                rows = await db.fetch("SELECT provider, est_usd FROM tts_usage WHERE month=$1", month)
                totals = {r["provider"]: float(r["est_usd"]) for r in rows}
            except Exception as e:
                print(f"[tts] failed to load usage (paid voices paused until it loads): {e}")
                self._load_failed_at = time.monotonic()
                return False
        self._load_failed_at = None
        if self._month is not None and self._month != month:
            self._unsaved = {}  # new month: last month's unwritten spend no longer counts toward the cap
        # Spend recorded while the DB was unreachable isn't in the DB yet; count it and write it on next record()
        for prov, (p_usd, *_rest) in self._unsaved.items():
            totals[prov] = totals.get(prov, 0.0) + p_usd
        self._month, self._totals = month, totals
        return True

    async def spent(self) -> float:
        async with self._lock:
            if not await self._ensure_loaded():
                return float("inf")
            return sum(self._totals.values())

    async def by_provider(self) -> Dict[str, float]:
        async with self._lock:
            if not await self._ensure_loaded():
                raise RuntimeError("usage totals unavailable (database unreachable)")
            return dict(self._totals)

    async def exhausted(self) -> bool:
        return await self.spent() >= self.cap

    async def record(self, provider: str, usd: float, chars: int = 0, audio_ms: int = 0):
        async with self._lock:
            loaded = await self._ensure_loaded()
            pending = self._unsaved.setdefault(provider, [0.0, 0, 0, 0])
            pending[0] += usd
            pending[1] += 1
            pending[2] += int(chars)
            pending[3] += int(audio_ms)
            if loaded:
                self._totals[provider] = self._totals.get(provider, 0.0) + usd
            db = self._db()
            if db is None or not loaded:
                if db is None:
                    self._unsaved.pop(provider, None)  # no DB configured: in-memory only
                return
            # Write this spend plus anything a previous failed write left behind
            for prov, (p_usd, p_req, p_chars, p_ms) in list(self._unsaved.items()):
                try:
                    await db.execute(
                        """
                        INSERT INTO tts_usage (month, provider, requests, chars, audio_ms, est_usd)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (month, provider) DO UPDATE SET
                            requests = tts_usage.requests + EXCLUDED.requests,
                            chars    = tts_usage.chars + EXCLUDED.chars,
                            audio_ms = tts_usage.audio_ms + EXCLUDED.audio_ms,
                            est_usd  = tts_usage.est_usd + EXCLUDED.est_usd
                        """,
                        self._month, prov, int(p_req), int(p_chars), int(p_ms), round(p_usd, 6),
                    )
                    self._unsaved.pop(prov, None)
                except Exception as e:
                    print(f"[tts] failed to record usage (will retry): {e}")
                    break


# ==========================
# Router
# ==========================
@dataclass
class SynthResult:
    audio: bytes
    fmt: str                         # 'ogg_opus' | 'wav'
    provider: str                    # provider that actually produced the audio
    fallback_reason: Optional[str]   # None, or why a paid voice fell back to Google
    cost_usd: float = 0.0            # estimated spend for this request (0 for Google and cache hits)
    voice_ref_used: Optional[str] = None  # the voice that actually spoke (differs from the request on fallback)
    model: Optional[str] = None           # provider model that produced it (for logs)


def parse_voice_ref(ref: Optional[str]) -> Tuple[str, str]:
    """'fish:<id>' / 'gemini:<id>' / '<google voice name>' -> (provider, id)."""
    ref = (ref or "").strip()
    low = ref.lower()
    if low.startswith("fish:"):
        return "fish", ref[5:].strip().lower()
    if low.startswith("gemini:"):
        return "gemini", ref[7:].strip().lower()
    return "google", ref or DEFAULT_GOOGLE_VOICE


def extract_fish_id(text: str) -> Optional[str]:
    """A bare 32-hex Fish model id, or one inside a fish.audio URL."""
    m = _FISH_ID_RE.search(text or "")
    return m.group(1).lower() if m else None


def google_language(voice_name: str) -> str:
    return "-".join(voice_name.split("-")[0:2]) if "-" in voice_name else DEFAULT_LANG


class TTSRouter:
    # (first chunk, max chunk) chars. Small first chunk = fast first audio; later chunks grow because
    # each is synthesized while the previous one plays. Gemini: few, big chunks (high fixed latency, low RPM).
    CHUNK_PLAN = {"google": (150, 900), "fish": (90, 600), "gemini": (220, 1200)}

    def __init__(self, google: GoogleTTSProvider, fish: Optional[FishTTSProvider],
                 gemini: Optional[GeminiTTSProvider], budget: BudgetTracker,
                 reference_loader: Optional[Callable[[str], Awaitable[Optional[Tuple[bytes, str]]]]] = None):
        self.google = google
        self.fish = fish
        self.gemini = gemini
        self.budget = budget
        self._cache = LRUCache(CACHE_MAX_ITEMS, max_bytes=64 * 1024 * 1024)
        # Unsaved Fish voices ('fish:ref:<key>') are spoken from a stored sample; loader returns (audio, transcript)
        self._reference_loader = reference_loader
        # Per-voice defaults for Fish voices (designed/cloned): async (voice_id) -> (default style, temperature)
        self.voice_settings_loader: Optional[Callable[[str], Awaitable[Optional[Tuple[Optional[str], Optional[float]]]]]] = None
        self._references: Dict[str, Tuple[bytes, str]] = {}

    def forget_reference(self, voice_id: str) -> None:
        self._references.pop(voice_id.removeprefix(FISH_REF_PREFIX), None)

    async def _reference(self, key: str) -> Tuple[bytes, str]:
        if key not in self._references:
            ref = await self._reference_loader(key) if self._reference_loader else None
            if not ref:
                raise ProviderError("fish", None, f"designed voice sample {key!r} not found")
            self._references[key] = ref
        return self._references[key]

    async def start(self):
        for p in (self.google, self.fish, self.gemini):
            if p:
                await p.start()

    async def close(self):
        for p in (self.google, self.fish, self.gemini):
            if p:
                await p.close()

    def chunk_plan(self, voice_ref: Optional[str]) -> Tuple[int, int]:
        return self.CHUNK_PLAN[parse_voice_ref(voice_ref)[0]]

    async def synth(self, voice_ref: Optional[str], text: str, style: Optional[str] = None,
                    rate: float = DEFAULT_RATE, fallback_refs: Sequence[str] = ()) -> SynthResult:
        """
        Speak with voice_ref. If a paid voice can't (budget, quota, error), try fallback_refs in order
        (Fish or Google refs; Gemini refs are skipped), then DEFAULT_GOOGLE_VOICE.
        SynthResult.voice_ref_used says which voice actually spoke.
        """
        provider, voice_id = parse_voice_ref(voice_ref)
        reason = None
        if provider != "google":
            client = self.fish if provider == "fish" else self.gemini
            if client is None:
                reason = "not_configured"
            elif await self.budget.exhausted():
                reason = "budget"
            else:
                try:
                    res = await self._synth_paid(provider, client, voice_id, text, style, rate)
                    res.voice_ref_used = voice_ref
                    return res
                except RateLimited as e:
                    print(f"[tts] {provider} rate limited: {e}")
                    reason = "gemini_limit" if provider == "gemini" else "rate_limited"
                except Exception as e:
                    print(f"[tts] {provider} synth failed: {e}")
                    reason = "balance" if getattr(e, "status", None) == 402 else "error"
            for ref in fallback_refs:
                res = await self._try_fallback(ref, text, style, rate)
                if res:
                    res.fallback_reason, res.voice_ref_used = reason, ref
                    return res
            voice_id = DEFAULT_GOOGLE_VOICE

        prepared, _ = tts_text.prepare(text, "google")
        if not prepared:
            raise ValueError("nothing to say after removing tags")
        audio = await self.google.synth(prepared, voice_name=voice_id,
                                        language_code=google_language(voice_id), speaking_rate=rate)
        return SynthResult(audio, "ogg_opus", "google", reason, voice_ref_used=voice_id)

    async def _try_fallback(self, ref: str, text: str, style: Optional[str], rate: float) -> Optional[SynthResult]:
        provider, voice_id = parse_voice_ref(ref)
        try:
            if provider == "fish":
                if not self.fish or await self.budget.exhausted():
                    return None
                return await self._synth_paid("fish", self.fish, voice_id, text, style, rate)
            if provider == "google":
                prepared, _ = tts_text.prepare(text, "google")
                if not prepared:
                    return None
                audio = await self.google.synth(prepared, voice_name=voice_id,
                                                language_code=google_language(voice_id), speaking_rate=rate)
                return SynthResult(audio, "ogg_opus", "google", None)
        except Exception as e:
            print(f"[tts] fallback voice {ref} failed: {e}")
        return None

    async def _synth_paid(self, provider, client, voice_id, text, style, rate) -> SynthResult:
        temperature = None
        if provider == "fish" and self.voice_settings_loader:
            try:
                settings = await self.voice_settings_loader(voice_id)
            except Exception as e:
                print(f"[tts] couldn't load voice settings for {voice_id}: {e}")
                settings = None
            if settings:
                voice_style, temperature = settings
                # The voice's own style comes first; the user's/message's delivery is added on top
                if voice_style:
                    style = f"{voice_style}, {style}" if style else voice_style
        prepared, sep_style = tts_text.prepare(text, provider, style)
        if not prepared:
            raise ValueError("nothing to say after removing tags")
        key = (provider, voice_id, prepared, sep_style, round(float(rate), 2), temperature)
        cached = self._cache.get(key)
        if cached is not None:
            return SynthResult(cached, audio_format(cached), provider, None, model="cache")

        if provider == "fish":
            extra = {"temperature": temperature} if temperature is not None else {}
            if voice_id.startswith(FISH_REF_PREFIX):
                sample = await self._reference(voice_id[len(FISH_REF_PREFIX):])
                audio = await client.synth(prepared, None, speed=rate, reference=sample, **extra)
            else:
                audio = await client.synth(prepared, voice_id, speed=rate, **extra)
            cost = fish_cost(prepared, client.model)
            model = client.model
            await self.budget.record("fish", cost, chars=len(prepared))
        else:
            voice, prefer = split_gemini_voice(voice_id)
            audio, model = await client.synth(prepared, voice, sep_style, prefer=prefer)
            secs = wav_duration(audio)
            if secs <= 0:
                # Unparseable WAV: estimate (~12 chars/s of speech) so spend still counts toward the cap
                print(f"[tts] couldn't read Gemini audio length ({len(audio)} bytes); estimating")
                secs = max(1.0, len(prepared) / 12)
            cost = gemini_cost(prepared, sep_style, secs, model)
            await self.budget.record("gemini", cost, chars=len(prepared), audio_ms=int(secs * 1000))
        self._cache.put(key, audio)
        return SynthResult(audio, audio_format(audio), provider, None, cost, model=model)
