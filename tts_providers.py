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
import io
import json
import re
import time
import wave
from dataclasses import dataclass
from datetime import date
from typing import Awaitable, Callable, Dict, Hashable, List, Optional, Tuple

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

GOOGLE_TTS_ENDPOINT = "https://texttospeech.googleapis.com/v1/text:synthesize?key={api_key}"
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
GEMINI_AUDIO_TOKENS_PER_SEC = 25
# model: (text input per 1M tokens, audio output per 1M tokens); introductory prices end 2026-12-31
GEMINI_USD_PER_M_TOKENS = {
    "gemini-3.8-flash-tts": ((0.50, 9.00), (1.00, 18.00)),
    "gemini-3.8-flash-lite-tts": ((0.50, 6.00), (1.00, 12.00)),
}
GEMINI_PRICE_CHANGE = date(2027, 1, 1)


def fish_cost(text: str, model: str) -> float:
    return len(text.encode("utf-8")) * FISH_USD_PER_M_BYTES.get(model, 15.0) / 1e6


def gemini_cost(text: str, style: Optional[str], audio_secs: float, model: str, today: Optional[date] = None) -> float:
    now, later = GEMINI_USD_PER_M_TOKENS.get(model, GEMINI_USD_PER_M_TOKENS["gemini-3.8-flash-tts"])
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

    Values (Tuple[bytes, int]): the compressed audio, and the last-access timestamp (ms) used for LRU eviction.
    """
    def __init__(self, max_items: int):
        self.max = max_items
        self.store: Dict[Hashable, Tuple[bytes, int]] = {}

    def get(self, key: Hashable) -> Optional[bytes]:
        val = self.store.get(key)
        if not val:
            return None
        audio, _ = val
        self.store[key] = (audio, _now_ms())
        return audio

    def put(self, key: Hashable, audio: bytes) -> None:
        if key in self.store:
            self.store[key] = (audio, _now_ms())
            return
        if len(self.store) >= self.max:
            oldest_key = min(self.store.items(), key=lambda kv: kv[1][1])[0]
            self.store.pop(oldest_key, None)
        self.store[key] = (audio, _now_ms())


def _now_ms() -> int:
    return int(time.time() * 1000)


class RpmLimiter:
    """Allows at most `rpm` request starts in any rolling 60s window."""
    def __init__(self, rpm: int):
        self.rpm = max(1, rpm)
        self.starts: List[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self, max_wait: float) -> bool:
        """Wait for a slot; give up (False) if that would take longer than max_wait seconds."""
        async with self.lock:
            now = time.monotonic()
            self.starts = [t for t in self.starts if now - t < 60]
            if len(self.starts) >= self.rpm:
                wait = 60 - (now - self.starts[0]) + 0.05
                if wait > max_wait:
                    return False
                await asyncio.sleep(wait)
                now = time.monotonic()
                self.starts = [t for t in self.starts if now - t < 60]
            self.starts.append(now)
            return True


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

        url = GOOGLE_TTS_ENDPOINT.format(api_key=self.api_key)
        session = await self._sess()
        async with self._sem:
            async with session.post(url, json=payload) as resp:
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
                    reference: Optional[Tuple[bytes, str]] = None) -> bytes:
        """Speak with a voice model (voice_id) or, for unsaved voices, an inline (audio, transcript) sample."""
        body = {"text": text, "format": "opus", "latency": "balanced"}
        if abs(speed - 1.0) > 1e-3:
            body["prosody"] = {"speed": min(2.0, max(0.5, float(speed)))}
        headers = {**self._auth(), "model": self.model}
        if reference:
            # Inline references are only accepted as MessagePack
            import msgpack
            body["references"] = [{"audio": reference[0], "text": reference[1]}]
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

    async def design(self, instruction: str, reference_text: str, n: int = 3) -> List[dict]:
        """Voice design candidates: [{id, index, audio (bytes), duration_ms, ...}]."""
        body = {"instruction": instruction[:2000], "reference_text": reference_text[:150], "n": max(1, min(4, n))}
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

    async def create_model(self, title: str, audio: bytes, text: str, signature: Optional[str] = None,
                           description: Optional[str] = None) -> dict:
        """Create a private voice model from one sample (e.g. a voice design candidate)."""
        def form(sig: Optional[str]) -> aiohttp.FormData:
            f = aiohttp.FormData()
            f.add_field("type", "tts")
            f.add_field("title", title[:100])
            f.add_field("train_mode", "fast")
            f.add_field("visibility", "private")
            f.add_field("texts", text)
            if description:
                f.add_field("description", description[:500])
            ext = {"wav": "wav", "ogg_opus": "ogg", "mp3": "mp3"}.get(audio_format(audio), "wav")
            f.add_field("voices", audio, filename=f"sample.{ext}", content_type="application/octet-stream")
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
    def __init__(self, api_key: str, model: str, rpm: int):
        super().__init__(timeout=45)
        self.api_key = api_key
        self.model = model
        self.limiter = RpmLimiter(rpm)
        self._voices: Optional[Dict[str, dict]] = None
        self._voices_lock = asyncio.Lock()

    def _auth(self) -> dict:
        return {"x-goog-api-key": self.api_key}

    async def synth(self, text: str, voice: str, style: Optional[str] = None, max_wait: float = 5.0) -> bytes:
        if not await self.limiter.acquire(max_wait):
            raise RateLimited("gemini", None, "client-side rate limit")
        content = {"type": "text", "text": text}
        if style:
            content["annotations"] = [{"type": "speech_metadata", "style": style}]
        body = {
            "model": self.model,
            "input": [{"type": "user_input", "content": [content]}],
            "response_format": {"type": "audio"},
            "generation_config": {"speech_config": [{"voice": voice}]},
        }
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
        return base64.b64decode(b64)

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
        self._lock = asyncio.Lock()

    @staticmethod
    def current_month() -> date:
        return date.today().replace(day=1)

    def _db(self):
        try:
            return self._db_getter()
        except Exception:
            return None

    async def _ensure_loaded(self):
        month = self.current_month()
        if self._month == month:
            return
        totals: Dict[str, float] = {}
        db = self._db()
        if db is not None:
            try:
                rows = await db.fetch("SELECT provider, est_usd FROM tts_usage WHERE month=$1", month)
                totals = {r["provider"]: float(r["est_usd"]) for r in rows}
            except Exception as e:
                print(f"[tts] failed to load usage: {e}")
        self._month, self._totals = month, totals

    async def spent(self) -> float:
        async with self._lock:
            await self._ensure_loaded()
            return sum(self._totals.values())

    async def by_provider(self) -> Dict[str, float]:
        async with self._lock:
            await self._ensure_loaded()
            return dict(self._totals)

    async def exhausted(self) -> bool:
        return await self.spent() >= self.cap

    async def record(self, provider: str, usd: float, chars: int = 0, audio_ms: int = 0):
        async with self._lock:
            await self._ensure_loaded()
            self._totals[provider] = self._totals.get(provider, 0.0) + usd
            db = self._db()
            if db is None:
                return
            try:
                await db.execute(
                    """
                    INSERT INTO tts_usage (month, provider, requests, chars, audio_ms, est_usd)
                    VALUES ($1, $2, 1, $3, $4, $5)
                    ON CONFLICT (month, provider) DO UPDATE SET
                        requests = tts_usage.requests + 1,
                        chars    = tts_usage.chars + EXCLUDED.chars,
                        audio_ms = tts_usage.audio_ms + EXCLUDED.audio_ms,
                        est_usd  = tts_usage.est_usd + EXCLUDED.est_usd
                    """,
                    self._month, provider, int(chars), int(audio_ms), round(usd, 6),
                )
            except Exception as e:
                print(f"[tts] failed to record usage: {e}")


# ==========================
# Router
# ==========================
@dataclass
class SynthResult:
    audio: bytes
    fmt: str                         # 'ogg_opus' | 'wav'
    provider: str                    # provider that actually produced the audio
    fallback_reason: Optional[str]   # None, or why a paid voice fell back to Google


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
    # Paid providers tolerate longer chunks: fewer requests (Gemini RPM) and steadier prosody.
    CHUNK_CHARS = {"google": 180, "fish": 400, "gemini": 400}

    def __init__(self, google: GoogleTTSProvider, fish: Optional[FishTTSProvider],
                 gemini: Optional[GeminiTTSProvider], budget: BudgetTracker,
                 reference_loader: Optional[Callable[[str], Awaitable[Optional[Tuple[bytes, str]]]]] = None):
        self.google = google
        self.fish = fish
        self.gemini = gemini
        self.budget = budget
        self._cache = LRUCache(CACHE_MAX_ITEMS)
        # Unsaved Fish voices ('fish:ref:<key>') are spoken from a stored sample; loader returns (audio, transcript)
        self._reference_loader = reference_loader
        self._references: Dict[str, Tuple[bytes, str]] = {}

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

    def chunk_limit(self, voice_ref: Optional[str]) -> int:
        return self.CHUNK_CHARS[parse_voice_ref(voice_ref)[0]]

    async def synth(self, voice_ref: Optional[str], text: str, style: Optional[str] = None,
                    rate: float = DEFAULT_RATE) -> SynthResult:
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
                    return await self._synth_paid(provider, client, voice_id, text, style, rate)
                except RateLimited as e:
                    print(f"[tts] {provider} rate limited: {e}")
                    reason = "rate_limited"
                except Exception as e:
                    print(f"[tts] {provider} synth failed: {e}")
                    reason = "balance" if getattr(e, "status", None) == 402 else "error"
            voice_id = DEFAULT_GOOGLE_VOICE

        prepared, _ = tts_text.prepare(text, "google")
        if not prepared:
            raise ValueError("nothing to say after removing tags")
        audio = await self.google.synth(prepared, voice_name=voice_id,
                                        language_code=google_language(voice_id), speaking_rate=rate)
        return SynthResult(audio, "ogg_opus", "google", reason)

    async def _synth_paid(self, provider, client, voice_id, text, style, rate) -> SynthResult:
        prepared, sep_style = tts_text.prepare(text, provider, style)
        if not prepared:
            raise ValueError("nothing to say after removing tags")
        key = (provider, voice_id, prepared, sep_style, round(float(rate), 2))
        cached = self._cache.get(key)
        if cached is not None:
            return SynthResult(cached, audio_format(cached), provider, None)

        if provider == "fish":
            if voice_id.startswith(FISH_REF_PREFIX):
                sample = await self._reference(voice_id[len(FISH_REF_PREFIX):])
                audio = await client.synth(prepared, None, speed=rate, reference=sample)
            else:
                audio = await client.synth(prepared, voice_id, speed=rate)
            await self.budget.record("fish", fish_cost(prepared, client.model), chars=len(prepared))
        else:
            audio = await client.synth(prepared, voice_id, sep_style)
            secs = wav_duration(audio)
            await self.budget.record("gemini", gemini_cost(prepared, sep_style, secs, client.model),
                                     chars=len(prepared), audio_ms=int(secs * 1000))
        self._cache.put(key, audio)
        return SynthResult(audio, audio_format(audio), provider, None)
