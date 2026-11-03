"""
Side-by-side TTS comparison: Gemini TTS vs Fish Audio (plus the current Google Cloud TTS voices as a baseline).

Measures time-to-first-byte / total latency per request, saves every clip, and writes an
index.html with audio players so quality and voice diversity can be judged by ear.

Needs in .env (or the environment):
    GEMINI_API_KEY      - AI Studio key (https://aistudio.google.com/apikey), paid tier recommended
    FISH_API_KEY        - https://fish.audio/app/api-keys
    GOOGLE_NARRATE_KEY  - existing key, used for the baseline

Usage:
    venv/Scripts/python.exe misc/tts_compare.py
    venv/Scripts/python.exe misc/tts_compare.py --quick
    venv/Scripts/python.exe misc/tts_compare.py --skip google --fish-models s2.1-pro,s2.1-pro-free
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import html
import io
import json
import os
import re
import statistics
import sys
import time
import wave
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp

ROOT = Path(__file__).resolve().parent.parent

GEMINI_INTERACTIONS = "https://generativelanguage.googleapis.com/v1beta/interactions"
GEMINI_VOICES = "https://generativelanguage.googleapis.com/v1beta/voices"
FISH_TTS = "https://api.fish.audio/v1/tts"
FISH_MODELS = "https://api.fish.audio/model"
GOOGLE_TTS = "https://texttospeech.googleapis.com/v1/text:synthesize"

# Pricing used for the cost projection (USD). Gemini audio output = 25 tokens/sec.
GEMINI_PRICES = {  # model: (text input per 1M tokens, audio output per 1M tokens)
    "gemini-3.8-flash-tts": (0.50, 9.00),
    "gemini-3.8-flash-lite-tts": (0.50, 6.00),
}
GEMINI_PRICES_2027 = {
    "gemini-3.8-flash-tts": (1.00, 18.00),
    "gemini-3.8-flash-lite-tts": (1.00, 12.00),
}
FISH_PRICES = {"s2.1-pro": 15.00, "s2-pro": 15.00, "s2.1-pro-free": 0.0, "drama-3": 50.00}  # per 1M UTF-8 bytes
GOOGLE_PRICES = {"Wavenet": 16.00, "Chirp3-HD": 30.00}  # per 1M chars (after free tier)
MONTHLY_CHARS = 240_000  # ~4k messages x ~60 chars

GEMINI_PREBUILT = [
    "Kore", "Puck", "Charon", "Fenrir", "Leda", "Zephyr", "Aoede", "Orus",
    "Enceladus", "Sulafat", "Algieba", "Despina", "Gacrux", "Schedar", "Achird", "Sadachbia",
]

# {laugh}/{sigh} are swapped for each provider's inline tag syntax (dropped for the baseline).
INLINE_TAGS = {
    "gemini": {"laugh": "<laugh>", "sigh": "<sigh>", "pause": "<short pause>"},
    "fish": {"laugh": "[laughing]", "sigh": "[sigh]", "pause": "[pause]"},
    "google": {"laugh": "", "sigh": "", "pause": ""},
}


@dataclass
class Sample:
    key: str
    text: str
    style: Optional[str] = None      # Gemini speech_metadata.style
    fish_tag: Optional[str] = None   # Fish leading [tag]


SAMPLES = [
    Sample("plain", "hey is anyone up for some games tonight? i'm on in like ten minutes"),
    Sample("excited_text", "NO WAY!!! we actually won that?? that was insane lmao"),
    Sample("excited_hint", "we actually won that, that was insane",
           style="screaming with disbelief and joy", fish_tag="excited"),
    Sample("sad_hint", "yeah... my cat passed away this morning. i don't really feel like playing",
           style="quiet, sad, holding back tears", fish_tag="sad"),
    Sample("angry_hint", "who keeps stealing my kills, seriously, stop it",
           style="genuinely angry and exasperated", fish_tag="angry"),
    Sample("sarcastic_hint", "oh great, another patch that fixes absolutely nothing. love that for us",
           style="dry and sarcastic", fish_tag="sarcastic"),
    Sample("whisper_hint", "don't tell anyone but i think the boss is about to quit",
           style="whispering conspiratorially", fish_tag="whispering"),
    Sample("inline_tags", "{laugh} okay that was actually hilarious. {sigh} anyway, back to work I guess"),
    Sample("slang", "brb gotta grab food, gg wp everyone, idk when i'll be back tbh"),
    Sample("long", "So here's the plan: we meet at the north gate at eight, Sam brings the snacks, "
                   "Jordan handles the driving, and nobody, and I mean nobody, is allowed to bring up "
                   "what happened last time with the tent. Deal? Great. See you all Saturday."),
]
QUICK_SAMPLES = {"plain", "excited_text", "sad_hint", "sarcastic_hint", "inline_tags"}

GALLERY_LINE = "Alright everyone, settle down. I have some news, and you're not going to believe it."


@dataclass
class Result:
    provider: str
    model: str
    voice: str
    voice_label: str
    sample: str
    text: str
    hint: str = ""
    ok: bool = False
    error: str = ""
    first_byte_s: Optional[float] = None
    total_s: Optional[float] = None
    audio_s: Optional[float] = None
    bytes: int = 0
    cost_usd: Optional[float] = None
    file: str = ""
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def load_env() -> dict:
    env = {}
    p = ROOT / ".env"
    if p.is_file():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("GEMINI_API_KEY", "FISH_API_KEY", "GOOGLE_NARRATE_KEY"):
        if os.environ.get(k):
            env[k] = os.environ[k]
    return env


def wav_duration(data: bytes) -> Optional[float]:
    try:
        with wave.open(io.BytesIO(data)) as w:
            frames = w.getnframes()
            rate = w.getframerate()
            # Streaming-style headers can carry a bogus frame count; fall back to byte length.
            if frames <= 0 or frames > rate * 3600:
                frames = (len(data) - 44) // (w.getsampwidth() * w.getnchannels())
            return frames / rate
    except Exception:
        return None


def find_audio_b64(obj) -> Optional[str]:
    """Return the last base64 audio block in a Gemini interactions response."""
    found = None
    if isinstance(obj, dict):
        if obj.get("type") == "audio" and isinstance(obj.get("data"), str):
            found = obj["data"]
        for v in obj.values():
            found = find_audio_b64(v) or found
    elif isinstance(obj, list):
        for v in obj:
            found = find_audio_b64(v) or found
    return found


RATE_LIMIT_HITS: dict[str, int] = {}


class RpmLimiter:
    """Spaces requests so at most `rpm` start in any rolling 60s window."""
    def __init__(self, rpm: int):
        self.rpm = rpm
        self.starts: list[float] = []
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            while True:
                now = time.monotonic()
                self.starts = [t for t in self.starts if now - t < 60]
                if len(self.starts) < self.rpm:
                    self.starts.append(now)
                    return
                await asyncio.sleep(60 - (now - self.starts[0]) + 0.05)


def diverse_pick(items: list, n: int, gender, group) -> list:
    """Alternate genders, preferring groups (persona/style) not used yet; items are pre-sorted by preference."""
    by_gender: dict[str, list] = {}
    for it in items:
        by_gender.setdefault(gender(it), []).append(it)
    picked, seen_groups = [], set()
    while len(picked) < n and any(by_gender.values()):
        for g in list(by_gender):
            pool = by_gender[g]
            if not pool or len(picked) >= n:
                continue
            choice = next((it for it in pool if group(it) not in seen_groups), pool[0])
            pool.remove(choice)
            picked.append(choice)
            seen_groups.add(group(choice))
    return picked


def slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)[:60]


def fill_tags(text: str, provider: str) -> str:
    out = text.format(**INLINE_TAGS[provider])
    return " ".join(out.split())


async def timed_post(session: aiohttp.ClientSession, url: str, **kw):
    """POST and return (status, body_bytes, first_byte_s, total_s), retrying 429/5xx."""
    attempts = 6
    for attempt in range(attempts):
        t0 = time.perf_counter()
        async with session.post(url, **kw) as resp:
            first = None
            buf = bytearray()
            async for chunk in resp.content.iter_any():
                if first is None:
                    first = time.perf_counter() - t0
                buf.extend(chunk)
            total = time.perf_counter() - t0
            if resp.status in (429, 500, 502, 503) and attempt < attempts - 1:
                if resp.status == 429:
                    RATE_LIMIT_HITS[url] = RATE_LIMIT_HITS.get(url, 0) + 1
                m = re.search(rb"retry in (\d+(?:\.\d+)?)s", bytes(buf))
                await asyncio.sleep(float(m.group(1)) + 1 if m else 2 ** attempt * 2)
                continue
            return resp.status, bytes(buf), first, total


# ---------------------------------------------------------------------------
# providers
# ---------------------------------------------------------------------------

async def synth_gemini(session, key, model, voice, text, style) -> tuple[bytes, float, float, dict]:
    content = {"type": "text", "text": text}
    if style:
        content["annotations"] = [{"type": "speech_metadata", "style": style}]
    body = {
        "model": model,
        "input": [{"type": "user_input", "content": [content]}],
        "response_format": {"type": "audio"},
        "generation_config": {"speech_config": [{"voice": voice}]},
    }
    status, raw, first, total = await timed_post(
        session, GEMINI_INTERACTIONS, json=body, headers={"x-goog-api-key": key})
    if status != 200:
        raise RuntimeError(f"HTTP {status}: {raw[:300].decode(errors='replace')}")
    data = json.loads(raw)
    b64 = find_audio_b64(data)
    if not b64:
        raise RuntimeError(f"no audio in response: {raw[:300].decode(errors='replace')}")
    usage = data.get("usage") or data.get("usage_metadata") or {}
    return base64.b64decode(b64), first, total, {"usage": usage}


async def synth_fish(session, key, model, voice_id, text) -> tuple[bytes, float, float, dict]:
    body = {"text": text, "reference_id": voice_id, "format": "wav", "latency": "normal"}
    status, raw, first, total = await timed_post(
        session, FISH_TTS, json=body,
        headers={"Authorization": f"Bearer {key}", "model": model})
    if status != 200:
        raise RuntimeError(f"HTTP {status}: {raw[:300].decode(errors='replace')}")
    return raw, first, total, {}


async def synth_google(session, key, voice, text) -> tuple[bytes, float, float, dict]:
    lang = "-".join(voice.split("-")[:2])
    body = {
        "input": {"text": text},
        "voice": {"languageCode": lang, "name": voice},
        "audioConfig": {"audioEncoding": "LINEAR16"},
    }
    status, raw, first, total = await timed_post(
        session, GOOGLE_TTS, json=body, headers={"x-goog-api-key": key})
    if status != 200:
        raise RuntimeError(f"HTTP {status}: {raw[:300].decode(errors='replace')}")
    return base64.b64decode(json.loads(raw)["audioContent"]), first, total, {}


# ---------------------------------------------------------------------------
# voice discovery
# ---------------------------------------------------------------------------

async def list_gemini_voices(session, key) -> list[dict]:
    voices, token = [], None
    while True:
        params = {"language_code": "en-US", "page_size": "1000"}
        if token:
            params["page_token"] = token
        async with session.get(GEMINI_VOICES, params=params, headers={"x-goog-api-key": key}) as r:
            if r.status != 200:
                print(f"[gemini] voice list failed: {r.status} {(await r.text())[:200]}")
                return voices
            d = await r.json()
        voices += d.get("voices", [])
        token = d.get("next_page_token") or d.get("nextPageToken")
        if not token:
            return voices


async def list_fish_voices(session, key, n) -> tuple[list[dict], int]:
    params = {"page_size": str(min(max(n, 1), 100)), "language": "en", "sort_by": "task_count",
              "visibility": "public"}
    async with session.get(FISH_MODELS, params=params, headers={"Authorization": f"Bearer {key}"}) as r:
        if r.status != 200:
            print(f"[fish] voice list failed: {r.status} {(await r.text())[:200]}")
            return [], 0
        d = await r.json()
    return d.get("items", []), int(d.get("total") or 0)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@dataclass
class Job:
    provider: str
    model: str
    voice: str
    voice_label: str
    sample: Sample
    use_hint: bool = True


def cost_for(r: Result, when: str = "now") -> Optional[float]:
    if not r.ok:
        return None
    if r.provider == "gemini":
        table = GEMINI_PRICES if when == "now" else GEMINI_PRICES_2027
        if r.model not in table or r.audio_s is None:
            return None
        pin, pout = table[r.model]
        in_tokens = (len(r.text) + len(r.hint)) / 4
        return in_tokens * pin / 1e6 + r.audio_s * 25 * pout / 1e6
    if r.provider == "fish":
        price = FISH_PRICES.get(r.model)
        return None if price is None else len(r.text.encode("utf-8")) * price / 1e6
    if r.provider == "google":
        fam = "Chirp3-HD" if "Chirp3" in r.voice else "Wavenet"
        return len(r.text) * GOOGLE_PRICES[fam] / 1e6
    return None


async def run_job(session, env, job: Job, out_dir: Path, sem: asyncio.Semaphore,
                  limiter: Optional[RpmLimiter] = None) -> Result:
    s = job.sample
    text = fill_tags(s.text, job.provider)
    hint = ""
    if job.provider == "fish" and job.use_hint and s.fish_tag:
        hint = f"[{s.fish_tag}]"
        text = f"{hint} {text}"
    elif job.provider == "gemini" and job.use_hint and s.style:
        hint = s.style
    r = Result(job.provider, job.model, job.voice, job.voice_label, s.key, text, hint)
    async with sem:
        if limiter:
            await limiter.wait()
        try:
            if job.provider == "gemini":
                audio, first, total, extra = await synth_gemini(
                    session, env["GEMINI_API_KEY"], job.model, job.voice, text, hint or None)
            elif job.provider == "fish":
                audio, first, total, extra = await synth_fish(
                    session, env["FISH_API_KEY"], job.model, job.voice, text)
            else:
                audio, first, total, extra = await synth_google(
                    session, env["GOOGLE_NARRATE_KEY"], job.voice, text)
        except Exception as e:
            r.error = str(e)[:400]
            print(f"  x {job.provider}/{job.model}/{job.voice_label}/{s.key}: {r.error[:120]}")
            return r
    fname = f"{slug(job.provider)}__{slug(job.model)}__{slug(job.voice_label)}__{s.key}.wav"
    (out_dir / "audio" / fname).write_bytes(audio)
    r.ok, r.first_byte_s, r.total_s, r.bytes, r.file, r.extra = True, first, total, len(audio), f"audio/{fname}", extra
    r.audio_s = wav_duration(audio)
    r.cost_usd = cost_for(r)
    print(f"  ok {job.provider}/{job.model}/{job.voice_label}/{s.key}: "
          f"first {first:.2f}s total {total:.2f}s audio {r.audio_s or 0:.1f}s")
    return r


def summarize(results: list[Result]) -> list[dict]:
    groups: dict[tuple[str, str], list[Result]] = {}
    for r in results:
        groups.setdefault((r.provider, r.model), []).append(r)
    rows = []
    for (prov, model), rs in sorted(groups.items()):
        ok = [r for r in rs if r.ok]
        chars = sum(len(r.text) for r in ok)

        def med(attr):
            vals = [getattr(r, attr) for r in ok if getattr(r, attr) is not None]
            return statistics.median(vals) if vals else None

        def p90(attr):
            vals = sorted(getattr(r, attr) for r in ok if getattr(r, attr) is not None)
            return vals[min(len(vals) - 1, int(len(vals) * 0.9))] if vals else None

        def monthly(when):
            costs = [cost_for(r, when) for r in ok]
            if not ok or any(c is None for c in costs) or not chars:
                return None
            return sum(costs) / chars * MONTHLY_CHARS

        rows.append({
            "provider": prov, "model": model, "requests": len(rs), "errors": len(rs) - len(ok),
            "median_first_byte_s": med("first_byte_s"), "p90_first_byte_s": p90("first_byte_s"),
            "median_total_s": med("total_s"), "p90_total_s": p90("total_s"),
            "realtime_factor": (sum(r.total_s for r in ok) / sum(r.audio_s for r in ok if r.audio_s))
            if ok and any(r.audio_s for r in ok) else None,
            "monthly_usd_now": monthly("now"), "monthly_usd_2027": monthly("2027"),
        })
    return rows


def fmt(v, spec=".2f"):
    return "" if v is None else format(v, spec)


def write_report(out_dir: Path, results: list[Result], summary: list[dict], voice_info: dict) -> Path:
    with open(out_dir / "results.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[k for k in asdict(results[0]).keys() if k != "extra"])
        w.writeheader()
        for r in results:
            d = asdict(r)
            d.pop("extra")
            w.writerow(d)
    (out_dir / "summary.json").write_text(json.dumps({"summary": summary, "voices": voice_info}, indent=2))

    esc = html.escape
    parts = [f"""<!doctype html><html><head><meta charset="utf-8"><title>TTS comparison</title>
<style>
body{{font:14px/1.4 system-ui,sans-serif;margin:24px;max-width:1200px;background:#fafafa;color:#222}}
table{{border-collapse:collapse;margin:8px 0 24px;width:100%}}
td,th{{border:1px solid #ddd;padding:4px 8px;text-align:left;vertical-align:middle}}
th{{background:#eee}} .err{{color:#b00}} audio{{height:32px;width:260px}}
.prov-gemini{{background:#eef5ff}} .prov-fish{{background:#effaf0}} .prov-google{{background:#f6f6f6}}
h2{{margin-top:36px}} code{{background:#eee;padding:1px 4px}}
</style></head><body>
<h1>TTS comparison &mdash; {esc(out_dir.name)}</h1>
<p>Monthly cost projected at {MONTHLY_CHARS:,} characters/month from the clips actually generated.
Latency is measured from this machine; "first byte" matters for Fish (streams) and the baseline,
Gemini unary responses arrive all at once. Realtime factor = generation time / audio length (lower is better).
Google baseline costs are list prices; current usage fits inside its free tier.</p>
<h2>Summary</h2><table><tr><th>provider</th><th>model</th><th>ok/req</th><th>median first byte</th>
<th>p90 first byte</th><th>median total</th><th>p90 total</th><th>realtime factor</th>
<th>$/month now</th><th>$/month 2027</th></tr>"""]
    for s in summary:
        parts.append(
            f"<tr class='prov-{s['provider']}'><td>{s['provider']}</td><td>{esc(s['model'])}</td>"
            f"<td>{s['requests'] - s['errors']}/{s['requests']}</td>"
            f"<td>{fmt(s['median_first_byte_s'])}s</td><td>{fmt(s['p90_first_byte_s'])}s</td>"
            f"<td>{fmt(s['median_total_s'])}s</td><td>{fmt(s['p90_total_s'])}s</td>"
            f"<td>{fmt(s['realtime_factor'])}</td>"
            f"<td>{fmt(s['monthly_usd_now'])}</td><td>{fmt(s['monthly_usd_2027'])}</td></tr>")
    parts.append("</table>")

    if voice_info:
        parts.append("<h2>Voice catalogs</h2><ul>")
        for k, v in voice_info.items():
            parts.append(f"<li><b>{esc(k)}</b>: {esc(str(v))}</li>")
        parts.append("</ul>")

    def table(rows: list[Result], show_text: bool):
        out = ["<table><tr><th>provider / model</th><th>voice</th><th>audio</th>"
               + ("<th>text sent</th>" if show_text else "")
               + "<th>hint</th><th>first byte</th><th>total</th><th>audio len</th></tr>"]
        for r in rows:
            player = (f"<audio controls preload='none' src='{esc(r.file)}'></audio>" if r.ok
                      else f"<span class='err'>{esc(r.error[:200])}</span>")
            out.append(
                f"<tr class='prov-{r.provider}'><td>{r.provider} / {esc(r.model)}</td>"
                f"<td>{esc(r.voice_label)}</td><td>{player}</td>"
                + (f"<td><code>{esc(r.text)}</code></td>" if show_text else "")
                + f"<td>{esc(r.hint)}</td><td>{fmt(r.first_byte_s)}</td><td>{fmt(r.total_s)}</td>"
                f"<td>{fmt(r.audio_s, '.1f')}</td></tr>")
        out.append("</table>")
        return "".join(out)

    gallery = [r for r in results if r.sample == "gallery"]
    if gallery:
        parts.append(f"<h2>Voice gallery</h2><p>Same line in every voice: <i>{esc(GALLERY_LINE)}</i></p>")
        parts.append(table(gallery, show_text=False))

    for s in SAMPLES:
        rows = [r for r in results if r.sample == s.key]
        if not rows:
            continue
        parts.append(f"<h2>{esc(s.key)}</h2><p><i>{esc(s.text.format(laugh='(laugh)', sigh='(sigh)', pause='(pause)'))}</i></p>")
        parts.append(table(rows, show_text=True))

    parts.append("</body></html>")
    path = out_dir / "index.html"
    path.write_text("".join(parts), encoding="utf-8")
    return path


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--quick", action="store_true", help="5 samples, 2 voices per model, smaller gallery")
    ap.add_argument("--skip", default="", help="comma list of providers to skip: gemini,fish,google")
    ap.add_argument("--gemini-models", default="gemini-3.8-flash-tts,gemini-3.8-flash-lite-tts")
    ap.add_argument("--fish-models", default="s2.1-pro")
    ap.add_argument("--google-voices", default="en-US-Wavenet-D,en-US-Chirp3-HD-Kore")
    ap.add_argument("--voices", type=int, default=3, help="voices per model for the sample grid")
    ap.add_argument("--gallery", type=int, default=12, help="voices per provider in the gallery (0 = off)")
    ap.add_argument("--fish-voice-ids", default="", help="comma list of Fish model ids (default: most used)")
    ap.add_argument("--concurrency", type=int, default=2, help="parallel requests per provider")
    ap.add_argument("--gemini-rpm", type=int, default=9, help="client-side requests/minute cap per Gemini model")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    env = load_env()
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    for prov, var in (("gemini", "GEMINI_API_KEY"), ("fish", "FISH_API_KEY"), ("google", "GOOGLE_NARRATE_KEY")):
        if prov not in skip and not env.get(var):
            print(f"[{prov}] {var} not set; skipping")
            skip.add(prov)
    if {"gemini", "fish", "google"} <= skip:
        sys.exit("No providers available.")

    n_voices = 2 if args.quick else args.voices
    n_gallery = min(args.gallery, 6) if args.quick else args.gallery
    samples = [s for s in SAMPLES if not args.quick or s.key in QUICK_SAMPLES]

    out_dir = Path(args.out) if args.out else ROOT / "data" / "tts_compare" / datetime.now().strftime("%Y%m%d_%H%M%S")
    (out_dir / "audio").mkdir(parents=True, exist_ok=True)

    jobs: list[Job] = []
    voice_info: dict = {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        if "gemini" not in skip:
            catalog = await list_gemini_voices(session, env["GEMINI_API_KEY"])
            (out_dir / "gemini_voices.json").write_text(json.dumps(catalog, indent=2))
            personas = sorted({v.get("persona") for v in catalog if v.get("persona")})
            voice_info["gemini"] = f"{len(catalog)} en-US voices across {len(personas)} personas: {', '.join(personas)}"
            grid = GEMINI_PREBUILT[:n_voices]
            picks = diverse_pick(catalog, n_gallery, lambda v: v.get("gender") or "?",
                                 lambda v: (v.get("persona"), v.get("pitch")))
            gallery = [(v["id"], f"{v.get('display_name') or v['id']} ({v.get('gender')}, {v.get('pitch')} pitch, "
                                 f"{v.get('persona')})") for v in picks]
            for model in [m.strip() for m in args.gemini_models.split(",") if m.strip()]:
                for v in grid:
                    jobs += [Job("gemini", model, v, v, s) for s in samples]
                    # Same emotional lines without the style hint, to see what it infers on its own.
                    jobs += [Job("gemini", model, v, f"{v} (no hint)", s, use_hint=False) for s in samples if s.style]
            if n_gallery:
                first_model = args.gemini_models.split(",")[0].strip()
                jobs += [Job("gemini", first_model, vid, label, Sample("gallery", GALLERY_LINE)) for vid, label in gallery]

        if "fish" not in skip:
            items, total = await list_fish_voices(session, env["FISH_API_KEY"], 100)
            (out_dir / "fish_voices.json").write_text(json.dumps(items, indent=2))
            voice_info["fish"] = f"{total:,}+ public English voice models (listing is capped)"
            if args.fish_voice_ids:
                fish_voices = [(i.strip(), i.strip()) for i in args.fish_voice_ids.split(",") if i.strip()]
            else:
                def fish_gender(it):
                    tags = set(it.get("tags") or [])
                    return "female" if "female" in tags else "male" if "male" in tags else "?"
                style_tags = {"young", "old", "middle-aged", "character-voice", "narration", "conversational"}
                picks = diverse_pick(items, max(n_voices, n_gallery), fish_gender,
                                     lambda it: tuple(sorted(set(it.get("tags") or []) & style_tags)))
                fish_voices = [(it["_id"], f"{it.get('title') or it['_id']} ({fish_gender(it)})") for it in picks]
            for model in [m.strip() for m in args.fish_models.split(",") if m.strip()]:
                for vid, label in fish_voices[:n_voices]:
                    jobs += [Job("fish", model, vid, label, s) for s in samples]
                    jobs += [Job("fish", model, vid, f"{label} (no hint)", s, use_hint=False) for s in samples if s.fish_tag]
            if n_gallery:
                first_model = args.fish_models.split(",")[0].strip()
                jobs += [Job("fish", first_model, vid, label, Sample("gallery", GALLERY_LINE))
                         for vid, label in fish_voices[:n_gallery]]

        if "google" not in skip:
            for v in [v.strip() for v in args.google_voices.split(",") if v.strip()]:
                model = "Chirp3-HD" if "Chirp3" in v else v.split("-")[2]
                jobs += [Job("google", model, v, v, s) for s in samples]

        print(f"Running {len(jobs)} requests -> {out_dir}")
        gemini_models = {j.model for j in jobs if j.provider == "gemini"}
        sems = {p: asyncio.Semaphore(args.concurrency) for p in ("fish", "google")}
        sems.update({("gemini", m): asyncio.Semaphore(args.concurrency) for m in gemini_models})
        limiters = {m: RpmLimiter(args.gemini_rpm) for m in gemini_models}
        results = await asyncio.gather(*[
            run_job(session, env, j, out_dir,
                    sems[("gemini", j.model)] if j.provider == "gemini" else sems[j.provider],
                    limiters.get(j.model) if j.provider == "gemini" else None)
            for j in jobs])

    summary = summarize(results)
    report = write_report(out_dir, results, summary, voice_info)
    print("\nprovider  model                         ok/req  med_first  med_total  rtf   $/mo now  $/mo 2027")
    for s in summary:
        print(f"{s['provider']:<9} {s['model']:<29} {s['requests'] - s['errors']:>3}/{s['requests']:<3} "
              f"{fmt(s['median_first_byte_s']):>9}  {fmt(s['median_total_s']):>9}  {fmt(s['realtime_factor']):>4}  "
              f"{fmt(s['monthly_usd_now']):>8}  {fmt(s['monthly_usd_2027']):>9}")
    if RATE_LIMIT_HITS:
        print(f"429 rate-limit responses: {RATE_LIMIT_HITS}")
    print(f"\nReport: {report}")


if __name__ == "__main__":
    asyncio.run(main())
