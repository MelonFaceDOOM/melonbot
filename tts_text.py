"""
Provider-specific text prep for narration.

Discord syntax (same for every voice):
  [whispering] don't tell anyone     -> leading bracket(s) = delivery style for the message
  that was hilarious [laugh] anyway  -> inline event tags (laugh, sigh, gasp, cough, pause, ...)

Gemini takes the style as a separate annotation and events as <angle> tags.
Fish takes both as inline [square] cues. Google has no emotion control, so tags are stripped.
"""
import re
from typing import List, Optional, Tuple

PROVIDERS = ("google", "fish", "gemini")

# canonical event -> (gemini tag, fish cue)
_EVENTS = {
    "laugh": ("<laugh>", "[laughing]"),
    "sigh": ("<sigh>", "[sighing]"),
    "gasp": ("<gasp>", "[gasp]"),
    "cough": ("<cough>", "[coughing]"),
    "breath": ("<breath>", "[breath]"),
    "yawn": ("<yawn>", "[yawning]"),
    "pause": ("<short pause>", "[break]"),
    "long pause": ("<long pause>", "[long-break]"),
}
_EVENT_ALIASES = {
    "laugh": "laugh", "laughs": "laugh", "laughing": "laugh",
    "sigh": "sigh", "sighs": "sigh", "sighing": "sigh",
    "gasp": "gasp", "gasps": "gasp", "gasping": "gasp",
    "cough": "cough", "coughs": "cough", "coughing": "cough",
    "breath": "breath", "breathes": "breath", "inhale": "breath",
    "yawn": "yawn", "yawns": "yawn", "yawning": "yawn",
    "pause": "pause", "short pause": "pause", "break": "pause",
    "long pause": "long pause", "long-pause": "long pause", "long break": "long pause", "long-break": "long pause",
}

# Chat abbreviations TTS models misread (Fish said "brb" as "burb", "lmao" as "lemme").
_SLANG = {
    "brb": "be right back", "idk": "I don't know", "idc": "I don't care", "tbh": "to be honest",
    "imo": "in my opinion", "imho": "in my humble opinion", "ngl": "not gonna lie", "omg": "oh my god",
    "afk": "A F K", "gg": "G G", "ggs": "G Gs", "wp": "well played", "ggwp": "G G, well played",
    "lmao": "L M A O", "lmfao": "L M F A O", "smh": "shaking my head", "nvm": "never mind",
    "btw": "by the way", "irl": "I R L", "fr": "for real", "rn": "right now", "ty": "thank you",
    "thx": "thanks", "np": "no problem", "pls": "please", "plz": "please", "tho": "though",
}
_SLANG_RE = re.compile(r"(?<![\w'])(" + "|".join(sorted(map(re.escape, _SLANG), key=len, reverse=True)) + r")(?![\w'])",
                       re.IGNORECASE)

_BRACKET_RE = re.compile(r"\[([^\[\]]{1,200})\]")
_LEADING_RE = re.compile(r"^\s*\[([^\[\]]{1,200})\]\s*")
MAX_STYLE_LEN = 200


def _event_key(tag: str) -> Optional[str]:
    return _EVENT_ALIASES.get(" ".join(tag.lower().split()))


def _collapse(s: str) -> str:
    return " ".join(s.split()).strip()


def split_leading_style(text: str) -> Tuple[Optional[str], str]:
    """Pull leading [style] cues off the message. Stacked cues are joined; event tags stay inline."""
    styles = []
    rest = text or ""
    while True:
        m = _LEADING_RE.match(rest)
        if not m or _event_key(m.group(1)):
            break
        styles.append(_collapse(m.group(1)))
        rest = rest[m.end():]
    style = ", ".join(s for s in styles if s)[:MAX_STYLE_LEN] or None
    return style, rest.strip()


def map_inline_tags(text: str, provider: str) -> str:
    def repl(m: re.Match) -> str:
        key = _event_key(m.group(1))
        if provider == "gemini":
            return _EVENTS[key][0] if key else ""
        if provider == "fish":
            return _EVENTS[key][1] if key else m.group(0)
        return ""
    return _collapse(_BRACKET_RE.sub(repl, text))


def expand_slang(text: str) -> str:
    return _SLANG_RE.sub(lambda m: _SLANG[m.group(1).lower()], text)


# ---------- chunking ----------
# Sentence ends (keeping closing quotes/brackets with the sentence) and line breaks
_SENT_SPLIT_RE = re.compile(r"(?<=[.!?…])\s+|(?<=[.!?…][\"')\]])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r"(?<=[,;:])\s+|\s+(?=[—–-]\s)")
# Run-ons without punctuation: break before a conjunction rather than at an arbitrary space
_CONJ_SPLIT_RE = re.compile(r"\s+(?=(?:and|but|so|then|because|which|when|while|until)\s)", re.IGNORECASE)
_TAG_SPACE = "\x00"


def _protect_tags(text: str) -> str:
    """Keep [multi word tags] in one piece while splitting."""
    return _BRACKET_RE.sub(lambda m: m.group(0).replace(" ", _TAG_SPACE), text)


def _split_words(piece: str, limit: int) -> List[str]:
    out, cur = [], ""
    for word in piece.split():
        while len(word) > limit:  # keyboard mash / long URL remnants: hard split
            if cur:
                out.append(cur)
                cur = ""
            out.append(word[:limit])
            word = word[limit:]
        if cur and len(cur) + 1 + len(word) > limit:
            out.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}" if cur else word
    if cur:
        out.append(cur)
    return out


def _split_long(sentence: str, limit: int) -> List[str]:
    """Split an over-long sentence at clauses, then conjunctions, then words; repack up to limit."""
    parts = []
    for clause in _CLAUSE_SPLIT_RE.split(sentence):
        for sub in (_CONJ_SPLIT_RE.split(clause) if len(clause) > limit else [clause]):
            parts.extend(_split_words(sub, limit) if len(sub) > limit else [sub])
    out, cur = [], ""
    for part in (p.strip() for p in parts):
        if not part:
            continue
        if cur and len(cur) + 1 + len(part) > limit:
            out.append(cur)
            cur = part
        else:
            cur = f"{cur} {part}" if cur else part
    if cur:
        out.append(cur)
    return out


def chunk_for_speech(text: str, first: int, cap: int, growth: float = 3.0, min_tail: int = 40) -> List[str]:
    """
    Split text for sequential TTS requests: a small first chunk so audio starts quickly, then chunks
    up to `growth`x the previous one (each is synthesized while the previous plays), capped at `cap`.
    Breaks prefer sentence ends, then clauses/conjunctions, and only then plain spaces.
    """
    text = _collapse_lines(_protect_tags(text or ""))
    if not text:
        return []
    if len(text) <= first:
        return [text.replace(_TAG_SPACE, " ")]

    chunks: List[str] = []
    cur, limit = "", first
    for sentence in (s.strip() for s in _SENT_SPLIT_RE.split(text)):
        if not sentence:
            continue
        for piece in (_split_long(sentence, limit) if len(sentence) > limit else [sentence]):
            if cur and len(cur) + 1 + len(piece) > limit:
                chunks.append(cur)
                cur = piece
                limit = min(cap, max(limit, int(len(chunks[-1]) * growth)))
            else:
                cur = f"{cur} {piece}" if cur else piece
            # Ship the first chunk as soon as it holds a full sentence/clause near the target size
            if not chunks and len(cur) >= first * 0.6:
                chunks.append(cur)
                cur = ""
                limit = min(cap, max(first, int(len(chunks[-1]) * growth)))
    if cur:
        # Fold a short leftover into the previous chunk, but never into the first (that one is kept small for speed)
        if len(chunks) > 1 and len(cur) < min_tail and len(chunks[-1]) + 1 + len(cur) <= cap:
            chunks[-1] = f"{chunks[-1]} {cur}"
        else:
            chunks.append(cur)
    return [c.replace(_TAG_SPACE, " ") for c in chunks]


def _collapse_lines(s: str) -> str:
    """Collapse spaces but keep line breaks (they're sentence boundaries)."""
    return "\n".join(" ".join(line.split()) for line in s.splitlines() if line.strip())


def prepare(text: str, provider: str, style: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """Return (text to send, style to send separately) for a provider."""
    if provider not in PROVIDERS:
        raise ValueError(f"unknown provider {provider!r}")
    if provider == "google":
        return map_inline_tags(text, "google"), None
    out = map_inline_tags(expand_slang(text), provider)
    style = _collapse(style)[:MAX_STYLE_LEN] if style else None
    if provider == "fish":
        return (f"[{style}] {out}" if style else out), None
    return out, style
