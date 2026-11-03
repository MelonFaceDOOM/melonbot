"""
Provider-specific text prep for narration.

Discord syntax (same for every voice):
  [whispering] don't tell anyone     -> leading bracket(s) = delivery style for the message
  that was hilarious [laugh] anyway  -> inline event tags (laugh, sigh, gasp, cough, pause, ...)

Gemini takes the style as a separate annotation and events as <angle> tags.
Fish takes both as inline [square] cues. Google has no emotion control, so tags are stripped.
"""
import re
from typing import Optional, Tuple

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
