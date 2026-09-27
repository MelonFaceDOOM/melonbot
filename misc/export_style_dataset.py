"""
Export per-user chat JSONL from message_archive for style fine-tuning.

Walks each channel in snowflake order, merges consecutive same-author bursts into
turns, and emits (conversation context → that user's next message) examples.

Train/valid split is by time (last --valid-frac of each author's examples), not shuffle.

Usage (from repo root):
  python misc/export_style_dataset.py --list-authors
  python misc/export_style_dataset.py --author-id 123 --author-name melon
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import PSQL_CREDENTIALS  # noqa: E402

DEFAULT_OUT_DIR = Path("data/style_export")
DEFAULT_SYSTEM = (
    "You are posting in Discord as {name}. Match their voice, length, and habits. "
    "Output only the Discord message text."
)
URL_RE = re.compile(r"https?://\S+|www\.\S+|discord\.gg/\S+", re.IGNORECASE)
SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9._-]+")
IMAGE_EXT = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}

CHANNEL_SQL = """
SELECT id, name
FROM message_archive.channels
WHERE skip = FALSE
  AND (%s::bigint IS NULL OR guild_id = %s)
ORDER BY id
"""

MESSAGES_SQL = """
SELECT
  m.id,
  m.author_id,
  m.content,
  m.created_at,
  m.reference_id,
  m.has_attachments,
  m.is_bot,
  m.webhook_id,
  COALESCE(NULLIF(u.global_name, ''), NULLIF(u.username, ''), m.author_id::text)
    AS author_name,
  (
    SELECT string_agg(a.filename, ', ' ORDER BY a.id)
    FROM message_archive.attachments a
    WHERE a.message_id = m.id
  ) AS attachment_names,
  COALESCE((
    SELECT SUM(r.count)::int
    FROM message_archive.reactions r
    WHERE r.message_id = m.id
  ), 0) AS reaction_count
FROM message_archive.messages m
LEFT JOIN users u ON u.id = m.author_id
WHERE m.channel_id = %s
  AND m.deleted_at IS NULL
ORDER BY m.id
"""

LIST_AUTHORS_SQL = """
SELECT
  m.author_id,
  COALESCE(NULLIF(u.global_name, ''), u.username) AS name,
  u.username,
  COUNT(*)::bigint AS n
FROM message_archive.messages m
LEFT JOIN users u ON u.id = m.author_id
WHERE m.deleted_at IS NULL
  AND m.is_bot = FALSE
  AND (%s::bigint IS NULL OR m.guild_id = %s)
GROUP BY m.author_id, name, u.username
ORDER BY n DESC
LIMIT %s
"""

LOOKUP_BY_ID_SQL = """
SELECT id, username, global_name
FROM users
WHERE id = ANY(%s)
"""

LOOKUP_BY_NAME_SQL = """
SELECT id, username, global_name
FROM users
WHERE LOWER(username) = ANY(%s)
   OR LOWER(global_name) = ANY(%s)
"""


@dataclass
class Msg:
    id: int
    author_id: int
    author_name: str
    content: str
    created_at: datetime
    reference_id: Optional[int]
    has_attachments: bool
    attachment_names: str
    reaction_count: int
    is_bot: bool
    webhook_id: Optional[int]


@dataclass
class Turn:
    author_id: int
    author_name: str
    created_at: datetime
    end_at: datetime
    texts: list[str] = field(default_factory=list)
    message_ids: list[int] = field(default_factory=list)
    reaction_count: int = 0
    is_reply: bool = False

    def add(self, msg: Msg, formatted: str) -> None:
        self.texts.append(formatted)
        self.message_ids.append(msg.id)
        self.end_at = msg.created_at
        self.reaction_count += msg.reaction_count
        if msg.reference_id:
            self.is_reply = True
        if msg.author_name:
            self.author_name = msg.author_name

    @property
    def content(self) -> str:
        return "\n".join(t for t in self.texts if t).strip()


@dataclass
class Example:
    author_id: int
    created_at: datetime
    is_reply: bool
    reaction_count: int
    messages: list[dict[str, str]]
    channel_id: int = 0

    @property
    def assistant(self) -> str:
        return self.messages[-1]["content"] if self.messages else ""

    @property
    def user_text(self) -> str:
        for msg in self.messages:
            if msg["role"] == "user":
                return msg["content"]
        return ""


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def display_name(username: Optional[str], global_name: Optional[str], user_id: int) -> str:
    for value in (global_name, username):
        if value and str(value).strip():
            return str(value).strip()
    return str(user_id)


def slug(name: str, user_id: int) -> str:
    cleaned = SAFE_NAME_RE.sub("_", name).strip("_").lower() or "user"
    return f"{user_id}_{cleaned[:48]}"


def format_body(msg: Msg) -> str:
    text = (msg.content or "").replace("\r\n", "\n").strip()
    if msg.attachment_names:
        labels = []
        for raw in msg.attachment_names.split(", "):
            name = raw.strip()
            if not name:
                continue
            suffix = Path(name).suffix.lower()
            kind = "image" if suffix in IMAGE_EXT else "file"
            labels.append(f"[{kind}: {name}]")
        extra = " ".join(labels)
        text = f"{text}\n{extra}".strip() if text else extra
    return text


def is_command(text: str, prefixes: tuple[str, ...]) -> bool:
    stripped = text.lstrip()
    if not stripped:
        return False
    first = stripped.split(None, 1)[0]
    return any(first.startswith(p) for p in prefixes if p)


def is_link_only(text: str) -> bool:
    leftover = URL_RE.sub("", text)
    leftover = re.sub(r"[\s\-_|*~`>]+", "", leftover)
    return leftover == ""


def skip_as_context(msg: Msg) -> bool:
    if msg.is_bot or msg.webhook_id:
        return True
    return False


def format_user_prompt(channel_name: Optional[str], context: Iterable[Turn]) -> str:
    header = f"Channel: #{channel_name}" if channel_name else "Channel: (unknown)"
    lines = [header, ""]
    for turn in context:
        body = turn.content
        if not body:
            continue
        indented = body.replace("\n", "\n  ")
        lines.append(f"[{turn.author_name}] {indented}")
    return "\n".join(lines).rstrip() + "\n"


def build_example(
    author_name: str,
    channel_name: Optional[str],
    context: list[Turn],
    target: Turn,
    system_template: str,
    channel_id: int,
) -> Example:
    system = system_template.format(name=author_name)
    return Example(
        author_id=target.author_id,
        created_at=target.created_at,
        is_reply=target.is_reply,
        reaction_count=target.reaction_count,
        channel_id=channel_id,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": format_user_prompt(channel_name, context)},
            {"role": "assistant", "content": target.content},
        ],
    )


def should_emit_target(
    target: Turn,
    context: list[Turn],
    *,
    min_context: int,
    min_chars: int,
    max_gap: timedelta,
    prefixes: tuple[str, ...],
    min_reactions: int,
    since: Optional[datetime],
) -> Optional[str]:
    if since is not None and _utc(target.created_at) < since:
        return "too_old"
    if len(context) < min_context:
        return "min_context"
    content = target.content
    if len(content) < min_chars:
        return "min_chars"
    if is_command(content, prefixes):
        return "command"
    if is_link_only(content):
        return "link_only"
    if target.reaction_count < min_reactions:
        return "min_reactions"
    gap = _utc(target.created_at) - _utc(context[-1].end_at)
    if gap > max_gap:
        return "max_gap"
    return None


def iter_channel_messages(conn, channel_id: int) -> Iterator[Msg]:
    cur = conn.cursor(name=f"exp_{channel_id}", cursor_factory=RealDictCursor)
    try:
        cur.itersize = 2000
        cur.execute(MESSAGES_SQL, (channel_id,))
        while True:
            rows = cur.fetchmany(2000)
            if not rows:
                break
            for row in rows:
                yield Msg(
                    id=int(row["id"]),
                    author_id=int(row["author_id"]),
                    author_name=str(row["author_name"] or row["author_id"]),
                    content=row["content"] or "",
                    created_at=_utc(row["created_at"]),
                    reference_id=row["reference_id"],
                    has_attachments=bool(row["has_attachments"]),
                    attachment_names=row["attachment_names"] or "",
                    reaction_count=int(row["reaction_count"] or 0),
                    is_bot=bool(row["is_bot"]),
                    webhook_id=row["webhook_id"],
                )
    finally:
        cur.close()


def process_channel(
    conn,
    channel_id: int,
    channel_name: Optional[str],
    target_ids: set[int],
    target_names: dict[int, str],
    args: argparse.Namespace,
    global_skips: Counter,
    skips_by_author: dict[int, Counter],
) -> list[Example]:
    merge_gap = timedelta(seconds=args.merge_gap_seconds)
    max_gap = timedelta(minutes=args.max_gap_minutes)
    prefixes = tuple(p for p in args.command_prefixes.split(",") if p)
    window: deque[Turn] = deque(maxlen=max(args.context_turns, 1))
    pending: Optional[Turn] = None
    out: list[Example] = []
    since = getattr(args, "since_cutoff", None)
    stride = max(1, args.channel_stride)
    targets_in_channel = 0

    def flush() -> None:
        nonlocal pending, targets_in_channel
        if pending is None:
            return
        turn = pending
        pending = None
        if turn.author_id in target_ids:
            ctx = list(window)
            reason = should_emit_target(
                turn,
                ctx,
                min_context=args.min_context,
                min_chars=args.min_chars,
                max_gap=max_gap,
                prefixes=prefixes,
                min_reactions=args.min_reactions,
                since=since,
            )
            author_skips = skips_by_author[turn.author_id]
            if reason is None and stride > 1:
                if targets_in_channel % stride != 0:
                    reason = "stride"
                targets_in_channel += 1
            if reason:
                author_skips[reason] += 1
            else:
                name = target_names.get(turn.author_id) or turn.author_name
                out.append(
                    build_example(
                        name,
                        channel_name,
                        ctx,
                        turn,
                        args.system_prompt,
                        channel_id,
                    )
                )
                author_skips["emitted"] += 1
        if turn.content:
            window.append(turn)

    for msg in iter_channel_messages(conn, channel_id):
        if skip_as_context(msg):
            global_skips["bot"] += 1
            continue
        body = format_body(msg)
        if not body:
            global_skips["empty"] += 1
            continue
        if (
            pending is not None
            and pending.author_id == msg.author_id
            and (msg.created_at - pending.end_at) <= merge_gap
        ):
            pending.add(msg, body)
            continue
        flush()
        pending = Turn(
            author_id=msg.author_id,
            author_name=msg.author_name,
            created_at=msg.created_at,
            end_at=msg.created_at,
        )
        pending.add(msg, body)
    flush()
    return out


def resolve_authors(conn, author_ids: list[int], author_names: list[str]) -> dict[int, str]:
    ids = [int(x) for x in author_ids]
    names = [n.strip().lower() for n in author_names if n.strip()]
    if not ids and not names:
        return {}
    rows = []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if ids:
            cur.execute(LOOKUP_BY_ID_SQL, (ids,))
            rows.extend(cur.fetchall())
        if names:
            cur.execute(LOOKUP_BY_NAME_SQL, (names, names))
            rows.extend(cur.fetchall())
    found: dict[int, str] = {}
    matched_names: set[str] = set()
    for row in rows:
        uid = int(row["id"])
        found[uid] = display_name(row["username"], row["global_name"], uid)
        if row["username"]:
            matched_names.add(row["username"].lower())
        if row["global_name"]:
            matched_names.add(row["global_name"].lower())
    for uid in ids:
        if uid not in found:
            found[uid] = str(uid)
    unresolved = [n for n in names if n not in matched_names]
    if unresolved:
        raise SystemExit(f"Could not resolve author name(s): {', '.join(unresolved)}")
    return found


def list_authors(conn, guild_id: Optional[int], limit: int) -> None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(LIST_AUTHORS_SQL, (guild_id, guild_id, limit))
        rows = cur.fetchall()
    print(f"{'author_id':<22} {'posts':>10}  name")
    for row in rows:
        name = row["name"] or row["username"] or ""
        print(f"{row['author_id']:<22} {row['n']:>10}  {name}")


def split_examples(examples: list[Example], valid_frac: float) -> tuple[list[Example], list[Example]]:
    ordered = sorted(examples, key=lambda e: (e.created_at, e.author_id))
    if not ordered:
        return [], []
    n_valid = int(round(len(ordered) * valid_frac))
    if len(ordered) < 10:
        return ordered, []
    n_valid = max(1, min(n_valid, len(ordered) // 2))
    cut = len(ordered) - n_valid
    return ordered[:cut], ordered[cut:]


def upsample_replies(examples: list[Example], factor: int) -> list[Example]:
    if factor <= 1:
        return examples
    extra: list[Example] = []
    for ex in examples:
        if ex.is_reply:
            extra.extend([ex] * (factor - 1))
    return examples + extra


def dedupe_exact_pairs(examples: list[Example], skips: Counter) -> list[Example]:
    """Drop identical (context, reply) pairs. Near-dupes from adjacent turns still remain."""
    seen: set[tuple[str, str]] = set()
    out: list[Example] = []
    for ex in examples:
        key = (ex.user_text, ex.assistant)
        if key in seen:
            skips["dupe_pair"] += 1
            continue
        seen.add(key)
        out.append(ex)
    return out


def cap_identical_replies(
    examples: list[Example], max_n: int, skips: Counter
) -> list[Example]:
    """Keep at most max_n copies of the exact same assistant text (e.g. repeated 'lol')."""
    if max_n <= 0:
        return examples
    counts: Counter = Counter()
    out: list[Example] = []
    for ex in examples:
        text = ex.assistant
        if counts[text] >= max_n:
            skips["identical_reply"] += 1
            continue
        counts[text] += 1
        out.append(ex)
    return out


def cap_per_channel(
    examples: list[Example], max_n: Optional[int], rng: random.Random, skips: Counter
) -> list[Example]:
    if max_n is None or max_n <= 0:
        return examples
    by_ch: dict[int, list[Example]] = defaultdict(list)
    for ex in examples:
        by_ch[ex.channel_id].append(ex)
    out: list[Example] = []
    for rows in by_ch.values():
        if len(rows) <= max_n:
            out.extend(rows)
            continue
        rng.shuffle(rows)
        out.extend(rows[:max_n])
        skips["max_per_channel"] += len(rows) - max_n
    return out


def subsample(examples: list[Example], max_n: Optional[int], rng: random.Random) -> list[Example]:
    if max_n is None or max_n <= 0 or len(examples) <= max_n:
        return examples
    return rng.sample(examples, max_n)


def write_jsonl(path: Path, examples: list[Example]) -> None:
    # Together's Windows CLI opens JSONL with the locale encoding (often cp1252),
    # not UTF-8. Dump with ensure_ascii so Discord unicode becomes \uXXXX and the
    # file is 7-bit ASCII.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="ascii", newline="\n") as f:
        for ex in examples:
            f.write(json.dumps({"messages": ex.messages}, ensure_ascii=True) + "\n")


def write_manifest(
    path: Path,
    author_id: int,
    author_name: str,
    train: list[Example],
    valid: list[Example],
    skips: Counter,
    args: argparse.Namespace,
) -> None:
    payload = {
        "author_id": author_id,
        "author_name": author_name,
        "train": len(train),
        "valid": len(valid),
        "reply_train": sum(1 for e in train if e.is_reply),
        "reply_valid": sum(1 for e in valid if e.is_reply),
        "skips": dict(skips),
        "params": {
            "context_turns": args.context_turns,
            "min_context": args.min_context,
            "min_chars": args.min_chars,
            "max_gap_minutes": args.max_gap_minutes,
            "merge_gap_seconds": args.merge_gap_seconds,
            "valid_frac": args.valid_frac,
            "min_reactions": args.min_reactions,
            "upsample_replies": args.upsample_replies,
            "guild_id": args.guild_id,
            "since_years": args.since_years,
            "channel_stride": args.channel_stride,
            "max_identical_reply": args.max_identical_reply,
            "max_per_channel": args.max_per_channel,
            "max_train": args.max_train,
            "max_valid": args.max_valid,
            "seed": args.seed,
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--list-authors", action="store_true", help="Print top posters and exit")
    p.add_argument("--list-limit", type=int, default=50)
    p.add_argument("--author-id", action="append", type=int, default=[], help="Repeatable Discord user id")
    p.add_argument("--author-name", action="append", default=[], help="Repeatable username or global_name")
    p.add_argument("--guild-id", type=int, default=None)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--context-turns", type=int, default=12)
    p.add_argument("--min-context", type=int, default=2)
    p.add_argument("--min-chars", type=int, default=8)
    p.add_argument("--max-gap-minutes", type=int, default=720, help="Drop if last context turn is older than this")
    p.add_argument("--merge-gap-seconds", type=int, default=120, help="Merge consecutive same-author messages")
    p.add_argument("--valid-frac", type=float, default=0.1)
    p.add_argument("--min-reactions", type=int, default=0)
    p.add_argument("--upsample-replies", type=int, default=1, help="Repeat reply examples in train only")
    p.add_argument("--command-prefixes", default="!,?", help="Comma-separated; skip target turns that start with these")
    p.add_argument("--system-prompt", default=DEFAULT_SYSTEM)
    p.add_argument("--limit-channels", type=int, default=None, help="Debug: only first N channels")
    p.add_argument(
        "--since-years",
        type=float,
        default=None,
        help="Keep target turns from the last N years only",
    )
    p.add_argument(
        "--channel-stride",
        type=int,
        default=1,
        help="Keep 1 of every N otherwise-valid target turns per channel (cuts sliding-window overlap)",
    )
    p.add_argument(
        "--max-identical-reply",
        type=int,
        default=0,
        help="Keep at most N examples with the exact same assistant text (0 = unlimited)",
    )
    p.add_argument(
        "--max-per-channel",
        type=int,
        default=None,
        help="After other filters, cap examples per channel (random drop)",
    )
    p.add_argument(
        "--max-train",
        type=int,
        default=None,
        help="Random subsample of train after time split",
    )
    p.add_argument(
        "--max-valid",
        type=int,
        default=None,
        help="Random subsample of valid (default: 10%% of --max-train if set)",
    )
    p.add_argument("--seed", type=int, default=0)
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    conn = psycopg2.connect(**PSQL_CREDENTIALS)
    try:
        if args.list_authors:
            list_authors(conn, args.guild_id, args.list_limit)
            return 0

        authors = resolve_authors(conn, args.author_id, args.author_name)
        if not authors:
            print("Pass --author-id and/or --author-name (or --list-authors).", file=sys.stderr)
            return 2

        print(f"Exporting {len(authors)} author(s):")
        for uid, name in authors.items():
            print(f"  {uid}  {name}")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(CHANNEL_SQL, (args.guild_id, args.guild_id))
            channels = cur.fetchall()
        if args.limit_channels:
            channels = channels[: args.limit_channels]
        if args.since_years:
            args.since_cutoff = datetime.now(timezone.utc) - timedelta(
                days=int(args.since_years * 365.25)
            )
            print(f"Keeping target turns since {args.since_cutoff.date().isoformat()}")
        else:
            args.since_cutoff = None

        rng = random.Random(args.seed)
        print(f"Scanning {len(channels)} channel(s)...")

        by_author: dict[int, list[Example]] = defaultdict(list)
        skips_by_author: dict[int, Counter] = {uid: Counter() for uid in authors}
        global_skips: Counter = Counter()
        target_ids = set(authors)

        for i, ch in enumerate(channels, 1):
            channel_id = int(ch["id"])
            channel_name = ch["name"]
            examples = process_channel(
                conn,
                channel_id,
                channel_name,
                target_ids,
                authors,
                args,
                global_skips,
                skips_by_author,
            )
            for ex in examples:
                by_author[ex.author_id].append(ex)
            if i % 25 == 0 or i == len(channels):
                total = sum(len(v) for v in by_author.values())
                print(
                    f"  {i}/{len(channels)} channels  examples={total}  "
                    f"scan={dict(global_skips)}",
                    flush=True,
                )

        args.out_dir.mkdir(parents=True, exist_ok=True)
        scan_path = args.out_dir / "scan.json"
        scan_path.write_text(
            json.dumps(
                {
                    "channels": len(channels),
                    "guild_id": args.guild_id,
                    "authors": {str(k): v for k, v in authors.items()},
                    "skips": dict(global_skips),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        for uid, name in authors.items():
            examples = by_author.get(uid, [])
            skips = skips_by_author[uid]
            examples = dedupe_exact_pairs(examples, skips)
            examples = cap_identical_replies(examples, args.max_identical_reply, skips)
            examples = cap_per_channel(examples, args.max_per_channel, rng, skips)
            train, valid = split_examples(examples, args.valid_frac)
            train = upsample_replies(train, args.upsample_replies)
            max_valid = args.max_valid
            if max_valid is None and args.max_train:
                max_valid = max(1, int(round(args.max_train * args.valid_frac)))
            n_train_before = len(train)
            n_valid_before = len(valid)
            train = subsample(train, args.max_train, rng)
            valid = subsample(valid, max_valid, rng)
            if args.max_train and n_train_before > len(train):
                skips["subsample_train"] = n_train_before - len(train)
            if max_valid and n_valid_before > len(valid):
                skips["subsample_valid"] = n_valid_before - len(valid)
            dest = args.out_dir / slug(name, uid)
            write_jsonl(dest / "train.jsonl", train)
            write_jsonl(dest / "valid.jsonl", valid)
            write_manifest(
                dest / "manifest.json",
                uid,
                name,
                train,
                valid,
                skips_by_author[uid],
                args,
            )
            print(f"Wrote {dest}  train={len(train)}  valid={len(valid)}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
