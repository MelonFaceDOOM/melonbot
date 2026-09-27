"""
Compare a Together chat model against held-out JSONL (real Melon replies).

Usage (from repo root, with TOGETHER_API_KEY set):

  python misc/eval_style_together.py --model mmjacobmm-ebe5/test --n 8

The GPU meter is running while the endpoint is deployed. Tear it down when done.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

from together import Together

DEFAULT_VALID = Path("data/style_export/pilot/117340965760532487_melon/valid.jsonl")


def load_examples(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def clip(text: str, n: int = 900) -> str:
    text = text.replace("\\n", "\n")
    if len(text) <= n:
        return text
    return text[: n - 3] + "..."


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--model", required=True, help="Together model / endpoint name")
    p.add_argument("--jsonl", type=Path, default=DEFAULT_VALID)
    p.add_argument("--n", type=int, default=8, help="How many examples to run")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--max-tokens", type=int, default=128)
    p.add_argument("--temperature", type=float, default=0.8)
    p.add_argument("--offset", type=int, default=None, help="Start at this line instead of random sample")
    args = p.parse_args()

    examples = load_examples(args.jsonl)
    if not examples:
        print(f"No examples in {args.jsonl}", file=sys.stderr)
        return 1

    rng = random.Random(args.seed)
    if args.offset is not None:
        picked = examples[args.offset : args.offset + args.n]
    else:
        k = min(args.n, len(examples))
        picked = rng.sample(examples, k)

    client = Together()
    print(f"model={args.model}  n={len(picked)}  from={args.jsonl}\n")

    for i, ex in enumerate(picked, 1):
        messages = ex["messages"]
        user = next(m["content"] for m in messages if m["role"] == "user")
        gold = next(m["content"] for m in messages if m["role"] == "assistant")
        channel = user.split("\n", 1)[0] if user.startswith("Channel:") else ""

        r = client.chat.completions.create(
            model=args.model,
            messages=messages[:-1],
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )
        pred = (r.choices[0].message.content or "").strip()

        print("=" * 72)
        print(f"[{i}/{len(picked)}] {channel}")
        print("--- context (tail) ---")
        print(clip(user, 700))
        print("--- model ---")
        print(pred)
        print("--- actual melon ---")
        print(gold)
        print()

    print("Done. If the dedicated endpoint is still up, delete or scale it to zero.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
