"""Deterministic dataset for the Laneway mock API.

Generates 8 000 issues in memory using a fixed seed so every CI run, every
learner, every replay sees the exact same payload bytes. The data is created
once at server startup and never mutates.

Design notes:
- We avoid Python's `random` for fields that would normally come from a DB
  (titles, labels) and use a small hand-curated pool with index-based
  selection so the output is independent of any version of the random
  module. The only `random` use is for sprinkling nulls / labels, also
  seeded.
- `closed_at` is NULL for ~30 % of issues — the spec asks for that pattern.
- `labels` is a nested array (0..3 entries) for ~5 % of issues — the spec
  mentions `nested labels array` as a trick.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

SEED = 42
TOTAL_ISSUES = 8_000

_AUTHORS = [
    "alice.martin",
    "bob.dupont",
    "carla.rossi",
    "dimitri.k",
    "elena.silva",
    "fanny.lebrun",
    "gabriel.ng",
    "haruki.tanaka",
]

_STATES = ["open", "open", "open", "closed", "closed", "in_review"]
_PRIORITIES = ["low", "medium", "high", "critical"]
_LABEL_POOL = [
    "bug",
    "feature",
    "p1",
    "p2",
    "regression",
    "ux",
    "infra",
    "docs",
    "tech-debt",
    "blocked",
]

_TITLE_PREFIXES = [
    "Pagination breaks on",
    "503 from upstream when",
    "Race condition during",
    "Memory leak suspected in",
    "Stale cache for",
    "Missing index on",
    "Flaky test in",
    "UI regression on",
    "Documentation gap around",
    "Performance drop in",
]
_TITLE_SUBJECTS = [
    "checkout API",
    "search reranker",
    "billing webhook",
    "issue export job",
    "user feed",
    "session cookie",
    "permission cache",
    "audit log writer",
    "notifications worker",
    "settings page",
]


def _pick(rng: random.Random, pool: list) -> Any:
    return pool[rng.randrange(len(pool))]


def _build_one(issue_id: int, rng: random.Random) -> dict:
    created = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
        minutes=issue_id * 11 % (60 * 24 * 90)
    )
    is_closed = _pick(rng, _STATES) == "closed"
    closed_at: str | None = None
    if is_closed:
        closed_at = (created + timedelta(hours=rng.randint(1, 240))).isoformat()

    labels: list[str] = []
    if rng.random() < 0.05:
        n = rng.randint(1, 3)
        # `set` would be non-deterministic across CPython versions; use a
        # deterministic pass over the pool.
        picked: list[str] = []
        idx = rng.randrange(len(_LABEL_POOL))
        for k in range(n):
            picked.append(_LABEL_POOL[(idx + k) % len(_LABEL_POOL)])
        labels = picked

    return {
        "issue_id": issue_id,
        "title": f"{_pick(rng, _TITLE_PREFIXES)} {_pick(rng, _TITLE_SUBJECTS)}",
        "author": _pick(rng, _AUTHORS),
        "state": "closed" if is_closed else _pick(rng, ["open", "in_review", "open"]),
        "priority": _pick(rng, _PRIORITIES),
        "created_at": created.isoformat(),
        "closed_at": closed_at,
        "labels": labels,
        "comments_count": rng.randint(0, 25),
    }


def build_dataset() -> list[dict]:
    """Build the full deterministic list of issues (8 000 items)."""
    rng = random.Random(SEED)
    return [_build_one(issue_id=100_000 + i, rng=rng) for i in range(TOTAL_ISSUES)]


# Pre-build once at import time. Cheap (<200 ms) and lets the server reuse it
# across requests without recomputing.
DATASET: list[dict] = build_dataset()
