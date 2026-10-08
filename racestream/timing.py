"""Producer and consumers for a race-timing stream.

  race:timing           the stream: one entry per completed lap
  group "strategy"      two workers share the work; each lap goes to exactly one of them
  group "leaderboard"   an independent group: sees every lap too, keeps its own position
  race:best             sorted set of each car's best lap (ZADD LT keeps the lower score)
"""
from __future__ import annotations

import random

from .resp import Redis, RedisError

STREAM = "race:timing"


def pairs(flat: list[str]) -> dict[str, str]:
    return dict(zip(flat[::2], flat[1::2]))


def entries(reply) -> list[tuple[str, dict]]:
    """XREADGROUP/XREAD reply -> [(id, fields)] for our one stream."""
    if not reply:
        return []
    _, items = reply[0]
    return [(eid, pairs(fields)) for eid, fields in items]


def produce(r: Redis, cars=("5", "24", "48", "77"), laps: int = 10, seed: int = 7, maxlen: int = 10_000) -> int:
    rng = random.Random(seed)
    base = {c: rng.uniform(40.6, 41.4) for c in cars}
    n = 0
    for lap in range(1, laps + 1):
        for car in cars:
            t = base[car] + lap * 0.02 + rng.gauss(0, 0.25)
            # MAXLEN ~ caps memory: Redis trims whole macro-nodes, so the cap is approximate and cheap.
            r("XADD", STREAM, "MAXLEN", "~", maxlen, "*", "car", car, "lap", lap, "lap_ms", round(t * 1000))
            n += 1
    return n


def ensure_group(r: Redis, group: str, start: str = "0") -> None:
    try:
        r("XGROUP", "CREATE", STREAM, group, start, "MKSTREAM")
    except RedisError as e:
        if "BUSYGROUP" not in str(e):           # already exists: fine, groups are durable
            raise


def read(r: Redis, group: str, consumer: str, count: int = 10, block_ms: int | None = None):
    args = ["XREADGROUP", "GROUP", group, consumer, "COUNT", count]
    if block_ms is not None:
        args += ["BLOCK", block_ms]
    return entries(r(*args, "STREAMS", STREAM, ">"))      # ">" = entries never delivered to this group


def ack(r: Redis, group: str, ids: list[str]) -> int:
    return r("XACK", STREAM, group, *ids) if ids else 0


def pending(r: Redis, group: str) -> tuple[int, dict[str, int]]:
    total, _, _, per = r("XPENDING", STREAM, group)
    return total, {c: int(n) for c, n in (per or [])}


def claim_stale(r: Redis, group: str, consumer: str, min_idle_ms: int) -> list[tuple[str, dict]]:
    """Take over entries a dead consumer read but never acknowledged (Redis 6.2+)."""
    _, claimed, *_ = r("XAUTOCLAIM", STREAM, group, consumer, min_idle_ms, "0-0")
    return [(eid, pairs(f)) for eid, f in claimed if f]


def update_best(r: Redis, laps: list[tuple[str, dict]]) -> None:
    for _, f in laps:
        # LT: only lower an existing score (a new best lap); new members are still added.
        r("ZADD", "race:best", "LT", f["lap_ms"], f["car"])


def leaderboard(r: Redis) -> list[tuple[str, int]]:
    flat = r("ZRANGE", "race:best", 0, -1, "WITHSCORES")
    return [(flat[i], int(float(flat[i + 1]))) for i in range(0, len(flat), 2)]


def last_n(r: Redis, n: int) -> list[tuple[str, dict]]:
    """What a dashboard that joins late shows first: the most recent history, newest first."""
    return [(eid, pairs(f)) for eid, f in r("XREVRANGE", STREAM, "+", "-", "COUNT", n)]
