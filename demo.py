"""Ten laps of timing data through a stream, two consumer groups, a crash and a late joiner."""
import time

from racestream import timing as t
from racestream.resp import Redis
from racestream.server import redis_url

with redis_url() as url:
    r = Redis(url)
    r("DEL", t.STREAM, "race:best")
    print(f"Redis {r('INFO', 'server').split('redis_version:')[1].split()[0]} at {url}")

    t.ensure_group(r, "strategy")
    t.ensure_group(r, "leaderboard")
    print(f"\n1. Timing loop publishes {t.produce(r)} laps with XADD (stream length {r('XLEN', t.STREAM)})")

    a = t.read(r, "strategy", "worker-a")
    t.ack(r, "strategy", [eid for eid, _ in a])
    b = t.read(r, "strategy", "worker-b")                     # worker-b reads 10 ... and dies before XACK
    print(f"\n2. Group 'strategy' splits the work: worker-a took {len(a)} (acked), worker-b took {len(b)} then crashed")
    total, per = t.pending(r, "strategy")
    print(f"   XPENDING: {total} delivered but unacknowledged -> {per}")

    rest = t.read(r, "strategy", "worker-a", count=100)
    t.ack(r, "strategy", [eid for eid, _ in rest])
    time.sleep(0.2)
    claimed = t.claim_stale(r, "strategy", "worker-a", min_idle_ms=150)
    t.ack(r, "strategy", [eid for eid, _ in claimed])
    print(f"\n3. worker-a finishes the {len(rest)} remaining laps, then XAUTOCLAIMs worker-b's {len(claimed)} stale ones")
    print(f"   XPENDING now: {t.pending(r, 'strategy')[0]}  -> every lap handled once the group settles (at-least-once)")

    laps = t.read(r, "leaderboard", "board-1", count=100)
    t.update_best(r, laps)
    t.ack(r, "leaderboard", [eid for eid, _ in laps])
    print(f"\n4. Group 'leaderboard' independently reads all {len(laps)} laps from the same stream")
    for pos, (car, ms) in enumerate(t.leaderboard(r), 1):
        print(f"   P{pos}  car {car:>2}  best {ms / 1000:.3f} s")

    print("\n5. A dashboard joins late: XREVRANGE for the last 4 laps, then it would XREAD BLOCK from '$'")
    for eid, f in t.last_n(r, 4):
        print(f"   {eid}  car {f['car']:>2}  lap {f['lap']:>2}  {int(f['lap_ms']) / 1000:.3f} s")

    print("\n6. XINFO GROUPS")
    for g in r("XINFO", "GROUPS", t.STREAM):
        info = t.pairs(g)
        print(f"   {info['name']:<12} consumers {info['consumers']}  pending {info['pending']}  "
              f"last-delivered {info['last-delivered-id']}  lag {info.get('lag', 'n/a')}")
    r.close()
