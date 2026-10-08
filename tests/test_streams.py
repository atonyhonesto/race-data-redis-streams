import threading
import time
import unittest

from racestream import timing as t
from racestream.resp import Redis, RedisError
from racestream.server import redis_url


class StreamTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ctx = redis_url()
        cls.url = cls.ctx.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.ctx.__exit__(None, None, None)

    def setUp(self):
        self.r = Redis(self.url)
        self.r("FLUSHDB")

    def tearDown(self):
        self.r.close()

    def test_resp_types(self):
        self.assertEqual(self.r("PING"), "PONG")
        self.assertEqual(self.r("INCR", "n"), 1)
        self.assertIsNone(self.r("GET", "missing"))
        with self.assertRaises(RedisError):
            self.r("NOSUCHCOMMAND")

    def test_each_lap_goes_to_one_consumer_per_group(self):
        t.ensure_group(self.r, "g")
        t.ensure_group(self.r, "g")                            # idempotent
        t.produce(self.r, laps=5)
        a = t.read(self.r, "g", "a", count=7)
        b = t.read(self.r, "g", "b", count=100)
        self.assertEqual(len(a) + len(b), 20)
        self.assertFalse({e for e, _ in a} & {e for e, _ in b})

    def test_groups_are_independent(self):
        t.ensure_group(self.r, "g1")
        t.ensure_group(self.r, "g2")
        t.produce(self.r, laps=3)
        self.assertEqual(len(t.read(self.r, "g1", "x", count=100)), 12)
        self.assertEqual(len(t.read(self.r, "g2", "y", count=100)), 12)

    def test_unacked_entries_can_be_claimed(self):
        t.ensure_group(self.r, "g")
        t.produce(self.r, laps=2)
        got = t.read(self.r, "g", "dead", count=5)
        self.assertEqual(t.pending(self.r, "g"), (5, {"dead": 5}))
        time.sleep(0.05)
        claimed = t.claim_stale(self.r, "g", "alive", min_idle_ms=20)
        self.assertEqual([e for e, _ in claimed], [e for e, _ in got])
        t.ack(self.r, "g", [e for e, _ in claimed])
        self.assertEqual(t.pending(self.r, "g")[0], 0)

    def test_best_lap_only_goes_down(self):
        t.update_best(self.r, [("1", {"car": "24", "lap_ms": "41000"}), ("2", {"car": "24", "lap_ms": "41500"}),
                               ("3", {"car": "24", "lap_ms": "40800"}), ("4", {"car": "5", "lap_ms": "40900"})])
        self.assertEqual(t.leaderboard(self.r), [("24", 40800), ("5", 40900)])

    def test_blocking_read_wakes_on_new_lap(self):
        t.ensure_group(self.r, "live", start="$")
        result = {}

        def wait():
            r2 = Redis(self.url)
            start = time.perf_counter()
            result["laps"] = t.read(r2, "live", "dash", block_ms=2000)
            result["waited"] = time.perf_counter() - start
            r2.close()

        th = threading.Thread(target=wait)
        th.start()
        time.sleep(0.2)
        self.r("XADD", t.STREAM, "*", "car", "24", "lap", 1, "lap_ms", 41000)
        th.join(3)
        self.assertEqual(result["laps"][0][1]["car"], "24")
        self.assertLess(result["waited"], 1.5)                 # woke on the write, not the timeout

    def test_maxlen_trims(self):
        t.produce(self.r, laps=250, maxlen=100)                  # 1000 entries
        self.assertLess(self.r("XLEN", t.STREAM), 1000)
        self.assertGreaterEqual(self.r("XLEN", t.STREAM), 100)  # "~" trims approximately, never below the cap


if __name__ == "__main__":
    unittest.main()
