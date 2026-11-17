import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from cache.memory import MemoryTileCache
from tile_cache import OvertureQueryError, TileOrchestrator


class TileConcurrencyTests(unittest.TestCase):
    def test_simultaneous_same_cell_misses_share_one_fetch(self) -> None:
        orchestrator = TileOrchestrator(cache=MemoryTileCache())
        fetch_started = threading.Event()
        release_fetch = threading.Event()
        calls = 0
        calls_lock = threading.Lock()
        expected = [{"geometry": b"geometry"}]

        def fetch(_bbox, conn=None):
            nonlocal calls
            with calls_lock:
                calls += 1
            fetch_started.set()
            self.assertTrue(release_fetch.wait(timeout=2))
            return expected

        with (
            patch("tile_cache._cell_bbox", return_value={}),
            patch("tile_cache._query_overture", side_effect=fetch),
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                leader = executor.submit(orchestrator._get_or_fetch, "cell")
                self.assertTrue(fetch_started.wait(timeout=2))
                follower = executor.submit(orchestrator._get_or_fetch, "cell")
                time.sleep(0.05)
                release_fetch.set()
                self.assertEqual(leader.result(timeout=2), expected)
                self.assertEqual(follower.result(timeout=2), expected)

        self.assertEqual(calls, 1)
        self.assertEqual(orchestrator._cache.get("cell").features, expected)

    def test_failed_fetch_is_not_cached_and_is_retried(self) -> None:
        orchestrator = TileOrchestrator(cache=MemoryTileCache())

        with (
            patch("tile_cache._cell_bbox", return_value={}),
            patch(
                "tile_cache._query_overture",
                side_effect=OvertureQueryError("source unavailable"),
            ),
        ):
            with self.assertRaises(OvertureQueryError):
                orchestrator._get_or_fetch("cell")

        self.assertIsNone(orchestrator._cache.get("cell"))

        expected = [{"geometry": b"geometry"}]
        with (
            patch("tile_cache._cell_bbox", return_value={}),
            patch("tile_cache._query_overture", return_value=expected) as fetch,
        ):
            self.assertEqual(orchestrator._get_or_fetch("cell"), expected)

        fetch.assert_called_once()
        self.assertEqual(orchestrator._cache.get("cell").features, expected)


if __name__ == "__main__":
    unittest.main()
