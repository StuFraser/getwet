"""
cache/memory.py — In-memory tile cache backend.

Stores tiles in a plain Python dict. Fast, zero dependencies, zero
configuration. Tiles are lost on restart — acceptable for ephemeral
hosting environments like Render or Azure F1 where the tile_cache
is a performance convenience rather than a critical data store.
"""

import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from cache.base import BaseTileCache, CachedTile

logger = logging.getLogger(__name__)


class MemoryTileCache(BaseTileCache):
    """
    In-memory cache backend.

    Thread safety: all compound access is protected by an RLock. The
    orchestrator handles in-flight fetch coalescing; this lock keeps cache
    reads, writes, iteration, and statistics internally consistent.
    If you move to multi-worker (multiple uvicorn workers), each worker
    will have its own independent cache — that's fine, just means more
    S3 fetches on first hit per worker.
    """

    def __init__(self) -> None:
        self._store: dict[str, CachedTile] = {}
        self._lock = threading.RLock()
        logger.info("Memory tile cache initialised")

    def get(self, h3_cell: str) -> Optional[CachedTile]:
        with self._lock:
            return self._store.get(h3_cell)

    def set(self, tile: CachedTile) -> None:
        with self._lock:
            self._store[tile.h3_cell] = tile
        logger.debug("Cached tile %s (%d features)", tile.h3_cell, tile.feature_count)

    def delete(self, h3_cell: str) -> None:
        with self._lock:
            self._store.pop(h3_cell, None)

    def list_stale(self, older_than: datetime) -> list[str]:
        with self._lock:
            return [
                cell
                for cell, tile in self._store.items()
                if tile.fetched_at < older_than
            ]

    def stats(self) -> dict:
        with self._lock:
            total_tiles = len(self._store)
            total_features = sum(t.feature_count for t in self._store.values())
            oldest = (
                min(t.fetched_at for t in self._store.values()).isoformat()
                if self._store
                else None
            )
        return {
            "backend": "memory",
            "total_tiles": total_tiles,
            "total_features": total_features,
            "oldest_tile": oldest,
            "stale_tiles": 0,  # calculated by caller with list_stale()
        }
