"""
cache/base.py — Abstract base class for tile cache backends.

All cache backends must implement this interface. The rest of the
codebase only ever talks to this contract — it has no knowledge of
whether tiles are stored in memory, SQLite, or anything else.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class CachedTile:
    """Represents a single cached H3 tile and its water features."""
    h3_cell: str
    fetched_at: datetime
    features: list[dict]

    @property
    def feature_count(self) -> int:
        return len(self.features)


class BaseTileCache(ABC):
    """
    Abstract interface for tile cache backends.

    A 'tile' is identified by its H3 cell string (e.g. '8500000ffffffff')
    and contains a list of water feature dicts fetched from Overture S3.
    """

    @abstractmethod
    def get(self, h3_cell: str) -> Optional[CachedTile]:
        """
        Retrieve a cached tile by H3 cell.
        Returns None if the tile is not in the cache.
        """
        ...

    @abstractmethod
    def set(self, tile: CachedTile) -> None:
        """
        Store a tile in the cache.
        Overwrites any existing entry for the same H3 cell.
        """
        ...

    @abstractmethod
    def delete(self, h3_cell: str) -> None:
        """Remove a tile from the cache."""
        ...

    @abstractmethod
    def list_stale(self, older_than: datetime) -> list[str]:
        """
        Return H3 cell identifiers for all tiles fetched before `older_than`.
        Used by eviction logic.
        """
        ...

    @abstractmethod
    def stats(self) -> dict:
        """
        Return a dict of cache statistics for the /health endpoint.
        Must include at minimum: total_tiles, total_features, stale_tiles.
        """
        ...