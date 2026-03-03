"""
tile_cache.py — H3-based tile orchestration.

Sits between the water query logic and the cache backend. Responsible for:
  - Converting a lat/lng to an H3 cell
  - Checking the cache for a fresh tile
  - Fetching from Overture S3 on cache miss or stale tile
  - Writing fetched features back to the cache
  - Exposing cache stats and stale tile eviction
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import json
import urllib.request

import duckdb
import h3

from cache import get_cache
from cache.base import BaseTileCache, CachedTile

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_REFRESH_DAYS: int = int(os.getenv("DATA_REFRESH_DAYS", "30"))

# If OVERTURE_RELEASE is set in .env, use it. Otherwise resolve from STAC.
_OVERTURE_RELEASE_OVERRIDE: str | None = os.getenv("OVERTURE_RELEASE")


def _resolve_overture_release() -> str:
    """
    Resolve the Overture release to query.
    - If OVERTURE_RELEASE is set in .env, pin to that version.
    - Otherwise fetch the latest from the Overture STAC catalog,
      which rebuilds daily and always points to the current release.
    - Falls back to a known-good release if the STAC request fails.
    """
    if _OVERTURE_RELEASE_OVERRIDE:
        logger.info("Using pinned Overture release: %s", _OVERTURE_RELEASE_OVERRIDE)
        return _OVERTURE_RELEASE_OVERRIDE

    try:
        with urllib.request.urlopen("https://stac.overturemaps.org", timeout=5) as resp:
            data = json.loads(resp.read())
            latest = data["latest"]
            logger.info("Resolved latest Overture release from STAC: %s", latest)
            return latest
    except Exception as exc:
        fallback = "2026-02-18.0"
        logger.warning(
            "Failed to resolve Overture release from STAC (%s) — using fallback: %s",
            exc, fallback,
        )
        return fallback


OVERTURE_RELEASE: str = _resolve_overture_release()

# H3 resolution 5 — average cell area ~252 km².
# Large enough to capture whole river/lake features,
# small enough to avoid pulling too much data per fetch.
H3_RESOLUTION: int = 5

# Overture Maps public S3 path — no auth required.
OVERTURE_S3_TEMPLATE = (
    "s3://overturemaps-us-west-2/release/{release}/theme=base/type=water/*"
)

# Small bbox buffer in degrees (~5km) so features straddling
# a cell boundary are captured. The point-in-polygon check in
# water.py is the authoritative filter.
BBOX_BUFFER_DEG: float = 0.05


# ---------------------------------------------------------------------------
# TileOrchestrator
# ---------------------------------------------------------------------------
class TileOrchestrator:
    """
    Manages on-demand fetching and caching of Overture water features
    organised by H3 hex cell.
    """

    def __init__(self, cache: Optional[BaseTileCache] = None) -> None:
        self._cache = cache or get_cache()

    def get_features_for_point(self, lat: float, lng: float) -> list[dict]:
        """
        Return all water features relevant to (lat, lng).
        Fetches from Overture S3 on cache miss or stale tile.
        """
        cell = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        cached = self._cache.get(cell)

        if cached is None:
            logger.info("Cache miss for H3 cell %s — fetching from Overture S3", cell)
            return self._fetch_and_cache(cell)

        if self._is_stale(cached):
            logger.info("Stale tile for H3 cell %s — refreshing from Overture S3", cell)
            return self._fetch_and_cache(cell)

        logger.debug("Cache hit for H3 cell %s (%d features)", cell, cached.feature_count)
        return cached.features

    def evict_stale(self) -> int:
        """Remove all stale tiles from the cache. Returns count evicted."""
        threshold = self._stale_threshold()
        stale_cells = self._cache.list_stale(older_than=threshold)
        for cell in stale_cells:
            self._cache.delete(cell)
        if stale_cells:
            logger.info("Evicted %d stale tile(s)", len(stale_cells))
        return len(stale_cells)

    def stats(self) -> dict:
        """Return cache stats enriched with refresh config."""
        base = self._cache.stats()
        threshold = self._stale_threshold()
        stale = self._cache.list_stale(older_than=threshold)
        return {
            **base,
            "stale_tiles": len(stale),
            "refresh_days": DATA_REFRESH_DAYS,
            "overture_release": OVERTURE_RELEASE,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_stale(self, tile: CachedTile) -> bool:
        return tile.fetched_at < self._stale_threshold()

    def _stale_threshold(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=DATA_REFRESH_DAYS)

    def _fetch_and_cache(self, cell: str) -> list[dict]:
        """Query Overture S3 for water features within the H3 cell bbox."""
        bbox = _cell_bbox(cell)
        features = _query_overture(bbox)

        tile = CachedTile(
            h3_cell=cell,
            fetched_at=datetime.now(timezone.utc),
            features=features,
        )
        self._cache.set(tile)
        return features


# ---------------------------------------------------------------------------
# Overture S3 query
# ---------------------------------------------------------------------------

def _query_overture(bbox: dict) -> list[dict]:
    """
    Query the Overture Maps S3 Parquet files for water features
    within the given bounding box.
    """
    s3_path = OVERTURE_S3_TEMPLATE.format(release=OVERTURE_RELEASE)

    sql = f"""
        INSTALL spatial; LOAD spatial;
        INSTALL httpfs;  LOAD httpfs;
        SET s3_region = 'us-west-2';

        SELECT
            names.primary                               AS name,
            CAST(names AS JSON)                         AS names_json,
            subtype,
            class,
            CAST(is_intermittent AS BOOLEAN)            AS is_intermittent,
            ST_AsGeoJSON(geometry)                      AS geom_json
        FROM read_parquet('{s3_path}', hive_partitioning=1)
        WHERE bbox.xmin <= {bbox['max_lng']}
          AND bbox.xmax >= {bbox['min_lng']}
          AND bbox.ymin <= {bbox['max_lat']}
          AND bbox.ymax >= {bbox['min_lat']}
    """

    try:
        duck = duckdb.connect()
        rows = duck.execute(sql).fetchall()
        duck.close()
    except Exception as exc:
        logger.error("Overture S3 fetch failed for bbox %s: %s", bbox, exc)
        # Return empty list — TileOrchestrator will cache the empty result
        # so we don't hammer S3 on every request after a transient failure.
        # The tile will be considered stale after DATA_REFRESH_DAYS and retried.
        return []

    features = []
    for row in rows:
        if not row[5]:  # skip null geometries
            continue

        # Extract English name from names JSON if available.
        # Overture names.common is a map of language -> list of name entries.
        name_en = None
        try:
            names_data = json.loads(row[1]) if row[1] else {}
            common = names_data.get("common", [])
            # common is a list of {language, value} objects
            for entry in common:
                if entry.get("language") == "en":
                    name_en = entry.get("value")
                    break
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

        features.append({
            "geometry": row[5],
            "name": row[0],
            "name_en": name_en,
            "subtype": row[2],
            "class": row[3],
            "is_salt": None,        # not in current Overture schema, inferred by water.py
            "is_intermittent": row[4],
        })

    logger.info("Fetched %d features from Overture S3", len(features))
    return features


# ---------------------------------------------------------------------------
# H3 helpers
# ---------------------------------------------------------------------------

def _cell_bbox(cell: str) -> dict:
    """
    Return the axis-aligned bounding box of an H3 cell with a small buffer,
    as {min_lat, max_lat, min_lng, max_lng}.
    """
    boundary = h3.cell_to_boundary(cell)  # list of (lat, lng) tuples
    lats = [p[0] for p in boundary]
    lngs = [p[1] for p in boundary]
    return {
        "min_lat": min(lats) - BBOX_BUFFER_DEG,
        "max_lat": max(lats) + BBOX_BUFFER_DEG,
        "min_lng": min(lngs) - BBOX_BUFFER_DEG,
        "max_lng": max(lngs) + BBOX_BUFFER_DEG,
    }