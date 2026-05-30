"""
tile_cache.py — H3-based tile orchestration.

Sits between the water query logic and the cache backend. Responsible for:
  - Converting a lat/lng to an H3 cell
  - Checking the cache for a fresh tile
  - Fetching from Cloudflare R2 on cache miss or stale tile
  - Writing fetched features back to the cache
  - Exposing cache stats and stale tile eviction
"""

import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import duckdb
import h3

from cache import get_cache
from cache.base import BaseTileCache, CachedTile

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_REFRESH_DAYS: int = int(os.getenv("DATA_REFRESH_DAYS", "30"))

# ---------------------------------------------------------------------------
# R2 configuration
# ---------------------------------------------------------------------------
R2_ENDPOINT      = os.getenv("R2_ENDPOINT")       # https://<account_id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY    = os.getenv("R2_SECRET_KEY")
R2_BUCKET        = os.getenv("R2_BUCKET", "getwet")
R2_PARQUET_KEY   = os.getenv("R2_PARQUET_KEY", "overture_water_global.parquet")

# ---------------------------------------------------------------------------
# Overture S3 configuration (preserved as fallback — commented out)
# ---------------------------------------------------------------------------
# import json
# import urllib.request
#
# _OVERTURE_RELEASE_OVERRIDE: str | None = os.getenv("OVERTURE_RELEASE")
#
# def _resolve_overture_release() -> str:
#     if _OVERTURE_RELEASE_OVERRIDE:
#         logger.info("Using pinned Overture release: %s", _OVERTURE_RELEASE_OVERRIDE)
#         return _OVERTURE_RELEASE_OVERRIDE
#     try:
#         with urllib.request.urlopen("https://stac.overturemaps.org", timeout=5) as resp:
#             data = json.loads(resp.read())
#             latest = data["latest"]
#             logger.info("Resolved latest Overture release from STAC: %s", latest)
#             return latest
#     except Exception as exc:
#         fallback = "2026-01-21.0"
#         logger.warning(
#             "Failed to resolve Overture release from STAC (%s) — using fallback: %s",
#             exc, fallback,
#         )
#         return fallback
#
# OVERTURE_RELEASE: str = _resolve_overture_release()
# OVERTURE_S3_TEMPLATE = (
#     "s3://overturemaps-us-west-2/release/{release}/theme=base/type=water/*"
# )

# H3 resolution 5 — average cell area ~252 km².
# Large enough to capture whole river/lake features,
# small enough to avoid pulling too much data per fetch.
H3_RESOLUTION: int = 5

# Small bbox buffer in degrees (~5km) so features straddling
# a cell boundary are captured. The point-in-polygon check in
# water.py is the authoritative filter.
BBOX_BUFFER_DEG: float = 0.05


# ---------------------------------------------------------------------------
# DuckDB connection helpers
# ---------------------------------------------------------------------------

def _init_connection(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Install and load required DuckDB extensions and configure R2 access
    on a fresh connection. Safe to call once per connection — extensions
    persist for its lifetime.
    """
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{R2_ENDPOINT.replace('https://', '')}';")
    conn.execute(f"SET s3_access_key_id='{R2_ACCESS_KEY_ID}';")
    conn.execute(f"SET s3_secret_access_key='{R2_SECRET_KEY}';")
    conn.execute("SET s3_region='auto';")
    conn.execute("SET s3_url_style='path';")


@contextmanager
def duck_connection():
    """
    Context manager that yields an initialised DuckDB connection
    and ensures it is closed on exit.

    Use this when a single connection should span multiple tile fetches
    where fetches are certain to be needed — e.g. cache warmup, where
    the intent is always to fetch all tiles in the ring.

    For on-demand paths (single check, batch with warm cache), connection
    creation is deferred to _query_r2 and only happens on actual
    cache misses.
    """
    conn = duckdb.connect()
    try:
        _init_connection(conn)
        yield conn
    finally:
        conn.close()


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

    def get_features_for_point(
        self,
        lat: float,
        lng: float,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> list[dict]:
        """
        Return all water features relevant to (lat, lng).
        Fetches from R2 on cache miss or stale tile.

        conn: optional existing DuckDB connection to reuse. Only used if a
        fetch is actually needed — cache hits return immediately without
        touching DuckDB at all. When omitted and a fetch is required, a
        fresh connection is created and closed inside _query_r2.
        """
        cell = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        cached = self._cache.get(cell)

        if cached is None:
            logger.info("Cache miss for H3 cell %s — fetching from R2", cell)
            return self._fetch_and_cache(cell, conn=conn)

        if self._is_stale(cached):
            logger.info("Stale tile for H3 cell %s — refreshing from R2", cell)
            return self._fetch_and_cache(cell, conn=conn)

        logger.debug("Cache hit for H3 cell %s (%d features)", cell, cached.feature_count)
        return cached.features

    def warm(self, lat: float, lng: float) -> dict:
        """
        Pre-warm the cache for the given location and its H3 ring-1 neighbours
        (the centre cell plus the 6 surrounding cells).

        Only fetches tiles that are missing or stale — safe to call repeatedly.
        A single DuckDB connection is created upfront and shared across all
        fetches, since warm() always intends to fetch multiple tiles.
        Returns a summary of what was warmed vs already cached.
        """
        centre = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        cells = h3.grid_disk(centre, 1)  # centre + 6 neighbours = 7 cells

        warmed = []
        already_warm = []

        with duck_connection() as conn:
            for cell in cells:
                cached = self._cache.get(cell)
                if cached is None or self._is_stale(cached):
                    self._fetch_and_cache(cell, conn=conn)
                    warmed.append(cell)
                else:
                    already_warm.append(cell)

        logger.info(
            "Warm request for (%.6f, %.6f) — warmed %d tile(s), %d already cached",
            lat, lng, len(warmed), len(already_warm),
        )
        return {
            "status": "ok",
            "warmed": len(warmed),
            "already_warm": len(already_warm),
        }

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
            "r2_bucket": R2_BUCKET,
            "r2_parquet_key": R2_PARQUET_KEY,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_stale(self, tile: CachedTile) -> bool:
        return tile.fetched_at < self._stale_threshold()

    def _stale_threshold(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=DATA_REFRESH_DAYS)

    def _fetch_and_cache(
        self,
        cell: str,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> list[dict]:
        """
        Query R2 for water features within the H3 cell bbox.

        conn: optional existing DuckDB connection to reuse. When omitted,
        _query_r2 creates and closes its own connection internally.
        """
        bbox = _cell_bbox(cell)
        features = _query_r2(bbox, conn=conn)

        tile = CachedTile(
            h3_cell=cell,
            fetched_at=datetime.now(timezone.utc),
            features=features,
        )
        self._cache.set(tile)
        return features


# ---------------------------------------------------------------------------
# R2 parquet query
# ---------------------------------------------------------------------------

def _query_r2(
    bbox: dict,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> list[dict]:
    """
    Query the consolidated R2 parquet for water features within the given
    bounding box.

    conn: optional existing DuckDB connection to reuse. When supplied,
    extensions are assumed already loaded and the connection is left open
    for the caller to manage. When omitted, a fresh connection is created,
    initialised, used, and closed here.

    Geometry is returned as raw WKB bytes — Shapely consumes this directly
    via wkb.loads() in water.py, no changes needed there.
    """
    parquet_path = f"s3://{R2_BUCKET}/{R2_PARQUET_KEY}"

    logger.info(
        "Fetching bbox: min_lat=%.6f, max_lat=%.6f, min_lng=%.6f, max_lng=%.6f",
        bbox['min_lat'], bbox['max_lat'], bbox['min_lng'], bbox['max_lng'],
    )

    sql = f"""
        SELECT
            name,
            subtype,
            class,
            is_intermittent,
            geom_wkb
        FROM read_parquet('{parquet_path}')
        WHERE min_lon <= {bbox['max_lng']}
          AND max_lon >= {bbox['min_lng']}
          AND min_lat <= {bbox['max_lat']}
          AND max_lat >= {bbox['min_lat']}
    """

    _own_conn = conn is None
    duck = None
    try:
        duck = duckdb.connect() if _own_conn else conn
        if _own_conn:
            _init_connection(duck)
        rows = duck.execute(sql).fetchall()
    except Exception as exc:
        logger.error("R2 fetch failed for bbox %s: %s", bbox, exc)
        return []
    finally:
        if _own_conn and duck is not None:
            duck.close()

    features = []
    for row in rows:
        if not row[4]:  # skip null geometries
            continue
        features.append({
            "geometry": bytes(row[4]) if not isinstance(row[4], bytes) else row[4],
            "name": row[0],
            "subtype": row[1],
            "class": row[2],
            "is_salt": None,      # not in current schema, inferred by water.py
            "is_intermittent": row[3],
        })

    logger.info("Fetched %d features from R2", len(features))
    return features


# ---------------------------------------------------------------------------
# Overture S3 query (preserved as fallback — commented out)
# ---------------------------------------------------------------------------
# def _query_overture(
#     bbox: dict,
#     conn: Optional[duckdb.DuckDBPyConnection] = None,
# ) -> list[dict]:
#     s3_path = OVERTURE_S3_TEMPLATE.format(release=OVERTURE_RELEASE)
#     sql = f"""
#         SELECT
#             names.primary                               AS name,
#             subtype,
#             class,
#             CAST(is_intermittent AS BOOLEAN)            AS is_intermittent,
#             ST_AsWKB(geometry)                          AS geom_wkb
#         FROM read_parquet('{s3_path}', hive_partitioning=1)
#         WHERE bbox.xmin <= {bbox['max_lng']}
#           AND bbox.xmax >= {bbox['min_lng']}
#           AND bbox.ymin <= {bbox['max_lat']}
#           AND bbox.ymax >= {bbox['min_lat']}
#     """
#     _own_conn = conn is None
#     duck = None
#     try:
#         duck = duckdb.connect() if _own_conn else conn
#         if _own_conn:
#             _init_connection(duck)
#         rows = duck.execute(sql).fetchall()
#     except Exception as exc:
#         logger.error("Overture S3 fetch failed for bbox %s: %s", bbox, exc)
#         return []
#     finally:
#         if _own_conn and duck is not None:
#             duck.close()
#     features = []
#     for row in rows:
#         if not row[4]:
#             continue
#         features.append({
#             "geometry": row[4],
#             "name": row[0],
#             "subtype": row[1],
#             "class": row[2],
#             "is_salt": None,
#             "is_intermittent": row[3],
#         })
#     return features


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