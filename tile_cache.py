"""H3-based caching and direct Overture Water queries."""

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

DATA_REFRESH_DAYS: int = int(os.getenv("DATA_REFRESH_DAYS", "30"))

# Pin a release for repeatable results. Update deliberately after validation.
OVERTURE_RELEASE: str = os.getenv("OVERTURE_RELEASE", "2026-10-21.0")
OVERTURE_S3_TEMPLATE = (
    "s3://overturemaps-us-west-2/release/{release}/theme=base/type=water/*"
)

# H3 is a request-cache key, not a data-partition scheme.
H3_RESOLUTION: int = 5
BBOX_BUFFER_DEG: float = 0.05


def _init_connection(conn: duckdb.DuckDBPyConnection) -> None:
    """Load extensions and configure unsigned reads from Overture's public S3."""
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET s3_access_key_id='';")
    conn.execute("SET s3_secret_access_key='';")
    conn.execute("SET s3_session_token='';")
    conn.execute("SET s3_region='us-west-2';")

    # These caches are in-process only, but help reused warm/batch connections.
    try:
        conn.execute("SET parquet_metadata_cache=true;")
        conn.execute("SET enable_http_metadata_cache=true;")
    except duckdb.Error:
        logger.debug("This DuckDB version does not support metadata-cache settings")


@contextmanager
def duck_connection():
    """Yield one initialized connection for a bounded sequence of tile fetches."""
    conn = duckdb.connect()
    try:
        _init_connection(conn)
        yield conn
    finally:
        conn.close()


class TileOrchestrator:
    """Fetch and cache direct Overture Water results by H3 cell."""

    def __init__(self, cache: Optional[BaseTileCache] = None) -> None:
        self._cache = cache or get_cache()

    def get_features_for_point(
        self,
        lat: float,
        lng: float,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> list[dict]:
        cell = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        cached = self._cache.get(cell)
        if cached is None:
            logger.info("Cache miss for H3 cell %s — fetching from Overture", cell)
            return self._fetch_and_cache(cell, conn=conn)
        if self._is_stale(cached):
            logger.info("Stale H3 cell %s — refreshing from Overture", cell)
            return self._fetch_and_cache(cell, conn=conn)
        logger.debug("Cache hit for H3 cell %s (%d features)", cell, cached.feature_count)
        return cached.features

    def warm(self, lat: float, lng: float) -> dict:
        """Warm the centre H3 cell and its ring-1 neighbours using one connection."""
        centre = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        cells = h3.grid_disk(centre, 1)
        warmed: list[str] = []
        already_warm: list[str] = []

        with duck_connection() as conn:
            for cell in cells:
                cached = self._cache.get(cell)
                if cached is None or self._is_stale(cached):
                    self._fetch_and_cache(cell, conn=conn)
                    warmed.append(cell)
                else:
                    already_warm.append(cell)
        return {"status": "ok", "warmed": len(warmed), "already_warm": len(already_warm)}

    def evict_stale(self) -> int:
        stale_cells = self._cache.list_stale(older_than=self._stale_threshold())
        for cell in stale_cells:
            self._cache.delete(cell)
        return len(stale_cells)

    def stats(self) -> dict:
        base = self._cache.stats()
        return {
            **base,
            "stale_tiles": len(self._cache.list_stale(older_than=self._stale_threshold())),
            "refresh_days": DATA_REFRESH_DAYS,
            "overture_release": OVERTURE_RELEASE,
        }

    def _is_stale(self, tile: CachedTile) -> bool:
        return tile.fetched_at < self._stale_threshold()

    def _stale_threshold(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=DATA_REFRESH_DAYS)

    def _fetch_and_cache(
        self,
        cell: str,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> list[dict]:
        features = _query_overture(_cell_bbox(cell), conn=conn)
        self._cache.set(CachedTile(
            h3_cell=cell,
            fetched_at=datetime.now(timezone.utc),
            features=features,
        ))
        return features


def _query_overture(
    bbox: dict,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> list[dict]:
    """Read just bbox-relevant Overture Water features as WKB."""
    source = OVERTURE_S3_TEMPLATE.format(release=OVERTURE_RELEASE)
    sql = f"""
        SELECT
            names.primary AS name,
            subtype,
            class,
            CAST(is_salt AS BOOLEAN) AS is_salt,
            CAST(is_intermittent AS BOOLEAN) AS is_intermittent,
            ST_AsWKB(geometry) AS geom_wkb
        FROM read_parquet('{source}', hive_partitioning = 1)
        WHERE bbox.xmin <= {bbox['max_lng']}
          AND bbox.xmax >= {bbox['min_lng']}
          AND bbox.ymin <= {bbox['max_lat']}
          AND bbox.ymax >= {bbox['min_lat']}
    """

    own_connection = conn is None
    duck = conn or duckdb.connect()
    try:
        if own_connection:
            _init_connection(duck)
        rows = duck.execute(sql).fetchall()
    except Exception as exc:
        logger.error("Overture fetch failed for bbox %s: %s", bbox, exc)
        return []
    finally:
        if own_connection:
            duck.close()

    features = []
    for name, subtype, water_class, is_salt, is_intermittent, geometry in rows:
        if not geometry:
            continue
        features.append({
            "geometry": bytes(geometry) if not isinstance(geometry, bytes) else geometry,
            "name": name,
            "subtype": subtype,
            "class": water_class,
            "is_salt": is_salt,
            "is_intermittent": is_intermittent,
        })
    logger.info("Fetched %d Overture features", len(features))
    return features


def _cell_bbox(cell: str) -> dict:
    """Return the cell's axis-aligned bbox plus a small geographic buffer."""
    boundary = h3.cell_to_boundary(cell)
    lats = [point[0] for point in boundary]
    lngs = [point[1] for point in boundary]
    return {
        "min_lat": min(lats) - BBOX_BUFFER_DEG,
        "max_lat": max(lats) + BBOX_BUFFER_DEG,
        "min_lng": min(lngs) - BBOX_BUFFER_DEG,
        "max_lng": max(lngs) + BBOX_BUFFER_DEG,
    }
