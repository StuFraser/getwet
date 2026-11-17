"""H3-based caching and direct Overture Water queries."""

import logging
import os
import threading
from concurrent.futures import Future
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import duckdb
import h3

from cache import get_cache
from cache.base import BaseTileCache, CachedTile

logger = logging.getLogger(__name__)

DATA_REFRESH_DAYS: int = int(os.getenv("DATA_REFRESH_DAYS", "30"))
MAX_CONCURRENT_OVERTURE_FETCHES: int = max(
    1, int(os.getenv("MAX_CONCURRENT_OVERTURE_FETCHES", "2"))
)
_REMOTE_FETCH_SEMAPHORE = threading.BoundedSemaphore(MAX_CONCURRENT_OVERTURE_FETCHES)

# Pin a release for repeatable results. Update deliberately after validation.
OVERTURE_RELEASE: str = os.getenv("OVERTURE_RELEASE", "2026-10-21.0")
OVERTURE_S3_TEMPLATE = (
    "s3://overturemaps-us-west-2/release/{release}/theme=base/type=water/*"
)

# H3 is a request-cache key, not a data-partition scheme.
H3_RESOLUTION: int = 5
BBOX_BUFFER_DEG: float = 0.05


class OvertureQueryError(RuntimeError):
    """A remote Overture query failed, so no cache entry was written."""


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
        self._inflight_lock = threading.Lock()
        self._inflight: dict[str, Future[list[dict]]] = {}

    def get_features_for_point(
        self,
        lat: float,
        lng: float,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> list[dict]:
        cell = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        return self._get_or_fetch(cell, conn=conn)

    def warm(self, lat: float, lng: float) -> dict:
        """Warm the centre H3 cell and its ring-1 neighbours using one connection."""
        centre = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        cells = h3.grid_disk(centre, 1)
        warmed: list[str] = []
        already_warm: list[str] = []

        cells_to_fetch = [
            cell for cell in cells
            if self._needs_fetch(cell)
        ]
        if not cells_to_fetch:
            return {"status": "ok", "warmed": 0, "already_warm": len(cells)}

        with duck_connection() as conn:
            for cell in cells:
                if self._needs_fetch(cell):
                    self._get_or_fetch(cell, conn=conn)
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

    def _needs_fetch(self, cell: str) -> bool:
        cached = self._cache.get(cell)
        return cached is None or self._is_stale(cached)

    def _stale_threshold(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=DATA_REFRESH_DAYS)

    def _get_or_fetch(
        self,
        cell: str,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> list[dict]:
        """
        Return a fresh tile, coalescing simultaneous misses for the same cell.

        The thread elected as leader performs the remote query. Other callers
        wait on its Future rather than opening their own DuckDB connection or
        querying the same Overture bbox. A failed leader publishes an exception
        and never creates an empty cache tile.
        """
        cached = self._cache.get(cell)
        if cached is not None and not self._is_stale(cached):
            logger.debug("Cache hit for H3 cell %s (%d features)", cell, cached.feature_count)
            return cached.features

        with self._inflight_lock:
            # Recheck after acquiring the lock: another request may have
            # completed the fetch between the initial cache read and now.
            cached = self._cache.get(cell)
            if cached is not None and not self._is_stale(cached):
                return cached.features

            future = self._inflight.get(cell)
            is_leader = future is None
            if is_leader:
                future = Future()
                self._inflight[cell] = future

        if not is_leader:
            logger.info("Joining in-flight Overture fetch for H3 cell %s", cell)
            return future.result()

        try:
            logger.info("Fetching H3 cell %s from Overture", cell)
            with _REMOTE_FETCH_SEMAPHORE:
                features = _query_overture(_cell_bbox(cell), conn=conn)
            self._cache.set(CachedTile(
                h3_cell=cell,
                fetched_at=datetime.now(timezone.utc),
                features=features,
            ))
            future.set_result(features)
            return features
        except Exception as exc:
            error = exc if isinstance(exc, OvertureQueryError) else OvertureQueryError(
                f"Overture fetch failed for H3 cell {cell}"
            )
            future.set_exception(error)
            raise error from exc
        finally:
            with self._inflight_lock:
                self._inflight.pop(cell, None)


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
    duck = None
    try:
        duck = conn or duckdb.connect()
        if own_connection:
            _init_connection(duck)
        rows = duck.execute(sql).fetchall()
    except Exception as exc:
        logger.error("Overture fetch failed for bbox %s: %s", bbox, exc)
        raise OvertureQueryError("Could not read Overture Water data") from exc
    finally:
        if own_connection and duck is not None:
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
