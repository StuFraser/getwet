"""
main.py — FastAPI application for the Overture Water API.

Routes:
  GET  /water/check       — Single coordinate water check
  POST /water/batch       — Bulk coordinate water check (up to 100 pairs)
  GET  /health            — Service health and cache stats
  POST /cache/evict       — Manually evict stale tiles
"""

import logging
import os
from typing import Annotated

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

load_dotenv()

from tile_cache import TileOrchestrator
from water import WaterService

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Overture Water API",
    description=(
        "On-demand global water body detection powered by Overture Maps. "
        "Drop a coordinate anywhere in the world and instantly know whether "
        "it falls within a water body, what type it is, and how close the "
        "nearest water is if not."
    ),
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Shared singletons
# Instantiated once at startup — TileOrchestrator creates the cache backend
# based on the CACHE_BACKEND env var (defaults to "memory").
# ---------------------------------------------------------------------------
_orchestrator = TileOrchestrator()
_service = WaterService(orchestrator=_orchestrator)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
_API_KEY = os.environ["API_KEY"]


def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    if x_api_key != _API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
            headers={"WWW-Authenticate": "ApiKey"},
        )


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class CoordinatePair(BaseModel):
    lat: float = Field(..., ge=-90, le=90, description="Latitude")
    lng: float = Field(..., ge=-180, le=180, description="Longitude")


class BatchRequest(BaseModel):
    coordinates: list[CoordinatePair] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Between 1 and 100 coordinate pairs.",
    )
    margin_m: float = Field(
        10.0,
        ge=0,
        le=500,
        description="Buffer in metres applied to each point before polygon test. Default 10m. Set to 0 for strict point-in-polygon.",
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get(
    "/water/check",
    summary="Check a single coordinate",
    dependencies=[Depends(require_api_key)],
)
def check_water(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lng: float = Query(..., ge=-180, le=180, description="Longitude"),
    margin_m: float = Query(10.0, ge=0, le=500, description="Buffer in metres applied to the point before polygon test. Default 10m. Set to 0 for strict point-in-polygon."),
):
    """
    Returns whether the coordinate falls within a water body.

    Response includes:
    - `is_water` — bool
    - `name` — water body name if available
    - `class` — raw Overture/OSM class (e.g. "harbour", "river")
    - `category` — normalised category (e.g. "harbour", "river", "ocean")
    - `is_salt` — true/false/null
    - `is_intermittent` — true/false/null
    - `confidence` — "high" | "medium" | "low"
    - `nearest_water` — name, category, class, distance_m (when is_water=false)
    """
    result = _service.check(lat, lng, margin_m=margin_m)
    return result.to_dict()


@app.post(
    "/water/batch",
    summary="Check multiple coordinates in one request",
    dependencies=[Depends(require_api_key)],
)
def check_water_batch(body: BatchRequest):
    """
    Accepts up to 100 coordinate pairs and returns a result for each.

    Coordinates that fall within the same H3 tile (~250km²) share a single
    cache lookup, making batch requests within a region very efficient.
    """
    results = []
    for coord in body.coordinates:
        result = _service.check(coord.lat, coord.lng, margin_m=body.margin_m)
        results.append({
            "lat": coord.lat,
            "lng": coord.lng,
            **result.to_dict(),
        })
    return {"results": results, "count": len(results)}


@app.get("/health", summary="Service health and cache statistics")
def health():
    """
    Returns service status and tile cache statistics.
    Does not require an API key — safe to use as an uptime monitor endpoint.
    """
    return {
        "status": "ok",
        "cache": _orchestrator.stats(),
    }


@app.post(
    "/cache/evict",
    summary="Evict stale tiles from the cache",
    dependencies=[Depends(require_api_key)],
)
def evict_cache():
    """
    Manually trigger eviction of tiles older than DATA_REFRESH_DAYS.
    Tiles are also refreshed lazily on next query, so this is optional —
    useful if you want to force a clean slate after an Overture release update.
    """
    evicted = _orchestrator.evict_stale()
    return {"evicted_tiles": evicted}