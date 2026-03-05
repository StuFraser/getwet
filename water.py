"""
water.py — Point-in-water detection and water body classification.

Receives a list of features from TileOrchestrator, builds a Shapely
STRtree spatial index, and performs point-in-polygon queries. Also
handles water class normalisation, confidence scoring, and nearest
water body detection when a point falls outside any water feature.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import duckdb
from shapely import wkb
from shapely.geometry import Point
from shapely.strtree import STRtree

from tile_cache import TileOrchestrator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Water class normalisation
# ---------------------------------------------------------------------------

CLASS_NORMALISATION: dict[str, str] = {
    # Oceans / seas
    "ocean": "ocean",
    "sea": "ocean",
    # Lakes / ponds
    "lake": "lake",
    "pond": "lake",
    "oxbow": "lake",
    "lagoon": "lake",
    # Rivers / streams
    "river": "river",
    "stream": "river",
    "drain": "river",
    "ditch": "river",
    # Reservoirs
    "reservoir": "reservoir",
    "basin": "reservoir",
    # Harbours / bays
    "harbour": "harbour",
    "harbor": "harbour",
    "bay": "harbour",
    "cove": "harbour",
    "inlet": "harbour",
    # Estuaries
    "estuary": "estuary",
    # Wetlands
    "wetland": "wetland",
    "marsh": "wetland",
    "swamp": "wetland",
    "bog": "wetland",
    "fen": "wetland",
    # Canals
    "canal": "canal",
    "waterway": "canal",
    # Catchall
    "water": "water",
}

# Salt water inference by category, used when Overture doesn't supply is_salt.
SALT_BY_CATEGORY: dict[str, Optional[bool]] = {
    "ocean": True,
    "harbour": True,
    "estuary": True,
    "lake": False,
    "river": False,
    "reservoir": False,
    "canal": False,
    "wetland": None,
    "water": None,
}

# Boundary distance thresholds for confidence scoring (metres).
CONFIDENCE_LOW_M: float = 50.0
CONFIDENCE_MEDIUM_M: float = 200.0


# ---------------------------------------------------------------------------
# Response dataclass
# ---------------------------------------------------------------------------

@dataclass
class WaterResult:
    is_water: bool
    name: Optional[str]
    subtype: Optional[str]
    water_class: Optional[str]
    category: Optional[str]
    is_salt: Optional[bool]
    is_intermittent: Optional[bool]
    confidence: str                 # "high" | "medium" | "low"
    nearest_water: Optional[dict]   # only populated when is_water=False

    def to_dict(self) -> dict:
        return {
            "is_water": self.is_water,
            "name": self.name,
            "subtype": self.subtype,
            "class": self.water_class,
            "category": self.category,
            "is_salt": self.is_salt,
            "is_intermittent": self.is_intermittent,
            "confidence": self.confidence,
            "nearest_water": self.nearest_water,
        }


# ---------------------------------------------------------------------------
# WaterService
# ---------------------------------------------------------------------------

class WaterService:
    """
    Performs point-in-water queries against tile-cached Overture features.

    The STRtree index is built per-call from the features returned by
    TileOrchestrator. This is fast in practice because:
      - Feature lists per H3 cell are small (typically 10–50 items)
      - The tile itself is served from the in-memory cache on warm paths
      - STRtree construction on a small list is microseconds
    """

    def __init__(self, orchestrator: Optional[TileOrchestrator] = None) -> None:
        self._orchestrator = orchestrator or TileOrchestrator()

    def check(
        self,
        lat: float,
        lng: float,
        margin_m: float = 10.0,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> "WaterResult":
        """
        Check whether (lat, lng) falls within a water body.

        margin_m: buffer in metres applied to the point before the polygon
        test, to account for geometry imprecision in source data. Defaults
        to 10m. Set to 0 for strict point-in-polygon behaviour.

        conn: optional existing DuckDB connection to reuse on cache miss.
        Passed through to TileOrchestrator unchanged — if the tile is
        already cached, no connection is used at all.
        """
        features = self._orchestrator.get_features_for_point(lat, lng, conn=conn)

        if not features:
            return WaterResult(
                is_water=False,
                name=None,
                subtype=None,
                water_class=None,
                category=None,
                is_salt=None,
                is_intermittent=None,
                confidence="low",
                nearest_water=None,
            )

        # Shapely uses (x=lng, y=lat) coordinate order
        point = Point(lng, lat)
        point_buffered = point.buffer(margin_m / 111_000) if margin_m > 0 else point

        geometries = [wkb.loads(f["geometry"]) for f in features]
        tree = STRtree(geometries)

        # --- Point-in-polygon ------------------------------------------------
        hits = tree.query(point_buffered, predicate="intersects")

        if len(hits) > 0:
            # Multiple hits — pick smallest (most specific) polygon.
            best_idx = min(hits, key=lambda i: geometries[i].area)
            feat = features[best_idx]
            raw_class = (feat.get("class") or "").lower()
            category = CLASS_NORMALISATION.get(raw_class, "water")
            is_salt = SALT_BY_CATEGORY.get(category)
            confidence = _boundary_confidence(point, geometries[best_idx])
            return WaterResult(
                is_water=True,
                name=feat.get("name"),
                subtype=feat.get("subtype"),
                water_class=feat.get("class"),
                category=category,
                is_salt=is_salt,
                is_intermittent=feat.get("is_intermittent"),
                confidence=confidence,
                nearest_water=None,
            )

        # --- Not in water — find nearest -------------------------------------
        return WaterResult(
            is_water=False,
            name=None,
            subtype=None,
            water_class=None,
            category=None,
            is_salt=None,
            is_intermittent=None,
            confidence="high",
            nearest_water=_nearest_water(point, geometries, features),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _boundary_confidence(point: Point, polygon) -> str:
    """
    Derive confidence from how close the point is to the polygon boundary.
    Points very close to the edge are downgraded because Overture geometry
    isn't always perfectly aligned with real-world shorelines.

    Conversion: 1 degree ≈ 111,000 metres at mid-latitudes.
    """
    dist_deg = point.distance(polygon.boundary)
    dist_m = dist_deg * 111_000

    if dist_m < CONFIDENCE_LOW_M:
        return "low"
    if dist_m < CONFIDENCE_MEDIUM_M:
        return "medium"
    return "high"


def _nearest_water(
    point: Point,
    geometries: list,
    features: list[dict],
) -> Optional[dict]:
    """
    Return metadata about the closest water body to the point,
    including its approximate distance in metres.
    """
    if not geometries:
        return None

    distances = [point.distance(geom) for geom in geometries]
    idx = min(range(len(distances)), key=lambda i: distances[i])
    dist_m = distances[idx] * 111_000

    feat = features[idx]
    raw_class = (feat.get("class") or "").lower()
    category = CLASS_NORMALISATION.get(raw_class, "water")

    return {
        "name": feat.get("name"),
        "category": category,
        "class": feat.get("class"),
        "distance_m": round(dist_m),
    }