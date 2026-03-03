"""
water.py — Point-in-water detection and water body classification.

Receives a list of features from TileOrchestrator, builds a Shapely
STRtree spatial index, and performs point-in-polygon queries. Also
handles water class normalisation, confidence scoring, and nearest
water body detection when a point falls outside any water feature.
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

from shapely.geometry import Point, shape
from shapely.strtree import STRtree

from tile_cache import TileOrchestrator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Water class normalisation
# ---------------------------------------------------------------------------
# Maps raw Overture/OSM class values to clean canonical categories.

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
    "wetland": None,    # could be either
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
    name_en: Optional[str]
    subtype: Optional[str]
    water_class: Optional[str]      # raw Overture class value
    category: Optional[str]         # normalised category
    is_salt: Optional[bool]
    is_intermittent: Optional[bool]
    confidence: str                 # "high" | "medium" | "low"
    nearest_water: Optional[dict]   # only populated when is_water=False

    def to_dict(self) -> dict:
        return {
            "is_water": self.is_water,
            "name": self.name,
            "name_en": self.name_en,
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

    def check(self, lat: float, lng: float, margin_m: float = 10.0) -> WaterResult:
        """
        Check whether (lat, lng) falls within a water body.

        margin_m: buffer in metres applied to the point before the
        polygon test, to account for geometry imprecision in source data.
        Defaults to 10m. Set to 0 for strict point-in-polygon behaviour.
        """
        features = self._orchestrator.get_features_for_point(lat, lng)

        if not features:
            # No features available — either a genuinely dry/remote area
            # or a transient S3 fetch failure. Return low confidence.
            return WaterResult(
                is_water=False,
                name=None,
                name_en=None,
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
        # Buffer the point by margin_m to account for geometry imprecision.
        # Consumer controls this — set to 0 for strict point-in-polygon.
        point_buffered = point.buffer(margin_m / 111_000) if margin_m > 0 else point

        geometries = [
            shape(json.loads(f["geometry"]) if isinstance(f["geometry"], str) else f["geometry"])
            for f in features
        ]
        tree = STRtree(geometries)

        # --- Point-in-polygon ------------------------------------------------
        hits = tree.query(point_buffered, predicate="intersects")

        if len(hits) > 0:
            # If multiple polygons contain the point (e.g. a river within
            # a broader water body), pick the smallest — most specific match.
            best_idx = min(hits, key=lambda i: geometries[i].area)
            feat = features[best_idx]
            raw_class = (feat.get("class") or "").lower()
            category = CLASS_NORMALISATION.get(raw_class, "water")

            # is_salt is no longer in the Overture schema — infer from category
            is_salt = SALT_BY_CATEGORY.get(category)

            confidence = _boundary_confidence(point, geometries[best_idx])
            return WaterResult(
                is_water=True,
                name=feat.get("name"),
                name_en=feat.get("name_en"),
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
            name_en=None,
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