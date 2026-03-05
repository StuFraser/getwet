# 💧 GetWet

> A lightweight, self-contained REST API for water body detection and classification — powered by Overture Maps data.

Drop a coordinate anywhere in the world and instantly know whether it falls within a natural water body, what type it is, whether it's fresh or salt water, what it's called — and if you're not in water, how far you are from the nearest one.

No regional data files. No upfront downloads. No billing surprises.

---

## ✨ Features

- **Global coverage** — works for any coordinate on Earth, not just preconfigured regions
- **On-demand tile fetching** — only fetches the ~250km² H3 tile covering your coordinate, straight from Overture's public S3
- **In-memory tile cache** — subsequent queries in the same area are instant
- **Point-in-water detection** — is a given lat/lng within a water body?
- **Water type classification** — ocean, lake, river, reservoir, estuary, harbour, wetland, canal, and more
- **Normalised categories** — clean canonical category alongside the raw Overture class
- **Fresh vs. salt water** — inferred from water type where not explicitly available
- **Multilingual names** — primary local name plus English translation where available
- **Nearest water** — when not in water, returns the closest water body and distance in metres
- **Adjustable margin** — consumer-controlled buffer to account for GPS or geometry imprecision
- **Batch endpoint** — check up to 100 coordinates in a single request
- **Auto-resolving Overture release** — always queries the latest Overture data via STAC catalog
- **Simple API key auth** — lightweight static key protection, no user management needed

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- pip

### Installation

```bash
git clone https://github.com/StuFraser/getwet.git
cd overture-water-api
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Configuration

```bash
cp .env.example .env
```

Edit `.env`:

```env
API_KEY=your-random-secret-key-here
CACHE_BACKEND=memory
DATA_REFRESH_DAYS=30
```

`OVERTURE_RELEASE` is optional — if not set, the latest release is resolved automatically from the Overture STAC catalog on startup.

### Run

```bash
uvicorn main:app --reload
```

The API is available at `http://localhost:8000`. On first query for any area, the service fetches just the relevant tile from Overture S3 — no upfront download required.

---

## 📡 API Reference

### `GET /water/check`

Check whether a coordinate falls within a water body.

**Headers**

| Header | Required | Description |
|---|---|---|
| `X-API-Key` | ✅ | Your configured API key |

**Query Parameters**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `lat` | float | ✅ | — | Latitude |
| `lng` | float | ✅ | — | Longitude |
| `margin_m` | float | ❌ | `10.0` | Buffer in metres applied to the point before polygon test. Accounts for GPS or geometry imprecision. Set to `0` for strict point-in-polygon. Max `500`. |

**Example Request**

```bash
curl -X GET "http://localhost:8000/water/check?lat=-36.8485&lng=174.7633&margin_m=10" \
  -H "X-API-Key: your-secret-key"
```

**Example Response — Water detected**

```json
{
  "is_water": true,
  "name": "Waitemata Harbour",
  "name_en": "Waitemata Harbour",
  "subtype": "water",
  "class": "harbour",
  "category": "harbour",
  "is_salt": true,
  "is_intermittent": false,
  "confidence": "high",
  "nearest_water": null
}
```

**Example Response — No water**

```json
{
  "is_water": false,
  "name": null,
  "name_en": null,
  "subtype": null,
  "class": null,
  "category": null,
  "is_salt": null,
  "is_intermittent": null,
  "confidence": "high",
  "nearest_water": {
    "name": "Waitemata Harbour",
    "category": "harbour",
    "class": "harbour",
    "distance_m": 142
  }
}
```

**Confidence values**

| Value | Meaning |
|---|---|
| `high` | Point is well inside the polygon (>200m from boundary) |
| `medium` | Point is close to the polygon boundary (50–200m) |
| `low` | Point is very close to the boundary (<50m), or no data available for this area |

---

### `POST /cache/warm`

Pre-warm the tile cache for a given location. Requires API key.

Fetches the H3 cell covering the coordinate plus its 6 immediate neighbours (~7 × 250km² ring), storing all water features in the cache ahead of any query traffic. Only tiles that are missing or stale are fetched — safe to call repeatedly.

**Headers**

| Header | Required | Description |
|---|---|---|
| `X-API-Key` | ✅ | Your configured API key |

**Query Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `lat` | float | ✅ | Latitude of the centre point |
| `lng` | float | ✅ | Longitude of the centre point |

**Example Request**

```bash
curl -X POST "http://localhost:8000/cache/warm?lat=-36.8485&lng=174.7633" \
  -H "X-API-Key: your-secret-key"
```

**Example Response**

```json
{
  "status": "ok",
  "warmed": 5,
  "already_warm": 2
}
```

> Useful on deploy or after an `OVERTURE_RELEASE` pin change to front-load the cold-start cost for a known area of interest, rather than absorbing it on live query traffic.

### `POST /water/batch`

Check multiple coordinates in a single request.

**Headers**

| Header | Required | Description |
|---|---|---|
| `X-API-Key` | ✅ | Your configured API key |

**Request Body**

```json
{
  "coordinates": [
    { "lat": -36.8485, "lng": 174.7633 },
    { "lat": -43.7841, "lng": 172.4364 }
  ],
  "margin_m": 10.0
}
```

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `coordinates` | array | ✅ | — | 1–100 `{lat, lng}` pairs |
| `margin_m` | float | ❌ | `10.0` | Buffer in metres, applied to all coordinates |

**Example Response**

```json
{
  "results": [
    {
      "lat": -36.8485,
      "lng": 174.7633,
      "is_water": true,
      "name": "Waitemata Harbour",
      ...
    }
  ],
  "count": 1
}
```

> Coordinates sharing the same H3 tile (~250km²) use a single cache lookup, making batch requests within a region very efficient.

---

### `GET /health`

Service health check and cache statistics. No API key required.

```json
{
  "status": "ok",
  "cache": {
    "backend": "memory",
    "total_tiles": 4,
    "total_features": 2341,
    "oldest_tile": "2026-03-01T21:00:00+00:00",
    "stale_tiles": 0,
    "refresh_days": 30,
    "overture_release": "2026-02-18.0"
  }
}
```

---

### `POST /cache/evict`

Manually evict stale tiles from the cache. Requires API key.

Tiles are also refreshed lazily on next query — this endpoint is optional, useful if you want to force a clean slate after pinning a new `OVERTURE_RELEASE`.

```json
{ "evicted_tiles": 2 }
```

---

## ⚙️ Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_KEY` | ✅ | — | Static API key for request authentication |
| `CACHE_BACKEND` | ❌ | `memory` | Cache backend. Currently supports `memory` |
| `DATA_REFRESH_DAYS` | ❌ | `30` | Days before a cached tile is considered stale |
| `OVERTURE_RELEASE` | ❌ | auto | Pin a specific Overture release (e.g. `2026-02-18.0`). If unset, resolved from STAC on startup |

---

## 🏗️ Project Structure

```
overture-water-api/
├── main.py              # FastAPI app & routes
├── water.py             # Point-in-water query logic & classification
├── tile_cache.py        # H3 tile orchestration & Overture S3 fetching
├── cache/
│   ├── __init__.py      # Backend factory (reads CACHE_BACKEND env var)
│   ├── base.py          # Abstract cache interface
│   └── memory.py        # In-memory backend
├── .env.example
├── requirements.txt
└── Dockerfile
```

---

## 🧠 How It Works

```
Query arrives (lat, lng)
        ↓
Convert to H3 cell (resolution 5, ~250km²)
        ↓
Check in-memory tile cache
  ├─ HIT + fresh  →  query local features  →  return result
  └─ MISS / stale
          ↓
    Fetch bbox from Overture S3
    (DuckDB spatial query on public Parquet files)
          ↓
    Store tile in memory cache
          ↓
    Query local features  →  return result
```

On a cold start for any given area, the first query fetches only the ~250km² tile covering that coordinate — typically 10–800 features depending on how water-dense the area is. Subsequent queries in the same area are served from memory with no network calls.

---

## 🐳 Docker

```bash
docker build -t overture-water-api .
docker run -p 8000:8000 --env-file .env overture-water-api
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| API Framework | FastAPI |
| Query Engine | DuckDB |
| Spatial Index | Shapely STRtree |
| Tile System | H3 (Uber) |
| Data Source | Overture Maps S3 (public) |
| Config | python-dotenv |

---

## 📦 Used By

- [AquaRipple](https://github.com/StuFraser/aqua-ripple) — Community water quality monitoring platform

---

## 📄 Data Attribution & Licensing

This service uses geospatial data from the **Overture Maps Foundation**.

### Overture Maps — Base/Water Theme

The water feature data is sourced from the Overture Maps Foundation `base` theme and distributed under the **Open Database License (ODbL) v1.0**, as it incorporates data derived from OpenStreetMap.

**Required attribution:**
> © OpenStreetMap contributors, Overture Maps Foundation

Under the ODbL:
- You are free to use, modify, and distribute the data
- Any derivative databases must also be licensed under ODbL
- Attribution to both OpenStreetMap contributors and Overture Maps Foundation is required

### Full License Texts

- [ODbL v1.0](https://opendatacommons.org/licenses/odbl/1-0/)
- [CDLA Permissive v2.0](https://cdla.dev/permissive-2-0/)
- [Overture Maps Attribution Requirements](https://docs.overturemaps.org/attribution/)

---

## 📝 License

This project's source code is licensed under the [MIT License](LICENSE).

Data used at runtime carries its own licensing terms as described above.

---

## 🙏 Acknowledgements

- [Overture Maps Foundation](https://overturemaps.org) — Open map data
- [OpenStreetMap contributors](https://www.openstreetmap.org/copyright) — Underlying community map data
- [DuckDB](https://duckdb.org) — Blazing fast local analytics
- [H3](https://h3geo.org) — Hexagonal spatial indexing
