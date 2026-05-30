# Water Data Refresh Process

Documents the process for refreshing the Overture Maps water feature data
stored in Cloudflare R2, used by GetWet for water body lookups.

This is a one-time setup and periodic refresh process. Normal GetWet operation
does not require running these scripts.

---

## Overview

```
Overture Maps S3 (public)
    │
    │  raw_parquet.py  — extract raw global water features
    ▼
overture_water_raw.parquet  (local, ~3.5GB, do not commit)
    │
    │  generate_simplified.py  — apply tiered geometry simplification
    ▼
overture_water_global.parquet  (local, ~3.4GB, do not commit)
    │
    │  upload_to_r2.py  — upload to Cloudflare R2
    ▼
Cloudflare R2  (getwet bucket)
    └─ overture_water_global.parquet
```

---

## Prerequisites

### Python dependencies
```bash
pip install duckdb pandas pyarrow boto3
```

### Cloudflare R2 credentials
Two API tokens are required — store securely, never commit:

| Token | Permission | Used by |
|---|---|---|
| Read & Write | Object Read & Write | `upload_to_r2.py` |
| Read Only | Object Read Only | GetWet `.env` |

Create tokens in Cloudflare Dashboard → R2 → Manage R2 API Tokens.

### Environment variables
Set before running upload script:
```bash
export R2_ENDPOINT="https://<account_id>.r2.cloudflarestorage.com"
export R2_ACCESS_KEY_ID="<read-write-access-key>"
export R2_SECRET_KEY="<read-write-secret-key>"
export R2_BUCKET="getwet"
```

---

## Step 1 — Extract Raw Overture Data

```bash
python3 scripts/raw_parquet.py
```

**What it does:**
- Connects to Overture's public S3 bucket (`overturemaps-us-west-2`, `us-west-2`)
- Extracts all named water features globally
- Subtypes: `lake`, `river`, `reservoir`, `canal`, `harbour`, `estuary`
- Outputs `overture_water_raw.parquet` with full unsimplified geometry

**Runtime:** 1-2 hours depending on connection speed

**Notes:**
- Requires at least 4GB RAM
- S3 bucket is public — no AWS credentials needed
- Pin the Overture release version and update this doc on refresh
- Do not commit the parquet file — add to `.gitignore`
- **Current pinned release:** `2026-04-15.0`

**Known issues:**
- 403 errors: ensure S3 credentials are explicitly set to empty strings to disable signing
- Region must be set to `us-west-2`
- Memory limit required on machines with less than 16GB RAM

---

## Step 2 — Generate Simplified Parquet

```bash
python3 scripts/generate_simplified.py
```

**What it does:**
Applies tiered geometry simplification to balance accuracy vs file size:

| Feature size | Simplification | Tolerance | Accuracy |
|---|---|---|---|
| Under 100KB | None | — | Full accuracy |
| 100KB – 500KB | ST_SimplifyPreserveTopology | 0.0001° (~10m) | High |
| 500KB – 1MB | ST_SimplifyPreserveTopology | 0.001° (~100m) | Medium |
| Over 1MB | ST_SimplifyPreserveTopology | 0.01° (~1km) | Low |

The low accuracy tier only affects ~10 genuinely massive features (Lake Huron,
Lake Superior, Great Slave Lake etc.) where precision at the boundary is
not meaningful for GetWet's use case.

Outputs `overture_water_global.parquet`.

**Expected output:**
```
Rows:       2,235,423
Avg bytes:  ~1,580
Max bytes:  ~999,137
Total MB:   ~3,368
Over 500KB: ~6
Over 1MB:   0
```

---

## Step 3 — Upload to Cloudflare R2

```bash
python3 scripts/upload_to_r2.py
```

**What it does:**
- Uploads `overture_water_global.parquet` to the `getwet` R2 bucket
- Overwrites any existing file with the same key

**Notes:**
- Ensure R2 environment variables are set before running
- Upload time depends on file size and connection speed (~3.4GB)
- Verify upload in Cloudflare R2 dashboard after completion
- The Read & Write token is only needed for this step — GetWet uses Read Only

---

## Step 4 — Verify

Confirm the file is accessible from R2 with a test query:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='<account_id>.r2.cloudflarestorage.com';")
con.execute("SET s3_access_key_id='<read-only-key>';")
con.execute("SET s3_secret_access_key='<read-only-secret>';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_url_style='path';")

result = con.execute("""
    SELECT COUNT(*) FROM read_parquet('s3://getwet/overture_water_global.parquet')
""").fetchone()
print(f"Rows: {result[0]:,}")
```

Expected: `Rows: 2,235,423`

---

## Refresh Schedule

Water features change slowly — new reservoirs, river diversions, coastline
corrections. Refreshing once or twice a year is sufficient.

To refresh:
1. Update `OVERTURE_RELEASE` in `scripts/raw_parquet.py`
2. Run Steps 1–4 above
3. Update the pinned release version in this document
4. GetWet picks up the new data automatically on next cache miss — no restart needed

---

## Cloudflare Free Tier Usage

| Resource | Free Limit | This file |
|---|---|---|
| R2 Storage | 10GB/month | ~3.4GB |
| R2 Class A ops (writes) | 1M/month | 1 per refresh |
| R2 Class B ops (reads) | 10M/month | 1 per GetWet query |

---

## Attribution

Water feature data sourced from [Overture Maps Foundation](https://overturemaps.org)
(release `2026-04-15.0`), derived from OpenStreetMap contributors.

License: [ODbL v1.0](https://opendatacommons.org/licenses/odbl/1-0/)
