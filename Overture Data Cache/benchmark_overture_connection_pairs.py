"""Benchmark direct Overture queries in fresh-connection pairs.

Each pair creates one DuckDB connection. Query 1 is its first remote Parquet
query; query 2 reuses that same connection. The next pair starts from a new
connection. This intentionally does not enable DuckDB metadata caches beyond
their defaults.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


HERE = Path(__file__).resolve().parent
RELEASE = "2026-10-21.0"
BBOX_BUFFER_DEG = 0.05
SOURCE = (
    f"s3://overturemaps-us-west-2/release/{RELEASE}/"
    "theme=base/type=water/*"
)


def read_points(path: Path) -> list[dict]:
    points = []
    for line in path.read_text().splitlines():
        if line.strip():
            lng, lat = (float(value.strip()) for value in line.split(","))
            points.append({"lat": lat, "lng": lng})
    return points


def initialise_connection() -> tuple[duckdb.DuckDBPyConnection, float]:
    started = time.perf_counter()
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_access_key_id='';")
    con.execute("SET s3_secret_access_key='';")
    con.execute("SET s3_session_token='';")
    con.execute("SET s3_region='us-west-2';")
    return con, time.perf_counter() - started


def query(con: duckdb.DuckDBPyConnection, point: dict) -> dict:
    lat, lng = point["lat"], point["lng"]
    started = time.perf_counter()
    rows = con.execute(f"""
        SELECT
            names.primary AS name,
            subtype,
            class,
            is_salt,
            is_intermittent,
            ST_AsWKB(geometry) AS geom_wkb
        FROM read_parquet('{SOURCE}', hive_partitioning = 1)
        WHERE bbox.xmin <= {lng + BBOX_BUFFER_DEG}
          AND bbox.xmax >= {lng - BBOX_BUFFER_DEG}
          AND bbox.ymin <= {lat + BBOX_BUFFER_DEG}
          AND bbox.ymax >= {lat - BBOX_BUFFER_DEG}
    """).fetchall()
    return {**point, "query_seconds": time.perf_counter() - started, "features": len(rows)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair", type=int, required=True, help="Zero-based pair index")
    parser.add_argument("--points", type=Path, default=HERE / "testpoints.txt")
    args = parser.parse_args()

    points = read_points(args.points)
    start = args.pair * 2
    selected = points[start : start + 2]
    if not selected:
        raise ValueError(f"Pair {args.pair} has no test points")

    con, setup_seconds = initialise_connection()
    try:
        queries = [query(con, point) for point in selected]
    finally:
        con.close()

    print(json.dumps({
        "pair": args.pair,
        "release": RELEASE,
        "connection_setup_seconds": setup_seconds,
        "queries": queries,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
