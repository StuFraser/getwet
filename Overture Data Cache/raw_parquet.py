import duckdb

print("Starting unsimplified extraction...")

con = duckdb.connect()
con.execute("SET memory_limit='4GB';")
con.execute("SET threads=2;")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_access_key_id='';")
con.execute("SET s3_secret_access_key='';")
con.execute("SET s3_session_token='';")
con.execute("SET s3_region='us-west-2';")

OVERTURE_RELEASE = "2026-04-15.0"

con.execute(f"""
COPY (
    SELECT
        id,
        names.primary                AS name,
        subtype,
        class,
        is_salt,
        is_intermittent,
        bbox.xmin                    AS min_lon,
        bbox.xmax                    AS max_lon,
        bbox.ymin                    AS min_lat,
        bbox.ymax                    AS max_lat,
        ST_AsWKB(geometry)           AS geom_wkb
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}/theme=base/type=water/*',
        hive_partitioning = 1
    )
    WHERE names.primary IS NOT NULL
      AND subtype IN (
          'lake', 'river', 'reservoir', 'canal',
          'harbour', 'estuary'
      )
)
TO 'overture_water_raw.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

print("Done. Release pinned:", OVERTURE_RELEASE)