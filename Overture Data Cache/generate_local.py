import os
import duckdb
import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

H3_RESOLUTION = 4
OUTPUT_DIR = "./dist_tiles"
WORKERS = 8 

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

print("Extracting geometries and distinct point coordinates via DuckDB...")
query = """
    WITH RECURSIVE series AS (
        SELECT 
            id, name, subtype, class, is_salt, is_intermittent,
            min_lon, max_lon, min_lat, max_lat, geom_wkb,
            ST_GeomFromWKB(geom_wkb) as geom,
            ST_NPoints(ST_GeomFromWKB(geom_wkb)) as total_pts
        FROM read_parquet('overture_water_global.parquet')
    ),
    expanded_points AS (
        SELECT 
            id, name, subtype, class, is_salt, is_intermittent,
            min_lon, max_lon, min_lat, max_lat, geom_wkb,
            ST_PointN(geom, CAST(generate_series AS INTEGER)) as pt
        FROM series,
             LATERAL generate_series(1, total_pts)
    )
    SELECT 
        id, name, subtype, class, is_salt, is_intermittent,
        min_lon, max_lon, min_lat, max_lat, geom_wkb,
        ST_X(pt) as lon,
        ST_Y(pt) as lat
    FROM expanded_points
    WHERE pt IS NOT NULL
"""

print("Running spatial extraction query...")
raw_df = con.execute(query).df()
print(f"Extracted {len(raw_df):,} vertex coordinates. Mapping to H3 grid...")

# Dictionary to collect rows grouped by H3 tile string
tiles = defaultdict(list)

# We group by ID first so we don't process duplicate points for the same feature
for _, group in raw_df.groupby('id'):
    # Get the static metadata from the first row of the feature group
    first_row = group.iloc[0]
    feature_data = {
        'id': first_row['id'],
        'name': first_row['name'],
        'subtype': first_row['subtype'],
        'class': first_row['class'],
        'is_salt': first_row['is_salt'],
        'is_intermittent': first_row['is_intermittent'],
        'min_lon': first_row['min_lon'],
        'max_lon': first_row['max_lon'],
        'min_lat': first_row['min_lat'],
        'max_lat': first_row['max_lat'],
        'geom_wkb': first_row['geom_wkb']
    }
    
    # Find all unique H3 cells this specific feature's vertices touch
    unique_cells = set()
    for _, row in group.iterrows():
        try:
            cell = h3.latlng_to_cell(row['lat'], row['lon'], H3_RESOLUTION)
            unique_cells.add(cell)
        except Exception:
            pass
            
    # Append the row metadata to every tile it intersects
    for cell in unique_cells:
        tiles[cell].append(feature_data)

def save_tile_local(cell, rows_list):
    try:
        tile_df = pd.DataFrame(rows_list)
        file_path = os.path.join(OUTPUT_DIR, f"{cell}.parquet")
        
        pq.write_table(
            pa.Table.from_pandas(tile_df), 
            file_path, 
            compression='zstd'
        )
        return True
    except Exception as e:
        print(f"Failed to write tile {cell}: {e}")
        return False

print(f"Writing {len(tiles):,} tile files to '{OUTPUT_DIR}'...")
success_count = 0

with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = [
        executor.submit(save_tile_local, str(cell), rows) 
        for cell, rows in tiles.items()
    ]
    
    for future in as_completed(futures):
        if future.result():
            success_count += 1

print("\n--- Processing Complete ---")
print(f"Successfully generated {success_count:,} local tile files.")