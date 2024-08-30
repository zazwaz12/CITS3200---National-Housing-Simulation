import polars as pl
import numpy as np
import matplotlib.pyplot as plt
from loguru import logger
from pathos.multiprocessing import ProcessingPool as Pool
from cytoolz import curry
import geopandas as gpd
from pyproj import CRS
import fiona

# Set the SHAPE_RESTORE_SHX option
fiona.drvsupport.supported_drivers['ESRI Shapefile'] = 'rw'
fiona.drvsupport.supported_drivers['SHAPE_RESTORE_SHX'] = 'YES'

# Read the shapefile
shapefile_path = "/content/SA1_2021_AUST_GDA2020.shp"
total_rows = len(pl.read_parquet(shapefile_path))
logger.info(f"Total number of rows in the shapefile: {total_rows}")

# Read the CSV file
csv_path = "/content/houses-sample.csv"
houses_df = pl.read_csv(csv_path)
logger.info(houses_df.head())

# Load the points from the CSV file
pnts = pl.read_csv(csv_path)

if 'LONGITUDE' in pnts.columns and 'LATITUDE' in pnts.columns:
    pnts = pnts.rename({'LONGITUDE': 'x', 'LATITUDE': 'y'})

# Convert points to GeoDataFrame (using GeoPandas for this part)
pnts_gdf = gpd.GeoDataFrame(
    pnts.to_pandas(),
    geometry=gpd.points_from_xy(pnts['x'], pnts['y']),
    crs="EPSG:7844"
)

# Project to a suitable projected CRS (GDA2020 / MGA zone 55)
projected_crs = CRS.from_epsg(7844)
pnts_gdf = pnts_gdf.to_crs(projected_crs)

# Read the entire shapefile
map_data = gpd.read_file("/content/SA1_2021_AUST_GDA2020.shp")
map_data = map_data.to_crs(projected_crs)

@curry
def process_chunk(map_data, chunk):
    # Perform spatial join for this chunk
    pnts_with_area = gpd.sjoin(chunk, map_data, how="left", predicate='within')

    # Identify points without an assigned area
    missing_points = pnts_with_area[pnts_with_area['SA2_NAME21'].isnull()]

    if not missing_points.empty:
        # Perform nearest join for missing points
        nearest_join = gpd.sjoin_nearest(missing_points[['geometry']], map_data, how="left")

        # Update the main result with nearest join results
        pnts_with_area.loc[missing_points.index, 'SA2_NAME21'] = nearest_join['SA2_NAME21']
        pnts_with_area.loc[missing_points.index, 'SA1_CODE21'] = nearest_join['SA1_CODE21']

    # Select and rename relevant columns
    result = pnts_with_area[['x', 'y', 'SA2_NAME21', 'SA1_CODE21']]
    result = result.rename(columns={'SA2_NAME21': 'area', 'SA1_CODE21': 'area_code'})
    return pl.DataFrame(result)

# Split points into chunks for parallel processing
num_cores = 4  # Adjust based on your system
chunks = np.array_split(pnts_gdf, num_cores)

# Parallel processing
with Pool(num_cores) as pool:
    all_results = pool.map(process_chunk(map_data), chunks)

# Combine all results
final_result = pl.concat(all_results)

# Check if there are still any missing areas
if final_result['area'].is_null().any() or final_result['area_code'].is_null().any():
    logger.warning("Some points were not assigned to an SA2 area even after nearest join. Please check your data.")

logger.info(final_result)

# Merge the final_result with the original points DataFrame to add area and area_code
houses_with_areas = pnts.join(final_result, on=['x', 'y'], how='left')

# Save the result to a CSV file
output_csv_path = "/content/houses_with_areas.csv"
houses_with_areas.write_csv(output_csv_path)

logger.info(f"CSV file saved to: {output_csv_path}")

# Display the first few rows of the resulting DataFrame
logger.info(houses_with_areas.head())

# Count the number of missing values in the 'area' column
missing_area_count = final_result['area'].is_null().sum()

logger.info(f"Number of missing values in the 'area' column: {missing_area_count}")

# Add random attributes for simulation
houses_with_attributes = houses_with_areas.with_columns(
    pl.Series("attribute", np.random.randint(1, 11, size=len(houses_with_areas)))
)

# Display the updated DataFrame
logger.info(houses_with_attributes.head())

def random_distribution(df, num_iterations=10):
    """
    Randomly redistributes points within their respective areas.

    Args:
        df: Polars DataFrame with 'x', 'y', 'attribute' and 'area_code' columns.
        num_iterations: Number of iterations to perform the random redistribution.

    Returns:
        Polars DataFrame representing the final state after random redistribution.
    """
    model_df = df.clone()
    model_df = model_df.with_columns([
        pl.col('y').alias('original_lat'),
        pl.col('x').alias('original_lon')
    ])

    for _ in range(num_iterations):
        # Group by area_code and randomly shuffle x and y coordinates within each group
        model_df = model_df.groupby('area_code').apply(
            lambda group: group.with_columns([
                pl.col('x').shuffle().alias('x'),
                pl.col('y').shuffle().alias('y')
            ])
        )

    return model_df

# Usage
area_codes = houses_with_attributes['area_code'].unique().to_list()[:4]
result_dfs = []

for area_code in area_codes:
    logger.info(f"Running random distribution for area code: {area_code}")
    area_df = houses_with_attributes.filter(pl.col('area_code') == area_code)
    result_df = random_distribution(area_df, num_iterations=10)
    result_dfs.append(result_df)

final_result_df = pl.concat(result_dfs)

# Visualization
for area_code in area_codes:
    area_result_df = final_result_df.filter(pl.col('area_code') == area_code)

    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    plt.scatter(area_result_df['original_lon'], area_result_df['original_lat'],
                c=area_result_df['attribute'].to_numpy(), cmap='tab10', s=50, alpha=0.7)
    plt.title(f"Original Positions for Area Code: {area_code}")
    plt.xlabel("Original Longitude")
    plt.ylabel("Original Latitude")

    plt.subplot(1, 2, 2)
    plt.scatter(area_result_df['x'], area_result_df['y'],
                c=area_result_df['attribute'].to_numpy(), cmap='tab10', s=50, alpha=0.7)
    plt.title(f"Final Positions after Random Distribution for Area Code: {area_code}")
    plt.xlabel("Final Longitude")
    plt.ylabel("Final Latitude")

    plt.tight_layout()
    plt.show()

logger.info("Random distribution completed.")

logger.info(final_result_df.head(20))
