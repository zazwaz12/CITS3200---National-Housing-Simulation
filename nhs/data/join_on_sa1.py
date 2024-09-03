from typing import Any, List  # Importing required types

import fiona
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import yaml
from fiona.drvsupport import supported_drivers
from loguru import logger
from pathos.multiprocessing import ProcessingPool as Pool
from pyproj import CRS

# Set the SHAPE_RESTORE_SHX option
supported_drivers["ESRI Shapefile"] = "rw"


def read_config(config_path: str) -> dict:
    """Read YAML configuration file."""
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    logger.info(f"Configuration loaded: {config}")
    return config


def read_shapefile(path: str) -> int:
    """Read shapefile and return total number of rows."""
    total_rows = len(pl.read_parquet(path))
    logger.info(f"Total number of rows in the shapefile: {total_rows}")
    return total_rows


def read_csv(path: str) -> pl.DataFrame:
    """Read CSV file and return as Polars DataFrame."""
    df = pl.read_csv(path)
    logger.info(df.head())
    return df


def prepare_points(df: pl.DataFrame, crs: str) -> Any:
    """Prepare points data for geospatial analysis."""
    if "LONGITUDE" in df.columns and "LATITUDE" in df.columns:
        df = df.rename({"LONGITUDE": "x", "LATITUDE": "y"})

    gdf = gpd.GeoDataFrame(
        df.to_pandas(),
        geometry=gpd.points_from_xy(df["x"], df["y"]),
        crs="EPSG:7844",  # Assuming input is in WGS84
    )  # type: ignore
    projected_crs = CRS.from_string(crs)
    return gdf.to_crs(projected_crs)


def process_chunk(map_data: Any, chunk: Any) -> pl.DataFrame:
    """Process a chunk of point data, joining with area data."""
    pnts_with_area = gpd.sjoin(chunk, map_data, how="left", predicate="within")
    missing_points = pnts_with_area[pnts_with_area["SA2_NAME21"].isnull()]

    if not missing_points.empty:
        nearest_join = gpd.sjoin_nearest(
            missing_points[["geometry"]], map_data, how="left"
        )
        pnts_with_area.loc[missing_points.index, "SA2_NAME21"] = nearest_join[
            "SA2_NAME21"
        ]
        pnts_with_area.loc[missing_points.index, "SA1_CODE21"] = nearest_join[
            "SA1_CODE21"
        ]

    result = pnts_with_area[["x", "y", "SA2_NAME21", "SA1_CODE21"]]
    result = result.rename(columns={"SA2_NAME21": "area", "SA1_CODE21": "area_code"})
    return pl.DataFrame(
        result.to_dict()
    )  # Convert to dict first to ensure compatibility with Polars



def parallel_process(pnts_gdf: Any, map_data: Any, num_cores: int) -> pl.DataFrame:
    """Parallel process point data."""
    chunks = np.array_split(pnts_gdf, num_cores)
    with Pool(num_cores) as pool:
        all_results = pool.map(lambda chunk: process_chunk(map_data, chunk), chunks)
    return pl.concat(all_results)


def random_distribution(df: pl.DataFrame, num_iterations: int = 10) -> pl.DataFrame:
    """Randomly shuffle lat-long pairs within their respective areas."""
    model_df = df.clone()
    model_df = model_df.with_columns([
        pl.col('y').alias('original_lat'),
        pl.col('x').alias('original_lon')
    ])

    for _ in range(num_iterations):
        # Loop over unique area_code values and shuffle lat-long pairs within each group
        area_codes = model_df['area_code'].unique().to_list()
        for area_code in area_codes:
            area_df = model_df.filter(pl.col('area_code') == area_code)
            shuffled_pairs = area_df[['x', 'y']].sample(frac=1).to_dict(as_series=False)
            model_df = model_df.with_columns([
                pl.when(pl.col('area_code') == area_code).then(pl.Series(shuffled_pairs['x'])).otherwise(pl.col('x')).alias('x'),
                pl.when(pl.col('area_code') == area_code).then(pl.Series(shuffled_pairs['y'])).otherwise(pl.col('y')).alias('y')
            ])

    return model_df


def visualize_results(final_result_df: pl.DataFrame, area_codes: List[str]) -> None:
    """Visualize the original and final positions of points."""
    for area_code in area_codes:
        area_result_df = final_result_df.filter(pl.col("area_code") == area_code)

        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        plt.scatter(
            area_result_df["original_lon"],
            area_result_df["original_lat"],
            c=area_result_df["attribute"].to_numpy(),
            cmap="tab10",
            s=50,
            alpha=0.7,
        )
        plt.title(f"Original Positions for Area Code: {area_code}")
        plt.xlabel("Original Longitude")
        plt.ylabel("Original Latitude")

        plt.subplot(1, 2, 2)
        plt.scatter(
            area_result_df["x"],
            area_result_df["y"],
            c=area_result_df["attribute"].to_numpy(),
            cmap="tab10",
            s=50,
            alpha=0.7,
        )
        plt.title(
            f"Final Positions after Random Distribution for Area Code: {area_code}"
        )
        plt.xlabel("Final Longitude")
        plt.ylabel("Final Latitude")

        plt.tight_layout()
        plt.show()


def main() -> None:
    config_path = "configurations.yaml"
    config = read_config(config_path)

    shapefile_path = config['shapefile_path']
    csv_path = config['csv_path']
    crs = config['crs']
    
    total_rows = read_shapefile(shapefile_path)
    houses_df = read_csv(csv_path)
    
    pnts_gdf = prepare_points(houses_df, crs)
    map_data = gpd.read_file(shapefile_path).to_crs(pnts_gdf.crs)
    
    num_cores = config.get('num_cores', 4)  # Default to 4 if not specified
    final_result = parallel_process(pnts_gdf, map_data, num_cores)
    
    if final_result['area'].is_null().any() or final_result['area_code'].is_null().any():
        logger.warning("Some points were not assigned to an SA1 area even after nearest join. Please check your data.")
    
    houses_with_areas = houses_df.join(final_result, on=['x', 'y'], how='left')
    
    output_csv_path = config['output_csv_path']
    houses_with_areas.write_csv(output_csv_path)
    logger.info(f"CSV file saved to: {output_csv_path}")
    
    missing_area_count = final_result['area'].is_null().sum()
    logger.info(f"Number of missing values in the 'area' column: {missing_area_count}")
    
    houses_with_attributes = houses_with_areas.with_columns(
        pl.Series("attribute", np.random.randint(1, 11, size=len(houses_with_areas)))
    )
    
    # Check if random distribution is enabled in the config
    if config.get('enable_random_distribution', False):
        area_codes = houses_with_attributes['area_code'].unique().to_list()[:4]
        result_dfs = []

        for area_code in area_codes:
            logger.info(f"Running random distribution for area code: {area_code}")
            area_df = houses_with_attributes.filter(pl.col('area_code') == area_code)
            result_df = random_distribution(area_df, num_iterations=config.get('num_iterations', 10))
            result_dfs.append(result_df)

        final_result_df = pl.concat(result_dfs)

        # Check if visualization is enabled in the config
        if config.get('enable_visualization', False):
            visualize_results(final_result_df, area_codes)
            logger.info("Visualization completed.")
        else:
            logger.info("Visualization skipped as per configuration.")

        logger.info("Random distribution completed.")
        logger.info(final_result_df.head(20))
    else:
        logger.info("Random distribution skipped as per configuration.")


if __name__ == "__main__":
    main()
