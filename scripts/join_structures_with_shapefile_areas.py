import argparse

import geopandas as gpd
import polars as pl
from fiona.drvsupport import supported_drivers
from loguru import logger
import numpy as np

from .context import nhs

config = nhs.config
geography = nhs.data.geography
read_csv = nhs.data.read_csv
parallel_map = nhs.utils.parallel.parallel_map
join_areas_with_points = nhs.data.join_areas_with_points



def main(config_path: str) -> None:
    # Required for fiona - reads shapefiles
    supported_drivers["ESRI Shapefile"] = "rw"

    try:
        data_config = config.data_config(config_path)
        simulation_config = config.simulation_config(config_path)
    except Exception:
        logger.critical(f"Failed to load configuration at {config_path}, terminating...")
        exit(1)
        
    houses_df = read_csv(data_config["gnaf_path"])
    if not isinstance(houses_df, pl.LazyFrame):
        logger.critical(f"Failed to load CSV file at {data_config["gnaf_path"]}, terminating...")
        exit(1)

    gdf = geography.to_geo_dataframe(houses_df, data_config["crs"])
    map_data: gpd.GeoDataFrame = gpd.read_file(data_config["shapefile_path"]).to_crs(gdf.crs)


    chunks = np.array_split(gdf, simulation_config["num_cores"])
    final_result = parallel_map(chunks, lambda chunk: join_areas_with_points(chunk, map_data), simulation_config["num_cores"]) # type: ignore
    final_result = pl.concat(final_result)

    # Check if any points were not assigned to an SA1 area 
    if (
        final_result["area"].is_null().any() # type: ignore
        or final_result["area_code"].is_null().any() # type: ignore
    ):
        logger.warning(
            "Some points were not assigned to an SA1 area even after nearest join. Please check your data."  # noqa: E501
        )

    # joins output from area-point mapping to orignial data
    houses_with_areas = houses_df.join(final_result, on=["x", "y"], how="left") # type: ignore

    missing_area_count = final_result["area"].is_null().sum() # type: ignore
    logger.info(f"Number of missing values in the 'area' column: {missing_area_count}")

    output_csv_path = data_config["output_csv_path"]
    houses_with_areas.collect().write_csv(output_csv_path)
    logger.info(f"CSV file saved to: {output_csv_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process house data and assign SA1 areas."
    )
    parser.add_argument(
        "-c", "--config_path", type=str, help="Path to the configuration YAML file",
        default="configurations.yaml"
    )
    args = parser.parse_args()

    main(args.config_path)
