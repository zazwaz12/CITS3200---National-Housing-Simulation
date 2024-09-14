import argparse

from debugpy.public_api import configure
import geopandas as gpd
import polars as pl
from fiona.drvsupport import supported_drivers
from loguru import logger
import numpy as np
import os

from context import nhs
from nhs.data.geography import join_coords_with_area

from loguru import logger




config = nhs.config
geography = nhs.data.geography
read_parquet = nhs.data.read_parquet
parallel_map = nhs.utils.parallel.parallel_map
join_coords_with_area = nhs.data.join_coords_with_area



def main(config_path: str) -> None:
    # Required for fiona - reads shapefiles
    supported_drivers["ESRI Shapefile"] = "rw"

    try:
        data_config = config.data_config(config_path)
        logger_config = config.logger_config(config_path)
    except Exception:
        logger.critical(f"Failed to load configuration at {config_path}, terminating...")
        exit(1)
    logger.enable("nhs")
    nhs.logging.config_logger(logger_config)
        
    # TODO: REMOVE hardcoding
    logger.info(f"Reading from {data_config['gnaf_path']}/TAS_ADDRESS_DEFAULT_GEOCODE_psv.parquet...")
    houses_df = read_parquet(f"{data_config["gnaf_path"]}/TAS_ADDRESS_DEFAULT_GEOCODE_psv.parquet")
    if not isinstance(houses_df, pl.LazyFrame):
        logger.critical(f"Failed to load CSV file at {data_config["gnaf_path"]}, terminating...")
        exit(1)
    logger.info("Converting to GeoDataFrame...")
    house_coords = geography.to_geo_dataframe(houses_df, data_config["crs"])
    
    logger.info(f"Reading from {data_config['shapefile_path']}...")
    area_polygons: gpd.GeoDataFrame = gpd.read_file(data_config["shapefile_path"]).to_crs(house_coords.crs)

    logger.info("Joining areas with points...")
    final_result = join_coords_with_area(house_coords, area_polygons)

    have_unassigned_point = np.any(final_result.select(pl.col("area", "area_code").is_null().any()).collect().to_numpy())
    # Check if any points were not assigned to an SA1 area 
    if have_unassigned_point:
        logger.warning(
            "Some points were not assigned to an SA1 area even after nearest join. Please check your data."  # noqa: E501
        )
    missing_area_count =  final_result.select(pl.col("area").is_null().sum()).collect().item()
    logger.info(f"Number of missing values in the 'area' column: {missing_area_count}")

    # joins output from area-point mapping to orignial data
    houses_with_areas = houses_df.join(final_result, on=["LONGITUDE", "LATITUDE"], how="left") # type: ignore
    output_csv_path = os.path.join(data_config["output_path"], "house_data_with_areas")
    houses_with_areas.sink_csv(output_csv_path)
    logger.info(f"CSV file saved to: {output_csv_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process house data and assign SA1 areas."
    )
    parser.add_argument(
        "-c", "--config_path", type=str, help="Path to the configuration YAML file",
        default="configurations.yml"
    )
    # TODO: extend to work with multiple files
    args = parser.parse_args()

    main(args.config_path)
