import argparse
import os
from typing import Literal

import geopandas as gpd
import polars as pl
from fiona.drvsupport import supported_drivers
from loguru import logger

import sys
sys.path.append(".")
sys.path.append("..")

from nhs.data import read_parquet, join_coords_with_area, to_geo_dataframe, read_shapefile
from nhs import logging, config



def main(
    config_path: str,
    in_fname: str,
    out_fname: str,
    strategy: Literal["join_nearest", "filter"] | None,
) -> None:
    # Required for fiona - reads shapefiles
    supported_drivers["ESRI Shapefile"] = "rw"

    try:
        data_config = config.data_config(config_path)
        logger_config = config.logger_config(config_path)
    except Exception as e:
        logger.critical(
            f"Failed to load configuration at {config_path} with exception {e}, terminating..."
        )
        exit(1)
    logger.enable("nhs")
    logging.config_logger(logger_config)

    # TODO: REMOVE hardcoding
    in_path = os.path.join(data_config["gnaf_path"], in_fname)
    logger.info(f"Reading from {in_path}...")
    houses_df = read_parquet(in_path)
    if not isinstance(houses_df, pl.LazyFrame):
        logger.critical(
            f"Failed to load file at {data_config["gnaf_path"]}, terminating..."
        )
        exit(1)

    logger.info("Converting to GeoDataFrame...")
    house_coords = to_geo_dataframe(houses_df, data_config["crs"])

    logger.info(f"Reading from {data_config['shapefile_path']}...")
    area_polygons: gpd.GeoDataFrame = read_shapefile(
        data_config["shapefile_path"], data_config["crs"]
    )

    logger.info("Joining areas with points...")
    joined_coords = join_coords_with_area(house_coords, area_polygons, strategy)

    # joins output from area-point mapping to orignial data
    output_csv_path = os.path.join(data_config["output_path"], out_fname)
    joined_coords.sink_csv(output_csv_path)
    logger.info(f"CSV file saved to: {output_csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process house data and assign SA1 areas."
    )
    parser.add_argument(
        "-i",
        "--input_fname",
        type=str,
        help="Name of the input GNAF address geocode parquet file, e.g. 'TAS_ADDRESS_DEFAULT_GEOCODE_psv.parquet'",
    )
    parser.add_argument(
        "-o",
        "--output_fname",
        type=str,
        help="Name of the output CSV file",
        default="house_data_with_areas.csv",
    )
    parser.add_argument(
        "-c",
        "--config_path",
        type=str,
        help="Path to the configuration YAML file",
        default="configurations.yml",
    )
    parser.add_argument(
        "-s",
        "--strategy",
        type=str,
        help="Strategy to handle failed joins, either 'join_nearest' or 'filter'. If not specified, no action is taken.",
        default=None,
    )
    args = parser.parse_args()

    main(args.config_path, args.input_fname, args.output_fname, args.strategy)
