"""
Randomly generate individuals with census features and assign them to GNAF addresses.

"""

import argparse
import os
from typing import Literal

from time import time
import polars as pl
from fiona.drvsupport import supported_drivers
from loguru import logger
import sys

sys.path.append(".")
sys.path.append("..")
from nhs.data import (
    read_parquet,
    join_coords_with_area,
    to_geo_dataframe,
    read_shapefile,
    join_census_with_coords,
    randomly_assign_census_features,
)
from nhs import config
from nhs.logging import config_logger
from nhs.utils import compute_in_parallel, log_time


def main(
    config_path: str,
    gnaf_fname: str,
    census_fname: str,
    out_fname: str,
    strategy: Literal["join_nearest", "filter"] | None,
) -> None:
    # Required for fiona - reads shapefiles
    supported_drivers["ESRI Shapefile"] = "rw"

    init_time = time()  # logs total time taken

    try:
        data_config = config.data_config(config_path)
        logger_config = config.logger_config(config_path)
        simulation_config = config.simulation_config(config_path)
    except Exception as e:
        logger.critical(
            f"Failed to load configuration at {config_path} with exception {e}, terminating..."
        )
        exit(1)

    logger.enable("nhs")
    config_logger(logger_config)

    gnaf_path = os.path.join(data_config["gnaf_path"], gnaf_fname)
    logger.info(f"Reading GNAF data from {gnaf_path}...")
    with log_time():
        houses_df = read_parquet(gnaf_path)
    if not isinstance(houses_df, pl.LazyFrame):
        logger.error(f"Failed to load GNAF file at {gnaf_path}, terminating...")
        exit(1)

    census_path = os.path.join(data_config["census_path"], census_fname)
    logger.info(f"Reading census data from {census_path}...")
    with log_time():
        census = read_parquet(census_path)
    if not isinstance(census, pl.LazyFrame):
        logger.error(f"Failed to load census file at {census_path}, terminating...")
        exit(1)

    logger.info(
        f"Reading shapefile from {data_config['shapefile_path']} and converting housing dataset to GeoDataFrame..."
    )
    with log_time():
        area_polygons, house_coords = compute_in_parallel(
            lambda: read_shapefile(data_config["shapefile_path"], data_config["crs"]),
            lambda: to_geo_dataframe(houses_df, data_config["crs"]),
        )

    logger.info("Joining coordinates with area polygons...")
    with log_time():
        joined_coords = join_coords_with_area(house_coords, area_polygons, strategy)

    out_path = os.path.join(data_config["output_path"], out_fname)
    logger.info(
        f"Randomly assigning census features to GNAF addresses and saving to {out_path}..."
    )
    with log_time():
        census_gnaf = join_census_with_coords(census, joined_coords)

        # TODO: is there a better way to handle column names instead of hard-coding?
        allocated = randomly_assign_census_features(
            census_gnaf,
            "SA1_CODE_2021",
            "LONGITUDE",
            "LATITUDE",
            simulation_config["census_features"],
        )
        logger.debug(allocated.explain(streaming=True))
        allocated.collect().write_csv(out_path)
    logger.info(
        f"Allocation complete, saved to {out_path} in {time() - init_time:.2f} s total."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Randomly assign census features to GNAF addresses."
    )
    parser.add_argument(
        "-g",
        "--gnaf_fname",
        type=str,
        help="Name of the input GNAF address geocode parquet file, e.g. 'TAS_ADDRESS_DEFAULT_GEOCODE_psv.parquet'",
        required=True,
    )
    parser.add_argument(
        "-i",
        "--census_fname",
        type=str,
        help="Name of the input census parquet file, e.g. 2021Census_G01_AUS_AUS.parquet",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_fname",
        type=str,
        help="Name of the output CSV file",
        default="random_allocation.csv",
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

    main(
        args.config_path,
        args.gnaf_fname,
        args.census_fname,
        args.output_fname,
        args.strategy,
    )
