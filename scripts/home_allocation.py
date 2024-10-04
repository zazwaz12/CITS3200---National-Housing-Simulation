"""
Randomly generate individuals with census features and assign them to GNAF addresses.

"""

import argparse
import os
import sys
from time import time
from typing import Literal

import polars as pl
from fiona.drvsupport import supported_drivers
from loguru import logger

sys.path.append(".")
sys.path.append("..")
from nhs import config
from nhs.data import (
    join_census_frames,
    join_census_with_coords,
    join_coords_with_area,
    randomly_assign_census_features,
    read_parquet,
    read_shapefile,
    read_spreadsheets,
    to_geo_dataframe,
)
from nhs.logging import config_logger
from nhs.utils import log_time


def join_gnaf_with_shapefile(
    gnaf_dir: str,
    gnaf_default_code_pattern: str,
    shapefile_dir: str,
    data_config: dict,
    strategy: Literal["join_nearest", "filter"] | None = None,
) -> pl.LazyFrame:
    with log_time():
        logger.info(f"Reading GNAF default geocode data from {gnaf_dir}...")
        geocode_lfs = read_spreadsheets(gnaf_dir, "parquet", gnaf_default_code_pattern)
        geocode_lfs = pl.concat(geocode_lfs.values(), how="diagonal_relaxed")  # type: ignore

        logger.info(f"Reading shapefile from {data_config['shapefile_path']}")
        area_polygons = read_shapefile(shapefile_dir, data_config["crs"])

    with log_time():
        logger.info("Converting GNAF addresses to GeoDataFrame...")
        with log_time():
            house_coords = (to_geo_dataframe(geocode_lfs, data_config["crs"]),)

    with log_time():
        logger.info("Joining coordinates with area polygons...")
        with log_time():
            joined_coords = join_coords_with_area(house_coords, area_polygons, strategy)
    return joined_coords


def main(
    gnaf_dir: str,
    gnaf_cache: str,
    gnaf_default_code_pattern: str,
    gnaf_address_details_pattern: str,
    shapefile_dir: str,
    census_dir: str,
    census_pattern: str,
    output_path: str,
    data_config: dict,
    simulation_config: dict,
    strategy: Literal["join_nearest", "filter"] | None = None,
) -> None:
    # Required for fiona - reads shapefiles
    supported_drivers["ESRI Shapefile"] = "rw"

    init_time = time()  # logs total time taken

    if not os.path.exists(gnaf_cache):
        logger.warning(
            f"Unable to find GNAF cache file at {gnaf_cache}, GNAF will be joined with shapefiles. Note that it's recommended to perform this join beforehand as this process is time-consuming."
        )
        joined_coords = join_gnaf_with_shapefile(
            gnaf_dir, gnaf_default_code_pattern, shapefile_dir, data_config, strategy
        )
    else:
        logger.info(f"Reading GNAF cache from {gnaf_cache}...")
        joined_coords = read_parquet(gnaf_cache)

    with log_time():
        logger.info(f"Reading census data from {census_dir}...")
        census_lfs = read_spreadsheets(census_dir, "parquet", census_pattern)
        census = join_census_frames(census_lfs)

    logger.info(
        f"Randomly assigning census features to GNAF addresses and saving to {output_path}..."
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
        allocated.collect().write_csv(output_path)
    logger.info(
        f"Allocation complete, saved to {output_path} in {time() - init_time:.2f} s total."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Randomly assign census features to GNAF addresses."
    )
    parser.add_argument(
        "-g",
        "--gnaf_dir",
        type=str,
        help="Directory containing the input GNAF address geocode parquet files",
        default=None,
    )
    parser.add_argument(
        "--gnaf_default_code_pattern",
        help="Regex pattern to match the names of GNAF files containing the default geocode in the directory to process. Defaults to r'[A-Z]+_ADDRESS_DEFAULT_GEOCODE_psv'",
        type=str,
        default=r"[A-Z]+_ADDRESS_DEFAULT_GEOCODE_psv",
    )
    parser.add_argument(
        "--gnaf_address_details_pattern",
        help="Regex pattern to match the names of GNAF files containing the address details in the directory to process. Defaults to r'[A-Z]+_ADDRESS_DETAIL_psv'",
        type=str,
        default=r"[A-Z]+_ADDRESS_DETAIL_psv",
    )
    parser.add_argument(
        "--gnaf_cache",
        help="Path to a file with GNAF default geocodes joined with the shapefile. If supplied, the shapefile and GNAF files are ignored.",
        type=str,
        default=None,
    )
    parser.add_argument(
        "-s",
        "--shapefile_dir",
        help="Path to a shapefile directory. Ignored if GNAF cache file is supplied.",
        type=str,
        default=None,
    )
    parser.add_argument(
        "-i",
        "--census_dir",
        type=str,
        help="Directory containing the input census parquet files, e.g. 2021Census_G01_AUS_AUS.parquet",
        default=None,
    )
    parser.add_argument(
        "--census_pattern",
        help="Regex pattern to match the names of census files in the directory to process. Defaults to r'2021Census_G\\d+[A-Z]?_AUST_SA1'",
        type=str,
        default=r"2021Census_G\d+[A-Z]?_AUST_SA1",
    )
    parser.add_argument(
        "-o",
        "--output_path",
        type=str,
        help="Path of the output CSV file",
        default=None,
    )
    parser.add_argument(
        "-c",
        "--config_path",
        type=str,
        help="Path to the configuration YAML file",
        default="configurations.yml",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        help="Strategy to handle failed joins, either 'join_nearest' or 'filter'. If not specified, no action is taken. Ignored if GNAF cache file is supplied.",
        default=None,
    )

    args = parser.parse_args()

    logger.enable("nhs")

    try:
        data_config = config.data_config(args.config_path)
        logger_config = config.logger_config(args.config_path)
        simulation_config = config.simulation_config(args.config_path)
    except Exception as e:
        logger.critical(
            f"Failed to load configuration at {args.config_path} with exception {e}, terminating..."
        )
        exit(1)
    config_logger(logger_config)

    main(
        gnaf_dir=args.gnaf_dir or data_config["gnaf_path"],
        gnaf_cache=args.gnaf_cache or data_config["gnaf_cache_file"],
        gnaf_default_code_pattern=args.gnaf_default_code_pattern,
        gnaf_address_details_pattern=args.gnaf_address_details_pattern,
        shapefile_dir=args.shapefile_dir or data_config["shapefile_path"],
        census_dir=args.census_dir or data_config["census_path"],
        census_pattern=args.census_pattern,
        output_path=(
            args.output_path
            or os.path.join(data_config["output_path"], "allocated.csv")
        ),
        strategy=args.strategy,
        data_config=data_config,
        simulation_config=simulation_config,
    )
