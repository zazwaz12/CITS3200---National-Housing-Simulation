"""
Join GNAF dataset with shapefile and cache the result as parquet file

Join is performed using sjoin() from geopandas which require the polars
LazyFrame to be converted to geopandas GeoDataFrame, an extremely expensive
operation. This script runs this operation once to avoid repeating it in
other scripts.
"""

import sys
from typing import Literal

import polars as pl

sys.path.append(".")
sys.path.append("..")
import argparse

from fiona import supported_drivers
from loguru import logger

from nhs.config import data_config, logger_config
from nhs.data import read_shapefile, read_spreadsheets
from nhs.data.geography import join_coords_with_area, to_geo_dataframe
from nhs.logging import config_logger


def main(
    gnaf_dir: str,
    shapefile_dir: str,
    pattern: str,
    output_name: str,
    data_config: dict,
    extension: str = "parquet",
    strategy: Literal["join_nearest", "filter"] | None = None,
):
    # Required for fiona - reads shapefiles
    supported_drivers["ESRI Shapefile"] = "rw"

    logger.info(f"Reading GNAF data from {gnaf_dir}...")
    lfs = read_spreadsheets(gnaf_dir, extension, pattern)
    gnaf = pl.concat(lfs.values(), how="diagonal_relaxed")  # type: ignore

    logger.info(f"Reading shapefile from {shapefile_dir}...")
    shapefile = read_shapefile(shapefile_dir, data_config["crs"])

    logger.info("Convert polars LazyFrame to geopandas GeoDataFrame...")
    coords = to_geo_dataframe(gnaf, data_config["crs"])

    logger.info("Joining areas with points...")
    joined_coords = join_coords_with_area(coords, shapefile, strategy)

    logger.info(f"Saving joined data to {output_name}...")
    joined_coords.sink_parquet(output_name)
    logger.info("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Join GNAF dataset with shapefile and cache the result as parquet file"
    )
    parser.add_argument(
        "-g",
        "--gnaf_dir",
        help="Path to a directory of GNAF datasets.",
        type=str,
        default=None,
    )
    parser.add_argument(
        "-s",
        "--shapefile_dir",
        help="Path to a shapefile directory",
        type=str,
        default=None,
    )
    parser.add_argument(
        "-o",
        "--output_name",
        help="Path to an output directory",
        type=str,
        default="gnaf.parquet",
    )
    parser.add_argument(
        "-e", "--extension", help="File extension of the output file", default="parquet"
    )
    parser.add_argument(
        "-c",
        "--config_path",
        help="Path to a configuration file",
        type=str,
        default="configurations.yml",
    )
    parser.add_argument(
        "-p",
        "--pattern",
        help="Regex pattern to match files in the directory to process. Defaults to r'[A-Z]+_ADDRESS_DEFAULT_GEOCODE_psv'",
        type=str,
        default=r"[A-Z]+_ADDRESS_DEFAULT_GEOCODE_psv",
    )
    parser.add_argument(
        "-n",
        "--strategy",
        type=str,
        help="Strategy to handle failed joins, either 'join_nearest' or 'filter'. If not specified, no action is taken.",
        default=None,
    )
    args = parser.parse_args()

    logger.enable("nhs")
    try:
        config_logger(logger_config(args.config_path))
        data_config = data_config(args.config_path)
    except Exception as e:
        logger.critical(
            f"Failed to load configuration at {args.config_path} with exception {e}, terminating..."
        )
        exit(1)

    main(
        gnaf_dir=args.gnaf_dir or data_config["gnaf_path"],
        shapefile_dir=args.shapefile_dir or data_config["shapefile_path"],
        pattern=args.pattern,
        output_name=args.output_name,
        extension=args.extension,
        data_config=data_config,
        strategy=args.strategy,
    )
