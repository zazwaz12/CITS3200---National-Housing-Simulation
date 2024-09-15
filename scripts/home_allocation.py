import argparse
import os
from fiona.drvsupport import supported_drivers
import geopandas as gpd
from loguru import logger
import polars as pl
from context import nhs
from typing import Literal

from nhs.config import simulation_config

geography = nhs.data.geography
read_parquet = nhs.data.read_parquet
join_coords_with_area = nhs.data.join_coords_with_area
to_geo_dataframe = nhs.data.to_geo_dataframe
read_shapefile = nhs.data.read_shapefile
join_coords_with_area = nhs.data.join_coords_with_area
join_census_with_coords = nhs.data.join_census_with_coords
randomly_assign_census_features = nhs.data.randomly_assign_census_features
sample_census_feature = nhs.data.sample_census_feature


def main(
    config_path: str,
    gnaf_fname: str,
    census_fname: str,
    out_fname: str,
    strategy: Literal["join_nearest", "filter"] | None,
) -> None:
    # Required for fiona - reads shapefiles
    supported_drivers["ESRI Shapefile"] = "rw"

    try:
        data_config = nhs.config.data_config(config_path)
        logger_config = nhs.config.logger_config(config_path)
        simulation_config = nhs.config.simulation_config(config_path)
    except Exception as e:
        logger.critical(
            f"Failed to load configuration at {config_path} with exception {e}, terminating..."
        )
        exit(1)

    logger.enable("nhs")
    nhs.logging.config_logger(logger_config)

    gnaf_path = os.path.join(data_config["gnaf_path"], gnaf_fname)
    logger.info(f"Reading GNAF data from {gnaf_path}...")
    houses_df = read_parquet(gnaf_path)
    if not isinstance(houses_df, pl.LazyFrame):
        logger.error(f"Failed to load GNAF file at {gnaf_path}, terminating...")
        exit(1)

    census_path = os.path.join(data_config["census_path"], census_fname)
    logger.info(f"Reading census data from {census_path}...")
    census = read_parquet(census_path)
    if not isinstance(census, pl.LazyFrame):
        logger.error(f"Failed to load census file at {census_path}, terminating...")
        exit(1)

    logger.info(f"Reading shapefile from {data_config['shapefile_path']}...")
    area_polygons: gpd.GeoDataFrame = read_shapefile(
        data_config["shapefile_path"], data_config["crs"]
    )

    logger.info("Converting housing dataset to GeoDataFrame...")
    house_coords = to_geo_dataframe(houses_df, data_config["crs"])

    logger.info("Joining coordinates with area polygons...")
    joined_coords = join_coords_with_area(house_coords, area_polygons, strategy)

    out_path = os.path.join(data_config["output_path"], out_fname)
    logger.info(
        f"Randomly assigning census features to GNAF addresses and saving to {out_path}..."
    )
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
