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
    load_gnaf_files_by_states,
    filter_and_join_gnaf_frames,
    filter_sa1_regions,
    filter_gnaf_cache,
)
from nhs.logging import config_logger
from nhs.utils import log_time





def write_csv_in_chunks(lf: pl.LazyFrame, output_path: str, chunk_size: int = 50000):
    """
    Write a LazyFrame to a CSV file in chunks.

    This function writes the data from the provided LazyFrame to a CSV file in chunks, 
    to handle large datasets efficiently by avoiding memory issues during writing.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing the data to be written.
    output_path : str
        The path to the output CSV file.
    chunk_size : int, optional
        The number of rows to write in each chunk. Default is 50000.

    Notes
    -----
    The first chunk will overwrite the existing file, while subsequent chunks 
    will append to the file.
    """
    first_chunk = True  
    for chunk in lf.collect(streaming=True).iter_slices(chunk_size):
        if first_chunk:
            chunk.write_csv(output_path) 
            first_chunk = False
        else:
            with open(output_path, "a") as f:
                f.write(chunk.write_csv())  

    logger.info(f"Data successfully written to {output_path} in chunks of {chunk_size} rows.")





def join_gnaf_with_shapefile(
    gnaf_dir: str,
    shapefile_dir: str,
    data_config: dict,
    strategy: Literal["join_nearest", "filter"] | None = None,
    states: list[str] = [],  
    building_types: list[str] = [],  
    postcodes: list[int] = [],  
    region_codes: list[str] = [], 
    sa2_codes: list[str] = [] 
) -> pl.LazyFrame:
    with log_time():
        logger.info(f"Reading GNAF data from {gnaf_dir}...")
        default_geocode_lf, address_detail_lf = load_gnaf_files_by_states(gnaf_dir, states)

    logger.info("Filtering GNAF data before spatial join...")
    if building_types or postcodes:
        geocode_lfs = filter_and_join_gnaf_frames(
            default_geocode_lf, address_detail_lf, building_types, postcodes
        )
    else:
        geocode_lfs = default_geocode_lf

    with log_time():
        logger.info(f"Reading shapefile from {data_config['shapefile_path']}")
        area_polygons = read_shapefile(shapefile_dir, data_config["crs"])

    with log_time():
        logger.info("Converting GNAF addresses to GeoDataFrame...")
        house_coords = to_geo_dataframe(geocode_lfs, data_config["crs"])

    with log_time():
        logger.info("Joining coordinates with area polygons...")
        joined_coords = join_coords_with_area(house_coords, area_polygons, strategy)

    with log_time():
        logger.info("Applying SA1 and SA2 filters to joined data...")
        joined_coords = filter_sa1_regions(
            joined_coords,
            region_codes=region_codes,  
            sa2_codes=sa2_codes         
        )

    return joined_coords


def main(
    gnaf_dir: str,
    gnaf_cache: str,
    shapefile_dir: str,
    census_dir: str,
    census_pattern: str,
    output_path: str,
    data_config: dict,
    filter_config: dict, 
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
            gnaf_dir, shapefile_dir, data_config, strategy,
            states=filter_config["states"], 
            building_types=filter_config["building_types"], 
            postcodes=filter_config["postcodes"], 
            region_codes=filter_config["region_codes"],  
            sa2_codes=filter_config["sa2_codes"]  
        )
    else:
        logger.info(f"Reading GNAF cache from {gnaf_cache}...")
        joined_coords = read_parquet(gnaf_cache)

        # Apply filtering to the cached GNAF data
        logger.info("Applying filters to cached GNAF data...")
        joined_coords = filter_gnaf_cache(
            joined_coords,
            states=filter_config["states"], 
            region_codes=filter_config["region_codes"],  
            sa2_codes=filter_config["sa2_codes"], 
            flat_type_codes=filter_config["building_types"], 
            postcodes=filter_config["postcodes"]  
        )
        
    with log_time():
        logger.info(f"Reading census data from {census_dir}...")
        logger.disable("nhs")
        census_lfs = read_spreadsheets(census_dir, "parquet", census_pattern)
        logger.enable("nhs")
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

        write_csv_in_chunks(allocated, output_path, chunk_size=10000)

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
        filter_config = config.filter_config(args.config_path) 
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
        shapefile_dir=args.shapefile_dir or data_config["shapefile_path"],
        census_dir=args.census_dir or data_config["census_path"],
        census_pattern=args.census_pattern,
        output_path=(
            args.output_path or os.path.join(data_config["output_path"], "allocated.csv")
        ),
        strategy=args.strategy,
        data_config=data_config,
        simulation_config=simulation_config,
        filter_config=filter_config 
    )




