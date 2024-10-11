#!/usr/bin/env python3
import argparse
import os
import sys

from loguru import logger

sys.path.append(".")
sys.path.append("..")

yaml_content = """
# Logging configuration settings
# See https://loguru.readthedocs.io/en/stable/api/logger.html
logging:
  # Path and naming convention for logs (including timestamp)
  log_file: "stdout"
  # Format of the log messages
  format: "<green>{time:YYYY-MM-DD at HH:mm:ss}</green> <level>{level: <8} {message}</level>"
  # TRUE for detailed backtrace logging for debugging
  backtrace: true
  # Extra diagnostic information
  diagnose: true
  # Log level; options: ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
  level: "INFO"

# Paths to data files and resources
data:
  # Path to the census data files
  census_path: "your own path/Desktop/DataFiles/AppStaging/census/2021 Census GCP All Geographies for AUS/SA1/AUS"
  # Path folder containing the GNAF (Geocoded National Address File) data
  gnaf_path: "your own path to the GNAF data directory/Desktop/DataFiles/AppStaging/Standard/"
  # File of GNAF data already joined with the shapefile
  # Recommended to use as converting GNAF to GeoDataFrames is time-consuming
  gnaf_cache_file: "your own path to the gnaf_cache.parquet file/Desktop/DataFiles/FilesOut/gnaf_cache.parquet"
  # Path to the shapefile containing region codes and polygon of the area
  shapefile_path: "your own path to the shapefile directory/Desktop/DataFiles/FilesIn/SA1_2021_AUST_SHP_GDA2020/SA1_2021_AUST_GDA2020.shp"
  # Path of the output CSV
  output_path: "your own path to the output CSV directory/Desktop/DataFiles/FilesOut/"
  # Coordinate Reference System (CRS) for spatial data
  # Use CRS 7844 - specified in the ABS 2021 datapack
  # 'EPSG:7844' is a CRS in the European Petroleum Survey Group system for defining geospatial coordinates
  # SEE ALSO WGS84 (EPSG:4326) - represents latitude and longitude on spherical Earth
  crs: "EPSG:7844"
  # The column in your data that contains SA2(!) area names
  sa2_area_column: "SA2_NAME21"
  # The column in your data that contains SA1(!) area codes
  sa1_area_code_column: "SA1_CODE21"

# Filter settings (for filtering the GNAF and census data)
filters:
  # States to filter (e.g., QLD, NSW)
  states: ["WA"]
  # List of SA1 region codes to filter
  region_codes: []
  # List of SA2 region codes to filter
  sa2_codes: []
  # List of building type codes to filter
  building_types: []
  # List of postcodes to filter
  postcodes: []

# Simulation settings
simulation:
  # List of census features to be used in the simulation
  census_features:
    - "Age_0_4_yr_M"
    - "Age_0_4_yr_F"
    - "Age_0_4_yr_P"
    - "Age_5_14_yr_M"
    - "Age_5_14_yr_F"
    - "Age_5_14_yr_P"
    - "Age_15_19_yr_M"
  # FALSE to skip visualization
  enable_visualization: false
"""


def main(args):
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(os.curdir)), "configurations.yml"
    )
    file_name = config_file_path.split("\\")[-1]
    try:
        if not os.path.exists(config_file_path) or args.overwrite:
            with open(config_file_path, "w") as file:
                file.write(yaml_content.strip())
            if args.overwrite:
                logger.info(f"{file_name} has been overwritten.")
            else:
                logger.info(f"{file_name} has been created.")
        else:
            logger.info(
                f"{file_name} already exists. Use --overwrite to overwrite the file."
            )
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    # Argument parser for command-line flags
    parser = argparse.ArgumentParser(
        description="Create or overwrite a YAML configuration file."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the existing YAML configuration file if it exists.",
        default=None,
    )

    args = parser.parse_args()
    main(args)
