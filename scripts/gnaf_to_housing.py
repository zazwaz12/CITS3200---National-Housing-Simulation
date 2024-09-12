import argparse
from context import nhs
import geopandas as gpd # type: ignore
from loguru import logger

geography = nhs.data.geography
read_csv = nhs.data.read_csv
filter_sa1_regions = nhs.data.filter_sa1_regions

# - INPUTS
#     - shapefile
#     - GNAF - (x, y)
#     - x,y - area name - area code
 
# 1. data loading
#     - load GNAF
# 2. filter by SA1 regions (**show that we can select all SA1 as well**)
#     - filter to some arbitrary SA1
# 3. change census col names more readable
#     - change column names so we can read it
# 4. geopandas stuff
#     - input GNAF and shape file
#     - output (x, y) : (area_name/code)

parse_config = nhs.config.parse_config

def main(configuration:str, sa1_codes:list[str]):
    config = parse_config(configuration)

    houses_df = read_csv(config["data"]["gnaf_path"])
    if (houses_df is None):
        logger.critical("Failed to load GNAF data")
        exit(1)

    gdf = geography.to_geo_dataframe(houses_df, config["data"]["crs"])
    map_data: gpd.GeoDataFrame = gpd.read_file(config["data"]["shapefile_path"]).to_crs(gdf.crs)
    filter_sa1_regions(houses_df, sa1_codes)
    gnaf = config["data"]["gnaf_path"]
    shapefile = config["data"]["shapefile_path"]

    num_cores = config["simulation"]["num_cores"]
    output = geography.parallel_process(gdf, map_data, num_cores)

    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert GNAF data to housing data")
    parser.add_argument("configuration", type=str, help="Path to the configuration yml file", default="configurations.yml")
    parser.add_argument("sa1_codes", type=str, help="List of SA1 area codes, separated by comma to output those regions", default="10901117612,10901117207")

    args = parser.parse_args()

    sa1_code_list = args.sa1_codes.split(",")

    main(args.configuration, sa1_code_list)