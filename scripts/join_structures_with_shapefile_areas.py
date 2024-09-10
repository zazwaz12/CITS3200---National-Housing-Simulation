import argparse

import geopandas as gpd
import polars as pl
from fiona.drvsupport import supported_drivers
from loguru import logger

from .context import nhs

config = nhs.config
geography = nhs.data.geography
read_csv = nhs.data.read_csv



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

    num_cores = simulation_config["num_cores"]
    final_result = geography.parallel_process(gdf, map_data, num_cores)

    if (
        final_result["area"].is_null().any()
        or final_result["area_code"].is_null().any()
    ):
        logger.warning(
            "Some points were not assigned to an SA1 area even after nearest join. Please check your data."  # noqa: E501
        )

    houses_with_areas = houses_df.join(final_result, on=["x", "y"], how="left")

    output_csv_path = data_config["output_csv_path"]
    houses_with_areas.collect().write_csv(output_csv_path)
    logger.info(f"CSV file saved to: {output_csv_path}")

    missing_area_count = final_result["area"].is_null().sum()
    logger.info(f"Number of missing values in the 'area' column: {missing_area_count}")

    if not simulation_config["enable_random_distribution"]:
        logger.info("Random distribution skipped as per configuration.")
        return

    area_codes = houses_with_areas["area_code"].unique().collect.to_list()[:4]
    result_dfs = []

    for area_code in area_codes:
        logger.info(f"Running random distribution for area code: {area_code}")
        area_df = houses_with_areas.filter(pl.col("area_code") == area_code)
        result_df = geography.random_distribution(
            area_df.collect(), num_iterations=config.get("num_iterations", 10)
        )
        result_dfs.append(result_df)

    final_result_df = pl.concat(result_dfs)

    # Check if visualization is enabled in the config
    if config.get("enable_visualization", False):
        geography.visualize_results(final_result_df, area_codes)
        logger.info("Visualization completed.")
    else:
        logger.info("Visualization skipped as per configuration.")

    logger.info("Random distribution completed.")
    logger.info(final_result_df.head(20))


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
