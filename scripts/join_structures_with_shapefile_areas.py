import argparse

import geopandas as gpd
import handling
import polars as pl
from fiona.drvsupport import supported_drivers
from loguru import logger

from ..nhs import config as user_config
from ..nhs.data import geography


def main() -> None:
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Process house data and assign SA1 areas."
    )
    parser.add_argument(
        "-c", type=str, help="Path to the configuration YAML file", required=True
    )
    args = parser.parse_args()

    # Load configuration from the provided path
    config_path = args.config
    try:
        config = user_config.read_user_config(config_path)
    except Exception as e:
        logger.critical(f"Failed to load configuration: {e}")
        exit(1)
    # Set the SHAPE_RESTORE_SHX option
    supported_drivers["ESRI Shapefile"] = "rw"

    config_path = "configurations.yaml"
    try:
        config = user_config.read_user_config(config_path)
    except Exception as e:
        logger.critical(f"Failed to load configuration: {e}")
        exit(1)

    shapefile_path = config["shapefile_path"]
    csv_path = config["csv_path"]
    crs = config["crs"]
    # area_column = config["area_column_name"]
    # area_code_column = config["area_code_column_name"]

    # total_rows = join_on_sa1.read_shapefile(shapefile_path)
    houses_df = handling.read_csv(csv_path)

    pnts_gdf = geography.prepare_points(houses_df, crs)
    map_data = gpd.read_file(shapefile_path).to_crs(pnts_gdf.crs)  # type: ignore

    num_cores = config.get("num_cores", 4)  # Default to 4 if not specified
    final_result = geography.parallel_process(pnts_gdf, map_data, num_cores)

    if (
        final_result["area"].is_null().any()
        or final_result["area_code"].is_null().any()
    ):
        logger.warning(
            "Some points were not assigned to an SA1 area even after nearest join. Please check your data."  # noqa: E501
        )

    houses_with_areas = houses_df.join(final_result, on=["x", "y"], how="left")

    output_csv_path = config["output_csv_path"]
    houses_with_areas.collect().write_csv(output_csv_path)
    logger.info(f"CSV file saved to: {output_csv_path}")

    missing_area_count = final_result["area"].is_null().sum()
    logger.info(f"Number of missing values in the 'area' column: {missing_area_count}")

    # Check if random distribution is enabled in the config
    if config.get("enable_random_distribution", False):
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
    else:
        logger.info("Random distribution skipped as per configuration.")


if __name__ == "__main__":
    main()
