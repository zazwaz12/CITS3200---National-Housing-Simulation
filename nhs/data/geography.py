import geopandas as gpd  # type: ignore
import matplotlib.pyplot as plt
import polars as pl
from pathos.multiprocessing import ProcessingPool as Pool  # type: ignore
from pyproj import CRS
from loguru import logger


def to_geo_dataframe(
    lf: pl.LazyFrame,
    crs: str,
    longitude_col: str = "LONGITUDE",
    latitude_col: str = "LATITUDE",
) -> gpd.GeoDataFrame:
    """
    Transform point data into a GeoDataFrame with a specified coordinate reference system.

    Parameters
    ----------
    lf : pl.LazyFrame
        A Polars LazyFrame containing two columns `longitude_col` and `latitude_col`
        which represent geographic coordinates of the points in the data.
    crs : str
        The coordinate reference system (CRS) in which the output geospatial data
        should be projected. The CRS is provided as an EPSG code (we use EPSG: 7844)
        as in line with ABS standard. This defines how the spatial data will be
        interpreted in terms of location, scale, and projection.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame with points data transformed into the specified coordinate
        reference system (CRS). The output data will have a geometry column with
        Point objects and be projected to the specified CRS.

    Notes
    -----
    - CRS (Coordinate Reference System) is needed to define how spatial data (like
      longitude and latitude) relates to the Earth's surface, e.g. "EPSG:7844".
    """
    df = lf.collect().to_pandas()
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[longitude_col], df[latitude_col]),  # type: ignore
        crs=CRS.from_string(crs),
    )


def join_areas_with_points(
    area_data: gpd.GeoDataFrame,
    points: gpd.GeoDataFrame,
    area_column: str = "SA2_NAME21",
    area_code_column: str = "SA1_CODE21",
) -> pl.LazyFrame:
    """
    Spatially join point data with spatial areas based on their geographic coordinates.

    Parameters
    ----------
    area_data : gpd.GeoDataFrame
        A GeoPandas DataFrame containing spatial polygon data that represents areas
        (e.g., districts or regions), including geometry (polygons) and area-specific
        attributes such as `area_column` and `area_code_column`, which represent area
        names and codes.
    points : gpd.GeoDataFrame
        A GeoPandas GeoDataFrame containing point data (e.g., latitude and longitude)
        to join with the corresponding areas in `area_data`. This should contain a
        `'geometry'` column representing points.

    Returns
    -------
    pl.LazyFrame
        A Polars LazyFrame with the point data from `points`, joined with the
        appropriate area data from `area_data`. The output includes the original
        coordinates `('LONGITUDE', 'LATITUDE')` and the corresponding area names
        `area_column` and area codes `area_code_column`.

    Notes
    -----
    - If any points cannot be matched with an area (i.e., points that do not fall
      within any polygon), a nearest neighbor spatial join is performed to assign
      the closest area.
    - The final result includes only the 'LONGITUDE', 'LATITUDE' coordinates and the corresponding
      area information.
    """

    pnts_with_area: gpd.GeoDataFrame = gpd.sjoin(points, area_data, how="left", predicate="within")  # type: ignore
    missing_points = pnts_with_area[pnts_with_area[area_column].isnull()]  # type: ignore

    # Perform a nearest join for points that couldn't be attributed to areas
    if not missing_points.empty:  # type: ignore
        logger.warning(
            "Some residences couldn't be attributed to areas. Projection may not be accurate for all points."
        )
        nearest_join = gpd.sjoin_nearest(
            missing_points[["geometry"]], area_data, how="left"  # type: ignore
        )
        pnts_with_area.loc[missing_points.index, area_column] = nearest_join[  # type: ignore
            area_column
        ]
        pnts_with_area.loc[missing_points.index, area_code_column] = nearest_join[  # type: ignore
            area_code_column
        ]

    result = pnts_with_area[["LONGITUDE", "LATITUDE", area_column, area_code_column]]  # type: ignore
    result = result.rename(columns={area_column: "area", area_code_column: "area_code"})  # type: ignore
    # Convert to dict first to ensure compatibility with Polars
    return pl.LazyFrame(result)


def shuffle_coordinates(
    coordinates: list[tuple[float, float]], seed: int
) -> list[tuple[float, float]]:
    """
    Shuffle a list of (x, y) coordinate pairs.

    Parameters
    ----------
    coordinates : list[tuple[float, float]]
        List of coordinate pairs to shuffle.
    seed : int
        Seed for the random number generator to ensure reproducibility.

    Returns
    -------
    list[tuple[float, float]]
        Shuffled list of coordinate pairs.
    """
    return (
        pl.DataFrame(
            {"x": [c[0] for c in coordinates], "y": [c[1] for c in coordinates]}
        )
        .sample(fraction=1, seed=seed)
        .to_numpy()
        .tolist()
    )


def assign_coordinates(
    df: pl.LazyFrame, area_code: str, coords: list[tuple[float, float]]
) -> pl.LazyFrame:
    """
    Assign coordinates to the dataframe for a specific `area_code`.

    Parameters
    ----------
    df : pl.LazyFrame
        Input LazyFrame containing 'LONGITUDE', 'LATITUDE', and 'area_code' columns.
    area_code : str
        The area code to which shuffled coordinates should be assigned.
    coords: list[tuple[float, float]]
        List of coordinate pairs.

    Returns
    -------
    pl.LazyFrame
        Dataframe with coordinates assigned for the specified `area_code`.
    """

    return df.with_columns(
        [
            pl.when(pl.col("area_code") == area_code)
            .then(pl.Series([coord[0] for coord in coords]))
            .otherwise(pl.col("x"))
            .alias("x"),
            pl.when(pl.col("area_code") == area_code)
            .then(pl.Series([coord[1] for coord in coords]))
            .otherwise(pl.col("y"))
            .alias("y"),
        ]
    )


def random_distribution(
    df: pl.DataFrame, num_iterations: int = 10, seed: int = 42
) -> pl.DataFrame:
    """
    Randomly shuffle lat-long pairs within their respective areas, maintaining uniqueness.

    This function shuffles coordinates within each area_code, ensuring that the number of unique
    coordinates remains the same before and after shuffling. It uses a seed for reproducibility.

    This was used in testing for Schelling's model of segregation, which worked with very limited
    success (something like 30minutes to move 5 people).

    Remains here in case of future expansion into distributions.

    Args:
        df (pl.DataFrame): Input dataframe containing 'x', 'y', and 'area_code' columns.
        num_iterations (int, optional): Number of shuffling iterations. Defaults to 10.
        seed (int, optional): Seed for the random number generator. Defaults to 42.

    Returns:
        pl.DataFrame: Dataframe with shuffled coordinates, maintaining original uniqueness within each area_code.
    """  # noqa: E501
    model_df = df.clone()
    model_df = model_df.with_columns(
        [pl.col("y").alias("original_lat"), pl.col("x").alias("original_lon")]
    )

    area_codes = model_df["area_code"].unique().to_list()

    for _ in range(num_iterations):
        for area_code in area_codes:
            area_df = model_df.filter(pl.col("area_code") == area_code)
            unique_coords = area_df[["x", "y"]].unique().to_numpy().tolist()
            shuffled_coords = shuffle_coordinates(unique_coords, seed)

            # Create a mapping from original to shuffled coordinates
            coord_map = dict(zip(unique_coords, shuffled_coords))

            # Apply the mapping to maintain uniqueness
            model_df = model_df.with_columns(
                [
                    pl.when(pl.col("area_code") == area_code)
                    .then(
                        pl.struct(["x", "y"]).map_elements(
                            lambda c: coord_map.get((c["x"], c["y"]), (c["x"], c["y"]))
                        )
                    )
                    .otherwise(pl.struct(["x", "y"]))
                ]
            ).unnest("literal")

    return model_df


def visualize_results(final_result_df: pl.DataFrame, area_codes: list[str]) -> None:
    """
    Visualize the original and final positions of points for given area codes.

    This function creates a side-by-side scatter plot comparison for each area code,
    showing the original geographic coordinates and their corresponding randomized
    positions.

    Parameters:
    -----------
    final_result_df : pl.DataFrame
        A Polars DataFrame containing the data with columns 'area_code', 'original_lon',
        'original_lat', 'x', 'y', and 'attribute'.
    area_codes : List[str]
        A list of area codes for which to visualize the results.

    Returns:
    --------
    None
        Displays plots but does not return any value.
    """  # noqa: E501
    for area_code in area_codes:
        area_result_df = final_result_df.filter(pl.col("area_code") == area_code)

        plt.figure(figsize=(12, 6))  # type: ignore
        plt.subplot(1, 2, 1)  # type: ignore
        plt.scatter(  # type: ignore
            area_result_df["original_lon"],
            area_result_df["original_lat"],
            c=area_result_df["attribute"].to_numpy(),
            cmap="tab10",
            s=50,
            alpha=0.7,
        )
        plt.title(f"Original Positions for Area Code: {area_code}")  # type: ignore
        plt.xlabel("Original Longitude")  # type: ignore
        plt.ylabel("Original Latitude")  # type: ignore

        plt.subplot(1, 2, 2)  # type: ignore
        plt.scatter(  # type: ignore
            area_result_df["x"],
            area_result_df["y"],
            c=area_result_df["attribute"].to_numpy(),
            cmap="tab10",
            s=50,
            alpha=0.7,
        )
        plt.title(  # type: ignore
            f"Final Positions after Random Distribution for Area Code: {area_code}"
        )
        plt.xlabel("Final Longitude")
        plt.ylabel("Final Latitude")

        plt.tight_layout()
        plt.show()
