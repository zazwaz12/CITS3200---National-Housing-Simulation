import geopandas as gpd  # type: ignore
import matplotlib.pyplot as plt
import polars as pl
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


def read_shapefile(shapefile_dir: str, crs: str) -> gpd.GeoDataFrame:
    """
    Read a shapefile as a GeoDataFrame with a specified coordinate reference system.

    Parameters
    ----------
    shapefile_dir : str
        The path to a folder containing the shapefile data to be read.
    crs : str
        The coordinate reference system (CRS) in which the output geospatial data
        should be projected. The CRS is provided as an EPSG code (we use EPSG: 7844)
        as in line with ABS standard. This defines how the spatial data will be
        interpreted in terms of location, scale, and projection.
    """
    return gpd.read_file(shapefile_dir).to_crs(CRS.from_string(crs))  # type: ignore


def join_coords_with_area(
    coords: gpd.GeoDataFrame,
    area_polygons: gpd.GeoDataFrame,
    join_nearest_on_null: bool = False,
) -> pl.LazyFrame:
    """
    Spatially join `coords` with `area_polygons` rows that contain the points in `coords`.

    Spatial join combines two GeoDataFrames based on the spatial relationship between
    the geometries in the two DataFrames. This function joins the points in `coords`
    with the areas in `area_polygons` if the coordinate points in `coords` fall within
    an area polygons in `area_polygons`.

    *All* rows from `coords` are included and duplicated if necessary to match multiple
    areas in `area_polygons`. Rows  in `area_polygons` are only included if a point in
    `coords` falls within the area.

    Parameters
    ----------
    coords : gpd.GeoDataFrame
        A `geopandas.GeoDataFrame` containing a `"geometry"` column with `POINT`
        objects representing the coordinates (longitude, latitude) of the points
        to be joined with the areas in `area_polygons`. Typically created from
        converting a `DataFrame` with longitude and latitude columns to a GeoDataFrame
        using `nhs.data.to_geo_dataframe`.
    area_polygons : gpd.GeoDataFrame
        A GeoPandas DataFrame containing a column with spatial polygon data that
        represents areas (e.g., districts or regions). Typically created from
        reading a shapefile using `nhs.data.read_shapefile`.
    join_nearest_on_null : bool, optional
        Whether to assign coordinates that do not fall within any area to the nearest
        area polygon. Defaults to False.

    Returns
    -------
    pl.LazyFrame
        A Polars LazyFrame with the point data from `coords`, joined with the
        appropriate area data from `area_polygons`. The output includes the original
        coordinates `('LONGITUDE', 'LATITUDE')` and the corresponding area names
        `area_column` and area codes `area_code_column`.
    """
    coords_with_area: gpd.GeoDataFrame = gpd.sjoin(coords, area_polygons, how="left", predicate="within")  # type: ignore

    area_polygons_only_column = area_polygons.columns.difference(coords.columns)  # type: ignore
    # unmapped coords will have rows with all null values for area polygon columns
    unmapped_coords = coords_with_area[area_polygons_only_column].isnull().all(axis=1)  # type: ignore
    if not unmapped_coords.all():  # type: ignore
        logger.warning(
            f"{len(coords_with_area[unmapped_coords])} coordinates couldn't be attributed to areas. {"Assigning coordinates to their nearest area polygon." if join_nearest_on_null else ""}" # type: ignore
        )

    # Perform a nearest join for coordinates that couldn't be attributed to areas
    if join_nearest_on_null:
        coords_with_area[unmapped_coords] = gpd.sjoin_nearest(
            coords[unmapped_coords], area_polygons, how="left"  # type: ignore
        )

    return pl.LazyFrame(coords_with_area)

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
