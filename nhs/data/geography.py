from typing import Any, Literal
import geopandas as gpd  # type: ignore
import pandas as pd
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

def _failed_join_strategy(
        unmapped_coords: pd.Series, # type: ignore
        coords_with_area: gpd.GeoDataFrame,
        strategy: Any,
):
    """
    Apply strategy to handle coordinates that could not be attributed to an area polygon.
    """
    match strategy:
        case "join_nearest":
            # Perform a nearest join for coordinates that couldn't be attributed to areas
            coords_with_area[unmapped_coords] = gpd.sjoin_nearest(
                coords[unmapped_coords], area_polygons, how="left"  # type: ignore
            )
        case "filter":
            coords_with_area = coords_with_area[~unmapped_coords] # type: ignore
        case None:
            pass
        case _:
            logger.warning(f"Invalid strategy {strategy} specified for join_coords_with_area, skipping...")
        
    return coords_with_area


def join_coords_with_area(
    coords: gpd.GeoDataFrame,
    area_polygons: gpd.GeoDataFrame,
    failed_join_strategy: Literal["join_nearest", "filter"] | None = None
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
    failed_join_strategy : Literal["join_nearest", "filter"], optional
        Strategy to handle coordinates that could not be attributed to an area polygon.
        If "join_nearest", the coordinates are assigned to the nearest area polygon.
        If "filter", the coordinates are filtered out. Defaults to None.

    Returns
    -------
    pl.LazyFrame
        A Polars LazyFrame spatially joined from `coords` and `area_polygons`. The
        `"geometry"` column is converted to the "Well-Known Text" (WKT) format in the
        `"geometry_wkt"` column for compatibility with Polars and `"geometry"` column
        is dropped.
    """
    coords_with_area: gpd.GeoDataFrame = gpd.sjoin(coords, area_polygons, how="left", predicate="within")  # type: ignore

    area_polygons_only_column = area_polygons.columns.difference(coords.columns)  # type: ignore
    # unmapped coords will have rows with all null values for area polygon columns
    unmapped_coords = coords_with_area[area_polygons_only_column].isnull().all(axis=1)  # type: ignore
    if not unmapped_coords.all():  # type: ignore
        logger.warning(
            f"{len(coords_with_area[unmapped_coords])} coordinates couldn't be attributed to areas. {"Assigning coordinates using strategy " + failed_join_strategy if failed_join_strategy else ""}" # type: ignore
        )

    coords_with_area = _failed_join_strategy(unmapped_coords, coords_with_area, failed_join_strategy) # type: ignore

    # Convert to Pandas Dataframe to avoid "geometry" warnings
    output = pd.DataFrame(coords_with_area)
    output["geometry"] = coords_with_area["geometry"].apply(lambda x: x.wkt)  # type: ignore
    return pl.LazyFrame(output)
