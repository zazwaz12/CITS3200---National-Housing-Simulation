from typing import List
import polars as pl

def filter_sa1_regions(
    lf: pl.LazyFrame, region_codes: List[str], sa1_column: str = "SA1_CODE_2021"
) -> pl.LazyFrame:
    """
    Filters the LazyFrame to include only rows with specified SA1 area codes.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing SA1 region codes and data to be filtered.
    region_codes : List[str]
        A list of SA1 area codes to filter for.
    sa1_column : str
        The name of the column containing the SA1 area codes. Defaults to "SA1_CODE_2021".

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only rows with the specified SA1 area codes.
    """
    return lf.filter(pl.col(sa1_column).is_in(region_codes))

def filter_building_types(
    lf: pl.LazyFrame, building_column: str = "CODE", building_types: List[str] = []
) -> pl.LazyFrame:
    """
    Filters the LazyFrame to include only rows with specified building types.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing building type codes and data to be filtered.
    building_column : str, optional
        The name of the column containing the building types. Defaults to "CODE".
    building_types : List[str]
        A list of building types to filter for.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only rows with the specified building types.
    """
    if not building_types:
        return lf  # If no building types are provided, return the original LazyFrame
    
    return lf.filter(pl.col(building_column).is_in(building_types))
