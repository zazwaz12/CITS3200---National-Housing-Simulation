from typing import List
import polars as pl

def filter_building_types(lf: pl.LazyFrame, building_column: str = "CODE", building_types: List[str] = []) -> pl.LazyFrame:
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
