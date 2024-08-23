from typing import List
import polars as pl

def filter_SA1_region_codes(lf: pl.LazyFrame, SA1_column: str = "SA1_CODE_2021", region_codes: List[str] = []) -> pl.LazyFrame:
    """
    Filters the LazyFrame to include only rows with specified SA1 area codes.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing the census data to be filtered.
    SA1_column : str
        The name of the column containing the SA1 area codes. Defaults to "SA1_CODE_2021".
    region_codes : List[str]
        A list of SA1 area codes to filter for.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only rows with the specified SA1 area codes.
    """

    if not region_codes:
        return lf    # If no region codes are provided for filtering, return the original LazyFrame
    
    return lf.filter(pl.col(SA1_column).is_in(region_codes))
