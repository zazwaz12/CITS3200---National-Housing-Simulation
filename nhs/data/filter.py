from typing import List

import polars as pl


<<<<<<< HEAD
def filter_sa1_regions(lf: pl.LazyFrame, region_codes: List[str], sa1_column: str = "SA1_CODE_2021") -> pl.LazyFrame:
=======
def filter_sa1_regions(
    lf: pl.LazyFrame, sa1_column: str = "SA1_CODE_2021", region_codes: List[str] = []
) -> pl.LazyFrame:
>>>>>>> e4a4ed74fb5d607802d3053f31307fc74056a516
    """
    Filters the LazyFrame to include only rows with specified SA1 area codes.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing SA1 region codes and data to be filtered.
    region_codes : List[str]
        A list of SA1 area codes to filter for.
    SA1_column : str
        The name of the column containing the SA1 area codes. Defaults to "SA1_CODE_2021".

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only rows with the specified SA1 area codes.
    """
<<<<<<< HEAD
    return lf.filter(pl.col(sa1_column).is_in(region_codes))



=======
    if not region_codes:
        return lf  # If no region codes are provided, return the original LazyFrame

    return lf.filter(pl.col(sa1_column).is_in(region_codes))
>>>>>>> e4a4ed74fb5d607802d3053f31307fc74056a516
