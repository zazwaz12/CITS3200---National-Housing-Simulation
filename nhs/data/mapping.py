import polars as pl
from typing import List


def map_state_to_sa1_codes(lf: pl.LazyFrame, states: List[str], sa1_column: str = "SA1_CODE_2021") -> pl.LazyFrame:
    """
    Filters the LazyFrame to include only rows corresponding to the given states, while keeping all columns.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing the SA1 area codes and other columns.
    states : List[str]
        A list of states to filter for, such as ["NSW", "VIC"].
    sa1_column : str, optional
        The name of the column containing the SA1 area codes, defaults to "SA1_CODE_2021".

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing all columns filtered by the given states.
    """

    # State mapping based on the first digit of SA1 code
    state_mapping = {
        "1": "NSW",  # New South Wales
        "2": "VIC",  # Victoria
        "3": "QLD",  # Queensland
        "4": "SA",   # South Australia
        "5": "WA",   # Western Australia
        "6": "TAS",  # Tasmania
        "7": "NT",   # Northern Territory
        "8": "ACT",  # Australian Capital Territory
        "9": "Other Territories"  # Other Territories
    }

    # Handle empty or invalid states
    if not states or not all(state in state_mapping.values() for state in states):
        return lf.filter(pl.lit(False))  # Return an empty LazyFrame

    # Get the digits corresponding to the given states
    state_digits = [key for key, value in state_mapping.items() if value in states]

    # Filter the LazyFrame based on the state digits, keeping all columns
    filtered_lf = lf.filter(pl.col(sa1_column).cast(pl.Utf8).str.slice(0, 1).is_in(state_digits))
    
    return filtered_lf



