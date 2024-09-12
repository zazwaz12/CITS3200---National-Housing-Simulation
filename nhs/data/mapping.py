import polars as pl

# The first digit of a SA1 code repersents the sate or territory
# Define a dictionary mapping the first digit of the SA1 code to the respective state

state_mapping = {
    "1": "NSW",  # New South Wales
    "2": "VIC",  # Victoria
    "3": "QLD",  # Queensland
    "4": "SA",   # South Australia
    "5": "WA",   # Western Australia
    "6": "TAS",  # Tasmania
    "7": "NT",   # Northern Territory
    "8": "ACT"   # Australian Capital Territory
}

def map_state_to_sa1_codes(lf: pl.LazyFrame, state: str, sa1_column: str = "SA1_CODE_2021") -> list:
    """
    Returns all SA1 area codes corresponding to the given state, optimizing for large datasets.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing the SA1 area codes.
    state : str
        The name of the state to filter for, such as "NSW", "VIC".
    sa1_column : str, optional
        The name of the column containing the SA1 area codes, defaults to "SA1_CODE_2021".

    Returns
    -------
    List[str]
        A list containing all SA1 area codes for the given state.
    """
    # Get the first digit corresponding to the given state
    state_digit = next(key for key, value in state_mapping.items() if value == state)

    # Select only the necessary column to reduce memory usage
    filtered_lf = lf.select([sa1_column]).filter(pl.col(sa1_column).str.slice(0, 1) == state_digit)
    
    # Perform the computation and return the result
    return filtered_lf.collect().to_series().to_list()
