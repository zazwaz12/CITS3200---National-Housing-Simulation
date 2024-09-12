#from nhs.utils.string import placeholder_matches
import polars as pl

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from nhs.utils.string import placeholder_matches



def filter_sa1_regions(
    lf: pl.LazyFrame, region_codes: list[str], sa1_column: str = "SA1_CODE_2021"
) -> pl.LazyFrame:
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
    return lf.filter(pl.col(sa1_column).is_in(region_codes))

def filter_files_selection(
    files: list[str],
    state: str = "*",
    data_type: str = "*"
) -> list[str]:
    """
    Filter files based on state and data type restrictions.

    Parameters
    ----------
    files : List[str]
        List of file names to be filtered.
    state : str, optional
        State filter, default is '*' to include all states.
    data_type : str, optional
        Data type filter, default is '*' to include all data types.

    Returns
    -------
    List[str]
        List of file names that match the given state and data type filters.
    """
    # Create the pattern for matching files based on the state and data type
    pattern = f"Standard/{state}_{data_type}.psv"
    placeholders = ["state", "data_type"]

    # Use the placeholder_matches function to get the relevant file matches
    matches = placeholder_matches(files, pattern, placeholders)
    print(matches)
    # Extract the filenames from the matches
    filtered_files = [file for file in files if any(match for match in matches)]
    
    return filtered_files

# Example usage:
files = [
    "Standard/ACT_ADDRESS_DEFAULT_GEOCODE.psv",
    "Standard/NSW_LOCATION_GEOCODE.psv",
    "Standard/ACT_OTHER.psv",
    "Standard/QLD_ADDRESS.psv"
]

filtered_files = filter_files_selection(files, state="ACT", data_type="ADDRESS_DEFAULT_GEOCODE")
print(filtered_files)

def filter_relevant_column(df: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame:
    """
    Filter columns from a LazyFrame based on the provided column names.

    Parameters
    ----------
    df : pl.LazyFrame
        The Polars LazyFrame to filter.
    columns : list[str]
        List of column names to retain in the LazyFrame.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only the specified columns.
    """
    existing_columns = [col for col in columns if col in df.columns]
    return df.select(existing_columns)
