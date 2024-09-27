import os
from typing import Callable, Literal

import polars as pl

from .handling import read_spreadsheets


def load_gnaf_files_by_states(
    gnaf_path: str, states: list[str] = []
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Load and filter ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL parquet files for the specified states
    from the given GNAF directory, and return them as LazyFrames.

    Parameters
    ----------
    gnaf_path : str
        The directory path where the GNAF parquet files are stored.
    states : list of str, optional
        A list of state/territory abbreviations (e.g., ["WA", "ACT"]). If not provided or an empty list,
        data for all states will be included. Default is an empty list.

    Returns
    -------
    tuple of (pl.LazyFrame, pl.LazyFrame)
        default_geocode_lf : pl.LazyFrame
            The merged ADDRESS_DEFAULT_GEOCODE data for the specified states, with an added "STATE" column.
        address_detail_lf : pl.LazyFrame
            The merged ADDRESS_DETAIL data for the specified states.

    Notes
    -----
    The function expects the files in the directory to follow this naming convention:
    - {state_name}_ADDRESS_DEFAULT_GEOCODE_psv.parquet: Contains the default geocode data for the state.
    - {state_name}_ADDRESS_DETAIL_psv.parquet: Contains the detailed address data for the state.

    Example file names:
    - "ACT_ADDRESS_DEFAULT_GEOCODE_psv.parquet"
    - "ACT_ADDRESS_DETAIL_psv.parquet"

    Expected columns:
    - `ADDRESS_DEFAULT_GEOCODE` files should include the following columns:
        - "ADDRESS_DEFAULT_GEOCODE_PID"
        - "DATE_CREATED"
        - "DATE_RETIRED"
        - "ADDRESS_DETAIL_PID"
        - "GEOCODE_TYPE_CODE"
        - "LATITUDE"
        - "LONGITUDE"
        - "STATE"
    - `ADDRESS_DETAIL` files should include the following columns:
        - "ADDRESS_DETAIL_PID"
        - "FLAT_TYPE_CODE"
        - "POSTCODE"
    """
    # List of all state codes
    all_state_codes = ["NSW", "ACT", "VIC", "QLD", "SA", "WA", "TAS", "NT", "OT"]

    # If the states list is empty, use all states
    if not states:
        states = all_state_codes

    # Use read_spreadsheets to load the files
    all_files = read_spreadsheets(gnaf_path, "parquet")

    # Filter out ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL files
    geocode_files = {
        key: lf.with_columns(pl.lit(state_name).alias("STATE"))
        for state_name in states
        for key, lf in all_files.items()
        if f"{state_name}_ADDRESS_DEFAULT_GEOCODE" in key
        and isinstance(lf, pl.LazyFrame)
    }

    detail_files = {
        key: lf.select(["ADDRESS_DETAIL_PID", "FLAT_TYPE_CODE", "POSTCODE"])
        for state_name in states
        for key, lf in all_files.items()
        if f"{state_name}_ADDRESS_DETAIL" in key and isinstance(lf, pl.LazyFrame)
    }

    # Concatenate all LazyFrames
    default_geocode_lf = (
        pl.concat(list(geocode_files.values())) if geocode_files else pl.LazyFrame()
    )
    address_detail_lf = (
        pl.concat(list(detail_files.values())) if detail_files else pl.LazyFrame()
    )

    return default_geocode_lf, address_detail_lf


def filter_and_join_gnaf_frames(
    default_geocode_lf: pl.LazyFrame,
    address_detail_lf: pl.LazyFrame,
    building_types: list[str] = [],
    postcodes: list[int] = [],
) -> pl.LazyFrame:
    """
    Filters and joins GNAF ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL LazyFrames based on optional building types
    and postcodes, and returns the resulting LazyFrame.

    Parameters
    ----------
    default_geocode_lf : pl.LazyFrame
        LazyFrame containing ADDRESS_DEFAULT_GEOCODE data.
    address_detail_lf : pl.LazyFrame
        LazyFrame containing ADDRESS_DETAIL data.
    building_types : list of str, optional
        Building types to filter by in the "FLAT_TYPE_CODE" column of `address_detail_lf`
        (e.g., ["flat", "unit"]). If empty, no filtering is applied.
    postcodes : list of int, optional
        Postcodes to filter by in the "POSTCODE" column of `address_detail_lf`.
        If empty, no filtering is applied.

    Returns
    -------
    pl.LazyFrame
        Joined LazyFrame with applied filters, if specified.

    Example
    -------
    >>> default_geocode_lf, address_detail_lf = load_gnaf_files_by_states("/path/to/gnaf", ["ACT", "NSW"])
    >>> filtered_lf = filter_and_join_gnaf_frames(
    ...     default_geocode_lf,
    ...     address_detail_lf,
    ...     building_types=["flat", "unit"],
    ...     postcodes=[2000, 2600]
    ... )
    >>> filtered_lf.collect()
    """
    # Replace null values in FLAT_TYPE_CODE with "unknown" and convert all values to lowercase
    address_detail_lf = address_detail_lf.with_columns(
        pl.col("FLAT_TYPE_CODE")
        .fill_null("unknown")  # Replace all null values with "unknown"
        .str.to_lowercase()  # Convert all values to lowercase
    )

    # Apply optional filtering based on building_types
    if building_types:  # Check if the list is not empty
        address_detail_lf = address_detail_lf.filter(
            pl.col("FLAT_TYPE_CODE").is_in(building_types)
        )

    # Apply optional filtering based on postcodes
    if postcodes:  # Check if the list is not empty
        address_detail_lf = address_detail_lf.filter(
            pl.col("POSTCODE").is_in(postcodes)
        )

    # Join using "ADDRESS_DETAIL_PID" as the key
    joined_lf = default_geocode_lf.join(
        address_detail_lf, on="ADDRESS_DETAIL_PID", how="inner"
    )

    return joined_lf


def filter_sa1_regions(
    lf: pl.LazyFrame, region_codes: list[int] = [], sa1_column: str = "SA1_CODE21"
) -> pl.LazyFrame:
    """
    Filters the LazyFrame to include only rows with specified SA1 area codes.
    If no region_codes are provided, return the original LazyFrame without filtering.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing SA1 region codes and data to be filtered.
    region_codes : List[int], optional
        A list of SA1 area codes to filter for. If empty, no filtering will be applied.
    sa1_column : str, optional
        The name of the column containing the SA1 area codes. Defaults to "SA1_CODE21".

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only rows with the specified SA1 area codes.
        If no region_codes are specified, returns the original LazyFrame.
    """
    if not region_codes:  # If the region_codes list is empty
        return lf  # Return the original LazyFrame
    return lf.filter(pl.col(sa1_column).is_in(region_codes))




