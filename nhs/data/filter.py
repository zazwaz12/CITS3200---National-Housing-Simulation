from typing import Literal

import polars as pl
from loguru import logger

from .handling import read_spreadsheets


def load_gnaf_files_by_states(
    gnaf_path: str,
    states: list[
        Literal["NSW", "ACT", "VIC", "QLD", "SA", "WA", "TAS", "NT", "OT"]
    ] = [],
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Load and filter ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL files for the given `states` from `gnaf_path`.

    Parameters
    ----------
    gnaf_path : str
        The directory path where the GNAF parquet files are stored. The files in the directory
        must two types of parquet files with the naming convention:
            - `"{state_name}_ADDRESS_DEFAULT_GEOCODE_psv.parquet"` for data containing the default
              geocode data for the state. e.g. "ACT_ADDRESS_DEFAULT_GEOCODE_psv.parquet"
            - `"{state_name}_ADDRESS_DETAIL_psv.parquet"`: for data containing the detailed address
              data for the state. e.g. "ACT_ADDRESS_DETAIL_psv.parquet"
    states : list of str, optional
        A list of state/territory abbreviations (e.g., ["WA", "ACT"]). If not provided or an empty list,
        data for all states will be included. Default is an empty list.

    Returns
    -------
    tuple of (pl.LazyFrame, pl.LazyFrame)
        A tuple containing two LazyFrames:
            - The merged ADDRESS_DEFAULT_GEOCODE data for the specified states, with an added "STATE" column.
            - The merged ADDRESS_DETAIL data for the specified states.

    Notes
    -----
    Expected columns:
    - `ADDRESS_DEFAULT_GEOCODE` files should include the following columns:
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

    # Temporarily disable logging
    logger.disable("nhs")

    # List of all state codes
    all_state_codes: list[
        Literal["NSW", "ACT", "VIC", "QLD", "SA", "WA", "TAS", "NT", "OT"]
    ] = ["NSW", "ACT", "VIC", "QLD", "SA", "WA", "TAS", "NT", "OT"]

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

    # Re-enable logging
    logger.enable("nhs")

    return default_geocode_lf, address_detail_lf


def filter_and_join_gnaf_frames(
    default_geocode_lf: pl.LazyFrame,
    address_detail_lf: pl.LazyFrame,
    building_types: list[str] = [],
    postcodes: list[int] = [],
) -> pl.LazyFrame:
    """
    Filters and joins ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL LazyFrames on `building_types` and `postcodes`.

    Parameters
    ----------
    default_geocode_lf : pl.LazyFrame
        LazyFrame containing ADDRESS_DEFAULT_GEOCODE data. Must have the columns:
            - "ADDRESS_DETAIL_PID": for joining with `address_detail_lf`.
    address_detail_lf : pl.LazyFrame
        LazyFrame containing ADDRESS_DETAIL data. Must have the column:
            - "ADDRESS_DETAIL_PID": for joining with `default_geocode_lf`.
            - "FLAT_TYPE_CODE": building type information.
            - "POSTCODE": postcode of the address.
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
    lf: pl.LazyFrame,
    region_codes: list[str] = [],
    sa2_codes: list[str] = [],
    sa1_column: str = "SA1_CODE21",
    sa2_column: str = "SA2_CODE21",
) -> pl.LazyFrame:
    """
    Filter `lf` to include only rows with specified SA1 and SA2 area codes.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing the data to be filtered.
    region_codes : list[str], optional
        A list of SA1 area codes to filter for. If empty, no filtering will be applied.
    sa2_codes : list[str], optional
        A list of SA2 area codes to filter for. If empty, no filtering will be applied.
    sa1_column : str, optional
        The name of the column containing the SA1 area codes. Defaults to "SA1_CODE21".
    sa2_column : str, optional
        The name of the column containing the SA2 area codes. Defaults to "SA2_CODE21".

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only rows that match the specified filter criteria.
        If no filters are specified, returns the original LazyFrame.
    """
    # Filter by SA1 codes if provided
    if region_codes:
        lf = lf.filter(pl.col(sa1_column).is_in(region_codes))

    # Filter by SA2 codes if provided
    if sa2_codes:
        lf = lf.filter(pl.col(sa2_column).is_in(sa2_codes))

    return lf


def filter_gnaf_cache(
    lf: pl.LazyFrame,
    states: list[str] = [],
    region_codes: list[str] = [],
    sa2_codes: list[str] = [],
    flat_type_codes: list[str] = [],
    postcodes: list[int] = [],
) -> pl.LazyFrame:
    """
    A function to filter the GNAF cache based on states, SA1 area codes, SA2 area codes, building types, and postcodes.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing the GNAF cache data.
    states : list[str], optional
        A list of states to filter. If not provided, no state filtering will be applied.
    region_codes : list[str], optional
        A list of SA1 area codes to filter. If not provided, no SA1 filtering will be applied.
    sa2_codes : list[str], optional
        A list of SA2 area codes to filter. If not provided, no SA2 filtering will be applied.
    flat_type_codes : list[str], optional
        A list of building type codes to filter. If not provided, no building type filtering will be applied.
    postcodes : list[int], optional
        A list of postcodes to filter. If not provided, no postcode filtering will be applied.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing the filtered GNAF data.
    """

    if states:
        lf = lf.filter(pl.col("STATE").is_in(states))

    if region_codes:
        lf = lf.filter(pl.col("SA1_CODE21").is_in(region_codes))

    if sa2_codes:
        lf = lf.filter(pl.col("SA2_CODE21").is_in(sa2_codes))

    if flat_type_codes:
        lf = lf.filter(pl.col("FLAT_TYPE_CODE").is_in(flat_type_codes))

    if postcodes:
        lf = lf.filter(pl.col("POSTCODE").is_in(postcodes))

    return lf
