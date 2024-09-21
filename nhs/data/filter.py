import glob

import polars as pl


def load_gnaf_files_by_states(
    gnaf_path: str, states: list[str] = []
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Load and filter ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL files for the specified states
    from the given GNAF directory, and return them as LazyFrames.

    Parameters:
    - gnaf_path: str, the directory path where the GNAF psv files are stored.
    - states: list[str], an optional list of state/territory names (e.g., ["WA", "ACT"]).
              If an empty list is provided, the function will default to including all states.

    Returns:
    - tuple: (default_geocode_lf, address_detail_lf)
      - default_geocode_lf: pl.LazyFrame, the merged ADDRESS_DEFAULT_GEOCODE data for the specified states,
                            with an added "STATE" column.
      - address_detail_lf: pl.LazyFrame, the merged ADDRESS_DETAIL data for the specified states.
    """
    # State codes
    all_state_codes = ["NSW", "ACT", "VIC", "QLD", "SA", "WA", "TAS", "NT", "OT"]

    # If states is an empty list, use all state codes
    if not states:
        states = all_state_codes

    default_geocode_lfs = []
    address_detail_lfs = []

    for state_name in states:
        # Find the corresponding ADDRESS_DEFAULT_GEOCODE file
        geocode_file_pattern = (
            f"{gnaf_path}/{state_name}_ADDRESS_DEFAULT_GEOCODE_psv.psv"
        )
        geocode_files = glob.glob(geocode_file_pattern)

        # Load the found file
        for geocode_file in geocode_files:
            lf = pl.scan_csv(
                geocode_file, separator="|"
            )  # Read PSV file using '|' separator
            # Add the state column
            lf = lf.with_columns(pl.lit(state_name).alias("STATE"))
            default_geocode_lfs.append(lf)

        # Find the corresponding ADDRESS_DETAIL file
        detail_file_pattern = f"{gnaf_path}/{state_name}_ADDRESS_DETAIL_psv.psv"
        detail_files = glob.glob(detail_file_pattern)

        # Load the found file
        for detail_file in detail_files:
            # Read the file and select relevant columns such as "ADDRESS_DETAIL_PID", "FLAT_TYPE_CODE", and "POSTCODE"
            lf = pl.scan_csv(detail_file, separator="|")
            address_detail_lfs.append(
                lf.select(["ADDRESS_DETAIL_PID", "FLAT_TYPE_CODE", "POSTCODE"])
            )

    # Return an empty LazyFrame if no files were found
    default_geocode_lf = (
        pl.concat(default_geocode_lfs) if default_geocode_lfs else pl.LazyFrame()
    )
    address_detail_lf = (
        pl.concat(address_detail_lfs) if address_detail_lfs else pl.LazyFrame()
    )

    return default_geocode_lf, address_detail_lf


def filter_and_join_gnaf_frames(
    default_geocode_lf: pl.LazyFrame,
    address_detail_lf: pl.LazyFrame,
    building_types: list[str] = [],
    postcodes: list[int] = [],
) -> pl.LazyFrame:
    """
    Filter and join GNAF ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL LazyFrames based on optional building types
    or postcodes, and return the joined result as a LazyFrame.

    Parameters:
    - default_geocode_lf: pl.LazyFrame, the LazyFrame containing ADDRESS_DEFAULT_GEOCODE data.
    - address_detail_lf: pl.LazyFrame, the LazyFrame containing ADDRESS_DETAIL data.
    - building_types: list[str], an optional list of building types to filter on (e.g., ["flat", "unit"]).
                      (If empty, no filtering on building types will be applied.)
    - postcodes: list[int], an optional list of postcodes to filter on.
                    (If empty, no filtering on postcodes will be applied.)

    Returns:
    - pl.LazyFrame: The joined LazyFrame containing data from both ADDRESS_DEFAULT_GEOCODE and ADDRESS_DETAIL,
                    with the applied filters if specified.
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
