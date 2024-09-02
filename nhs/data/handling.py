import os
from typing import Callable, Literal, Dict

import polars as pl
from loguru import logger

from nhs.utils.logging import log_entry_exit
from nhs.utils.path import list_files
from nhs.utils.string import placeholder_matches


@logger.catch()
@log_entry_exit()
def read_psv(file_path: str) -> pl.LazyFrame | None:
    """
    Load a .psv file into a polars LazyFrame, returning None if exception occurs
    """
    return pl.scan_csv(file_path, separator="|")


@logger.catch()
@log_entry_exit()
def read_csv(file_path: str) -> pl.LazyFrame | None:
    """
    Load a .csv file into a polars LazyFrame, returning None if exception occurs
    """
    return pl.scan_csv(file_path)


@logger.catch()
@log_entry_exit()
def read_xlsx(file_path: str, sheet_id: None | int = 1) -> dict[str, pl.LazyFrame] | pl.LazyFrame | None:
    """
    Load a .xlsx file into a polars `LazyFrame`, returning None if exception occurs.
    Function returns lazyFrame if sheet_id = 1 and 0 returns dictionary, so default sheet_id is 1.
    **NOTE**: Polars use `xlsx2csv` to read .xlsx files, so whole CSV file is read
    """
    frames = pl.read_excel(file_path, sheet_id=sheet_id)
    if isinstance(frames, dict):
        return {name: df.lazy() for name, df in frames.items()}
    return frames.lazy()


def __get_spreadsheet_reader(
        file_extension: str) -> Callable[..., pl.LazyFrame | None]:
    """
    Maps file extension to corresponding reader function
    """
    return {
        ".psv": read_psv,
        ".csv": read_csv,
    }[file_extension]


@log_entry_exit(level="INFO")
def read_spreadsheets(
        file_dir_pattern: str, extension: Literal["csv", "psv"]) -> dict[str, pl.LazyFrame | None]:
    """
    Return dictionary of key and polars `LazyFrame` given directory of PSV, CSV, or XLSX files.
    If a file cannot be read, the value will be None.

    **Warning**: If file is XLSX, the whole file is read (i.e. no lazy evaluation). A `LazyFrame` is
    still returned for consistency.

    Parameters
    ----------
    file_dir_pattern: str
        Path to directory containing .psv, .csv, or .xlsx files. Optionally, you can include the name of
        the PSV file with a placeholder `"{key}"` - the resulting dictionary will use
        the string specified by `"{key}"` as the key. If omitted, the file name will be
        used as the key. See example.
    extension: str
        File extension to read. Must be one of `psv`, `csv`, or `xlsx`.

    Returns
    -------
    dict[str, pl.LazyFrame]
        Dictionary of polars LazyFrames, with keys as matched from key_pattern

    Examples
    --------
    Assume we have the following files in the directory `"path/to/psv_files/"`:
        - file1.psv
        - file2.psv

    >>> read_spreadsheets("path/to/psv_files/")
    {'file1.psv': <LazyFrame>, 'file2.psv': <LazyFrame>}
    >>> read_spreadsheets("path/to/psv_files/{key}.psv")
    {'file1': <LazyFrame>, 'file2': <LazyFrame>}
    >>> read_spreadsheets("path/to/psv_files/file{key}.psv")
    {'1': <LazyFrame>, '2': <LazyFrame>}
    """
    files = list_files(os.path.dirname(file_dir_pattern))
    files = list(filter(lambda x: x.endswith(f".{extension}"), files))
    reader = __get_spreadsheet_reader(f".{extension}")

    if "{key}" not in file_dir_pattern:
        keys = map(os.path.basename, files)
    else:
        # placeholder_matches return list of tuples ("key",), [0] to get "key"
        keys = map(
            lambda x: x[0], placeholder_matches(files, file_dir_pattern, ["key"])
        )

    return {key: val for key, val in zip(keys, map(reader, files))}


@log_entry_exit(level="INFO")
def standardize_names(df_dict: dict[str, pl.LazyFrame | None], census_metadata: pl.LazyFrame | None,
                      census_code_col: str, abbreviation_column_name: str, long_column_name: str) -> dict[str, pl.LazyFrame | None]:
    """
    Standardise the column names of a polar Lazy frame dictionary to make them more readable

    This function will
        1. Expand abbreviated names.
        2. Converting to lower case.

    If any parameters are None, this function returns prev_csv_pl_df without change. Keys in df_dict must have format 'G{int + letter}'.


    Parameters:
    --------
    df_dict : dict[str, pl.LazyFrame]
        A dictionary containing census data as value and filename as key or None. a dictionary of name-dataframe pair. Names must exist in the census_metadata[census_code_col]. All column names in the dataframes in df_dict with keys in format 'G{int + letter}'.
    census_metadata : [pl.LazyFrame | None]
        Census_metadata: Census metadata lazy frame with one standard sheet has a column called identification containing list of keys in the df_dict dictionary to standarise, and columns called abbreviated.... and long_column)names containing the abbreviated and long column names for data frames in the df_dict or None. If don't have abbreviated column, long column and census_code_col then df_dict won't be changed.
    census_code_col : str
        Identify different files in df_dict. Could not be None, name of the column in census_metadata that contains names in the df_dict to standarise. e.g. if df_dict have the pair "G03": and "G03" exist in the census_code_col column in metada, then the lazy frame will be standarised
    abbreviation_column_name : str
        Column name of abbreviated names. Could not be None, short will be the default value.
    long_column_name : str
        Column name of expanded names. Could not be None, long will be the default value.

    Returns:
    --------
    dict[str, pl.LazyFrame]
        A dictionary with expanded column names based on where for each dataframe, columns with names in long_column_names are replaced with corresponding names in abbreviated... and converted to snake_case

    Examples
    --------
    >>> def standardize_names({"files_identify1_with_abbreviated_names": <LazyFrame>,"files_identify2_with_abbreviated_names": <LazyFrame>},"standard_sheet1's" <LazyFrame>, "identification", "abbreviation_column_name", "Long_column_name")
    {"file_identify1_with_expanded_names": <LazyFrame>, "file_identify2_with_expanded_names": <LazyFrame>}
    """
    result_dict = {}
    for row in census_metadata.collect().iter_rows(named=True):
        key = row[census_code_col]
        short = row[abbreviation_column_name]
        long = row[long_column_name]
        result_dict.setdefault(key, {})[short] = long.lower()

    for key in df_dict:
        value = df_dict[key].rename(result_dict[key])
        df_dict[key] = value
    return df_dict
