import os
from typing import Callable, Literal

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
def read_xlsx(
    file_path: str, sheet_id: None | int = 1
) -> dict[str, pl.LazyFrame] | pl.LazyFrame | None:
    """
    Load a .xlsx file into a polars `LazyFrame`, returning None if exception occurs.
    Function returns lazyFrame if sheet_id = 1 and 0 returns dictionary, so default sheet_id is 1.
    **NOTE**: Polars use `xlsx2csv` to read .xlsx files, so whole CSV file is read
    """
    frames = pl.read_excel(file_path, sheet_id=sheet_id)
    if isinstance(frames, dict):
        return {name: df.lazy() for name, df in frames.items()}  # type: ignore
    return frames.lazy()


def __get_spreadsheet_reader(file_extension: str) -> Callable[..., pl.LazyFrame | None]:
    """
    Maps file extension to corresponding reader function
    """
    return {
        ".psv": read_psv,
        ".csv": read_csv,
    }[file_extension]


@log_entry_exit(level="INFO")
def read_spreadsheets(
    file_dir_pattern: str, extension: Literal["csv", "psv"]
) -> dict[str, pl.LazyFrame | None]:
    """
    Return dictionary of key and polars `LazyFrame` given directory of PSV, CSV files.
    If a file cannot be read, the value will be None.

    Parameters
    ----------
    file_dir_pattern: str
        Path to directory containing .psv, .csv, or .xlsx files. Optionally, you can include the name of
        the PSV file with a placeholder `"{key}"` - the resulting dictionary will use
        the string specified by `"{key}"` as the key. If omitted, the file name will be
        used as the key. See example.
        For Windows users. add "\\" between Folder and files. Example: c:/path/to/csv_folder\\csv_files.
        Using os.path.dirname(path) to find out where files are located.
    extension: str
        File extension to read. Must be one of `psv`, `csv`.
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
def standardize_names(
    df_dict: dict[str, pl.LazyFrame],
    census_metadata: pl.LazyFrame,
    census_code_col: str = "DataPackfile",
    abbreviation_column_name: str = "Short",
    long_column_name: str = "Long",
) -> dict[str, pl.LazyFrame]:
    """
    Standardise the column names of a polar Lazy frame dictionary to make them more readable

    This function will expand abbreviated names using a census metadata frame containing
    mappings of abbreviated names to full names in the columns `abbreviation_column_name` and `long_column_name`.
    Names are also convert to snake_case.

    Parameters:
    --------
    df_dict : dict[str, pl.LazyFrame]
        A dictionary containing census data as value and filename as key. Names
        must exist in `census_metadata[census_code_col]`.
    census_metadata : pl.LazyFrame
        Lazy frame containing a column called `census_code_col` that contains
        the keys in `df_dict` to standardise, and columns called `abbreviation_column_name`
        and `long_column_name` containing the abbreviated and long column names
        for frames in the `df_dict`.
    census_code_col : str
        Column name in `census_metadata` that contains the keys in `df_dict` to standardise.
        e.g. if `df_dict` have the key `"G03"` and `"G03"` exist `census_metadata[census_code_col]`
        then the lazy frame will be standarised.
    abbreviation_column_name : str
        Column name in `census_metadata` containing the long, unabbreviated names.

    Returns:
    --------
    dict[str, pl.LazyFrame]
        A dictionary with standarised column names.

    Examples
    --------
    >>> frames = {
    ...     "file_identify1": pl.LazyFrame({"SHORT1": [1, 2, 3], "SHORT2": [4, 5, 6]}),
    ...     "file_identify2": pl.LazyFrame({"SHORT1": [1, 2, 3], "SHORT2": [4, 5, 6]})
    ... }
    >>> metadata = pl.LazyFrame({
    ...     "file_names": ["file_identify1"],
    ...     "abbreviated": ["SHORT1", "SHORT2"],
    ...     "unabbreviated": ["LONG 1", "LONG 2"]
    ... })
    >>> frames_out = standardize_names(frames, metadata, "file_names", "abbreviated", "unabbreviated")
    >>> frames_out["file_identify1"].columns
    ["long_1", "long_2"]
    >>> frames_out["file_identify2"].columns # No change, name not in `metadata["file_names"]`
    ["SHORT1", "SHORT2"]
    """
    result_dict: dict[str, dict[str, str]] = {}
    for row in census_metadata.collect().iter_rows(named=True):
        key = row[census_code_col]
        short = row[abbreviation_column_name]
        long = row[long_column_name]
        result_dict.setdefault(key, {})[short] = long.lower().replace(" ", "_")  # type: ignore

    for key in df_dict:
        value = df_dict[key].rename(result_dict[key])
        df_dict[key] = value
    return df_dict
