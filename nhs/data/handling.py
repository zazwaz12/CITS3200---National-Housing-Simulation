import os
from typing import Callable, Literal, Dict

import polars as pl
from loguru import logger
from polars import DataFrame

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
def read_xlsx(file_path: str) -> dict[str, DataFrame]:
    """
    Load a .xlsx file into a polars `LazyFrame`, returning None if exception occurs

    **NOTE**: Polars use `xlsx2csv` to read .xlsx files, so whole CSV file is read
    """
    return pl.read_excel(file_path, sheet_id=0)


def __get_spreadsheet_reader(
    file_extension: str,
) -> Callable[[str], pl.LazyFrame | None]:
    """
    Maps file extension to corresponding reader function
    """
    return {
        ".psv": read_psv,
        ".csv": read_csv,
        ".xlsx": read_xlsx,
    }[file_extension]


@log_entry_exit(level="INFO")
def read_spreadsheets(
    file_dir_pattern: str, extension: Literal["csv", "psv", "xlsx"]
) -> dict[str, pl.LazyFrame | None]:
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
    Assume we have the following files in the directory `"path/to/psv
_files/"`:
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
