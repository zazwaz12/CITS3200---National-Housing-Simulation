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
def read_xlsx(file_path: str, sheet_id: None | int = None) -> dict[str, pl.LazyFrame] | pl.LazyFrame | None:
    """
    Load a .xlsx file into a polars `LazyFrame`, returning None if exception occurs

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
        ".xlsx": read_xlsx,
    }[file_extension]


@log_entry_exit(level="INFO")
def read_spreadsheets(
        file_dir_pattern: str, extension: Literal["csv", "psv", "xlsx"],
        sheet_id: None | int = None) -> dict[str, pl.LazyFrame | None]:
    """
    Return dictionary of key and polars `LazyFrame` given directory of PSV, CSV, or XLSX files.
    If a file cannot be read, the value will be None.

    **Warning**: If file is XLSX, the whole file is read (i.e. no lazy evaluation). A `LazyFrame` is
    still returned for consistency.

    Parameters
    ----------
    sheet_id
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

    return {key: reader(file, sheet_id) for key, file in zip(keys, files)} if extension == "xlsx" else {
        key: reader(file) for key, file in zip(keys, files)
    }



@log_entry_exit(level="INFO")
def standarise_names(df_dict: dict[str, pl.LazyFrame | None], census_metadata: pl.LazyFrame | None,
                     identification: str, abbreviation_column_name: str,
                     long_column_name: str) -> dict[str, pl.LazyFrame | None]:
    """
    Process the column names of a polar Lazy frame dictionary to make them more readable.
        1. Expand abbreviated names.
        2. Converting to lower case.
    If any parameters are None, this function returns prev_csv_pl_df without change.
    The filename in both dictionaries are checked or filtered to be passed.

    Parameters:
    --------
    param1 : dict[str, pl.LazyFrame]
        df_dict: A dictionary containing census data as value and filename as key or None.
    param2 : [pl.LazyFrame | None]
        census_metadata: Census metadata lazy frame with one standard sheet or None.
        Blank files won't change prev_csv_pl_df.
    param3 : str
        identification: Identify different files in df_dict. Could not be None
    param4 : str
    abbreviation_column_name: column name of abbreviated names. Could not be None
    param5 : str
    long_column_name: column name of expanded names. Could not be None

    Returns:
    --------
    dict[str, pl.LazyFrame]
        A dictionary with updated column names based on the provided mapping.

    Examples
    --------
        Assume we have the following files in the directory `"path/to/csv_files/"`:
            - 2021Census_G01_AUS_AUS.csv
            - 2021Census_G02_AUS_AUS.csv....
        and "path/to/xlsx_files/":
            - Metadata_2021_GCP_DataPack_R1_R2.xlsx
            - 2021Census_geog_desc_1st_2nd_3rd_release.xlsx
        Standard file in second sheet of Metadata_2021_GCP_DataPack_R1_R2.xlsx
        "name of XLSX" : {"name of sheet1" : <dataframe>, "name of sheet2": <dataframe> ...}
        By using read_spreadsheets to get dictionary containing dataFrames.
        >>> def standarise_names({"files_identify1_with_abbreviated_names": <LazyFrame>,
        >>>"files_identify2_with_abbreviated_names": <LazyFrame>},
        >>>"standard_sheet1's" <LazyFrame>, "identification", "abbreviation_column_name", "Long_column_name")
        {"file_identify1_with_expanded_names": <LazyFrame>, "file_identify2_with_expanded_names": <LazyFrame>}
    """
    result_dict = {}
    for row in census_metadata.collect().iter_rows(named=True):
        key = row[identification]
        short = row[abbreviation_column_name]
        long = row[long_column_name]
        result_dict.setdefault(key, {})[short] = long

    for key in df_dict:
        try:
            value = df_dict[key].rename(result_dict[key.split('_')[1]])
            df_dict[key] = value
        except KeyError:
            continue
    return df_dict
