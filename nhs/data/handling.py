from typing import Iterable, Optional
import os
import polars as pl
from nhs.utils.path import list_files
from nhs.utils.string import placeholder_matches
from loguru import logger


@logger.catch()
def read_psv(file_path: str) -> Optional[pl.LazyFrame]:
    """
    Load a .psv file into a polars LazyFrame, returning None if exception occurs
    """
    return pl.scan_csv(file_path, separator="|")


def read_all_psv(file_dir_pattern: str) -> dict[str, pl.LazyFrame | None]:
    """
    Return dictionary of key and polars LazyFrames given directory of .psv files

    If a file cannot be read, the value will be None.

    Parameters
    ----------
    file_dir_pattern: str
        Path to directory containing .psv files. Optionally, you can include the name of
        the PSV file with a placeholder `"{key}"` - the resulting dictionary will use
        the string specified by `"{key}"` as the key. If omitted, the file name will be
        used as the key. See example.

    Returns
    -------
    dict[str, pl.LazyFrame]
        Dictionary of polars LazyFrames, with keys as matched from key_pattern

    Examples
    --------
    Assume we have the following files in the directory `"path/to/psv_files/"`:
        - file1.psv
        - file2.psv

    >>> read_all_psv("path/to/psv_files/")
    {'file1.psv': <LazyFrame>, 'file2.psv': <LazyFrame>}
    >>> read_all_psv("path/to/psv_files/{key}.psv")
    {'file1': <LazyFrame>, 'file2': <LazyFrame>}
    >>> read_all_psv("path/to/psv_files/file{key}.psv")
    {'1': <LazyFrame>, '2': <LazyFrame>}
    """
    psv_files = list_files(os.path.dirname(file_dir_pattern))
    psv_files = list(filter(lambda x: x.endswith(".psv"), psv_files))
    if "{key}" not in file_dir_pattern:
        keys = map(os.path.basename, psv_files)
    else:
        # placeholder_matches return list of tuples ("key",), [0] to get "key"
        keys = map(
            lambda x: x[0], placeholder_matches(psv_files, file_dir_pattern, ["key"])
        )
    return {key: val for key, val in zip(keys, map(read_psv, psv_files))}
