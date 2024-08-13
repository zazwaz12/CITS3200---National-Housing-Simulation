from typing import Iterable, Optional
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


def read_all_psv(file_dir: str, key_pattern: str) -> dict[str, pl.LazyFrame | None]:
    """
    Return dictionary of polars LazyFrames given directory of .psv files and key pattern

    If a file cannot be read, the value will be None.

    Parameters
    ----------
    file_dir : str
        Directory containing .psv files
    key_pattern : str
        String specifying which part of the path to use as the key using `"{key}"`, e.g.
        `"./DataFiles/FilesIn/Authority Code/Authority_Code_{key}_psv.psv"` will use
        any value in place of `"{key}"` as the key

    Returns
    -------
    dict[str, pl.LazyFrame]
        Dictionary of polars LazyFrames, with keys as matched from key_pattern
    """
    psv_files: Iterable[str] = list_files(file_dir)
    psv_files = list(filter(lambda x: x.endswith(".psv"), psv_files))
    keys: tuple[str] = placeholder_matches(psv_files, key_pattern, ["key"])
    return {key[0]: val for key, val in zip(keys, map(read_psv, psv_files))}
