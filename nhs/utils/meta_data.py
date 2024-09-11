import polars as pl
import re

def file_shapes_reporting(files_dict: dict[str, pl.LazyFrame]) -> dict[str, str]:
    shapes = {}

    for file_name, lazyframe in files_dict.items():
        # Collect the shape of the LazyFrame (rows, columns)
        shape = lazyframe.collect().shape
        shapes[file_name] = f"{shape[0]},{shape[1]}"

    return shapes # type: ignore

def match_files_by_keywords(file_list: list[str], keywords: list[str]) -> list[str]:
    """
    Match files that contain only the specified keywords in their names.

    Parameters
    ----------
    file_list : list[str]
        List of file names to search through.
    keywords : list[str]
        List of keywords to match in file names.

    Returns
    -------
    list[str]
        List of files that match the keywords.
    """

    if not keywords:
        return []  # Return an empty list if no keywords are provided

    # Create a regex pattern that requires all keywords to be present in the filename
    pattern = r".*".join([re.escape(keyword) for keyword in keywords])  # Escape special characters in keywords
    regex = re.compile(f".*{pattern}.*")  # Match files that contain all keywords in any order
    
    # Return files that match the regex pattern
    return [file for file in file_list if regex.search(file)]