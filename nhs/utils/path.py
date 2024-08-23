"""
Utility functions for working with file paths
"""

import os

from nhs.utils.logging import log_entry_exit


@log_entry_exit()
def list_files(path: str, list_hidden: bool = False) -> list[str]:
    """
    Return list of full file paths in a given path

    Parameters
    ----------
    list_hidden: bool
        Whether to include hidden files (starting with a dot '.')
    """
    return [
        os.path.join(root, file)
        for root, dirs, files in os.walk(path)
        for file in files
        if list_hidden or not file.startswith(".")
    ]
