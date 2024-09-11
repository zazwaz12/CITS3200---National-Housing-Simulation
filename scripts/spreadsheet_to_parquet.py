"""
Produce a directory of parquet files from a directory of spreadsheets
"""

import argparse
import os
from pathlib import Path

import polars as pl
from context import nhs
from loguru import logger
from tqdm import tqdm

# Utility functions for file listing and reading spreadsheets
list_files = nhs.utils.path.list_files
get_reader = nhs.data.handling.get_spreadsheet_reader


@logger.catch()
def save_parquet(path: str, input_dir: str, output_dir: str):
    # Define relative output file path
    relative_path = Path(path).relative_to(input_dir)
    output_dir = input_dir.replace("FilesIn", "AppStaging")
    output_file_path = Path(output_dir) / relative_path.with_suffix(".parquet")

    # Skip if file already exists
    if output_file_path.exists():
        logger.warning(f"File {output_file_path} already exists. Skipping.")
        return

    if "Standard" in str(path) and "DEFAULT_GEOCODE" not in str(path):
        logger.warning(f"Only taking DEFAULT GEOCODE from GNAF for now. Skipping.")
        return

    # Create output directories if not exist
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read and convert spreadsheet to Parquet
    df = get_reader(Path(path).suffix)(path)
    if not isinstance(df, pl.LazyFrame):
        logger.error(f"Failed to read {path}")
        return
    df.collect().write_parquet(output_file_path)


def convert_to_parquet(input: str, output: str):
    # Filter for supported spreadsheet files
    paths = list_files(input)
    paths = filter(lambda x: x.endswith((".xlsx", ".xls", ".csv", ".psv")), paths)

    # Convert each file
    for path in tqdm(paths, desc="Converting spreadsheets to Parquet"):
        save_parquet(path, input, output)


def main():
    # Setup argument parsing for input and output directories
    parser = argparse.ArgumentParser(
        description="Convert spreadsheets to Parquet files."
    )

    parser.add_argument(
        "input", default=os.path.normpath("./FilesIn"), type=str, help="Input Directory"
    )
    parser.add_argument(
        "output",
        default=os.path.normpath("./Appstaging"),
        type=str,
        help="Output Directory",
    )

    # Execute conversion
    args = parser.parse_args()
    convert_to_parquet(args.input, args.output)


if __name__ == "__main__":
    main()
