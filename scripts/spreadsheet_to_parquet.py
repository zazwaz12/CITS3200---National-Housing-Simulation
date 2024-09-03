"""
Produce a directory of parquet files from a directory of spreadsheets
"""

from tqdm import tqdm
import argparse
from pathlib import Path
from loguru import logger
import polars as pl
from context import nhs

list_files = nhs.utils.path.list_files
get_reader = nhs.data.handling.get_spreadsheet_reader


def convert_to_parquet(input_dir: str, output_dir: str):
    paths = list_files(input_dir)
    paths = filter(lambda x: x.endswith((".xlsx", ".xls", ".csv", ".psv")), paths)

    for path in tqdm(paths, desc="Converting spreadsheets to Parquet"):
        relative_path = Path(path).relative_to(input_dir)
        output_file_path = Path(output_dir) / relative_path.with_suffix(".parquet")

        if output_file_path.exists():
            logger.warning(f"File {output_file_path} already exists. Skipping.")
            continue

        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        df = get_reader(Path(path).suffix)(path)
        if not isinstance(df, pl.LazyFrame):
            logger.error(f"Failed to read {path}")
            continue
        df.collect().write_parquet(output_file_path)


def main():
    parser = argparse.ArgumentParser(
        description="Convert spreadsheets to Parquet files while maintaining directory structure."
    )
    parser.add_argument(
        "input_dir",
        type=str,
        help="Path to the input directory containing spreadsheets.",
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Path to the output directory where Parquet files will be saved.",
    )

    args = parser.parse_args()

    convert_to_parquet(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
