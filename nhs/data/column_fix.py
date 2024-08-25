from typing import Iterable, Optional, Dict, Any, Tuple
import os
import polars as pl
import pandas as pd
from polars import LazyFrame

from nhs.utils.path import list_files
from nhs.utils.string import placeholder_matches
from loguru import logger
from .handling import read_spreadsheets


"""
Process the column names of a Pandas DataFrame to make them more readable.

Args:
    prev_csv_pl_df (dict[str, LazyFrame | None]): A dictionary containing Pandas DataFrames or None.
    xlsx_pl_df: A Pandas DataFrame from an Excel file.

Returns:
    dict[str, LazyFrame | None]: A dictionary with updated column names based on the provided mapping.
    
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
    column_readable(
        read_spreadsheets("path/to/csv_files/", 'csv'), 
        read_spreadsheets("path/to/xlsx_files/", 'xlsx')
        ) -> dict[str, LazyFrame | None]:
"""


def column_readable(prev_csv_pl_df: dict[str, LazyFrame | None], xlsx_pl_df) -> dict[str, LazyFrame | None]:
    df = xlsx_pl_df['Metadata_2021_GCP_DataPack_R1_R2.xlsx']['Cell Descriptors Information']

    result_dict = {}

    for row in df.iter_rows(named=True):
        key = row["DataPackfile"]
        short = row["Short"]
        long = row["Long"]
        result_dict.setdefault(key, {})[short] = long

    for key in prev_csv_pl_df:
        try:
            value = prev_csv_pl_df[key].rename(result_dict[key.split('_')[1]])
            prev_csv_pl_df[key] = value
        except KeyError:
            continue
    return prev_csv_pl_df


# if __name__ == '__main__':
#     new_pl_df = column_readable(read_spreadsheets(
#         'C:/Users/IamNo/Desktop/DataFiles/FilesIn/census/2021 Census GCP All Geographies for AUS/AUS/'
#         '2021Census_G01_AUS_AUS.csv', 'csv'), read_spreadsheets('C:/Users/IamNo/Desktop/DataFiles/FilesIn/census'
#                                                                 '/Metadata/Metadata_2021_GCP_DataPack_R1_R2.xlsx',
#                                                                 'xlsx'))
#     print(new_pl_df["2021Census_G02_AUS_AUS.csv"].collect().collect_schema().names())
