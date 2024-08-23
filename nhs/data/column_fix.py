from typing import Iterable, Optional, Dict, Any
import os
import polars as pl
import pandas as pd
from polars import LazyFrame

from nhs.utils.path import list_files
from nhs.utils.string import placeholder_matches
from loguru import logger
from handling import read_spreadsheets

#the standard_file to modify column name. should be store in the lazy_frame while read_all_files and be used next.
standard_file = 'C:/Users/IamNo/Desktop/DataFiles/FilesIn/census/Metadata/Metadata_2021_GCP_DataPack_R1_R2.xlsx'


def column_readable() -> dict[str, LazyFrame | None]:
    pl_df = read_spreadsheets(
        'C:/Users/IamNo/Desktop/DataFiles/FilesIn/census/2021 Census GCP All Geographies for AUS/AUS/'
        '2021Census_G01_AUS_AUS.csv', 'csv')
    return pl_df


#Read fullname from metadata DataPack to get datapack_file dictionary[Short column name: long column name] to rename the
#lazyframe that read_all_xlsx.
def read_multiple_sheets(excel_files: str, sheet_name: str, data_pack_file: str) -> dict[Any, Any]:
    df = pl.read_excel(excel_files, sheet_name=sheet_name)
    filtered_df = df.filter(pl.col("DataPackfile") == data_pack_file)
    key_value_dict = dict(zip(filtered_df["Short"].to_list(), filtered_df["Long"].to_list()))
    return key_value_dict


if __name__ == '__main__':
    pl_df = column_readable()
    for key in pl_df:
        value = pl_df[key].rename(read_multiple_sheets(standard_file, 'Cell Descriptors Information',
                                                       key.split('_')[1]))
        pl_df[key] = value
        print(pl_df[key].collect().collect_schema().names())
