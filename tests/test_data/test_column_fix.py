from ..context import nhs
import polars as pl
from polars import LazyFrame
import pandas as pd

column_readable = nhs.data.handling.standarise_names


class TestColumnReadable:
    # Converts xlsx_pl_df to a dictionary of column mappings correctly
    def test_converts_xlsx_pl_df_to_dict_correctly(self):
        prev_csv_pl_df = {
            'file_G01': pl.DataFrame({'A': [1, 2], 'B': [3, 4]}).lazy(),
            'file_G02': pl.DataFrame({'C': [5, 6], 'D': [7, 8]}).lazy()
        }

        xlsx_pl_df = {
            'Metadata_2021_GCP_DataPack_R1_R2.xlsx': {
                'Cell Descriptors Information': pl.DataFrame({
                    'DataPackfile': ['G01', 'G02'],
                    'Short': ['A', 'C'],
                    'Long': ['Alpha', 'Charlie']
                })
            }
        }

        result = column_readable(prev_csv_pl_df, xlsx_pl_df)

        assert result['file_G01'].columns == ['Alpha', 'B']
        assert result['file_G02'].columns == ['Charlie', 'D']

        # xlsx_pl_df is empty

    def test_xlsx_pl_df_is_empty(self):
        prev_csv_pl_df = {
            'file_G01': pl.DataFrame({'A': [1, 2], 'B': [3, 4]}).lazy(),
            'file_G02': pl.DataFrame({'C': [5, 6], 'D': [7, 8]}).lazy()
        }

        xlsx_pl_df = {
            'Metadata_2021_GCP_DataPack_R1_R2.xlsx': {
                'Cell Descriptors Information': pl.DataFrame({
                    'DataPackfile': [],
                    'Short': [],
                    'Long': []
                })
            }
        }

        result = column_readable(prev_csv_pl_df, xlsx_pl_df)

        assert result == prev_csv_pl_df

    # Renames columns in prev_csv_pl_df based on the mappings
    def test_renames_columns(self):
        prev_csv_pl_df = {
            'file_G01': LazyFrame({}),
            'file_G02': LazyFrame({})
        }
        xlsx_pl_df = {
            'Metadata_2021_GCP_DataPack_R1_R2.xlsx': {
                'Cell Descriptors Information': pl.DataFrame({
                    'DataPackfile': ['G01', 'G02'],
                    'Short': ['short_1', 'short_2'],
                    'Long': ['long_1', 'long_2']
                })
            }
        }

        # Exercise
        updated_prev_csv_pl_df = column_readable(prev_csv_pl_df, xlsx_pl_df)

        # Verify
        assert isinstance(updated_prev_csv_pl_df, dict)
        assert isinstance(updated_prev_csv_pl_df['file_G01'], LazyFrame)
        assert isinstance(updated_prev_csv_pl_df['file_G02'], LazyFrame)

    # Handles multiple rows in xlsx_pl_df correctly
    def test_handles_multiple_rows(self):
        # Setup
        from nhs.data.column_fix import column_readable
        from polars import LazyFrame

        prev_csv_pl_df = {
            'file_G01_data': LazyFrame({'col1': [1, 2], 'short_1': [3, 4]}),
            'file_G02_data': LazyFrame({'col2': [5, 6], 'short_2': [7, 8]})
        }
        xlsx_pl_df = {
            'Metadata_2021_GCP_DataPack_R1_R2.xlsx': {
                'Cell Descriptors Information': pl.DataFrame({
                    'DataPackfile': ['G01', 'G02'],
                    'Short': ['short_1', 'short_2'],
                    'Long': ['long_1', 'long_2']
                })
            }
        }

        # Exercise
        updated_prev_csv_pl_df = column_readable(prev_csv_pl_df, xlsx_pl_df)

        # Verify
        assert isinstance(updated_prev_csv_pl_df, dict)
        assert isinstance(updated_prev_csv_pl_df['file_G01_data'], LazyFrame)
        assert isinstance(updated_prev_csv_pl_df['file_G02_data'], LazyFrame)
        assert 'long_1' in updated_prev_csv_pl_df['file_G01_data'].columns
        assert 'long_2' in updated_prev_csv_pl_df['file_G02_data'].columns
