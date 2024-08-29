from gzip import READ

import polars as pl
from pytest_mock import MockerFixture

from ..context import nhs

read_spreadsheets = nhs.data.handling.read_spreadsheets
read_xlsx = nhs.data.handling.read_xlsx

LIST_FILES_PATCH = "nhs.data.handling.list_files"
READ_PSV_PATCH = "nhs.data.handling.read_psv"
READ_CSV_PATCH = "nhs.data.handling.read_csv"
READ_XLSX_PATCH = "nhs.data.handling.read_xlsx"


class TestReadSpreadsheets:

    # Reads all .psv files in a given directory and returns a dictionary of LazyFrames
    def test_reads_all_psv_files(self, mocker: MockerFixture):
        mocker.patch(
            LIST_FILES_PATCH,
            return_value=["path/to/psv_files/file1.psv", "path/to/psv_files/file2.psv"],
        )
        mocker.patch(READ_PSV_PATCH, side_effect=[pl.LazyFrame(), pl.LazyFrame()])

        result = read_spreadsheets("path/to/psv_files/", "psv")

        assert len(result) == 2
        assert "file1.psv" in result
        assert "file2.psv" in result
        assert isinstance(result["file1.psv"], pl.LazyFrame)
        assert isinstance(result["file2.psv"], pl.LazyFrame)

    # Directory contains no .psv files
    def test_no_psv_files_in_directory(self, mocker: MockerFixture):
        mock_list_files = mocker.patch(LIST_FILES_PATCH)
        mock_list_files.return_value = []

        result = read_spreadsheets("path/to/psv_files/", "psv")

        assert result == {}

    # Uses specified placeholder to extract keys from file names
    def test_read_all_csv_with_placeholder(self, mocker: MockerFixture):
        mocker.patch(
            LIST_FILES_PATCH,
            return_value=["path/to/csv_files/file1.csv", "path/to/csv_files/file2.csv"],
        )
        mocker.patch(READ_CSV_PATCH, side_effect=[pl.LazyFrame(), pl.LazyFrame()])

        result = read_spreadsheets("path/to/csv_files/fil{key}.csv", "csv")

        assert len(result) == 2
        assert "e1" in result
        assert "e2" in result
        assert isinstance(result["e1"], pl.LazyFrame)
        assert isinstance(result["e2"], pl.LazyFrame)


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
