from gzip import READ

import polars as pl
from pytest_mock import MockerFixture

import nhs.data.handling

read_spreadsheets = nhs.data.handling.read_spreadsheets
read_xlsx = nhs.data.handling.read_xlsx
standardize_names = nhs.data.handling.standardize_names
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

    # Read one xlsx file with default sheet_id.
    def test_read_xlsx_with_default_sheet_id(self, mocker):
        mocker.patch(
            LIST_FILES_PATCH,
            return_value="Path/to/xlsx/test.xlsx",
        )
        mock_lazy_frame = mocker.Mock(spec=pl.LazyFrame)
        mock_lazy_frame.lazy.return_value = mock_lazy_frame
        mocker.patch("polars.read_excel", return_value=mock_lazy_frame)

        result = read_xlsx("Path/to/xlsx", 1)
        assert result == mock_lazy_frame
        assert isinstance(result, pl.LazyFrame)

    # Read non_existent_file_path with different sheet_id.
    def test_non_existent_file_path(self, mocker):
        mocker.patch(
            LIST_FILES_PATCH,
            return_value="Path/to/xlsx/non_existent_file.xlsx",
        )
        mocker.patch("polars.read_excel", side_effect=FileNotFoundError)
        result = read_xlsx("Path/to/xlsx/", 1)
        assert result is None
        mocker.patch("polars.read_excel", side_effect=Exception("File not found"))
        result2 = read_xlsx("Path/to/xlsx/", 0)
        assert result2 is None

    # Successfully read multiple sheets from an .xlsx file and return a dictionary of LazyFrames
    def test_read_multiple_sheets(self, mocker):
        mock_lazy_frame1 = mocker.Mock(spec=pl.LazyFrame)
        mock_lazy_frame2 = mocker.Mock(spec=pl.LazyFrame)
        mock_lazy_frame1.lazy.return_value = mock_lazy_frame1
        mock_lazy_frame2.lazy.return_value = mock_lazy_frame2
        mocker.patch(LIST_FILES_PATCH, return_value="Path/to/xlsx/file.xlsx")
        mocker.patch(
            "polars.read_excel",
            return_value={"Sheet1": mock_lazy_frame1, "Sheet2": mock_lazy_frame2},
        )

        result = read_xlsx("Path/to/xlsx/file.xlsx", 0)

        assert isinstance(result, dict)
        for key, value in result.items():
            assert isinstance(key, str)
            assert isinstance(value, pl.LazyFrame)

    # Successfully read multiple sheets from an .xlsx file and return a dictionary of LazyFrames.
    def test_read_multiple_sheets_with_sheet_id_equals_0(self, mocker):
        mock_sheet_data = {
            "Cell Descriptors Information": pl.DataFrame(
                {
                    "DataPackfile": ["G01", "G02"],
                    "Short": ["A", "C"],
                    "Long": ["Alpha", "Charlie"],
                }
            ),
            "Table Number, Name, Population": pl.DataFrame(
                {
                    "DataPackfile": ["G03", "G04"],
                    "Short": ["B", "D"],
                    "Long": ["Banana", "Ddddd"],
                }
            ),
        }

        mock_lazy_frame_data = {k: v.lazy() for k, v in mock_sheet_data.items()}

        mocker.patch(
            LIST_FILES_PATCH,
            return_value="Path/to/xlsx/Metadata_2021_GCP_DataPack_R1_R2.xlsx",
        )

        mocker.patch("polars.read_excel", return_value=mock_lazy_frame_data)

        expected_keys = [
            "Cell Descriptors Information",
            "Table Number, Name, Population",
        ]

        result = read_xlsx("Path/to/xlsx/", 0)

        assert isinstance(result, dict)
        assert sorted(result.keys()) == sorted(expected_keys)
        for key, value in result.items():
            assert isinstance(key, str)
            assert isinstance(value, pl.LazyFrame)


class TestColumnReadable:
    # Standardize column names correctly when all parameters are valid
    def test_standardize_names_valid_parameters(self, mocker):
        df_dict = {
            "G01": pl.LazyFrame({"short_col": [1, 2, 3]}),
            "G02": pl.LazyFrame({"short_col": [4, 5, 6]}),
        }
        census_metadata = pl.LazyFrame(
            {
                "identification": ["G01", "G02"],
                "abbreviation_column_name": ["short_col", "short_col"],
                "long_column_name": ["long_col", "long_col"],
            }
        )
        expected_df_dict = {
            "G01": pl.LazyFrame({"long_col": [1, 2, 3]}),
            "G02": pl.LazyFrame({"long_col": [4, 5, 6]}),
        }

        result = standardize_names(
            df_dict,
            census_metadata,
            "identification",
            "abbreviation_column_name",
            "long_column_name",
        )

        assert isinstance(result, dict)
        for key in result:
            assert isinstance(result[key], pl.LazyFrame)
            assert isinstance(expected_df_dict[key], pl.LazyFrame)

            result_df = result[key].collect()
            expected_df = expected_df_dict[key].collect()

            assert result_df.equals(expected_df)
