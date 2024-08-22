from gzip import READ
from pytest_mock import MockerFixture
import polars as pl
from ..context import nhs

read_all_psv = nhs.data.handling.read_all_psv

LIST_FILES_PATCH = "nhs.data.handling.list_files"
READ_PSV_PATCH = "nhs.data.handling.read_psv"


class TestReadAllPsv:

    # Reads all .psv files in a given directory and returns a dictionary of LazyFrames
    def test_reads_all_psv_files(self, mocker: MockerFixture):
        mocker.patch(
            LIST_FILES_PATCH,
            return_value=["path/to/psv_files/file1.psv", "path/to/psv_files/file2.psv"],
        )
        mocker.patch(READ_PSV_PATCH, side_effect=[pl.LazyFrame(), pl.LazyFrame()])

        result = read_all_psv("path/to/psv_files/")

        assert len(result) == 2
        assert "file1.psv" in result
        assert "file2.psv" in result
        assert isinstance(result["file1.psv"], pl.LazyFrame)
        assert isinstance(result["file2.psv"], pl.LazyFrame)

    # Directory contains no .psv files
    def test_no_psv_files_in_directory(self, mocker: MockerFixture):
        mock_list_files = mocker.patch(LIST_FILES_PATCH)
        mock_list_files.return_value = []

        result = read_all_psv("path/to/psv_files/")

        assert result == {}

    # Uses specified placeholder to extract keys from file names
    def test_read_all_psv_with_placeholder(self, mocker: MockerFixture):
        mocker.patch(
            LIST_FILES_PATCH,
            return_value=["path/to/psv_files/file1.psv", "path/to/psv_files/file2.psv"],
        )
        mocker.patch(READ_PSV_PATCH, side_effect=[pl.LazyFrame(), pl.LazyFrame()])

        result = read_all_psv("path/to/psv_files/fil{key}.psv")

        assert len(result) == 2
        assert "e1" in result
        assert "e2" in result
        assert isinstance(result["e1"], pl.LazyFrame)
        assert isinstance(result["e2"], pl.LazyFrame)
