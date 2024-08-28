from pytest_mock import MockerFixture
import os
from ..context import nhs

PATCH_OS_WALK = "os.walk"

list_files = nhs.utils.path.list_files


class TestListFiles:

    # returns all files in a directory
    def test_returns_all_files_in_directory(self, mocker: MockerFixture):
        mocker.patch(
            PATCH_OS_WALK,
            return_value=[
                ("/path", ("subdir",), ("file1.txt", "file2.txt", "file$pec!al.txt")),
                ("/path/subdir", (), ("file3.txt",)),
            ],
        )

        result = list(list_files("/path"))
        expected = [
            "/path/file1.txt",
            "/path/file2.txt",
            "/path/file$pec!al.txt",
            "/path/subdir/file3.txt",
        ]
        result = [os.path.normpath(path) for path in result]
        expected = [os.path.normpath(path) for path in expected]
        assert result == expected

    # empty directory returns no files
    def test_empty_directory_returns_no_files(self, mocker: MockerFixture):
        mocker.patch(PATCH_OS_WALK, return_value=[("/empty_path", (), ())])

        result = list(list_files("/empty_path"))
        assert result == []

    # directory with only subdirectories returns no files
    def test_directory_with_only_subdirectories_returns_no_files(
        self, mocker: MockerFixture
    ):
        mocker.patch(
            PATCH_OS_WALK,
            return_value=[
                ("/path", ("subdir1", "subdir2"), ()),
                ("/path/subdir1", (), ()),
                ("/path/subdir2", (), ()),
            ],
        )

        result = list(list_files("/path"))
        expected = []
        assert list(result) == expected

    # handles directories with hidden files
    def test_handles_directories_with_hidden_files(self, mocker: MockerFixture):
        mocker.patch(
            PATCH_OS_WALK,
            return_value=[
                ("/path", ("subdir",), ("file1.txt", ".hidden_file", "file2.txt")),
                ("/path/subdir", (), ("file3.txt",)),
            ],
        )

        result = list(list_files("/path"))
        expected = ["/path/file1.txt", "/path/file2.txt", "/path/subdir/file3.txt"]

        result = [os.path.normpath(path) for path in result]
        expected = [os.path.normpath(path) for path in expected]

        assert result == expected
