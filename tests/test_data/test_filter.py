import unittest

import pandas as pd
import polars as pl
import pytest

import nhs.data.filter

filter_sa1_regions = nhs.data.filter.filter_sa1_regions


class TestFilterSa1RegionCodes:

    # Fixture to create a sample LazyFrame
    @pytest.fixture
    def sample_lazyframe(self):
        data = {
            "SA1_CODE_2021": ["123456", "789012", "345678", "901234", "567890"],
            "value": [10, 20, 30, 40, 50],
        }
        return pl.DataFrame(data).lazy()

    def test_filter_with_valid_region_codes(self, sample_lazyframe: pl.LazyFrame):
        # Filtering with valid region codes
        result = filter_sa1_regions(
            sample_lazyframe, ["123456", "901234"], "SA1_CODE_2021"
        ).collect()

        expected_data = {"SA1_CODE_2021": ["123456", "901234"], "value": [10, 40]}

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_empty_region_codes(self, sample_lazyframe: pl.LazyFrame):
        # Test with empty region codes (should return an empty LazyFrame)
        result = filter_sa1_regions(sample_lazyframe, [], "SA1_CODE_2021").collect()

        # Expect an empty DataFrame when no region codes are provided
        expected = pl.DataFrame({"SA1_CODE_2021": [], "value": []})

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_codes(self, sample_lazyframe: pl.LazyFrame):
        # Test with region codes that don't match any rows (should return an empty DataFrame)
        result = filter_sa1_regions(
            sample_lazyframe, ["999999"], "SA1_CODE_2021"
        ).collect()

        expected = pl.DataFrame({"SA1_CODE_2021": [], "value": []})
        assert result.to_dicts() == expected.to_dicts()


# Sample data for tests
data = {
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["New York", "Los Angeles", "Chicago"],
}


def filter_relevant_column(df: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame:
    """
    Filter columns from a LazyFrame based on the provided column names.
    If a column does not exist, it will be ignored.
    """
    existing_columns = [col for col in columns if col in df.columns]
    return df.select(existing_columns)


class TestFilterRelevantColumn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.df = pl.DataFrame(data).lazy()

    def assertFrameEqual(self, df1: pl.LazyFrame, df2: pl.LazyFrame):
        """Helper function to assert two Polars DataFrames are equal."""
        df1_collected = df1.collect().to_pandas()
        df2_collected = df2.collect().to_pandas()

        # Compare shapes
        self.assertEqual(df1_collected.shape, df2_collected.shape)

        # Sort columns for comparison
        df1_collected = df1_collected.sort_index(axis=1)
        df2_collected = df2_collected.sort_index(axis=1)

        # Compare contents
        pd.testing.assert_frame_equal(df1_collected, df2_collected)

    def test_filter_relevant_column_basic(self):
        columns_to_retain = ["name", "city"]
        filtered_df = filter_relevant_column(self.df, columns_to_retain)
        expected_df = pl.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "city": ["New York", "Los Angeles", "Chicago"],
            }
        ).lazy()

        self.assertFrameEqual(filtered_df, expected_df)

    def test_filter_relevant_column_columns_not_present(self):
        columns_to_retain = ["name", "country"]
        filtered_df = filter_relevant_column(self.df, columns_to_retain)
        expected_df = pl.DataFrame({"name": ["Alice", "Bob", "Charlie"]}).lazy()

        self.assertFrameEqual(filtered_df, expected_df)

    def test_filter_relevant_column_empty_columns_list(self):
        columns_to_retain = []
        filtered_df = filter_relevant_column(self.df, columns_to_retain)
        filtered_collected = filtered_df.collect().to_pandas()

        # Adjusted expected shape
        self.assertEqual(
            filtered_collected.shape, (0, 0)
        )  # Expect 0 rows and 0 columns

    def test_filter_relevant_column_no_columns(self):
        columns_to_retain = ["age", "name", "city"]
        filtered_df = filter_relevant_column(self.df, columns_to_retain)
        expected_df = pl.DataFrame(data).lazy()  # Expect the whole DataFrame

        self.assertFrameEqual(filtered_df, expected_df)


if __name__ == "__main__":
    unittest.main()
