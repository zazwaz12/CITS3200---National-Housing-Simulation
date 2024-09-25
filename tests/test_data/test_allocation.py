import polars as pl
import pytest
from ..context import nhs

join_census_with_coords = nhs.data.join_census_with_coords
sample_census_feature = nhs.data.sample_census_feature
randomly_assign_census_features = nhs.data.randomly_assign_census_features
calculate_proportions = nhs.data.calculate_proportions


class TestJoinCensusWithCoords:
    # Joins two LazyFrames on specified columns successfully
    def test_join_successful(self):
        # Create sample data
        census_data = {"CODE_2021": ["123", "45", "3"], "feature1": [24, 65, 234]}
        coords_data = {
            "CODE21": [123, 45, 3],
            "longitude": [134.56, 456.12, 21.124],
            "latitude": [-34.56, -23.12, 12.124],
        }

        # Create LazyFrames
        census = pl.LazyFrame(census_data)
        coords = pl.LazyFrame(coords_data)

        # Perform join
        result = join_census_with_coords(
            census, coords, left_code_col="CODE_2021", right_code_col="CODE21"
        )

        # Collect result to DataFrame for assertion
        result_df = result.collect()

        # Expected result
        expected_df = pl.DataFrame(
            {
                "CODE_2021": [123, 45, 3],
                "feature1": [24, 65, 234],
                "longitude": [134.56, 456.12, 21.124],
                "latitude": [-34.56, -23.12, 12.124],
            }
        )

        # Assert the result matches the expected DataFrame
        assert result_df.equals(expected_df)

    # Check that inner join discards rows with no matching values
    def test_inner_join_discards_row(self):
        # Create sample data
        census_data = {"CODE_2021": ["124", "45", "3"], "feature1": [24, 65, 234]}
        coords_data = {
            "CODE21": [123, 45, 3],
            "longitude": [134.56, 456.12, 21.124],
            "latitude": [-34.56, -23.12, 12.124],
        }

        # Create LazyFrames
        census = pl.LazyFrame(census_data)
        coords = pl.LazyFrame(coords_data)

        # Perform join
        result = join_census_with_coords(
            census, coords, left_code_col="CODE_2021", right_code_col="CODE21"
        )

        # Collect result to DataFrame for assertion
        result_df = result.collect()

        # Expected result
        expected_df = pl.DataFrame(
            {
                "CODE_2021": [45, 3],
                "feature1": [65, 234],
                "longitude": [456.12, 21.124],
                "latitude": [-23.12, 12.124],
            }
        )

        # Assert the result matches the expected DataFrame
        assert result_df.equals(expected_df)

    # One or both LazyFrames are empty
    def test_empty_lazyframes(self):
        # Create empty LazyFrames
        census = pl.LazyFrame({"SA1_CODE_2021": [], "data": []})
        coords = pl.LazyFrame({"SA1_CODE21": [], "coords": []})

        # Perform join
        result = join_census_with_coords(census, coords)

        # Collect result to DataFrame for assertion
        result_df = result.collect()

        # Expected result is an empty DataFrame with the same schema
        expected_df = pl.DataFrame({"SA1_CODE_2021": [], "data": [], "coords": []})

        # Assert the result matches the expected empty DataFrame
        assert result_df.equals(expected_df)


class TestSampleCensusFeature:
    # Correctly samples rows based on feature_col values
    def test_correct_sampling_based_on_feature_col(self):
        pl.set_random_seed(42)

        # Create mock data
        data = {
            "code_col": ["A", "A", "A", "A", "A", "A", "B", "B", "B", "B"],
            "long_col": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "lat_col": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
            "feature_col": [4, 4, 4, 4, 4, 4, 2, 2, 2, 2],
        }
        census = pl.LazyFrame(data)

        # Call the function
        result = sample_census_feature(
            census, "code_col", "long_col", "lat_col", "feature_col"
        )

        # Collect the result
        result_df = result.collect()

        count = result_df.group_by("code_col").agg([pl.col("feature_col").sum()])

        # Assertions
        assert isinstance(result, pl.LazyFrame)
        assert result_df.shape == (4 + 2, 4)
        assert count.filter(pl.col("code_col") == "A").select("feature_col").item() == 4
        assert count.filter(pl.col("code_col") == "B").select("feature_col").item() == 2

    # Handles empty LazyFrame input
    def test_handles_empty_lazyframe_input(self):
        # Create an empty LazyFrame
        census = pl.LazyFrame(
            {"code_col": [], "long_col": [], "lat_col": [], "feature_col": []}
        )

        # Call the function
        result = sample_census_feature(
            census, "code_col", "long_col", "lat_col", "feature_col"
        )

        # Collect the result
        result_df = result.collect()

        # Assertions
        assert result_df.equals(census.collect())

    # Correctly samples rows based on feature_col values
    def test_sampling_fewer_coords_than_sample_size(self):
        pl.set_random_seed(42)

        # Create mock data
        data = {
            "code_col": ["A", "A", "A", "B"],
            "long_col": [1.0, 2.0, 3.0, 4.0],
            "lat_col": [10.0, 20.0, 30.0, 40.0],
            "feature_col": [4, 4, 4, 2],
        }
        census = pl.LazyFrame(data)

        # Call the function
        result = sample_census_feature(
            census, "code_col", "long_col", "lat_col", "feature_col"
        )

        # Collect the result
        result_df = result.collect()

        count = result_df.group_by("code_col").agg([pl.col("feature_col").sum()])

        # Assertions
        assert isinstance(result, pl.LazyFrame)
        assert result_df.shape == (4 + 2, 4)
        assert count.filter(pl.col("code_col") == "A").select("feature_col").item() == 4
        assert count.filter(pl.col("code_col") == "B").select("feature_col").item() == 2


class TestRandomlyAssignCensusFeatures:
    # Correctly assigns census features to GNAF coordinates
    def test_correct_assignment_of_census_features(self):
        # Create mock data
        data = {
            "code_col": ["A", "A", "B", "B"],
            "long_col": [1.0, 1.1, 2.0, 2.1],
            "lat_col": [1.0, 1.1, 2.0, 2.1],
            "feature_1": [7, 7, 12, 12],
            "feature_2": [3, 3, 4, 4],
            "feature_3": [5, 5, 6, 6],
        }
        census = pl.LazyFrame(data)

        # Call the function
        result = randomly_assign_census_features(
            census,
            code_col="code_col",
            long_col="long_col",
            lat_col="lat_col",
            feature_cols=["feature_1", "feature_2", "feature_3"],
        )

        count = result.group_by("code_col").agg(
            [pl.col("feature_1", "feature_2", "feature_3").sum()]
        )

        # Assertions
        assert isinstance(result, pl.LazyFrame)
        collected_result = result.collect()
        assert set(collected_result.columns) == {
            "person_id",
            "code_col",
            "long_col",
            "lat_col",
            "feature_1",
            "feature_2",
            "feature_3",
        }
        assert (
            count.filter(pl.col("code_col") == "A").select("feature_1").collect().item()
            == 7
        )
        assert (
            count.filter(pl.col("code_col") == "B").select("feature_1").collect().item()
            == 12
        )
        assert (
            count.filter(pl.col("code_col") == "A").select("feature_2").collect().item()
            == 3
        )
        assert (
            count.filter(pl.col("code_col") == "B").select("feature_2").collect().item()
            == 4
        )
        assert (
            count.filter(pl.col("code_col") == "A").select("feature_3").collect().item()
            == 5
        )
        assert (
            count.filter(pl.col("code_col") == "B").select("feature_3").collect().item()
            == 6
        )

    # Handles empty census LazyFrame
    def test_handles_empty_census_lazyframe(self):
        # Create empty LazyFrame
        census = pl.LazyFrame(
            {
                "code_col": [],
                "long_col": [],
                "lat_col": [],
                "feature_1": [],
                "feature_2": [],
            }
        )

        # Call the function
        result = randomly_assign_census_features(
            census,
            code_col="code_col",
            long_col="long_col",
            lat_col="lat_col",
            feature_cols=["feature_1", "feature_2"],
        )

        # Assertions
        assert isinstance(result, pl.LazyFrame)
        collected_result = result.collect()
        assert collected_result.shape == (0, 6)

class TestCalculateProportions:
    def test_multiple_groups(self):
        # Input grouping dictionary
        grouping = {"A": 4, "A": 4, "B": 2, "C": 1}
        
        # Run the function to get the LazyFrame and collect the result
        lf = calculate_proportions(grouping)
        result = lf.collect()

        # Expected DataFrame
        expected = pl.DataFrame({
            "group_col": ["A", "B", "C"],
            "value_col": [4, 2, 1],  # Ensure value_col is consistent with integer values
            "proportion": [0.571, 0.286, 0.143]  # Adjusted to match the rounding behavior in the function
        })

        # Print the result (optional)
        print(result)

        # Access the result columns using the correct methods for Polars
        group_col_result = result.get_column("group_col").to_list()
        value_col_result = result.get_column("value_col").to_list()
        proportion_result = result.get_column("proportion").to_list()

        # Assert the columns match as expected
        assert group_col_result == expected['group_col'].to_list()
        assert value_col_result == expected['value_col'].to_list()
        assert proportion_result == expected['proportion'].to_list()

    def test_same_value_groups(self):
        # Input grouping dictionary with sum of values for keys
        grouping = {"A": 3, "A":3, "B":3, "B": 3}  # Aggregated values (3+3 for A, 3+3 for B)

        # Run the function to get the LazyFrame and collect the result
        lf = calculate_proportions(grouping)
        result = lf.collect()  # Collect to DataFrame for comparison

        # Expected DataFrame
        expected = pl.DataFrame({
            "group_col": ["A", "B"],
            "value_col": [3, 3],  # Total values for A and B
            "proportion": [0.5, 0.5]  # Proportion of total
        })

        # Access the result columns using Polars methods
        group_col_result = result.get_column("group_col").to_list()
        value_col_result = result.get_column("value_col").to_list()
        proportion_result = result.get_column("proportion").to_list()

        # Assert the columns match as expected
        assert group_col_result == expected['group_col'].to_list()
        assert value_col_result == expected['value_col'].to_list()
        assert proportion_result == expected['proportion'].to_list()

    def test_empty_input(self):
        with pytest.raises(ValueError) as excinfo:
            calculate_proportions({})  # Call the function with an empty dictionary

        # Assert that the error message is as expected
        assert str(excinfo.value) == "The grouping dictionary is empty."
