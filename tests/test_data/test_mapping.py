import polars as pl
import pytest

from ..context import nhs 

map_state_to_sa1_codes = nhs.data.mapping.map_state_to_sa1_codes


class TestMapStateToSa1Codes:
    
    # Fixture to create a sample LazyFrame
    @pytest.fixture
    def sample_lazyframe(self):
        """
        Sample LazyFrame with SA1 codes and corresponding values.
        """
        data = {
            "SA1_CODE_2021": ["101234567", "201234567", "301234567", "401234567", "501234567"],
            "value": [100, 200, 300, 400, 500],
        }
        return pl.DataFrame(data).lazy()

    def test_filter_with_valid_state(self, sample_lazyframe):
        """
        Test filtering using a valid state (e.g., NSW).
        """
        result = map_state_to_sa1_codes(sample_lazyframe, ["NSW"], "SA1_CODE_2021").collect()

        expected_data = {
            "SA1_CODE_2021": ["101234567"],
            "value": [100]
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_state(self, sample_lazyframe):
        """
        Test filtering with a state that doesn't match any SA1 codes.
        """
        result = map_state_to_sa1_codes(sample_lazyframe, ["NT"], "SA1_CODE_2021").collect()

        expected = pl.DataFrame({"SA1_CODE_2021": [], "value": []})

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_other_territories(self, sample_lazyframe):
        """
        Test filtering using 'Other Territories'.
        """
        result = map_state_to_sa1_codes(sample_lazyframe, ["Other Territories"], "SA1_CODE_2021").collect()

        expected = pl.DataFrame({"SA1_CODE_2021": [], "value": []})

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_invalid_state(self, sample_lazyframe):
        """
        Test filtering with an invalid state name, should return an empty LazyFrame or handle error.
        """
        result = map_state_to_sa1_codes(sample_lazyframe, ["ABC"], "SA1_CODE_2021").collect()

        # Since 'ABC' is not a valid state, the result should be empty
        expected = pl.DataFrame({"SA1_CODE_2021": [], "value": []})
        
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_multiple_states(self, sample_lazyframe):
        """
        Test filtering with multiple states at the same time.
        """
        states = ["NSW", "VIC"]
        result = map_state_to_sa1_codes(sample_lazyframe, states, "SA1_CODE_2021").collect()

        expected_data = {
            "SA1_CODE_2021": ["101234567", "201234567"],
            "value": [100, 200]
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()
