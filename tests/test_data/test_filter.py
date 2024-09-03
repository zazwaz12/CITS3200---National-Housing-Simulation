import pytest
import polars as pl


from ..context import nhs

filter_building_types = nhs.data.filter.filter_building_types


class TestFilterBuildingTypes:
  
    # Fixture to create a sample LazyFrame
    @pytest.fixture
    def sample_lazyframe(self):
        data = {
            "CODE": ["APT", "HSE", "OFFC", "BLDG", "UNIT"],
            "NAME": ["Apartment", "House", "Office", "Building", "Unit"],
            "DESCRIPTION": ["Apartment Description", "House Description", "Office Description", "Building Description", "Unit Description"]
        }
        return pl.DataFrame(data).lazy()

    def test_filter_with_valid_building_types(self, sample_lazyframe):
        # Filtering with valid building types
        result = filter_building_types(sample_lazyframe, "CODE", ["APT", "HSE"]).collect()
        
        expected_data = {
            "CODE": ["APT", "HSE"],
            "NAME": ["Apartment", "House"],
            "DESCRIPTION": ["Apartment Description", "House Description"]
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_empty_building_types(self, sample_lazyframe):
        # Test with empty building types (should return the original LazyFrame)
        result = filter_building_types(sample_lazyframe).collect()
        
        expected_data = {
            "CODE": ["APT", "HSE", "OFFC", "BLDG", "UNIT"],
            "NAME": ["Apartment", "House", "Office", "Building", "Unit"],
            "DESCRIPTION": ["Apartment Description", "House Description", "Office Description", "Building Description", "Unit Description"]
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_building_types(self, sample_lazyframe):
        # Test with building types that don't match any rows (should return an empty DataFrame)
        result = filter_building_types(sample_lazyframe, "CODE", ["XYZ"]).collect()
        
        expected = pl.DataFrame({
            "CODE": [],
            "NAME": [],
            "DESCRIPTION": []
        })
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_mixed_valid_and_invalid_building_types(self, sample_lazyframe):
        # Test with a mix of valid and invalid building types
        result = filter_building_types(sample_lazyframe, "CODE", ["APT", "XYZ"]).collect()
        
        expected_data = {
            "CODE": ["APT"],
            "NAME": ["Apartment"],
            "DESCRIPTION": ["Apartment Description"]
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()
