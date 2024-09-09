import polars as pl
import pytest

from ..context import nhs

filter_sa1_regions = nhs.data.filter.filter_sa1_regions
filter_building_types = nhs.data.filter.filter_building_types

class TestFilterSa1RegionCodes:

    # Fixture to create a sample LazyFrame
    @pytest.fixture
    def sample_lazyframe(self):
        data = {
            "SA1_CODE_2021": ["123456", "789012", "345678", "901234", "567890"],
            "value": [10, 20, 30, 40, 50],
        }
        return pl.DataFrame(data).lazy()

    def test_filter_with_valid_region_codes(self, sample_lazyframe):
        # Filtering with valid region codes
        result = filter_sa1_regions(
            sample_lazyframe, ["123456", "901234"], "SA1_CODE_2021"
        ).collect()

        expected_data = {"SA1_CODE_2021": ["123456", "901234"], "value": [10, 40]}

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_empty_region_codes(self, sample_lazyframe):
        # Test with empty region codes (should return an empty LazyFrame)
        result = filter_sa1_regions(sample_lazyframe, [], "SA1_CODE_2021").collect()

        # Expect an empty DataFrame when no region codes are provided
        expected = pl.DataFrame({"SA1_CODE_2021": [], "value": []})

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_codes(self, sample_lazyframe):
        # Test with region codes that don't match any rows (should return an empty DataFrame)
        result = filter_sa1_regions(
            sample_lazyframe, ["999999"], "SA1_CODE_2021"
        ).collect()

        expected = pl.DataFrame({"SA1_CODE_2021": [], "value": []})
        assert result.to_dicts() == expected.to_dicts()




class TestFilterBuildingTypes:

    # Fixture to create a sample LazyFrame for building type tests
    @pytest.fixture
    def sample_lazyframe_building(self):
        data = {
            "CODE": ["APT", "HSE", "OFFC", "BLDG", "UNIT"],
            "NAME": ["Apartment", "House", "Office", "Building", "Unit"],
            "DESCRIPTION": [
                "Apartment Description",
                "House Description",
                "Office Description",
                "Building Description",
                "Unit Description",
            ],
        }
        return pl.DataFrame(data).lazy()

    def test_filter_with_valid_building_types(self, sample_lazyframe_building):
        # Filtering with valid building types
        result = filter_building_types(
            sample_lazyframe_building, "CODE", ["APT", "HSE"]
        ).collect()

        expected_data = {
            "CODE": ["APT", "HSE"],
            "NAME": ["Apartment", "House"],
            "DESCRIPTION": ["Apartment Description", "House Description"],
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_empty_building_types(self, sample_lazyframe_building):
        # Test with empty building types (should return the original LazyFrame)
        result = filter_building_types(sample_lazyframe_building).collect()

        expected_data = {
            "CODE": ["APT", "HSE", "OFFC", "BLDG", "UNIT"],
            "NAME": ["Apartment", "House", "Office", "Building", "Unit"],
            "DESCRIPTION": [
                "Apartment Description",
                "House Description",
                "Office Description",
                "Building Description",
                "Unit Description",
            ],
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_building_types(self, sample_lazyframe_building):
        # Test with building types that don't match any rows (should return an empty DataFrame)
        result = filter_building_types(sample_lazyframe_building, "CODE", ["XYZ"]).collect()

        expected = pl.DataFrame({"CODE": [], "NAME": [], "DESCRIPTION": []})
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_mixed_valid_and_invalid_building_types(
        self, sample_lazyframe_building
    ):
        # Test with a mix of valid and invalid building types
        result = filter_building_types(
            sample_lazyframe_building, "CODE", ["APT", "XYZ"]
        ).collect()

        expected_data = {
            "CODE": ["APT"],
            "NAME": ["Apartment"],
            "DESCRIPTION": ["Apartment Description"],
        }

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()