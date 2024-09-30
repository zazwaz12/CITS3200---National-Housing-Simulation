from unittest.mock import patch

import polars as pl
import pytest

from ..context import nhs

load_gnaf_files_by_states = nhs.data.filter.load_gnaf_files_by_states
filter_and_join_gnaf_frames = nhs.data.filter.filter_and_join_gnaf_frames
filter_sa1_regions = nhs.data.filter.filter_sa1_regions
read_spreadsheets = nhs.data.handling.read_spreadsheets



@pytest.fixture
def sample_geocode_data():
    return pl.DataFrame({
        "ADDRESS_DETAIL_PID": ["1001", "1002"],
        "LATITUDE": [34.5, 35.0],
        "LONGITUDE": [150.3, 149.1],
    }).lazy()


@pytest.fixture
def sample_detail_data():
    return pl.DataFrame({
        "ADDRESS_DETAIL_PID": ["1001", "1002"],
        "FLAT_TYPE_CODE": ["flat", "unit"],
        "POSTCODE": [2000, 2600],
    }).lazy()


class TestLoadGnafFilesByStates:
    
    @patch("nhs.data.filter.read_spreadsheets")
    def test_load_files_for_valid_states(self, mock_read_spreadsheets, sample_geocode_data, sample_detail_data):
        mock_read_spreadsheets.return_value = {
            "NSW_ADDRESS_DEFAULT_GEOCODE.parquet": sample_geocode_data,
            "NSW_ADDRESS_DETAIL.parquet": sample_detail_data,
        }

        result_geocode_lf, result_detail_lf = load_gnaf_files_by_states("/fake/path", ["NSW"])

        expected_geocode = pl.DataFrame({
            "ADDRESS_DETAIL_PID": ["1001", "1002"],
            "LATITUDE": [34.5, 35.0],
            "LONGITUDE": [150.3, 149.1],
            "STATE": ["NSW", "NSW"],
        })

        expected_detail = pl.DataFrame({
            "ADDRESS_DETAIL_PID": ["1001", "1002"],
            "FLAT_TYPE_CODE": ["flat", "unit"],
            "POSTCODE": [2000, 2600],
        })

        assert result_geocode_lf.collect().to_dicts() == expected_geocode.to_dicts()
        assert result_detail_lf.collect().to_dicts() == expected_detail.to_dicts()

    @patch("nhs.data.filter.read_spreadsheets")
    def test_load_files_with_no_matching_states(self, mock_read_spreadsheets):
        mock_read_spreadsheets.return_value = {}
        result_geocode_lf, result_detail_lf = load_gnaf_files_by_states("/fake/path", ["VIC"])

        expected_geocode = pl.DataFrame({"ADDRESS_DETAIL_PID": [], "LATITUDE": [], "LONGITUDE": [], "STATE": []})
        expected_detail = pl.DataFrame({"ADDRESS_DETAIL_PID": [], "FLAT_TYPE_CODE": [], "POSTCODE": []})

        assert result_geocode_lf.collect().to_dicts() == expected_geocode.to_dicts()
        assert result_detail_lf.collect().to_dicts() == expected_detail.to_dicts()

    @patch("nhs.data.filter.read_spreadsheets")
    def test_load_files_for_multiple_states(self, mock_read_spreadsheets):
        mock_read_spreadsheets.return_value = {
            "NSW_ADDRESS_DEFAULT_GEOCODE.parquet": pl.DataFrame({
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "LATITUDE": [34.5, 35.0],
                "LONGITUDE": [150.3, 149.1],
                "STATE": ["NSW", "NSW"],
            }).lazy(),
            "ACT_ADDRESS_DEFAULT_GEOCODE.parquet": pl.DataFrame({
                "ADDRESS_DETAIL_PID": ["1234", "4321"],
                "LATITUDE": [33.9, 34.4],
                "LONGITUDE": [149.8, 150.1],
                "STATE": ["ACT", "ACT"],
            }).lazy(),
            "NSW_ADDRESS_DETAIL.parquet": pl.DataFrame({
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "FLAT_TYPE_CODE": ["flat", "unit"],
                "POSTCODE": [2000, 2600],
            }).lazy(),
            "ACT_ADDRESS_DETAIL.parquet": pl.DataFrame({
                "ADDRESS_DETAIL_PID": ["1234", "4321"],
                "FLAT_TYPE_CODE": ["apartment", "house"],
                "POSTCODE": [2610, 2620],
            }).lazy(),
        }

        result_geocode_lf, result_detail_lf = load_gnaf_files_by_states("/fake/path", ["NSW", "ACT"])

        expected_geocode = pl.DataFrame({
            "ADDRESS_DETAIL_PID": ["1001", "1002", "1234", "4321"],
            "LATITUDE": [34.5, 35.0, 33.9, 34.4],
            "LONGITUDE": [150.3, 149.1, 149.8, 150.1],
            "STATE": ["NSW", "NSW", "ACT", "ACT"],
        })

        expected_detail = pl.DataFrame({
            "ADDRESS_DETAIL_PID": ["1001", "1002", "1234", "4321"],
            "FLAT_TYPE_CODE": ["flat", "unit", "apartment", "house"],
            "POSTCODE": [2000, 2600, 2610, 2620],
        })

        assert result_geocode_lf.collect().to_dicts() == expected_geocode.to_dicts()
        assert result_detail_lf.collect().to_dicts() == expected_detail.to_dicts()


class TestFilterAndJoinGnafFrames:

    @pytest.fixture
    def default_geocode_data(self):
        return pl.DataFrame({
            "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
            "LATITUDE": [34.5, 35.0, 36.0],
            "LONGITUDE": [150.3, 149.1, 148.5],
        }).lazy()

    @pytest.fixture
    def address_detail_data(self):
        return pl.DataFrame({
            "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
            "FLAT_TYPE_CODE": ["Flat", None, "Unit"],
            "POSTCODE": [2000, 2600, 3000],
        }).lazy()

    @pytest.mark.parametrize("building_types,postcodes,expected_pids", [
        ([], [], ["1001", "1002", "1003"]),  # No filters
        (["flat"], [], ["1001"]),  # Filter by building type
        ([], [2600], ["1002"]),  # Filter by postcode
        (["unit"], [3000], ["1003"]),  # Filter by both building type and postcode
    ])
    def test_filter_and_join(self, default_geocode_data, address_detail_data, building_types, postcodes, expected_pids):
        result_lf = filter_and_join_gnaf_frames(default_geocode_data, address_detail_data, building_types, postcodes)
        result_pids = result_lf.collect().select("ADDRESS_DETAIL_PID").to_series().to_list()

        assert result_pids == expected_pids

    def test_no_matching_filters(self, default_geocode_data, address_detail_data):
        result_lf = filter_and_join_gnaf_frames(default_geocode_data, address_detail_data, building_types=["apartment"])
        assert result_lf.collect().height == 0


class TestFilterSa1RegionCodes:

    # Fixture to create a sample LazyFrame
    @pytest.fixture
    def sample_lazyframe(self):
        data = {
            "SA1_CODE21": [123456, 789012, 345678, 901234, 567890],
            "value": [10, 20, 30, 40, 50],
        }
        return pl.DataFrame(data).lazy()

    def test_filter_with_valid_region_codes(self, sample_lazyframe):
        # Filtering with valid region codes
        result = filter_sa1_regions(
            sample_lazyframe, [123456, 901234], "SA1_CODE21"
        ).collect()

        expected_data = {"SA1_CODE21": [123456, 901234], "value": [10, 40]}

        expected = pl.DataFrame(expected_data)
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_empty_region_codes(self, sample_lazyframe):
        # Test with empty region codes (should return the original LazyFrame)
        result = filter_sa1_regions(sample_lazyframe, [], "SA1_CODE21").collect()

        # Expect the original DataFrame when no region codes are provided
        expected = sample_lazyframe.collect()

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_codes(self, sample_lazyframe):
        # Test with region codes that don't match any rows (should return an empty DataFrame)
        result = filter_sa1_regions(sample_lazyframe, [999999], "SA1_CODE21").collect()

        expected = pl.DataFrame({"SA1_CODE21": [], "value": []})
        assert result.to_dicts() == expected.to_dicts()
