from unittest.mock import patch

import polars as pl
import pytest

from ..context import nhs

load_gnaf_files_by_states = nhs.data.filter.load_gnaf_files_by_states
filter_and_join_gnaf_frames = nhs.data.filter.filter_and_join_gnaf_frames
filter_sa1_regions = nhs.data.filter.filter_sa1_regions
read_spreadsheets = nhs.data.handling.read_spreadsheets
filter_gnaf_cache = nhs.data.filter.filter_gnaf_cache


@pytest.fixture
def sample_geocode_data():
    return pl.DataFrame(
        {
            "ADDRESS_DETAIL_PID": ["1001", "1002"],
            "LATITUDE": [34.5, 35.0],
            "LONGITUDE": [150.3, 149.1],
        }
    ).lazy()


@pytest.fixture
def sample_detail_data():
    return pl.DataFrame(
        {
            "ADDRESS_DETAIL_PID": ["1001", "1002"],
            "FLAT_TYPE_CODE": ["flat", "unit"],
            "POSTCODE": [2000, 2600],
        }
    ).lazy()


class TestLoadGnafFilesByStates:

    @patch("nhs.data.filter.read_spreadsheets")
    def test_load_files_for_valid_states(
        self, mock_read_spreadsheets, sample_geocode_data, sample_detail_data
    ):
        mock_read_spreadsheets.return_value = {
            "NSW_ADDRESS_DEFAULT_GEOCODE.parquet": sample_geocode_data,
            "NSW_ADDRESS_DETAIL.parquet": sample_detail_data,
        }

        result_geocode_lf, result_detail_lf = load_gnaf_files_by_states(
            "/fake/path", ["NSW"]
        )

        expected_geocode = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "LATITUDE": [34.5, 35.0],
                "LONGITUDE": [150.3, 149.1],
                "STATE": ["NSW", "NSW"],
            }
        )

        expected_detail = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "FLAT_TYPE_CODE": ["flat", "unit"],
                "POSTCODE": [2000, 2600],
            }
        )

        assert result_geocode_lf.collect().to_dicts() == expected_geocode.to_dicts()
        assert result_detail_lf.collect().to_dicts() == expected_detail.to_dicts()

    @patch("nhs.data.filter.read_spreadsheets")
    def test_load_files_with_no_matching_states(self, mock_read_spreadsheets):
        mock_read_spreadsheets.return_value = {}
        result_geocode_lf, result_detail_lf = load_gnaf_files_by_states(
            "/fake/path", ["VIC"]
        )

        expected_geocode = pl.DataFrame(
            {"ADDRESS_DETAIL_PID": [], "LATITUDE": [], "LONGITUDE": [], "STATE": []}
        )
        expected_detail = pl.DataFrame(
            {"ADDRESS_DETAIL_PID": [], "FLAT_TYPE_CODE": [], "POSTCODE": []}
        )

        assert result_geocode_lf.collect().to_dicts() == expected_geocode.to_dicts()
        assert result_detail_lf.collect().to_dicts() == expected_detail.to_dicts()

    @patch("nhs.data.filter.read_spreadsheets")
    def test_load_files_for_multiple_states(self, mock_read_spreadsheets):
        mock_read_spreadsheets.return_value = {
            "NSW_ADDRESS_DEFAULT_GEOCODE.parquet": pl.DataFrame(
                {
                    "ADDRESS_DETAIL_PID": ["1001", "1002"],
                    "LATITUDE": [34.5, 35.0],
                    "LONGITUDE": [150.3, 149.1],
                    "STATE": ["NSW", "NSW"],
                }
            ).lazy(),
            "ACT_ADDRESS_DEFAULT_GEOCODE.parquet": pl.DataFrame(
                {
                    "ADDRESS_DETAIL_PID": ["1234", "4321"],
                    "LATITUDE": [33.9, 34.4],
                    "LONGITUDE": [149.8, 150.1],
                    "STATE": ["ACT", "ACT"],
                }
            ).lazy(),
            "NSW_ADDRESS_DETAIL.parquet": pl.DataFrame(
                {
                    "ADDRESS_DETAIL_PID": ["1001", "1002"],
                    "FLAT_TYPE_CODE": ["flat", "unit"],
                    "POSTCODE": [2000, 2600],
                }
            ).lazy(),
            "ACT_ADDRESS_DETAIL.parquet": pl.DataFrame(
                {
                    "ADDRESS_DETAIL_PID": ["1234", "4321"],
                    "FLAT_TYPE_CODE": ["apartment", "house"],
                    "POSTCODE": [2610, 2620],
                }
            ).lazy(),
        }

        result_geocode_lf, result_detail_lf = load_gnaf_files_by_states(
            "/fake/path", ["NSW", "ACT"]
        )

        expected_geocode = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1234", "4321"],
                "LATITUDE": [34.5, 35.0, 33.9, 34.4],
                "LONGITUDE": [150.3, 149.1, 149.8, 150.1],
                "STATE": ["NSW", "NSW", "ACT", "ACT"],
            }
        )

        expected_detail = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1234", "4321"],
                "FLAT_TYPE_CODE": ["flat", "unit", "apartment", "house"],
                "POSTCODE": [2000, 2600, 2610, 2620],
            }
        )

        assert result_geocode_lf.collect().to_dicts() == expected_geocode.to_dicts()
        assert result_detail_lf.collect().to_dicts() == expected_detail.to_dicts()


class TestFilterAndJoinGnafFrames:

    @pytest.fixture
    def default_geocode_data(self):
        return pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
                "LATITUDE": [34.5, 35.0, 36.0],
                "LONGITUDE": [150.3, 149.1, 148.5],
            }
        ).lazy()

    @pytest.fixture
    def address_detail_data(self):
        return pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
                "FLAT_TYPE_CODE": ["Flat", None, "Unit"],
                "POSTCODE": [2000, 2600, 3000],
            }
        ).lazy()

    @pytest.mark.parametrize(
        "building_types,postcodes,expected_pids",
        [
            ([], [], ["1001", "1002", "1003"]),  # No filters
            (["flat"], [], ["1001"]),  # Filter by building type
            ([], [2600], ["1002"]),  # Filter by postcode
            (["unit"], [3000], ["1003"]),  # Filter by both building type and postcode
        ],
    )
    def test_filter_and_join(
        self,
        default_geocode_data,
        address_detail_data,
        building_types,
        postcodes,
        expected_pids,
    ):
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types, postcodes
        )
        result_pids = (
            result_lf.collect().select("ADDRESS_DETAIL_PID").to_series().to_list()
        )

        assert result_pids == expected_pids

    def test_no_matching_filters(self, default_geocode_data, address_detail_data):
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types=["apartment"]
        )
        assert result_lf.collect().height == 0


class TestFilterSa1Regions:
    @pytest.fixture
    def sample_lazyframe(self):
        # Create a sample LazyFrame to use in tests with the correct column names
        data = {
            "SA1_CODE_2021": ["101", "102", "103", "104", "105"],
            "SA2_CODE_2021": ["201", "202", "203", "204", "205"],
            "value": [10, 20, 30, 40, 50],
        }
        return pl.DataFrame(data).lazy()

    def test_filter_with_valid_sa1_codes(self, sample_lazyframe):
        # Filtering with valid SA1 codes
        result = filter_sa1_regions(
            sample_lazyframe, region_codes=["101", "104"], sa1_column="SA1_CODE_2021"
        ).collect()

        expected_data = {
            "SA1_CODE_2021": ["101", "104"],
            "SA2_CODE_2021": ["201", "204"],
            "value": [10, 40],
        }
        expected = pl.DataFrame(expected_data)

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_valid_sa2_codes(self, sample_lazyframe):
        # Filtering with valid SA2 codes
        result = filter_sa1_regions(
            sample_lazyframe, sa2_codes=["202", "204"], sa2_column="SA2_CODE_2021"
        ).collect()

        expected_data = {
            "SA1_CODE_2021": ["102", "104"],
            "SA2_CODE_2021": ["202", "204"],
            "value": [20, 40],
        }
        expected = pl.DataFrame(expected_data)

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_empty_codes(self, sample_lazyframe):
        # Test with empty region and SA2 codes (should return the original LazyFrame)
        result = filter_sa1_regions(
            sample_lazyframe, [], [], "SA1_CODE_2021", "SA2_CODE_2021"
        ).collect()

        # Expect the original DataFrame when no codes are provided
        expected = sample_lazyframe.collect()

        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_sa1_codes(self, sample_lazyframe):
        # Test with SA1 codes that don't match any rows (should return an empty DataFrame)
        result = filter_sa1_regions(
            sample_lazyframe, region_codes=["999"], sa1_column="SA1_CODE_2021"
        ).collect()

        expected = pl.DataFrame({"SA1_CODE_2021": [], "SA2_CODE_2021": [], "value": []})
        assert result.to_dicts() == expected.to_dicts()

    def test_filter_with_no_matching_sa2_codes(self, sample_lazyframe):
        # Test with SA2 codes that don't match any rows (should return an empty DataFrame)
        result = filter_sa1_regions(
            sample_lazyframe, sa2_codes=["999"], sa2_column="SA2_CODE_2021"
        ).collect()

        expected = pl.DataFrame({"SA1_CODE_2021": [], "SA2_CODE_2021": [], "value": []})
        assert result.to_dicts() == expected.to_dicts()


class TestFilterGnafCache:
    @pytest.fixture
    def sample_lazyframe(self):
        # Create a sample LazyFrame to use in tests
        data = {
            "STATE": ["NSW", "VIC", "QLD", "NSW", "SA"],
            "SA1_CODE21": ["101", "102", "103", "104", "105"],
            "SA2_CODE21": ["201", "202", "203", "204", "205"],
            "FLAT_TYPE_CODE": ["A", "B", "C", "A", "B"],
            "POSTCODE": [2000, 3000, 4000, 5000, 6000],
        }
        return pl.LazyFrame(data)

    def test_filter_by_states(self, sample_lazyframe):
        result = filter_gnaf_cache(sample_lazyframe, states=["NSW", "QLD"])
        expected = sample_lazyframe.filter(pl.col("STATE").is_in(["NSW", "QLD"]))
        assert result.collect().to_dicts() == expected.collect().to_dicts()

    def test_filter_by_region_codes(self, sample_lazyframe):
        result = filter_gnaf_cache(sample_lazyframe, region_codes=["101", "104"])
        expected = sample_lazyframe.filter(pl.col("SA1_CODE21").is_in(["101", "104"]))
        assert result.collect().to_dicts() == expected.collect().to_dicts()

    def test_filter_by_sa2_codes(self, sample_lazyframe):
        result = filter_gnaf_cache(sample_lazyframe, sa2_codes=["202", "204"])
        expected = sample_lazyframe.filter(pl.col("SA2_CODE21").is_in(["202", "204"]))
        assert result.collect().to_dicts() == expected.collect().to_dicts()

    def test_filter_by_flat_type_codes(self, sample_lazyframe):
        result = filter_gnaf_cache(sample_lazyframe, flat_type_codes=["A"])
        expected = sample_lazyframe.filter(pl.col("FLAT_TYPE_CODE").is_in(["A"]))
        assert result.collect().to_dicts() == expected.collect().to_dicts()

    def test_filter_by_postcodes(self, sample_lazyframe):
        result = filter_gnaf_cache(sample_lazyframe, postcodes=[2000, 4000])
        expected = sample_lazyframe.filter(pl.col("POSTCODE").is_in([2000, 4000]))
        assert result.collect().to_dicts() == expected.collect().to_dicts()

    def test_filter_with_multiple_conditions(self, sample_lazyframe):
        result = filter_gnaf_cache(
            sample_lazyframe,
            states=["NSW"],
            region_codes=["101", "104"],
            sa2_codes=["201"],
            flat_type_codes=["A"],
            postcodes=[2000],
        )
        expected = sample_lazyframe.filter(
            (pl.col("STATE").is_in(["NSW"]))
            & (pl.col("SA1_CODE21").is_in(["101", "104"]))
            & (pl.col("SA2_CODE21").is_in(["201"]))
            & (pl.col("FLAT_TYPE_CODE").is_in(["A"]))
            & (pl.col("POSTCODE").is_in([2000]))
        )
        assert result.collect().to_dicts() == expected.collect().to_dicts()
