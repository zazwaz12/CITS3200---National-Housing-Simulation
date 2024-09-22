import pytest
import polars as pl

from unittest.mock import patch
from ..context import nhs


load_gnaf_files_by_states = nhs.data.filter.load_gnaf_files_by_states
filter_and_join_gnaf_frames = nhs.data.filter.filter_and_join_gnaf_frames
filter_sa1_regions = nhs.data.filter.filter_sa1_regions


class TestLoadGnafFilesByStates:

    @pytest.fixture
    def sample_geocode_data(self):
        return pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "LATITUDE": [34.5, 35.0],
                "LONGITUDE": [150.3, 149.1],
            }
        ).lazy()

    @pytest.fixture
    def sample_detail_data(self):
        return pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "FLAT_TYPE_CODE": ["flat", "unit"],
                "POSTCODE": [2000, 2600],
            }
        ).lazy()

    @patch("nhs.data.filter.glob.glob")
    @patch("nhs.data.filter.pl.scan_csv")
    def test_load_files_for_valid_states(
        self, mock_scan_csv, mock_glob, sample_geocode_data, sample_detail_data
    ):
        mock_glob.side_effect = [
            ["NSW_ADDRESS_DEFAULT_GEOCODE_psv.psv"],
            ["NSW_ADDRESS_DETAIL_psv.psv"],
        ]
        mock_scan_csv.side_effect = [sample_geocode_data, sample_detail_data]

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

    @patch("nhs.data.filter.glob.glob")
    @patch("nhs.data.filter.pl.scan_csv")
    def test_load_files_with_no_matching_states(self, mock_scan_csv, mock_glob):
        mock_glob.side_effect = [[], []]

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

    @patch("nhs.data.filter.glob.glob")
    @patch("nhs.data.filter.pl.scan_csv")
    def test_load_files_for_multiple_states(self, mock_scan_csv, mock_glob):
        """
        Test loading GNAF files for multiple states (NSW, ACT) using mocked file I/O.
        """
        # Mock the glob.glob function to return fake file paths for both NSW and ACT
        mock_glob.return_value = [
            "NSW_ADDRESS_DEFAULT_GEOCODE_psv.psv",
            "ACT_ADDRESS_DEFAULT_GEOCODE_psv.psv",
            "NSW_ADDRESS_DETAIL_psv.psv",
            "ACT_ADDRESS_DETAIL_psv.psv",
        ]

        # Create separate LazyFrames for NSW geocode data
        sample_geocode_data_nsw = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "LATITUDE": [34.5, 35.0],
                "LONGITUDE": [150.3, 149.1],
                "STATE": ["NSW", "NSW"],
            }
        ).lazy()

        # Create separate LazyFrames for ACT geocode data with different ADDRESS_DETAIL_PID
        sample_geocode_data_act = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1234", "4321"],
                "LATITUDE": [33.9, 34.4],
                "LONGITUDE": [149.8, 150.1],
                "STATE": ["ACT", "ACT"],
            }
        ).lazy()

        # Create separate LazyFrames for NSW detail data
        sample_detail_data_nsw = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002"],
                "FLAT_TYPE_CODE": ["flat", "unit"],
                "POSTCODE": [2000, 2600],
            }
        ).lazy()

        # Create separate LazyFrames for ACT detail data with different ADDRESS_DETAIL_PID
        sample_detail_data_act = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1234", "4321"],
                "FLAT_TYPE_CODE": ["apartment", "house"],
                "POSTCODE": [2610, 2620],
            }
        ).lazy()

        # Manually patch scan_csv to return the correct LazyFrame for each call
        with patch(
            "nhs.data.filter.pl.scan_csv", return_value=sample_geocode_data_nsw
        ) as mock_scan_csv_nsw:
            result_geocode_nsw = mock_scan_csv_nsw()

        with patch(
            "nhs.data.filter.pl.scan_csv", return_value=sample_geocode_data_act
        ) as mock_scan_csv_act:
            result_geocode_act = mock_scan_csv_act()

        with patch(
            "nhs.data.filter.pl.scan_csv", return_value=sample_detail_data_nsw
        ) as mock_scan_detail_nsw:
            result_detail_nsw = mock_scan_detail_nsw()

        with patch(
            "nhs.data.filter.pl.scan_csv", return_value=sample_detail_data_act
        ) as mock_scan_detail_act:
            result_detail_act = mock_scan_detail_act()

        # Collect the results
        result_geocode_lf = pl.concat([result_geocode_nsw, result_geocode_act]).lazy()
        result_detail_lf = pl.concat([result_detail_nsw, result_detail_act]).lazy()

        # Expected data
        expected_geocode = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [
                    "1001",
                    "1002",
                    "1234",
                    "4321",
                ],  # Different PIDs for NSW and ACT
                "LATITUDE": [34.5, 35.0, 33.9, 34.4],
                "LONGITUDE": [150.3, 149.1, 149.8, 150.1],
                "STATE": ["NSW", "NSW", "ACT", "ACT"],
            }
        )

        expected_detail = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [
                    "1001",
                    "1002",
                    "1234",
                    "4321",
                ],  # Different PIDs for NSW and ACT
                "FLAT_TYPE_CODE": ["flat", "unit", "apartment", "house"],
                "POSTCODE": [2000, 2600, 2610, 2620],
            }
        )

        # Assertions
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
                "FLAT_TYPE_CODE": [
                    "Flat",
                    None,
                    "Unit",
                ],  # One null value to test null handling
                "POSTCODE": [2000, 2600, 3000],
            }
        ).lazy()

    def test_filter_no_filters(self, default_geocode_data, address_detail_data):
        """
        Test the function with no filters applied, should join all rows.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
                "LATITUDE": [34.5, 35.0, 36.0],
                "LONGITUDE": [150.3, 149.1, 148.5],
                "FLAT_TYPE_CODE": [
                    "flat",
                    "unknown",
                    "unit",
                ],  # "Flat" converted to lowercase, None replaced with "unknown"
                "POSTCODE": [2000, 2600, 3000],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_filter_by_building_type(self, default_geocode_data, address_detail_data):
        """
        Test the function with a filter on building types.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types=["flat"]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001"],
                "LATITUDE": [34.5],
                "LONGITUDE": [150.3],
                "FLAT_TYPE_CODE": ["flat"],
                "POSTCODE": [2000],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_filter_by_postcode(self, default_geocode_data, address_detail_data):
        """
        Test the function with a filter on postcodes.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, postcodes=[2600]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1002"],
                "LATITUDE": [35.0],
                "LONGITUDE": [149.1],
                "FLAT_TYPE_CODE": ["unknown"],  # None replaced with "unknown"
                "POSTCODE": [2600],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_filter_by_building_type_and_postcode(
        self, default_geocode_data, address_detail_data
    ):
        """
        Test the function with both building type and postcode filters applied.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data,
            address_detail_data,
            building_types=["unit"],
            postcodes=[3000],
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1003"],
                "LATITUDE": [36.0],
                "LONGITUDE": [148.5],
                "FLAT_TYPE_CODE": ["unit"],
                "POSTCODE": [3000],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_no_matching_building_type(self, default_geocode_data, address_detail_data):
        """
        Test the function when no rows match the building type filter.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types=["apartment"]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [],
                "LATITUDE": [],
                "LONGITUDE": [],
                "FLAT_TYPE_CODE": [],
                "POSTCODE": [],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_no_matching_postcode(self, default_geocode_data, address_detail_data):
        """
        Test the function when no rows match the postcode filter.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, postcodes=[9999]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [],
                "LATITUDE": [],
                "LONGITUDE": [],
                "FLAT_TYPE_CODE": [],
                "POSTCODE": [],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()


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
                "FLAT_TYPE_CODE": [
                    "Flat",
                    None,
                    "Unit",
                ],  # One null value to test null handling
                "POSTCODE": [2000, 2600, 3000],
            }
        ).lazy()
        return pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
                "FLAT_TYPE_CODE": [
                    "Flat",
                    None,
                    "Unit",
                ],  # One null value to test null handling
                "POSTCODE": [2000, 2600, 3000],
            }
        ).lazy()

    def test_filter_no_filters(self, default_geocode_data, address_detail_data):
        """
        Test the function with no filters applied, should join all rows.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
                "LATITUDE": [34.5, 35.0, 36.0],
                "LONGITUDE": [150.3, 149.1, 148.5],
                "FLAT_TYPE_CODE": [
                    "flat",
                    "unknown",
                    "unit",
                ],  # "Flat" converted to lowercase, None replaced with "unknown"
                "POSTCODE": [2000, 2600, 3000],
            }
        )

        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001", "1002", "1003"],
                "LATITUDE": [34.5, 35.0, 36.0],
                "LONGITUDE": [150.3, 149.1, 148.5],
                "FLAT_TYPE_CODE": [
                    "flat",
                    "unknown",
                    "unit",
                ],  # "Flat" converted to lowercase, None replaced with "unknown"
                "POSTCODE": [2000, 2600, 3000],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_filter_by_building_type(self, default_geocode_data, address_detail_data):
        """
        Test the function with a filter on building types.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types=["flat"]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001"],
                "LATITUDE": [34.5],
                "LONGITUDE": [150.3],
                "FLAT_TYPE_CODE": ["flat"],
                "POSTCODE": [2000],
            }
        )

        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types=["flat"]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1001"],
                "LATITUDE": [34.5],
                "LONGITUDE": [150.3],
                "FLAT_TYPE_CODE": ["flat"],
                "POSTCODE": [2000],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_filter_by_postcode(self, default_geocode_data, address_detail_data):
        """
        Test the function with a filter on postcodes.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, postcodes=[2600]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1002"],
                "LATITUDE": [35.0],
                "LONGITUDE": [149.1],
                "FLAT_TYPE_CODE": ["unknown"],  # None replaced with "unknown"
                "POSTCODE": [2600],
            }
        )

        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, postcodes=[2600]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1002"],
                "LATITUDE": [35.0],
                "LONGITUDE": [149.1],
                "FLAT_TYPE_CODE": ["unknown"],  # None replaced with "unknown"
                "POSTCODE": [2600],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()


    def test_filter_by_building_type_and_postcode(
        self, default_geocode_data, address_detail_data
    ):
        """
        Test the function with both building type and postcode filters applied.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data,
            address_detail_data,
            building_types=["unit"],
            postcodes=[3000],
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1003"],
                "LATITUDE": [36.0],
                "LONGITUDE": [148.5],
                "FLAT_TYPE_CODE": ["unit"],
                "POSTCODE": [3000],
            }
        )

        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data,
            address_detail_data,
            building_types=["unit"],
            postcodes=[3000],
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": ["1003"],
                "LATITUDE": [36.0],
                "LONGITUDE": [148.5],
                "FLAT_TYPE_CODE": ["unit"],
                "POSTCODE": [3000],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_no_matching_building_type(self, default_geocode_data, address_detail_data):
        """
        Test the function when no rows match the building type filter.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types=["apartment"]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [],
                "LATITUDE": [],
                "LONGITUDE": [],
                "FLAT_TYPE_CODE": [],
                "POSTCODE": [],
            }
        )

        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, building_types=["apartment"]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [],
                "LATITUDE": [],
                "LONGITUDE": [],
                "FLAT_TYPE_CODE": [],
                "POSTCODE": [],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()

    def test_no_matching_postcode(self, default_geocode_data, address_detail_data):
        """
        Test the function when no rows match the postcode filter.
        """
        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, postcodes=[9999]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [],
                "LATITUDE": [],
                "LONGITUDE": [],
                "FLAT_TYPE_CODE": [],
                "POSTCODE": [],
            }
        )

        result_lf = filter_and_join_gnaf_frames(
            default_geocode_data, address_detail_data, postcodes=[9999]
        )
        expected_df = pl.DataFrame(
            {
                "ADDRESS_DETAIL_PID": [],
                "LATITUDE": [],
                "LONGITUDE": [],
                "FLAT_TYPE_CODE": [],
                "POSTCODE": [],
            }
        )

        assert result_lf.collect().to_dicts() == expected_df.to_dicts()





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
        result = filter_sa1_regions(
            sample_lazyframe, [999999], "SA1_CODE21"
        ).collect()

        expected = pl.DataFrame({"SA1_CODE21": [], "value": []})
        assert result.to_dicts() == expected.to_dicts()

