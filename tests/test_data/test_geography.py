import geopandas as gpd
import polars as pl
from shapely.geometry import Point, Polygon
from pytest_mock import MockerFixture
from ..context import nhs

join_coords_with_area = nhs.data.geography.join_coords_with_area


class TestJoinCoordsWithArea:
    # Successfully joins coordinates with area polygons when all coordinates fall within areas
    def test_successful_join_all_coords_within_areas(self, mocker: MockerFixture):
        # Create mock data
        coords_data = {"geometry": [Point(1, 1), Point(2, 2), Point(31, 31)]}
        area_data = {
            "geometry": [
                Polygon([(0, 0), (0, 3), (3, 3), (3, 0)]),
                Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
                Polygon([(30, 30), (30, 35), (35, 35), (35, 30)]),
            ]
        }

        coords = gpd.GeoDataFrame(coords_data, crs="EPSG:4326")  # type: ignore
        area_polygons = gpd.GeoDataFrame(area_data, crs="EPSG:4326")  # type: ignore

        # Call the function
        result = join_coords_with_area(coords, area_polygons)

        assert isinstance(result, pl.LazyFrame)
        assert (result.collect()["index_right"] == [0, 0, 2]).all()

    # No coordinates fall within any area polygons
    def test_no_coords_within_any_areas(self, mocker: MockerFixture):
        # Create mock data
        coords_data = {"geometry": [Point(10, 10), Point(20, 20)]}
        area_data = {"geometry": [Polygon([(0, 0), (0, 3), (3, 3), (3, 0)])]}

        coords = gpd.GeoDataFrame(coords_data, crs="EPSG:4326")  # type: ignore
        area_polygons = gpd.GeoDataFrame(area_data, crs="EPSG:4326")  # type: ignore

        # Call the function
        result = join_coords_with_area(coords, area_polygons)

        # Assertions
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape[0] == 2
        assert result.collect()["index_right"].is_null().all()

    # Handles the "join_nearest" strategy correctly by assigning nearest area polygons
    def test_join_nearest_strategy_assigns_nearest_polygon(self, mocker: MockerFixture):
        # Create mock data
        coords_data = {"geometry": [Point(30, 30), Point(20, 20)]}
        area_data = {"geometry": [Polygon([(0, 0), (0, 3), (3, 3), (3, 0)])]}

        coords = gpd.GeoDataFrame(coords_data, crs="EPSG:4326")  # type: ignore
        area_polygons = gpd.GeoDataFrame(area_data, crs="EPSG:4326")  # type: ignore

        # Call the function
        result = join_coords_with_area(
            coords, area_polygons, failed_join_strategy="join_nearest"
        )

        # Assertions
        assert isinstance(result, pl.LazyFrame)
        assert (result.collect()["index_right"] == [0, 0]).all()

    # Handles the "filter" strategy correctly by filtering out unmapped coordinates
    def test_filter_strategy_filters_out_unmapped_coords(self, mocker: MockerFixture):
        # Create mock data
        coords_data = {"geometry": [Point(1, 1), Point(30, 30), Point(20, 20)]}
        area_data = {"geometry": [Polygon([(0, 0), (0, 3), (3, 3), (3, 0)])]}

        coords = gpd.GeoDataFrame(coords_data, crs="EPSG:4326")  # type: ignore
        area_polygons = gpd.GeoDataFrame(area_data, crs="EPSG:4326")  # type: ignore

        # Call the function
        result = join_coords_with_area(
            coords, area_polygons, failed_join_strategy="filter"
        )

        # Assertions
        assert isinstance(result, pl.LazyFrame)
        assert len(result.collect()) == 1
        assert all(result.collect()["geometry"] == Point(1, 1))
        assert all(result.collect()["index_right"] == [0])
