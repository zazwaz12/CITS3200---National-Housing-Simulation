from typing import List
import polars as pl
import pytest

def filter_SA1_region_codes(lf: pl.LazyFrame, SA1_column: str = "SA1_CODE_2021", region_codes: List[str] = []) -> pl.LazyFrame:
    """
    Filters the LazyFrame to include only rows with specified SA1 area codes.

    Parameters
    ----------
    lf : pl.LazyFrame
        The LazyFrame containing SA1 region codes to be filtered.
    SA1_column : str
        The name of the column containing the SA1 area codes. Defaults to "SA1_CODE_2021".
    region_codes : List[str]
        A list of SA1 area codes to filter for.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing only rows with the specified SA1 area codes.
    """
    if not region_codes:
        return lf  # If no region codes are provided, return the original LazyFrame
    
    return lf.filter(pl.col(SA1_column).is_in(region_codes))


# Fixture to create a sample LazyFrame
@pytest.fixture
def sample_lazyframe():
    data = {
        "SA1_CODE_2021": ["123456", "789012", "345678", "901234", "567890"],
        "value": [10, 20, 30, 40, 50]
    }
    return pl.DataFrame(data).lazy()



# Filtering with valid region codes
def test_filter_with_valid_region_codes(sample_lazyframe):
    result = filter_SA1_region_codes(sample_lazyframe, "SA1_CODE_2021", ["123456", "901234"]).collect()

    expected_data = {
        "SA1_CODE_2021": ["123456", "901234"],
        "value": [10, 40]
    }
    expected = pl.DataFrame(expected_data)

    # Compare the DataFrames as lists of dictionaries
    assert result.to_dicts() == expected.to_dicts()


# Test with empty region codes (should return the original LazyFrame)
def test_filter_with_empty_region_codes(sample_lazyframe):
    result = filter_SA1_region_codes(sample_lazyframe).collect()

    expected_data = {
        "SA1_CODE_2021": ["123456", "789012", "345678", "901234", "567890"],
        "value": [10, 20, 30, 40, 50]
    }
    expected = pl.DataFrame(expected_data)

    assert result.to_dicts() == expected.to_dicts()


 # Test with region codes that don't match any rows (should return an empty DataFrame)
def test_filter_with_no_matching_codes(sample_lazyframe):
    result = filter_SA1_region_codes(sample_lazyframe, "SA1_CODE_2021", ["999999"]).collect()

    expected = pl.DataFrame({
        "SA1_CODE_2021": [],
        "value": []
    })

    assert result.to_dicts() == expected.to_dicts()

if __name__ == "__main__":
    pytest.main()
