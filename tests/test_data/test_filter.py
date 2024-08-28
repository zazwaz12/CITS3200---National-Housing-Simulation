from nhs.data.filter import filter_sa1_regions
import pytest
import polars as pl



class TestFilterSa1RegionCodes:
  
  # Fixture to create a sample LazyFrame
  @pytest.fixture
  def sample_lazyframe(self):
      data = {
          "SA1_CODE_2021": ["123456", "789012", "345678", "901234", "567890"],
          "value": [10, 20, 30, 40, 50]
      }
      return pl.DataFrame(data).lazy()
  


  
  def test_filter_with_valid_region_codes(self,sample_lazyframe):
      # Filtering with valid region codes
      result = filter_sa1_regions(sample_lazyframe, "SA1_CODE_2021", ["123456", "901234"]).collect()
  
      expected_data = {
          "SA1_CODE_2021": ["123456", "901234"],
          "value": [10, 40]
      }

      expected = pl.DataFrame(expected_data)
      assert result.to_dicts() == expected.to_dicts()


  
  def test_filter_with_empty_region_codes(self,sample_lazyframe):
      # Test with empty region codes (should return the original LazyFrame)
      result = filter_sa1_regions(sample_lazyframe).collect()
  
      expected_data = {
          "SA1_CODE_2021": ["123456", "789012", "345678", "901234", "567890"],
          "value": [10, 20, 30, 40, 50]
      }

      expected = pl.DataFrame(expected_data)
      assert result.to_dicts() == expected.to_dicts()


  
  def test_filter_with_no_matching_codes(self,sample_lazyframe):
      # Test with region codes that don't match any rows (should return an empty DataFrame)
      result = filter_sa1_regions(sample_lazyframe, "SA1_CODE_2021", ["999999"]).collect()
  
      expected = pl.DataFrame({
          "SA1_CODE_2021": [],
          "value": []
      })
      assert result.to_dicts() == expected.to_dicts()


