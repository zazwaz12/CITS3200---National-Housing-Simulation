import polars as pl
import pytest
from .context import nhs

class TestFilterSa1RegionCodes:
  
  # This test checks that the function correctly filters the LazyFrame when given valid region codes that match some rows
  def test_filter_with_valid_sa1_codes():
    
    data = {
        "SA1_CODE_2021": ["101", "102", "103", "104"],
        "Tot_P_M": ["1001", "1002", "1003", "1004"],
        "Age_Yr_55": ["9", "123","233", "2"]         
    } 
    lf = pl.LazyFrame(data)
    region_codes = ["101", "103"]

    filtered_lf = filter_SA1_region_codes(lf, region_codes=region_codes)
    result = filtered_lf.collect().to_dict()
    
    expected_result  = {
        "SA1_CODE_2021": ["101", "103"],
        "Tot_P_M": ["1001", "1003"],
        "Age_Yr_55": ["9","233"] 
    }

    assert result == expected_result

# This test checks that if no region codes are provided (i.e., the list is empty), the function returns the original LazyFrame without any filtering.
def test_filter_with_empty_region_codes():

    data = {
        "SA1_CODE_2021": ["101", "102", "103", "104"],
        "value": [10, 20, 30, 40]
    }
    lf = pl.LazyFrame(data)

    # Run the filter function with an empty region code list
    filtered_lf = filter_SA1_region_codes(lf, region_codes=[])
    result = filtered_lf.collect().to_dict()
    
    expected_result  = {

    }

    assert result == expected_result



def test_filter_with_non_matching_codes():
    data = {
        "SA1_CODE_2021": ["101", "102", "103", "104"],
        "Tot_P_M": ["1001", "1002", "1003", "1004"],
        "Age_Yr_55": ["9", "123","233", "2"]         
    } 
    lf = pl.LazyFrame(data)

    region_codes = ["999"]
    filtered_lf = filter_SA1_region_codes(lf, region_codes=region_codes)

    result = filtered_lf.collect().to_dict()
    
    expected_result  = {

    }
  
    assert result == expected_result



def test_filter_with_partial_matching_codes():
    data = {
        "SA1_CODE_2021": ["101", "102", "103", "104"],
        "Tot_P_M": ["1001", "1002", "1003", "1004"],
        "Age_Yr_55": ["9", "123","233", "2"]         
    } 
    lf = pl.LazyFrame(data)


    region_codes = ["101", "999"]
    filtered_lf = filter_SA1_region_codes(lf, region_codes=region_codes)


    result = filtered_lf.collect().to_dict()
    
    expected_result  = {

    }

    assert result == expected_result
