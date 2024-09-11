import polars as pl
import nhs.utils.meta_data
import unittest

file_shapes_reporting =nhs.utils.meta_data.file_shapes_reporting
match_files_by_keywords =nhs.utils.meta_data.match_files_by_keywords

def create_sample_lazyframes():
    # Sample data for testing
    data1 = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
        "city": ["New York", "Los Angeles", "Chicago"]
    }

    data2 = {
        "product": ["Laptop", "Smartphone"],
        "price": [1200, 800],
        "quantity": [10, 20]
    }

    # Convert the sample data into LazyFrames
    lf1 = pl.DataFrame(data1).lazy()
    lf2 = pl.DataFrame(data2).lazy()

    return {"file1": lf1, "file2": lf2}

def test_file_shapes_reporting():
    # Create sample LazyFrames
    files_dict = create_sample_lazyframes()

    # Call the file_shapes_reporting function
    shapes = file_shapes_reporting(files_dict)

    # Assertions to verify the output
    assert shapes["file1"] == "3,3", "Shape mismatch for file1"
    assert shapes["file2"] == "2,3", "Shape mismatch for file2"
    print("All tests passed!")

class TestMatchFilesByKeywords(unittest.TestCase):
    def test_match_single_keyword(self):
        file_list = ['report_2024.pdf', 'data_2023.csv', 'analysis_2024.xlsx']
        keywords = ['2024']
        expected_result = ['report_2024.pdf', 'analysis_2024.xlsx']
        result = match_files_by_keywords(file_list, keywords)
        self.assertEqual(result, expected_result)

    def test_match_multiple_keywords(self):
        file_list = ['report_2024.pdf', 'data_2023.csv', 'data_report_2024.csv', 'summary_report_2023.txt']
        keywords = ['report', '2024']
        expected_result = ['report_2024.pdf', 'data_report_2024.csv']
        result = match_files_by_keywords(file_list, keywords)
        self.assertEqual(result, expected_result)

    def test_no_matches(self):
        file_list = ['data_2023.csv', 'summary_2022.txt']
        keywords = ['2024']
        expected_result = []
        result = match_files_by_keywords(file_list, keywords)
        self.assertEqual(result, expected_result)

    def test_empty_file_list(self):
        file_list = []
        keywords = ['2024']
        expected_result = []
        result = match_files_by_keywords(file_list, keywords)
        self.assertEqual(result, expected_result)

    def test_empty_keywords(self):
        file_list = ['data_2023.csv', 'summary_2024.txt']
        keywords = []
        expected_result = []
        result = match_files_by_keywords(file_list, keywords)
        self.assertEqual(result, expected_result)


# Run the test
if __name__ == "__main__":
    test_file_shapes_reporting()
