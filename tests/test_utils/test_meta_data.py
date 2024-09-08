import polars as pl
import nhs.utils.meta_data

file_shapes_reporting =nhs.utils.meta_data.file_shapes_reporting

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

# Run the test
if __name__ == "__main__":
    test_file_shapes_reporting()
