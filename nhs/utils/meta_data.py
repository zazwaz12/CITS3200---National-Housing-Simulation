import polars as pl

def file_shapes_reporting(files_dict: dict[str, pl.LazyFrame]) -> None:
    shapes = {}

    for file_name, lazyframe in files_dict.items():
        # Collect the shape of the LazyFrame (rows, columns)
        shape = lazyframe.collect().shape
        shapes[file_name] = shape

    return