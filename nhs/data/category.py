"""
Set of functions for filtering data based on census categories.
"""
import re

import polars as pl

from nhs.logging import log_entry_exit


def get_sex_pattern(sex: str | list[str] | None):
    """
    Return regex pattern to match one or more sex
    """
    MAPPING = {
        "m": "males",
        "f": "females",
        "p": "persons",
        "male": "males",
        "female": "females",
        "person": "persons",
    }

    if not sex:
        return "|".join(set(MAPPING.values()))
    elif isinstance(sex, str):
        return MAPPING.get(sex.lower(), sex.lower())
    elif isinstance(sex, list):
        return "|".join([MAPPING[i.lower()] for i in sex])


@log_entry_exit(level="INFO")
def category_system(category_required: str, data_pack_file: pl.LazyFrame):
    """
    Filters a `LazyFrame` based on the provided category string to get list contain strings relate to category_required.

    Parameters
    ----------
    category_required : str
        A string containing the category to filter by.
        - gender: 'male' or 'female'
        - comparison_operator: one of 'over', 'more than', 'greater than', 'above', 'under', 'less than', 'below'
        - age: an integer or decimal number
        - ect

    data_pack_file : pl.LazyFrame
        A Polars LazyFrame containing the data to be filtered. It should have at least the columns relate to census_data

    Returns
    -------
    list
        A list of values from column that meet the filtering criteria.

    Examples
    --------
    >>> data_pack_file = pl.DataFrame({
    ...     "Long": ["Age 30-40", "Age 50-60"],
    ...     "Columnheadingdescriptioninprofile": ["Male data", "Female data"]
    ... }).lazy()
    >>> category_required = "male_over_30"
    >>> category_system(category_required, data_pack_file)
    [30-40]
    """
    categories_list = category_required.split("_")

    gender_pattern = r"\bmale\b|\bfemale\b|\bperson\b"
    age_op_pattern = r"\b(over|more than|greater than|above|under|less than|below)\b"
    age_value_pattern = r"\b\d+(\.\d+)?\b"
    marital_status_pattern = r"\bmarried\b|\bwidowed\b"

    def is_valid_category_list(categories):
        return (
            len(categories) == 3
            and re.search(gender_pattern, categories[0], re.IGNORECASE)
            and re.search(age_op_pattern, categories[1], re.IGNORECASE)
            and re.search(age_value_pattern, categories[2])
        )

    def is_valid_marital_category_list(categories):
        return (
            len(categories) == 4
            and re.search(marital_status_pattern, categories[0], re.IGNORECASE)
            and re.search(gender_pattern, categories[1], re.IGNORECASE)
            and re.search(age_op_pattern, categories[2], re.IGNORECASE)
            and re.search(age_value_pattern, categories[3])
        )

    if is_valid_category_list(categories_list):
        result = (
            category_for_age_and_gender(
                data_pack_file,
                categories_list[0],
                categories_list[1].lower(),
                int(categories_list[2]),
            )
            .collect()["Long"]
            .to_list()
        )
    elif is_valid_marital_category_list(categories_list):
        result = (
            category_for_age_and_gender(
                data_pack_file,
                categories_list[1],
                categories_list[2].lower(),
                int(categories_list[3]),
                categories_list[0].lower(),
            )
            .collect()["Long"]
            .to_list()
        )
    else:
        result = []

    return result


def category_for_age_and_gender(
    data_pack_file: pl.LazyFrame,
    gender: str,
    comparison_operator: str,
    age: int,
    marital_status: str = "None",
) -> pl.LazyFrame:
    """
    Filters a `LazyFrame` based on gender and age criteria.

    Parameters
    ----------
    marital_status
    data_pack_file : pl.LazyFrame
        A Polars LazyFrame containing the data to be filtered. It should have at least the columns:
        - age ranges or values
        - containing gender information

    gender : str
        The gender to filter by. Should be either 'male' or 'female'.

    comparison_operator : str
        The comparison operator to use for filtering age. Must be one of:
        - 'over'
        - 'more than'
        - 'greater than'
        - 'above'
        - 'under'
        - 'less than'
        - 'below'
        - TBA

    age : int
        The age to compare against.

    Returns
    -------
    pl.LazyFrame
        A Polars LazyFrame filtered based on the provided gender and age criteria.

    Examples
    --------
    >>> data_pack_file = pl.DataFrame({
    ...     "Long": ["Age 30-40", "Age 50-60"],
    ...     "Columnheadingdescriptioninprofile": ["Male data", "Female data"]
    ... }).lazy()
    >>> category_for_age_and_gender(data_pack_file, 'male', 'over', 30).collect()
    shape: (1, 2)
    ┌────────────┬───────────────────────────────────┐
    │ Long       │ Columnheadingdescriptioninprofile │
    │ ---        │ ---                               │
    │ str        │ str                               │
    ├────────────┼───────────────────────────────────┤
    │ Age 30-40  │ Male data                         │
    └────────────┴───────────────────────────────────┘
    """
    comparison_mapping = {
        "over": lambda expr, value: expr > value,
        "more than": lambda expr, value: expr > value,
        "greater than": lambda expr, value: expr > value,
        "above": lambda expr, value: expr > value,
        "under": lambda expr, value: expr < value,
        "less than": lambda expr, value: expr < value,
        "below": lambda expr, value: expr < value,
    }

    # Extract numbers and prepare the filtered LazyFrame
    filtered_lf = (
        data_pack_file.with_columns(
            [
                pl.col("Long")
                .str.extract(r"(\d+)", 1)
                .cast(pl.Int32)
                .alias("first_num"),
                pl.col("Long")
                .str.extract(r"(?:\D*\d+\D+(\d+))", 1)
                .cast(pl.Int32)
                .alias("second_num"),
            ]
        )
        .filter(
            pl.col("Columnheadingdescriptioninprofile")
            .str.to_lowercase()
            .str.contains(f"\\b{gender}")
        )
        .filter(
            comparison_mapping[comparison_operator](pl.col("first_num"), age)
            & comparison_mapping[comparison_operator](pl.col("second_num"), age)
        )
    )

    if marital_status != "None":
        filtered_lf = filtered_lf.filter(
            pl.col("Columnheadingdescriptioninprofile")
            .str.to_lowercase()
            .str.contains(f"\\b{marital_status}")
        )
    else:
        filtered_lf = filtered_lf.filter(pl.col("Long").str.contains("Age"))

    filtered_lf = filtered_lf.select(
        [
            col
            for col in data_pack_file.collect_schema().names()
            if col not in ["first_num", "second_num"]
        ]
    )

    return filtered_lf


# if __name__ == "__main__":
#     path_to_xlsx_file = 'C:/Users/IamNo/Desktop/DataFiles/FilesIn/census/Metadata/Metadata_2021_GCP_DataPack_R1_R2.xlsx'
#     df = read_xlsx(path_to_xlsx_file, 0)
#     new_df = df['Cell Descriptors Information']
#     better_df = specify_row_to_be_header(2, new_df)
#     print(category_system("female_over_30", better_df))
