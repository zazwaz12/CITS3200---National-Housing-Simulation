import polars as pl
import re


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
    categories_list = category_required.split('_')
    result = []

    if (len(categories_list) == 3 and
            re.search(r'\bmale\b|\bfemale\b', categories_list[0], re.IGNORECASE) and
            re.search(r"\b(over|more than|greater than|above|under|less than|below)\b", categories_list[1],
                      re.IGNORECASE) and
            re.search(r"\b\d+(\.\d+)?\b", categories_list[2])):
        result = category_for_age_and_gender(
            data_pack_file,
            categories_list[0],
            categories_list[1].lower(),
            int(categories_list[2])
        ).collect()['Long'].to_list()

    return result


def category_for_age_and_gender(data_pack_file: pl.LazyFrame, gender: str, comparison_operator: str, age: int):
    """
        Filters a `LazyFrame` based on gender and age criteria.

        Parameters
        ----------
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
        ┌────────────┬───────────────────────────┐
        │ Long       │ Columnheadingdescriptioninprofile │
        │ ---        │ ---                           │
        │ str        │ str                           │
        ├────────────┼───────────────────────────┤
        │ Age 30-40  │ Male data                     │
        └────────────┴───────────────────────────┘
        """
    comparison_mapping = {
        'over': lambda x, y: x > y,
        'more than': lambda x, y: x > y,
        'greater than': lambda x, y: x > y,
        'above': lambda x, y: x > y,
        'under': lambda x, y: x < y,
        'less than': lambda x, y: x < y,
        'below': lambda x, y: x < y
    }

    filtered_lf = (
        data_pack_file
        .with_columns([
            pl.col('Long')
            .str.extract(r'(\d+)', 1)
            .cast(pl.Int32)
            .alias('first_num'),
            pl.col('Long')
            .str.extract(r'(?:\D*\d+\D+(\d+))', 1)
            .cast(pl.Int32)
            .alias('second_num')
        ])
        .filter(
            (pl.col('Columnheadingdescriptioninprofile').str.to_lowercase().str.contains(f'\\b{gender}')) &
            (pl.col('Long').str.contains('Age'))
        )
        .filter(
            comparison_mapping[comparison_operator](pl.col('first_num'), age) &
            comparison_mapping[comparison_operator](pl.col('second_num'), age)
        )
        .select([col for col in data_pack_file.collect_schema().names() if col not in ['first_num', 'second_num']])
    )

    return filtered_lf
