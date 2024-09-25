from functools import reduce

import polars as pl
from loguru import logger

from ..logging import log_entry_exit


@log_entry_exit()
def join_census_with_coords(
    census: pl.LazyFrame,
    coords: pl.LazyFrame,
    left_code_col: str = "SA1_CODE_2021",
    right_code_col: str = "SA1_CODE21",
):
    """
    Inner join the census and gnaf coordinate LazyFrames on specified columns.

    Only rows with matching values in `left_code_col` and `right_code_col` of `census` and `coords`
    respectively will be included in the output.

    **Note**: If `left_code_col` and `right_code_col` have different data types, then `left_code_col`
    in `census` will be type-casted to the data type of `right_code_col` in `coords`. Output will
    also use `left_code_col` if `left_code_col` and `right_code_col` have different names.

    Parameters
    ----------
    census : pl.LazyFrame
        The census dataframe to join.
    coords : pl.LazyFrame
        The gnaf coordinate dataframe to join.
    left_code_col : str
        The column in the census dataframe to join on.
    right_code_col : str
        The column in the gnaf coordinate dataframe to join on.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame containing the column `right_code_col` and the rest of the columns
        from `census` and `coords`.

    Examples
    --------
    >>> census = pl.LazyFrame({
    ...     "CODE_2021": ["123", "45", "3"],
    ...     "feature1": [24, 65, 234]
    ... })
    >>> coords = pl.LazyFrame({
    ...     "CODE21": [123, 45, 3],
    ...     "longitude": [134.56, 456.12, 21.124],
    ...     "latitude": [-34.56, -23.12, 12.124],
    ... })
    >>> join_census_with_coords(
    ...     census, coords, left_code_col="CODE_2021", right_code_col="CODE21"
    ... ).collect()
        shape: (3, 4)
        ┌───────────┬──────────┬───────────┬──────────┐
        │ CODE_2021 ┆ feature1 ┆ longitude ┆ latitude │
        │ ---       ┆ ---      ┆ ---       ┆ ---      │
        │ i64       ┆ i64      ┆ f64       ┆ f64      │
        ╞═══════════╪══════════╪═══════════╪══════════╡
        │ 123       ┆ 24       ┆ 134.56    ┆ -34.56   │
        │ 45        ┆ 65       ┆ 456.12    ┆ -23.12   │
        │ 3         ┆ 234      ┆ 21.124    ┆ 12.124   │
        └───────────┴──────────┴───────────┴──────────┘
    """
    census_type_casted = census.with_columns(
        pl.col(left_code_col).cast(coords.collect_schema()[right_code_col])
    )

    return census_type_casted.join(
        coords, left_on=left_code_col, right_on=right_code_col, how="inner"
    )

@log_entry_exit()
def calculate_proportions(group_values_dict: dict) -> pl.LazyFrame:
    
    if not group_values_dict:
        raise ValueError("The grouping dictionary is empty.")

    # Extract the keys (grouping) and values (values) from the dictionary
    unique_dict = {}
    
    for group, value in group_values_dict.items():
        if group not in unique_dict:
            unique_dict[group] = value
    
    # Construct a LazyFrame from the unique dictionary
    lf = pl.LazyFrame({
        "group_col": list(unique_dict.keys()),
        "value_col": list(unique_dict.values())
    })
    
    # Define the total sum computation
    total_sum_expr = lf.select(pl.col("value_col").sum()).collect().get_column("value_col")[0]
    
    # Add a new column for the proportions using a LazyFrame
    lf = lf.with_columns((pl.col("value_col") / total_sum_expr).round(3).alias("proportion"))
    
    # Collect to evaluate the LazyFrame (for demonstration purposes)
    print(lf.collect())
    return lf


@log_entry_exit()
def sample_census_feature(
    census: pl.LazyFrame, code_col: str, long_col: str, lat_col: str, feature_col: str
):
    """
    Randomly sample rows from each group in the census LazyFrame using value of `feature_col` as sample size.

    Parameters
    ----------
    census: pl.LazyFrame
        A `LazyFrame` to sample from, with columns `code_col`, `long_col`, `lat_col`,
        and `feature_col`.
    code_col : str
        Column in `census` that contains some identifier/code.
    long_col : str
        Column in `census` that contains the longitude.
    lat_col : str
        Column in `census` that contains the latitude.
    feature_col : str
        Column in `census` for a feature where each row is the
        sample size for each group.

    Returns
    -------
    pl.LazyFrame
        A `LazyFrame` with the columns `[code_col, long_col, lat_col, feature_col]`
        where each `code_col` group have N number of randomly sampled rows. N
        is the value of `feature_col` for each group.

    Examples
    --------
    >>> census = pl.LazyFrame({
    ...     "code_col": ["A", "A", "A", "B"],
    ...     "long_col": [1.0, 2.0, 3.0, 4.0],
    ...     "lat_col": [10.0, 20.0, 30.0, 40.0],
    ...     "feature_col": [4, 4, 4, 2],
    ... })
    >>> sample_census_feature(
    ...     census, "code_col", "long_col", "lat_col", "feature_col"
    ... ).collect()
    shape: (6, 4)
    ┌──────────┬─────────────┬──────────┬─────────┐
    │ code_col ┆ feature_col ┆ long_col ┆ lat_col │
    │ ---      ┆ ---         ┆ ---      ┆ ---     │
    │ str      ┆ bool        ┆ f64      ┆ f64     │
    ╞══════════╪═════════════╪══════════╪═════════╡
    │ A        ┆ true        ┆ 1.0      ┆ 10.0    │
    │ A        ┆ true        ┆ 1.0      ┆ 10.0    │
    │ A        ┆ true        ┆ 2.0      ┆ 20.0    │
    │ A        ┆ true        ┆ 3.0      ┆ 30.0    │
    │ B        ┆ true        ┆ 4.0      ┆ 40.0    │
    │ B        ┆ true        ┆ 4.0      ┆ 40.0    │
    └──────────┴─────────────┴──────────┴─────────┘
    """
    if census.select(pl.len()).collect().item() == 0:
        logger.warning("Empty census data provided.")
        return census.select(pl.col(code_col, long_col, lat_col, feature_col))

    return (
        census.select(
            pl.col(code_col, long_col, lat_col, feature_col)
            # repeat (long, lat) to ensure num coordinates is bigger than sample size
            .repeat_by(pl.col(feature_col)).flatten()
        )
        .filter(
            pl.int_range(pl.len()).shuffle().over(code_col) < pl.col(feature_col)
        )  # sample N rows
        .with_columns(
            pl.lit(True).alias(feature_col)
        )  # fill the feature column with True
    )


@log_entry_exit()
def _get_feat_non_feat_cols(feats_1: pl.LazyFrame, feats_2: pl.LazyFrame):
    """
    Get list of feature and non-feature columns from two LazyFrames.

    **NOTE**: assumes `feats_1` and `feats_2` have the same schema that only
    differs in the feature columns.
    """
    feats1_cols = feats_1.collect_schema().names()
    feats2_cols = feats_2.collect_schema().names()
    non_feat_cols = list(set(feats1_cols).intersection(feats2_cols))
    feature_cols = list(set(feats1_cols).union(feats2_cols).difference(non_feat_cols))
    return feature_cols, non_feat_cols


@log_entry_exit()
def _join_sampled_census_features(feats_1: pl.LazyFrame, feats_2: pl.LazyFrame):
    """
    Join two LazyFrames with sampled census features and coalesce the feature columns.
    """
    feat_cols, non_feat_cols = _get_feat_non_feat_cols(feats_1, feats_2)

    return feats_1.join(
        feats_2, on=non_feat_cols, how="full", coalesce=True
    ).with_columns(pl.col(*feat_cols).fill_null(False))


@log_entry_exit()
def _stack_sampled_census_features(feats_1: pl.LazyFrame, feats_2: pl.LazyFrame):
    """
    Vertically stack two LazyFrames with sampled census features, uniting shared columns.
    """
    feat_cols, _ = _get_feat_non_feat_cols(feats_1, feats_2)

    return pl.concat([feats_1, feats_2], how="diagonal_relaxed").with_columns(
        pl.col(*feat_cols).fill_null(False)
    )


@log_entry_exit()
def randomly_assign_census_features(
    census: pl.LazyFrame,
    code_col: str,
    long_col: str,
    lat_col: str,
    feature_cols: list[str],
    index_col: str = "person_id",
    ignore_total: bool = True,
    total_prefix: str = "Tot_",
):
    """
    Randomly assign census features to the GNAF coordinates.

    Parameters
    ----------
    census : pl.LazyFrame
        A `LazyFrame` containing the columns `code_col`,
        `long_col`, `lat_col`, and all columns in `feature_cols`.
    code_col : str
        Column in `census` that contains some identifier/code.
    long_col : str
        Column in `census` that contains the longitude.
    lat_col : str
        Column in `census` that contains the latitude.
    feature_cols : list[str]
        List of column names in `census` to assign to the GNAF coordinates.
        Each row in these columns have a number indicating the number of
        individuals with that feature for a particular code in `code_col`.
    index_col : str
        Column name to assign the row index to.
    ignore_total : bool
        Whether to ignore columns that start with `total_prefix` indicating the
        total count for a row.
    total_prefix : str
        Prefix used to identify columns that are totals, only used if `ignore_total`
        is `True`. Defaults to "Tot_".

    Returns
    -------
    pl.LazyFrame
        A `LazyFrame` with the columns `[code_col, long_col, lat_col, *feature_cols]`
        where each `[code_col, long_col, lat_col]` rows are randomly assigned to
        a combination of the `feature_cols` columns. Each row in the columns `feature_cols`
        is a **one-hot encoded** value (`True` or `False` indicating the presence of a
        feature for the corresponding `[code_col, long_col, lat_col]`.

    Examples
    --------
    >>> census = pl.LazyFrame({
    ...     "code_col": ["A", "A", "B", "B"],
    ...     "long_col": [1.0, 1.1, 2.0, 2.1],
    ...     "lat_col": [1.0, 1.1, 2.0, 2.1],
    ...     "feature_1": [7, 7, 12, 12],
    ...     "feature_2": [3, 3, 4, 4],
    ...     "feature_3": [5, 5, 6, 6],
    ... })
    >>> randomly_assign_census_features(
    ...     census,
    ...     code_col="code_col",
    ...     long_col="long_col",
    ...     lat_col="lat_col",
    ...     feature_cols=["feature_1", "feature_2", "feature_3"],
    ... ).collect()
    shape: (37, 7)
    ┌───────────┬──────────┬──────────┬─────────┬───────────┬───────────┬───────────┐
    │ person_id ┆ code_col ┆ long_col ┆ lat_col ┆ feature_1 ┆ feature_2 ┆ feature_3 │
    │ ---       ┆ ---      ┆ ---      ┆ ---     ┆ ---       ┆ ---       ┆ ---       │
    │ i64       ┆ str      ┆ f64      ┆ f64     ┆ bool      ┆ bool      ┆ bool      │
    ╞═══════════╪══════════╪══════════╪═════════╪═══════════╪═══════════╪═══════════╡
    │ 0         ┆ A        ┆ 1.0      ┆ 1.0     ┆ true      ┆ false     ┆ false     │
    │ 1         ┆ A        ┆ 1.0      ┆ 1.0     ┆ true      ┆ false     ┆ false     │
    │ 2         ┆ A        ┆ 1.0      ┆ 1.0     ┆ true      ┆ false     ┆ false     │
    │ 3         ┆ A        ┆ 1.1      ┆ 1.1     ┆ true      ┆ false     ┆ false     │
    │ 4         ┆ A        ┆ 1.1      ┆ 1.1     ┆ true      ┆ false     ┆ false     │
    │ …         ┆ …        ┆ …        ┆ …       ┆ …         ┆ …         ┆ …         │
    │ 32        ┆ B        ┆ 2.0      ┆ 2.0     ┆ false     ┆ false     ┆ true      │
    │ 33        ┆ B        ┆ 2.0      ┆ 2.0     ┆ false     ┆ false     ┆ true      │
    │ 34        ┆ B        ┆ 2.1      ┆ 2.1     ┆ false     ┆ false     ┆ true      │
    │ 35        ┆ B        ┆ 2.1      ┆ 2.1     ┆ false     ┆ false     ┆ true      │
    │ 36        ┆ B        ┆ 2.1      ┆ 2.1     ┆ false     ┆ false     ┆ true      │
    └───────────┴──────────┴──────────┴─────────┴───────────┴───────────┴───────────┘
    """
    if ignore_total:
        census = census.select(
            [col for col in census.collect_schema() if not col.startswith(total_prefix)]
        )

    sampled_features = [
        sample_census_feature(census, code_col, long_col, lat_col, feat_col)
        for feat_col in feature_cols
    ]
    joined = reduce(_stack_sampled_census_features, sampled_features)
    return joined.with_columns(
        pl.int_range(pl.len()).alias(index_col)  # assign row index
    ).select(
        pl.col(index_col, code_col, long_col, lat_col, *feature_cols)
    )  # reassign column order
