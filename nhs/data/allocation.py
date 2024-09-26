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
    table_config: dict,
    code_col: str,
    long_col: str,
    lat_col: str,
    index_col: str = "person_id",
    ignore_total: bool = True,
    total_prefix: str = "Tot_",
):
    """
    Randomly assign census features to the GNAF coordinates, handling multi-response features based on configuration.

    This function works with LazyFrames and uses Polars' lazy evaluation capabilities.

    Parameters
    ----------
    census : pl.LazyFrame
        A `LazyFrame` containing the columns `code_col`,
        `long_col`, `lat_col`, and all columns in `table_config["census_features"]`.
    table_config : dict
        A dictionary containing the configuration for the current table, including:
        - name: str, the name of the table
        - multi_response: bool, whether the table is multi-response
        - census_features: list[str], list of feature columns
    code_col : str
        Column in `census` that contains some identifier/code.
    long_col : str
        Column in `census` that contains the longitude.
    lat_col : str
        Column in `census` that contains the latitude.
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
        A `LazyFrame` with the columns `[index_col, code_col, long_col, lat_col, *feature_cols]`
        where each `[code_col, long_col, lat_col]` row is randomly assigned to
        feature columns. For multi-response tables, multiple features can be True.
        For non-multi-response tables, only one feature will be True.
    """
    if ignore_total:
        census = census.select(
            [col for col in census.columns if not col.startswith(total_prefix)]
        )

    feature_cols = table_config["census_features"]

    # Use sample_census_feature for each feature column
    sampled_features = [
        sample_census_feature(census, code_col, long_col, lat_col, feature_col)
        for feature_col in feature_cols
    ]

    # Combine the sampled features
    if table_config["multi_response"]:
        result = reduce(_join_sampled_census_features, sampled_features)
    else:
        result = reduce(_stack_sampled_census_features, sampled_features)

    # Add index column
    result = result.with_row_count(index_col)

    return result.select(pl.col(index_col, code_col, long_col, lat_col, *feature_cols))
