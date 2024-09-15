from functools import reduce
import polars as pl


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
    """
    census_type_casted = census.with_columns(
        pl.col(left_code_col).cast(coords.collect_schema()[right_code_col])
    )

    return census_type_casted.join(
        coords, left_on=left_code_col, right_on=right_code_col, how="inner"
    )


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
    """
    return (
        census.select(pl.col(code_col, feature_col, long_col, lat_col))
        .filter(
            pl.int_range(pl.len()).shuffle().over(code_col) < pl.col(feature_col)
        )  # sample N rows
        .with_columns(
            pl.lit(True).alias(feature_col)
        )  # fill the feature column with True
    )


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


def _join_sampled_census_features(feats_1: pl.LazyFrame, feats_2: pl.LazyFrame):
    """
    Join two LazyFrames with sampled census features and coalesce the feature columns.
    """
    feat_cols, non_feat_cols = _get_feat_non_feat_cols(feats_1, feats_2)

    return feats_1.join(
        feats_2, on=non_feat_cols, how="full", coalesce=True
    ).with_columns(pl.col(*feat_cols).fill_null(False))


def _stack_sampled_census_features(feats_1: pl.LazyFrame, feats_2: pl.LazyFrame):
    """
    Vertically stack two LazyFrames with sampled census features, uniting shared columns.
    """
    feat_cols, _ = _get_feat_non_feat_cols(feats_1, feats_2)

    return pl.concat([feats_1, feats_2], how="diagonal_relaxed").with_columns(
        pl.col(*feat_cols).fill_null(False)
    )


def randomly_assign_census_features(
    census: pl.LazyFrame,
    code_col: str,
    long_col: str,
    lat_col: str,
    feature_cols: list[str],
    index_col: str = "person_id",
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

    Returns
    -------
    pl.LazyFrame
        A `LazyFrame` with the columns `[code_col, long_col, lat_col, *feature_cols]`
        where each `[code_col, long_col, lat_col]` rows are randomly assigned to
        a combination of the `feature_cols` columns. Each row in the columns `feature_cols`
        is a **one-hot encoded** value (`True` or `False` indicating the presence of a
        feature for the corresponding `[code_col, long_col, lat_col]`.
    """
    sampled_features = [
        sample_census_feature(census, code_col, long_col, lat_col, feat_col)
        for feat_col in feature_cols
    ]
    joined = reduce(_stack_sampled_census_features, sampled_features)
    return joined.select(
        pl.col(code_col, long_col, lat_col, *feature_cols)
    ).with_row_index(index_col)
