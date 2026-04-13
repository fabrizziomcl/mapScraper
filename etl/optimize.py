"""
Schema optimization module.
Applies memory-efficient type casts to a raw DataFrame before compression.
Converts numeric strings to native types and high-cardinality text columns
to Categorical encoding for better Parquet dictionary compression.
"""

import polars as pl


def optimize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Casts columns to optimal types for storage and analytical performance.

    Transformations applied:
      - 'stars'   : Utf8 -> Float32  (rating value)
      - 'reviews' : Utf8 -> Int32    (count, nullable)
      - 'category': Utf8 -> Categorical (dictionary-encoded in Parquet)

    Columns that do not exist in the DataFrame are silently skipped.

    Args:
        df: Input DataFrame with all-string columns.

    Returns:
        DataFrame with optimized column types.
    """
    existing_cols = set(df.columns)
    expressions = []

    if "stars" in existing_cols:
        expressions.append(pl.col("stars").cast(pl.Float32, strict=False))

    if "reviews" in existing_cols:
        expressions.append(
            pl.when(pl.col("reviews") == "")
            .then(None)
            .otherwise(pl.col("reviews"))
            .cast(pl.Int32, strict=False)
            .alias("reviews")
        )

    if "category" in existing_cols:
        expressions.append(pl.col("category").cast(pl.Categorical))

    if expressions:
        df = df.with_columns(expressions)

    return df
