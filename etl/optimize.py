"""Type optimization for Parquet output."""

from __future__ import annotations

import polars as pl

# Categorical dictionary encoding only helps when values repeat. Skip the
# cast when the column is mostly unique — the dictionary would be as large
# as the data and we'd pay RAM for nothing.
CATEGORICAL_MAX_UNIQUE_RATIO = 0.5


def optimize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast `stars` to Float32, legacy `reviews` to Int32, `category` to Categorical."""
    cols = set(df.columns)
    expressions: list[pl.Expr] = []

    if "stars" in cols:
        expressions.append(pl.col("stars").cast(pl.Float32, strict=False))

    if "reviews" in cols:  # legacy column; new scrapes don't emit it
        expressions.append(
            pl.when(pl.col("reviews") == "")
            .then(None)
            .otherwise(pl.col("reviews"))
            .cast(pl.Int32, strict=False)
            .alias("reviews")
        )

    cat_expr = _safe_categorical(df, "category")
    if cat_expr is not None:
        expressions.append(cat_expr)

    return df.with_columns(expressions) if expressions else df


def _safe_categorical(df: pl.DataFrame, col: str) -> pl.Expr | None:
    """Return a Categorical cast iff cardinality is low enough to benefit."""
    if col not in df.columns:
        return None
    n = len(df)
    if n == 0:
        return pl.col(col).cast(pl.Categorical)
    if df[col].n_unique() / n > CATEGORICAL_MAX_UNIQUE_RATIO:
        return None
    return pl.col(col).cast(pl.Categorical)
