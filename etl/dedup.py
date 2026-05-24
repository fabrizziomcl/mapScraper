"""Deduplicate district-level CSVs by Google Place ID."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def load_and_deduplicate(csv_paths: list[Path]) -> tuple[pl.DataFrame, int]:
    """Read CSVs, drop empty/duplicate IDs. Returns (df, raw_row_count).

    All columns are ingested as Utf8 to avoid cross-file schema conflicts.
    `how="diagonal"` fills missing columns with nulls instead of erroring.
    """
    if not csv_paths:
        return pl.DataFrame(), 0

    dataframes = []
    for path in csv_paths:
        try:
            df = pl.read_csv(str(path), infer_schema_length=0, ignore_errors=True)
            if not df.is_empty():
                dataframes.append(df)
        except Exception:
            continue

    if not dataframes:
        return pl.DataFrame(), 0

    raw_count = sum(len(df) for df in dataframes)
    df = pl.concat(dataframes, how="diagonal")
    df = df.filter(pl.col("id").is_not_null() & (pl.col("id") != ""))
    df = df.unique(subset=["id"])
    return df, raw_count
