"""
Deduplication module.
Reads raw district-level CSVs and removes duplicate records based on the
Google Place ID column ('id'). Handles schema inconsistencies across files
by reading all columns as strings before applying filters.
"""

from pathlib import Path
import polars as pl


def load_and_deduplicate(csv_paths: list[Path]) -> tuple[pl.DataFrame, int]:
    """
    Reads multiple CSV files into a single DataFrame, drops rows with
    empty or null IDs, and removes duplicates by Place ID.

    All columns are initially ingested as Utf8 (String) to prevent schema
    conflicts between files that may have been generated at different times
    or with slightly different content shapes.

    Args:
        csv_paths: List of absolute Path objects pointing to CSV files.

    Returns:
        A tuple: (deduplicated Polars DataFrame, raw_row_count).
    """
    if not csv_paths:
        return pl.DataFrame(), 0

    dataframes = []
    
    # Process files individually to handle varying schema lengths (e.g. absent columns)
    # Using eager read avoids massive Lazy AST recursion on thousands of files
    for p in csv_paths:
        try:
            df = pl.read_csv(
                str(p), 
                infer_schema_length=0, 
                ignore_errors=True
            )
            if not df.is_empty():
                dataframes.append(df)
        except Exception:
            continue
            
    if not dataframes:
        return pl.DataFrame(), 0
        
    raw_count = sum(len(df) for df in dataframes)
    
    # how="diagonal" fills missing columns with nulls instead of throwing an error
    df = pl.concat(dataframes, how="diagonal")

    df = df.filter(pl.col("id").is_not_null() & (pl.col("id") != ""))
    df = df.unique(subset=["id"])

    return df, raw_count
