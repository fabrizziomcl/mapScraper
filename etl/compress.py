"""
Compression module.
Writes an optimized Polars DataFrame to a Parquet file using Zstandard
compression. Provides configurable compression levels for balancing speed
against file size.
"""

from pathlib import Path
import polars as pl


def write_parquet(df: pl.DataFrame, output_path: Path, compression_level: int = 9) -> int:
    """
    Serializes a DataFrame to a ZSTD-compressed Parquet file.

    Args:
        df: DataFrame to write.
        output_path: Destination file path (will be created or overwritten).
        compression_level: ZSTD level (1-22). Higher = smaller file, slower write.
                           Default 9 is a strong balance for cold storage.

    Returns:
        Size of the written Parquet file in bytes.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(
        output_path,
        compression="zstd",
        compression_level=compression_level,
    )

    return output_path.stat().st_size
