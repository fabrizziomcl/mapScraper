"""
Reporting module.
Calculates and formats compression statistics for pipeline output.
Provides human-readable summaries of storage savings per department
and across the entire run.
"""


def format_bytes(size_bytes: int) -> str:
    """Converts a byte count to a human-readable string (KB, MB, GB)."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.2f} GB"


def compression_summary(dept_name: str, csv_bytes: int, parquet_bytes: int, unique_rows: int) -> str:
    """
    Builds a one-line summary string for a processed department.

    Args:
        dept_name: Name of the department (used as label).
        csv_bytes: Total size of raw CSV input files in bytes.
        parquet_bytes: Size of the output Parquet file in bytes.
        unique_rows: Number of unique records after deduplication.

    Returns:
        Formatted summary string.
    """
    if csv_bytes > 0:
        ratio = (1 - parquet_bytes / csv_bytes) * 100
    else:
        ratio = 0.0

    return (
        f"  {dept_name:<25} | "
        f"CSV: {format_bytes(csv_bytes):>10} -> "
        f"Parquet: {format_bytes(parquet_bytes):>10} | "
        f"Reduction: {ratio:5.1f}% | "
        f"Unique rows: {unique_rows:,}"
    )
