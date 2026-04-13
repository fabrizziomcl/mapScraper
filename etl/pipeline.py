"""
Data Preprocessing Pipeline.
Orchestrates the full ETL flow: scan -> deduplicate -> optimize -> compress.
Processes each department directory independently, consolidating all nested
district CSVs into a single ZSTD-compressed Parquet file per region.

Usage:
    python etl/pipeline.py
    python etl/pipeline.py --input-dir data_test --output-dir data_parquet
"""

import time
import argparse
import json
from pathlib import Path
import polars as pl

from etl.dedup import load_and_deduplicate
from etl.optimize import optimize_schema
from etl.compress import write_parquet
from etl.report import compression_summary, format_bytes


def process_department(dept_path: Path, output_dir: Path) -> dict | None:
    """
    Runs the full pipeline for a single department directory.

    Steps:
      1. Discover all CSV files recursively under the department path.
      2. Load and deduplicate by Google Place ID.
      3. Optimize column types for analytical storage.
      4. Write to ZSTD-compressed Parquet.
      5. Return a stats dict for the summary report.

    Args:
        dept_path: Path to a department directory containing province subdirs.
        output_dir: Destination directory for Parquet output files.

    Returns:
        A dict with keys {name, csv_bytes, parquet_bytes, unique_rows, elapsed}
        or None if the department had no processable data.
    """
    dept_name = dept_path.name
    csv_files = list(dept_path.rglob("*.csv"))

    if not csv_files:
        print(f"  [SKIP] {dept_name} -- no CSV files found")
        return None

    csv_bytes = sum(f.stat().st_size for f in csv_files)
    start = time.time()

    # Step 1-2: Load and deduplicate
    df, raw_count = load_and_deduplicate(csv_files)

    if df.is_empty():
        print(f"  [SKIP] {dept_name} -- no valid rows after deduplication")
        return None

    # Step 3: Optimize types
    df = optimize_schema(df)

    # Step 4: Compress and write
    regions_dir = output_dir / "regions"
    regions_dir.mkdir(parents=True, exist_ok=True)
    output_file = regions_dir / f"{dept_name}.parquet"
    parquet_bytes = write_parquet(df, output_file)

    elapsed = time.time() - start
    unique_rows = len(df)

    summary = compression_summary(dept_name, csv_bytes, parquet_bytes, unique_rows)
    print(f"  [DONE] {summary} ({elapsed:.2f}s)")

    return {
        "name": dept_name,
        "csv_bytes": csv_bytes,
        "parquet_bytes": parquet_bytes,
        "unique_rows": unique_rows,
        "raw_count": raw_count,
        "elapsed": elapsed,
        "output_file": str(output_file.resolve())
    }


def run_pipeline(input_dir: Path, output_dir: Path):
    """
    Scans all department directories under input_dir and processes each one.
    Prints a final aggregate report with total compression statistics.

    Args:
        input_dir: Root data directory (e.g. 'data/' or 'data_test/').
        output_dir: Root output directory for Parquet files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    departments = sorted([d for d in input_dir.iterdir() if d.is_dir()])

    if not departments:
        print(f"No department directories found in '{input_dir}'.")
        return

    print("=" * 80)
    print("ETL PIPELINE")
    print(f"  Input:       {input_dir.resolve()}")
    print(f"  Output:      {output_dir.resolve()}")
    print(f"  Departments: {len(departments)}")
    print(f"  Engine:      Polars (multithreaded)")
    print("=" * 80)

    results = []
    overall_start = time.time()

    for dept_dir in departments:
        stats = process_department(dept_dir, output_dir)
        if stats:
            results.append(stats)

    # Aggregate report
    total_csv = sum(r["csv_bytes"] for r in results)
    total_parquet = sum(r["parquet_bytes"] for r in results)
    total_unique_dept = sum(r["unique_rows"] for r in results)
    total_raw_count = sum(r.get("raw_count", 0) for r in results)

    # Consolidation into Perú.parquet and Perú.csv
    print(f"\n  [INFO] Consolidating all regions into Perú.csv and Perú.parquet...")
    parquet_files = [r["output_file"] for r in results if Path(r["output_file"]).exists()]
    
    if parquet_files:
        peru_lf = pl.scan_parquet(parquet_files)
        peru_lf = peru_lf.unique(subset=["id"])
        peru_df = peru_lf.collect()
        peru_unique_rows = len(peru_df)
        
        peru_dir = output_dir / "Peru"
        peru_dir.mkdir(parents=True, exist_ok=True)
        
        peru_parquet_path = peru_dir / "Perú.parquet"
        peru_csv_path = peru_dir / "Perú.csv"
        
        peru_df.write_parquet(peru_parquet_path, compression="zstd", compression_level=9)
        peru_df.write_csv(peru_csv_path)
        
        peru_parquet_size = peru_parquet_path.stat().st_size
        peru_csv_size = peru_csv_path.stat().st_size
    else:
        peru_unique_rows = 0
        peru_parquet_size = 0
        peru_csv_size = 0

    overall_elapsed = time.time() - overall_start

    print("\n" + "=" * 80)
    print("SUMMARY")
    print(f"  Departments processed: {len(results)}")
    print(f"  Total raw records:                 {total_raw_count:,}")
    print(f"  Unique records (Department level): {total_unique_dept:,}")
    print(f"  Total unique records (Perú):       {peru_unique_rows:,}")
    print(f"  Raw CSV size (all source files):   {format_bytes(total_csv)}")
    print(f"  Final Perú.csv size:               {format_bytes(peru_csv_size)}")
    print(f"  Final Perú.parquet size:           {format_bytes(peru_parquet_size)}")
    
    if total_csv > 0:
        overall_reduction = (1 - peru_parquet_size / total_csv) * 100
        print(f"  Final size reduction:              {overall_reduction:.2f}%")
    else:
        overall_reduction = 0.0
        
    print(f"  Total time:                        {overall_elapsed:.2f}s")
    print("=" * 80)

    # Save report to JSON
    report_data = {
        "departments_processed": len(results),
        "total_raw_records": total_raw_count,
        "unique_records_department_level": total_unique_dept,
        "total_unique_records_peru": peru_unique_rows,
        "raw_csv_size_bytes": total_csv,
        "raw_csv_size_formatted": format_bytes(total_csv),
        "department_parquets_size_bytes": total_parquet,
        "department_parquets_size_formatted": format_bytes(total_parquet),
        "peru_csv_size_bytes": peru_csv_size,
        "peru_csv_size_formatted": format_bytes(peru_csv_size),
        "peru_parquet_size_bytes": peru_parquet_size,
        "peru_parquet_size_formatted": format_bytes(peru_parquet_size),
        "overall_reduction_percentage": overall_reduction,
        "total_time_seconds": overall_elapsed,
        "department_details": results
    }
    
    report_path = output_dir / "etl_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=4, ensure_ascii=False)
    print(f"  [INFO] Report saved to {report_path.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="MapScraper Data ETL Pipeline")
    parser.add_argument(
        "--input-dir", type=str, default="data",
        help="Path to input data directory (default: data)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="data_parquet",
        help="Path to output Parquet directory (default: data_parquet)",
    )
    args = parser.parse_args()

    # Resolve relative to the project root (parent of utils/)
    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / args.input_dir
    output_path = project_root / args.output_dir

    if not input_path.exists() or not input_path.is_dir():
        print(f"Error: Input directory '{input_path}' does not exist.")
        return

    run_pipeline(input_path, output_path)


if __name__ == "__main__":
    main()
