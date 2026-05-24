"""ETL orchestrator: per-department CSV scan → consolidated Parquet."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import polars as pl

from etl.compress import write_parquet
from etl.dedup import load_and_deduplicate
from etl.optimize import optimize_schema
from etl.report import compression_summary, format_bytes


def process_department(dept_path: Path, output_dir: Path) -> dict | None:
    """Run the full pipeline on one department. None if no data."""
    dept_name = dept_path.name
    csv_files = list(dept_path.rglob("*.csv"))
    if not csv_files:
        print(f"  [SKIP] {dept_name} -- no CSV files found")
        return None

    csv_bytes = sum(f.stat().st_size for f in csv_files)
    start = time.time()

    df, raw_count = load_and_deduplicate(csv_files)
    if df.is_empty():
        print(f"  [SKIP] {dept_name} -- no valid rows after deduplication")
        return None

    df = optimize_schema(df)

    regions_dir = output_dir / "regions"
    regions_dir.mkdir(parents=True, exist_ok=True)
    output_file = regions_dir / f"{dept_name}.parquet"
    parquet_bytes = write_parquet(df, output_file)

    elapsed = time.time() - start
    print(f"  [DONE] {compression_summary(dept_name, csv_bytes, parquet_bytes, len(df))} "
          f"({elapsed:.2f}s)")

    return {
        "name": dept_name,
        "csv_bytes": csv_bytes,
        "parquet_bytes": parquet_bytes,
        "unique_rows": len(df),
        "raw_count": raw_count,
        "elapsed": elapsed,
        "output_file": str(output_file.resolve()),
    }


def run_pipeline(input_dir: Path, output_dir: Path) -> None:
    """Scan input_dir per-department and consolidate into a national Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    departments = sorted(d for d in input_dir.iterdir() if d.is_dir())
    if not departments:
        print(f"No department directories found in '{input_dir}'.")
        return

    print("=" * 80)
    print("ETL PIPELINE")
    print(f"  Input:       {input_dir.resolve()}")
    print(f"  Output:      {output_dir.resolve()}")
    print(f"  Departments: {len(departments)}")
    print("=" * 80)

    results: list[dict] = []
    overall_start = time.time()
    for dept_dir in departments:
        stats = process_department(dept_dir, output_dir)
        if stats:
            results.append(stats)

    summary = _consolidate(results, output_dir)
    summary["total_time_seconds"] = time.time() - overall_start
    _print_summary(summary)
    _save_report(summary, results, output_dir)


def _consolidate(results: list[dict], output_dir: Path) -> dict:
    """Merge all department parquets into a single national parquet+CSV."""
    print("\n  [INFO] Consolidating regions into Perú.csv and Perú.parquet...")
    parquet_files = [r["output_file"] for r in results if Path(r["output_file"]).exists()]

    if not parquet_files:
        return {
            "departments_processed": 0,
            "total_raw_records": 0,
            "unique_records_department_level": 0,
            "total_unique_records_peru": 0,
            "raw_csv_size_bytes": 0,
            "peru_csv_size_bytes": 0,
            "peru_parquet_size_bytes": 0,
            "overall_reduction_percentage": 0.0,
        }

    df = pl.scan_parquet(parquet_files).unique(subset=["id"]).collect()
    peru_dir = output_dir / "Peru"
    peru_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = peru_dir / "Perú.parquet"
    csv_path = peru_dir / "Perú.csv"
    df.write_parquet(parquet_path, compression="zstd", compression_level=9)
    df.write_csv(csv_path)

    total_csv = sum(r["csv_bytes"] for r in results)
    peru_parquet_size = parquet_path.stat().st_size
    overall_reduction = (1 - peru_parquet_size / total_csv) * 100 if total_csv else 0.0

    return {
        "departments_processed": len(results),
        "total_raw_records": sum(r.get("raw_count", 0) for r in results),
        "unique_records_department_level": sum(r["unique_rows"] for r in results),
        "total_unique_records_peru": len(df),
        "raw_csv_size_bytes": total_csv,
        "peru_csv_size_bytes": csv_path.stat().st_size,
        "peru_parquet_size_bytes": peru_parquet_size,
        "overall_reduction_percentage": overall_reduction,
    }


def _print_summary(s: dict) -> None:
    print("\n" + "=" * 80)
    print("SUMMARY")
    print(f"  Departments processed: {s['departments_processed']}")
    print(f"  Total raw records:                 {s['total_raw_records']:,}")
    print(f"  Unique records (Department level): {s['unique_records_department_level']:,}")
    print(f"  Total unique records (Perú):       {s['total_unique_records_peru']:,}")
    print(f"  Raw CSV size (all source files):   {format_bytes(s['raw_csv_size_bytes'])}")
    print(f"  Final Perú.csv size:               {format_bytes(s['peru_csv_size_bytes'])}")
    print(f"  Final Perú.parquet size:           {format_bytes(s['peru_parquet_size_bytes'])}")
    print(f"  Final size reduction:              {s['overall_reduction_percentage']:.2f}%")
    if "total_time_seconds" in s:
        print(f"  Total time:                        {s['total_time_seconds']:.2f}s")
    print("=" * 80)


def _save_report(summary: dict, results: list[dict], output_dir: Path) -> None:
    payload = {
        **summary,
        "raw_csv_size_formatted": format_bytes(summary["raw_csv_size_bytes"]),
        "peru_csv_size_formatted": format_bytes(summary["peru_csv_size_bytes"]),
        "peru_parquet_size_formatted": format_bytes(summary["peru_parquet_size_bytes"]),
        "department_details": results,
    }
    path = output_dir / "etl_report.json"
    path.write_text(json.dumps(payload, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"  [INFO] Report saved to {path.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="mapScraper ETL pipeline")
    parser.add_argument("--input-dir", type=str, default="data")
    parser.add_argument("--output-dir", type=str, default="data_parquet")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / args.input_dir
    output_path = project_root / args.output_dir

    if not input_path.exists() or not input_path.is_dir():
        print(f"Error: input directory '{input_path}' does not exist.")
        return
    run_pipeline(input_path, output_path)


if __name__ == "__main__":
    main()
