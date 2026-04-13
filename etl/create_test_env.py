"""
Test Environment Generator.
Copies a small subset of department directories from the production data
folder into a sandbox directory for safe pipeline testing.

Usage:
    python etl/create_test_env.py
    python etl/create_test_env.py --src data --dest data_test --num-deps 3
"""

import shutil
import argparse
from pathlib import Path


def setup_test_env(src: Path, dest: Path, num_deps: int):
    """
    Clones a limited number of department directories into a test sandbox.

    If the destination already exists it is wiped first to ensure a clean
    state. Only the first N departments (alphabetically) are copied.

    Args:
        src: Path to the source data directory.
        dest: Path to the destination test directory.
        num_deps: Number of departments to copy.
    """
    if not src.exists():
        print(f"Error: Source directory '{src}' does not exist.")
        return

    if dest.exists():
        print(f"Cleaning existing test environment at '{dest}'...")
        shutil.rmtree(dest)

    dest.mkdir(parents=True)

    departments = sorted([d for d in src.iterdir() if d.is_dir()])

    if not departments:
        print("No department directories found in source data.")
        return

    selected = departments[:num_deps]
    print(f"Copying {len(selected)} department(s) to '{dest}':")

    for dep in selected:
        print(f"  -> {dep.name}")
        shutil.copytree(dep, dest / dep.name)

    print(f"Test environment ready at '{dest}'.")


def main():
    parser = argparse.ArgumentParser(description="Create a test data sandbox")
    parser.add_argument(
        "--src", type=str, default="data",
        help="Source data directory (default: data)",
    )
    parser.add_argument(
        "--dest", type=str, default="data_test",
        help="Destination test directory (default: data_test)",
    )
    parser.add_argument(
        "--num-deps", type=int, default=2,
        help="Number of departments to copy (default: 2)",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    src_path = project_root / args.src
    dest_path = project_root / args.dest

    setup_test_env(src_path, dest_path, args.num_deps)


if __name__ == "__main__":
    main()
