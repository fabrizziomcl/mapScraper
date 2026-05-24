"""Clone a few department directories into a sandbox for pipeline testing."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def setup_test_env(src: Path, dest: Path, num_deps: int) -> None:
    """Copy the first N departments from `src` into a fresh `dest`."""
    if not src.exists():
        print(f"Error: source directory '{src}' does not exist.")
        return
    if dest.exists():
        print(f"Cleaning existing test environment at '{dest}'...")
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    departments = sorted(d for d in src.iterdir() if d.is_dir())
    if not departments:
        print("No department directories found in source data.")
        return

    selected = departments[:num_deps]
    print(f"Copying {len(selected)} department(s) to '{dest}':")
    for dep in selected:
        print(f"  -> {dep.name}")
        shutil.copytree(dep, dest / dep.name)
    print(f"Test environment ready at '{dest}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an ETL test sandbox")
    parser.add_argument("--src", type=str, default="data")
    parser.add_argument("--dest", type=str, default="data_test")
    parser.add_argument("--num-deps", type=int, default=2)
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    setup_test_env(project_root / args.src, project_root / args.dest, args.num_deps)


if __name__ == "__main__":
    main()
