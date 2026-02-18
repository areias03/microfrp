#!/usr/bin/env python3
"""
Combine multiple TSV manifest files into a single TSV using polars.

Usage:
    python scripts/combine_manifests.py OUTPUT INPUT [INPUT...]
Example:
    python scripts/combine_manifests.py results/manifests/all_samples.tsv results/manifests/sample1.tsv results/manifests/sample2.tsv

Notes:
- Columns are aligned by name; missing columns are filled with nulls.
- Files are read in the order passed on the command line.
- Use --dedup to remove exact duplicate rows after concatenation.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Combine TSV manifest files into one TSV using polars."
    )
    p.add_argument("output", help="Path to the combined output TSV")
    p.add_argument(
        "inputs",
        nargs="+",
        help="Input manifest TSV files (provide one or more, in desired order)",
    )
    p.add_argument(
        "--dedup",
        action="store_true",
        help="Drop exact duplicate rows from the combined manifest",
    )
    return p.parse_args()


def read_manifest(path: Path) -> pl.DataFrame:
    # Use tab separator and let polars infer dtypes
    return pl.read_csv(path, separator="\t")


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dfs = []
    for p in args.inputs:
        p_path = Path(p)
        if not p_path.exists():
            print(f"Warning: input file not found, skipping: {p_path}", file=sys.stderr)
            continue
        try:
            df = read_manifest(p_path)
        except Exception as e:
            print(f"Error reading '{p_path}': {e}", file=sys.stderr)
            sys.exit(1)
        dfs.append(df)

    if not dfs:
        print("No valid input files were provided/read. Exiting.", file=sys.stderr)
        sys.exit(1)

    # polars.concat aligns columns by name and fills missing values with nulls
    combined = pl.concat(dfs)

    if args.dedup:
        combined = combined.unique()

    # Write with tab separator and include header
    combined.write_csv(output_path, separator="\t")

    print(f"Wrote combined manifest to: {output_path}")


if __name__ == "__main__":
    main()
