from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Combine TSV manifest files into one TSV using polars."
    )
    parser.add_argument("output", help="Path to the combined output TSV")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Input manifest TSV files (provide one or more, in desired order)",
    )
    parser.add_argument(
        "--dedup",
        action="store_true",
        help="Drop exact duplicate rows from the combined manifest",
    )
    args = parser.parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dfs = []
    for parser in args.inputs:
        p_path = Path(parser)
        if not p_path.exists():
            print(f"Warning: input file not found, skipping: {p_path}", file=sys.stderr)
            continue
        try:
            df = pl.read_csv(p_path)
        except Exception as e:
            print(f"Error reading '{p_path}': {e}", file=sys.stderr)
            sys.exit(1)
        dfs.append(df)

    if not dfs:
        print("No valid input files were provided/read. Exiting.", file=sys.stderr)
        sys.exit(1)

    combined = pl.concat(dfs)

    if args.dedup:
        combined = combined.unique()

    combined.write_csv(output_path)

    print(f"Wrote combined manifest to: {output_path}")


if __name__ == "__main__":
    main()
