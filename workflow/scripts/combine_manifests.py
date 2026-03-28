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
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Number of files to process at once (default: 100)",
    )
    args = parser.parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Process files in chunks to avoid memory issues
    valid_files = []
    for file_path in args.inputs:
        p_path = Path(file_path)
        if not p_path.exists():
            continue
        valid_files.append(p_path)

    if not valid_files:
        print("No valid input files were provided/read. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Process files in chunks and write incrementally
    first_chunk = True
    
    for i in range(0, len(valid_files), args.chunk_size):
        chunk_files = valid_files[i:i + args.chunk_size]
        
        dfs = []
        for p_path in chunk_files:
            try:
                df = pl.read_csv(p_path)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading '{p_path}': {e}", file=sys.stderr)
                sys.exit(1)
        
        if not dfs:
            continue
            
        chunk_combined = pl.concat(dfs)
        
        # Write chunk to output
        if first_chunk:
            # First chunk: create new file with header
            chunk_combined.write_csv(output_path)
            first_chunk = False
        else:
            # Subsequent chunks: append without header
            with open(output_path, 'ab') as f:
                chunk_combined.write_csv(f, include_header=False)
        
        # Free memory
        del dfs
        del chunk_combined

    # If deduplication is needed, read and deduplicate the combined file
    if args.dedup:
        combined = pl.read_csv(output_path)
        combined = combined.unique()
        combined.write_csv(output_path)


if __name__ == "__main__":
    main()
