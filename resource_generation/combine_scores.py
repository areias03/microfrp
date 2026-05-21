import argparse
import polars as pl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine classifications from multiple files."
    )
    parser.add_argument("files", nargs="+", help="Path to input classification files.")
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path to the output combined classification file.",
    )
    args = parser.parse_args()

    dfs = [pl.read_csv(file) for file in args.files]
    df_joined = dfs[0]
    print(df_joined)
    for df in dfs[1:]:
        print(df)
        df_joined = df_joined.join(df, on="taxon")

    df_joined.write_csv(args.output, separator="\t")
