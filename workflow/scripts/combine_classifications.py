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
    for df in dfs[1:]:
        df_joined = df_joined.join(df, on="taxon", how="outer")
    classifications = df_joined.with_columns(
        pl.concat_str(pl.exclude("taxon").str.slice(0, 1)).alias("functional_role")
    ).write_csv(args.output)
