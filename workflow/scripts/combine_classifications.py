import argparse
import polars as pl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine classifications from multiple files."
    )
    parser.add_argument(
        "cp", "-c", help="Path to input consumer/producer classification file."
    )
    parser.add_argument(
        "it", "-i", help="Path to input interaction tendency classification file."
    )
    parser.add_argument(
        "output",
        "-o",
        help="Path to the output combined classification file.",
    )
    args = parser.parse_args()

    cp_classifications = pl.read_csv(args.cp)
    interaction_classifications = pl.read_csv(args.it)
    classifications = cp_classifications.join(
        interaction_classifications, on="taxon", how="outer"
    ).fill_null("Unclassified")

    classifications = classifications.with_columns(
        pl.concat_str(pl.exclude("taxon").str.slice(0, 1)).alias("functional_role")
    ).write_csv(args.output)
