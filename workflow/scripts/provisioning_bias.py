from typing import Union

import polars as pl


def provisioning_bias(
    interactions: pl.DataFrame,
    taxa: Union[None, str] = None,
) -> pl.DataFrame:
    """
    Calculate provisioning bias classifications for taxa based on exchange fluxes and MES.

    Parameters
    ----------
    interactions : pl.DataFrame
        The interactions between taxa and metabolites, including fluxes.
    taxa : str, optional
        A specific taxon to analyze (default: None, which analyzes all taxa).

    Returns
    -------
    pl.DataFrame
        The scores and classifications for each taxon.

    """
    if taxa is not None:
        interactions = interactions.filter(pl.col("focal") == taxa)

    result = (
        interactions.group_by("focal")
        .agg(
            [
                pl.col("class")
                .filter(pl.col("class") == "provided")
                .count()
                .alias("provided_count"),
                pl.col("class")
                .filter(pl.col("class") == "received")
                .count()
                .alias("received_count"),
            ]
        )
        .with_columns(
            [
                (
                    (pl.col("provided_count") - pl.col("received_count"))
                    / (pl.col("provided_count") + pl.col("received_count"))
                ).alias("provisioning_bias_score"),
            ]
        )
        .select(
            [
                "focal",
                "provisioning_bias_score",
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("provisioning_bias_score") > 0)
                .then(pl.lit("Provider"))
                .when(pl.col("provisioning_bias_score") < 0)
                .then(pl.lit("Receiver"))
                .otherwise(pl.lit("Balanced"))
                .alias("provisioning_bias"),
            ]
        )
    )
    return result.rename({"focal": "taxon"}).select(["taxon", "provisioning_bias"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Calculate provisioning biases for taxa."
    )
    parser.add_argument("interactions", type=str, help="Path to interactions CSV file.")
    parser.add_argument(
        "--taxa",
        type=str,
        default=None,
        help="Focal taxon to analyze (default: all taxa).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path to output CSV file for scores and classifications.",
    )
    args = parser.parse_args()

    interactions = pl.read_csv(args.interactions)

    classifications = provisioning_bias(
        interactions=interactions,
        taxa=args.taxa,
    ).write_csv(args.output)
