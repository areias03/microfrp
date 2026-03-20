from typing import Union

import polars as pl


def interaction_tendencies(
    interactions: pl.DataFrame,
    taxa: Union[None, str] = None,
) -> pl.DataFrame:
    """
    Calculate consumer/producer scores for taxa based on exchange fluxes and MES.

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
                pl.col("class")
                .filter(pl.col("class") == "co-consumed")
                .count()
                .alias("co_consumed_count"),
            ]
        )
        .with_columns(
            [
                (pl.col("provided_count") + pl.col("received_count")).alias(
                    "positive_count"
                ),
            ]
        )
        .with_columns(
            [
                ((pl.col("positive_count") / pl.col("co_consumed_count")).log()).alias(
                    "log_ratio"
                ),
            ]
        )
        .select(
            [
                "focal",
                "log_ratio",
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("log_ratio") >= pl.col("log_ratio").quantile(0.5))
                .then(pl.lit("Cooperator"))
                .when(pl.col("log_ratio") < pl.col("log_ratio").quantile(0.5))
                .then(pl.lit("Uncooperator"))
                .alias("interaction_classification")
            ]
        )
    )
    return result.rename({"focal": "taxon", "log_ratio": "interaction_score"})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Calculate interaction tendency scores for taxa."
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

    classifications = interaction_tendencies(
        interactions=interactions,
        taxa=args.taxa,
    ).write_csv(args.output)
