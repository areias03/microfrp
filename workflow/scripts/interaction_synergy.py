import polars as pl


def interaction_synergy(
    cooperation_alignment: pl.DataFrame,
    provisioning_bias: pl.DataFrame,
) -> pl.DataFrame:
    """Calculate the interaction synergy between cooperation alignment and provisioning bias.

    Args:
        cooperation_alignment (pl.DataFrame): A DataFrame containing the cooperation alignment values.
        provisioning_bias (pl.DataFrame): A DataFrame containing the provisioning bias values.

    Returns:
        pl.DataFrame: A DataFrame containing the interaction synergy values.
    """
    interaction_synergy = (
        cooperation_alignment.join(provisioning_bias, on="taxon")
        .with_columns(
            [
                pl.when(
                    (pl.col("cooperation_alignment") == "High")
                    & (pl.col("provisioning_bias") == "Provider")
                )
                .then(pl.lit("Primary"))
                .when(
                    (pl.col("cooperation_alignment") == "High")
                    & (pl.col("provisioning_bias") == "Balanced")
                )
                .then(pl.lit("Mutualistic"))
                .when(
                    (pl.col("cooperation_alignment") == "High")
                    & (pl.col("provisioning_bias") == "Receiver")
                )
                .then(pl.lit("Dependent"))
                .when(
                    (pl.col("cooperation_alignment") == "Low")
                    & (pl.col("provisioning_bias") == "Provider")
                )
                .then(pl.lit("Marginal"))
                .when(
                    (pl.col("cooperation_alignment") == "Low")
                    & (pl.col("provisioning_bias") == "Balanced")
                )
                .then(pl.lit("Neutral"))
                .when(
                    (pl.col("cooperation_alignment") == "Low")
                    & (pl.col("provisioning_bias") == "Receiver")
                )
                .then(pl.lit("Passive"))
                .alias("interaction_synergy")
            ]
        )
        .select(
            [
                "taxon",
                "interaction_synergy",
            ]
        )
    )
    return interaction_synergy


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Calculate interaction synergies for taxa."
    )
    parser.add_argument(
        "-c", "--cooperation_alignment", type=str, help="Path to interactions CSV file."
    )
    parser.add_argument(
        "-p", "--provisioning_bias", type=str, help="Path to interactions CSV file."
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path to output CSV file for scores and classifications.",
    )
    args = parser.parse_args()

    classifications = interaction_synergy(
        cooperation_alignment=pl.read_csv(args.cooperation_alignment),
        provisioning_bias=pl.read_csv(args.provisioning_bias),
    ).write_csv(args.output)
