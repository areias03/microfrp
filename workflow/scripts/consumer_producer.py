from typing import Union

import polars as pl


def consumer_producer(
    exchanges: pl.DataFrame,
    mes: pl.DataFrame,
    taxa: Union[None, str] = None,
):
    """Calculate the consumer/producer score for a taxon.

    This parameter is defined as the sum of all import and export fluxes
    multiplied by their repective metabolite's Metabolite Exchange Score (MES).
    It represents the harmonic mean of all fluxes for a single taxon. A negative
    value indicates that the taxon consumes more impactful metabolites than what
    it produces. A positive value indicates higher rates of production of impactful
    metabolites to the whole community.

    :param exchanges: The exchanges.
    :type results: :class:`polars.DataFrame`

    :param mes: The Metabolite Exchange Scores.
    :type results: :class:`polars.DataFrame`

    :param taxa: The focal taxa to use. Can be a single taxon or None in which case all taxa are considered.
    :type taxa: Union[str, None]

    polars.DataFrame
        The scores and classifications for each taxon.

    References
    ----------
    .. [1] Marcelino, V.R., et al.
           Disease-specific loss of microbial cross-feeding interactions in the human gut
           Nat Commun 14, 6546 (2023). https://doi.org/10.1038/s41467-023-42112-w


    """
    exchanges = exchanges.filter(exchanges["taxon"] != "medium").drop("")
    mes = mes.drop(
        col for col in mes.columns if col not in ["sample_id", "metabolite", "MES"]
    )

    if taxa is not None:
        exchanges = exchanges.filter(exchanges["taxon"] == taxa)

    exchanges = exchanges.join(mes, on=["sample_id", "metabolite"])

    exchanges = exchanges.with_columns(reaction_score=pl.col("flux") * pl.col("MES"))

    exchanges = exchanges.group_by("taxon").agg(
        cp_score=pl.col("reaction_score").sum(),
    )

    classifications = exchanges.with_columns(
        pl.when(pl.col("cp_score") >= pl.col("cp_score").quantile(0.75))
        .then(pl.lit("Producer"))
        .when(pl.col("cp_score") <= pl.col("cp_score").quantile(0.25))
        .then(pl.lit("Consumer"))
        .otherwise(pl.lit("Mixed"))
        .alias("uptake_tendency")
    )

    return classifications.select(["taxon", "uptake_tendency"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Calculate consumer/producer scores for taxa."
    )
    parser.add_argument("exchanges", type=str, help="Path to exchanges CSV file.")
    parser.add_argument("mes", type=str, help="Path to MES CSV file.")
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

    exchanges = pl.read_csv(args.exchanges)
    mes = pl.read_csv(args.mes)

    classifications = consumer_producer(
        exchanges=exchanges,
        mes=mes,
        taxa=args.taxa,
    ).write_csv(args.output)
