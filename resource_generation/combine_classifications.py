import argparse

import polars as pl

# Explicit, injective short code per axis. The previous implementation built
# `functional_role` by concatenating the first letter of every classification,
# which collided (Primary/Passive -> "P", Mutualistic/Marginal -> "M") and
# depended on the input column order, so the composite role could not be
# uniquely decoded back to its axes.
ROLE_CODES = {
    "exchange_tendency": {"Producer": "Prod", "Consumer": "Cons", "Mixed": "Mix"},
    "cooperation_alignment": {"High": "Hi", "Low": "Lo"},
    "provisioning_bias": {
        "Provider": "Prov",
        "Receiver": "Recv",
        "Balanced": "Bal",
    },
    "interaction_synergy": {
        "Primary": "Prim",
        "Mutualistic": "Mut",
        "Dependent": "Dep",
        "Marginal": "Marg",
        "Neutral": "Neut",
        "Passive": "Pass",
    },
}


def combine_classifications(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    """Join per-axis classification tables and derive the composite role.

    The composite ``functional_role`` is built from a fixed axis order using
    explicit per-axis codes, so it is uniquely decodable and independent of the
    order in which the input tables are supplied.
    """
    df_joined = dfs[0]
    for df in dfs[1:]:
        df_joined = df_joined.join(df, on="taxon")

    axis_order = [axis for axis in ROLE_CODES if axis in df_joined.columns]
    code_exprs = [pl.col(axis).replace(ROLE_CODES[axis]) for axis in axis_order]

    return df_joined.with_columns(
        pl.concat_str(code_exprs, separator="-").alias("functional_role")
    )


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
    combine_classifications(dfs).write_csv(args.output, separator="\t")
