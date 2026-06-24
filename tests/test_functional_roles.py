"""Unit tests for the functional-role scoring functions.

These functions are the scientifically load-bearing part of the pipeline, so
the tests pin down sign conventions, threshold boundaries, and the edge cases
(empty/zero denominators) that are easy to regress.
"""

import polars as pl
import pytest

from exchange_tendency import exchange_tendency
from cooperation_alignment import cooperation_alignment
from provisioning_bias import provisioning_bias
from interaction_synergy import interaction_synergy


# --------------------------------------------------------------------------- #
# exchange_tendency
# --------------------------------------------------------------------------- #
def test_exchange_tendency_scores_and_labels():
    # One exchange per taxon with MES == 1 so et_score == flux. Scores span
    # [-10, 10] so the 0.25/0.75 quantiles land cleanly at -5 and 5.
    exchanges = pl.DataFrame(
        {
            "taxon": ["A", "B", "C", "D", "E", "medium"],
            "sample_id": ["s1"] * 6,
            "metabolite": ["m1"] * 6,
            "flux": [10.0, 5.0, 0.0, -5.0, -10.0, 99.0],
        }
    )
    mes = pl.DataFrame(
        {
            "sample_id": ["s1"],
            "metabolite": ["m1"],
            "MES": [1.0],
            "extra": [123],  # dropped by the function
        }
    )

    result = exchange_tendency(exchanges, mes)
    scores = dict(zip(result["taxon"], result["et_score"]))
    labels = dict(zip(result["taxon"], result["exchange_tendency"]))

    # "medium" is excluded entirely.
    assert "medium" not in scores
    assert scores == {"A": 10.0, "B": 5.0, "C": 0.0, "D": -5.0, "E": -10.0}
    # >= q0.75 (5) -> Producer, <= q0.25 (-5) -> Consumer, else Mixed.
    assert labels == {
        "A": "Producer",
        "B": "Producer",
        "C": "Mixed",
        "D": "Consumer",
        "E": "Consumer",
    }


def test_exchange_tendency_sums_signed_flux_times_mes():
    # A net consumer: imports (negative flux) outweigh exports.
    exchanges = pl.DataFrame(
        {
            "taxon": ["A", "A", "A"],
            "sample_id": ["s1", "s1", "s1"],
            "metabolite": ["m1", "m2", "m3"],
            "flux": [2.0, -1.0, -3.0],
        }
    )
    mes = pl.DataFrame(
        {
            "sample_id": ["s1", "s1", "s1"],
            "metabolite": ["m1", "m2", "m3"],
            "MES": [1.0, 2.0, 1.0],
        }
    )
    result = exchange_tendency(exchanges, mes)
    # 2*1 + (-1)*2 + (-3)*1 = -3
    assert result["et_score"].item() == pytest.approx(-3.0)


def test_exchange_tendency_handles_missing_index_column():
    # No unnamed "" column present: the function must not raise.
    exchanges = pl.DataFrame(
        {
            "taxon": ["A"],
            "sample_id": ["s1"],
            "metabolite": ["m1"],
            "flux": [1.0],
        }
    )
    mes = pl.DataFrame({"sample_id": ["s1"], "metabolite": ["m1"], "MES": [1.0]})
    result = exchange_tendency(exchanges, mes)
    assert result["et_score"].item() == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# cooperation_alignment
# --------------------------------------------------------------------------- #
def test_cooperation_alignment_bounded_ratio_and_split():
    interactions = pl.DataFrame(
        {
            "focal": ["X", "X", "X", "Y", "Y", "Y", "Y"],
            "partner": ["p"] * 7,
            "class": [
                "provided",
                "provided",
                "received",
                "co-consumed",
                "co-consumed",
                "co-consumed",
                "co-consumed",
            ],
            "flux": [1.0] * 7,
            "sample_id": ["s1"] * 7,
        }
    )
    result = cooperation_alignment(interactions)
    scores = dict(zip(result["taxon"], result["ca_score"]))
    labels = dict(zip(result["taxon"], result["cooperation_alignment"]))

    # X: positive=3, co_consumed=0 -> 3/3 = 1.0 ; Y: positive=0, cc=4 -> 0/4 = 0.0
    assert scores["X"] == pytest.approx(1.0)
    assert scores["Y"] == pytest.approx(0.0)
    # mean ratio = 0.5; >= mean -> High, < mean -> Low
    assert labels == {"X": "High", "Y": "Low"}


# --------------------------------------------------------------------------- #
# provisioning_bias
# --------------------------------------------------------------------------- #
def test_provisioning_bias_scores_labels_and_zero_denominator():
    interactions = pl.DataFrame(
        {
            "focal": ["P", "P", "P", "P", "R", "R", "R", "R", "B"],
            "partner": ["q"] * 9,
            "class": [
                "provided",
                "provided",
                "provided",
                "received",
                "provided",
                "received",
                "received",
                "received",
                "co-consumed",
            ],
            "flux": [1.0] * 9,
            "sample_id": ["s1"] * 9,
        }
    )
    result = provisioning_bias(interactions)
    scores = dict(zip(result["taxon"], result["pb_score"]))
    labels = dict(zip(result["taxon"], result["provisioning_bias"]))

    # P: (3-1)/4 = 0.5 ; R: (1-3)/4 = -0.5 ; B: only co-consumed -> 0/0 -> 0.0
    assert scores["P"] == pytest.approx(0.5)
    assert scores["R"] == pytest.approx(-0.5)
    assert scores["B"] == pytest.approx(0.0)
    assert labels == {"P": "Provider", "R": "Receiver", "B": "Balanced"}


# --------------------------------------------------------------------------- #
# interaction_synergy
# --------------------------------------------------------------------------- #
def test_interaction_synergy_full_truth_table():
    ca_values = ["High", "High", "High", "Low", "Low", "Low"]
    pb_values = ["Provider", "Balanced", "Receiver", "Provider", "Balanced", "Receiver"]
    expected = ["Primary", "Mutualistic", "Dependent", "Marginal", "Neutral", "Passive"]
    taxa = [f"t{i}" for i in range(6)]

    ca = pl.DataFrame({"taxon": taxa, "cooperation_alignment": ca_values})
    pb = pl.DataFrame({"taxon": taxa, "provisioning_bias": pb_values})

    result = interaction_synergy(ca, pb)
    got = dict(zip(result["taxon"], result["interaction_synergy"]))
    assert got == dict(zip(taxa, expected))
