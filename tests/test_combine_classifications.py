"""Unit tests for combining per-axis classifications into a composite role."""

import polars as pl

from combine_classifications import combine_classifications


def _axis_frames():
    taxa = ["T1", "T2"]
    et = pl.DataFrame({"taxon": taxa, "exchange_tendency": ["Producer", "Consumer"]})
    ca = pl.DataFrame({"taxon": taxa, "cooperation_alignment": ["High", "Low"]})
    pb = pl.DataFrame({"taxon": taxa, "provisioning_bias": ["Provider", "Receiver"]})
    isyn = pl.DataFrame({"taxon": taxa, "interaction_synergy": ["Primary", "Passive"]})
    return et, ca, pb, isyn


def test_functional_role_uses_explicit_codes():
    et, ca, pb, isyn = _axis_frames()
    result = combine_classifications([et, ca, pb, isyn])
    roles = dict(zip(result["taxon"], result["functional_role"]))
    assert roles == {
        "T1": "Prod-Hi-Prov-Prim",
        "T2": "Cons-Lo-Recv-Pass",
    }


def test_functional_role_is_order_independent():
    et, ca, pb, isyn = _axis_frames()
    base = combine_classifications([et, ca, pb, isyn])
    shuffled = combine_classifications([pb, isyn, et, ca])

    base_roles = dict(zip(base["taxon"], base["functional_role"]))
    shuffled_roles = dict(zip(shuffled["taxon"], shuffled["functional_role"]))
    assert base_roles == shuffled_roles


def test_primary_and_passive_no_longer_collide():
    # Both used to collapse to "P" under first-letter encoding.
    et, ca, pb, isyn = _axis_frames()
    result = combine_classifications([et, ca, pb, isyn])
    roles = result["functional_role"].to_list()
    assert roles[0].endswith("Prim")
    assert roles[1].endswith("Pass")
    assert roles[0] != roles[1]
