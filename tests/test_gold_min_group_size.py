"""
Tests for `_apply_min_group_size`, the k-anonymity guard duplicated across the 4
Gold tables that need it (AGENTS.md section 4 — no published row may represent
fewer than 20 distinct children).

STATUS: written 2026-08-11, NEVER RUN — see tests/conftest.py docstring for why
(needs a plain-pyspark environment separate from this project's databricks-connect
dependency). Reviewed by hand only.

These tests import the REAL function from each of the 4 source files (not a copy),
so they also catch accidental drift between the 4 "identical" copies — the risk
that was flagged when we decided to duplicate this helper instead of sharing a
module (AGENTS.md section 8).
"""

import pytest

from conftest import import_pipeline_module

GOLD_FILES_WITH_HELPER = [
    "gold/agg_scores_by_cohort.py",
    "gold/agg_scores_by_segment.py",
    "gold/funnel_completion.py",
    "gold/gap_analysis_summary.py",
]


@pytest.fixture(params=GOLD_FILES_WITH_HELPER)
def apply_min_group_size(request, spark):
    """Parametrized over all 4 files: every test using this fixture runs once per
    copy of the helper, so a single test function proves consistency across all 4."""
    module = import_pipeline_module(request.param, spark)
    return module._apply_min_group_size


def _rows(spark, rows, columns):
    return spark.createDataFrame(rows, columns)


def test_small_group_gets_relabeled_to_other(spark, apply_min_group_size):
    # 3 categories x "year_month": one month has 25 distinct children (>= 20, keeps
    # its label), the other has only 5 (< 20, must become "other").
    rows = [(f"child_{i}", "2024-02", "self_regulation") for i in range(25)] + [
        (f"child_{i}", "2024-03", "self_regulation") for i in range(5)
    ]
    df = _rows(spark, rows, ["child_id", "year_month", "category"])

    result = apply_min_group_size(df, group_cols=["year_month", "category"], relabel_cols=["year_month"])
    labels = {row["year_month"] for row in result.select("year_month").distinct().collect()}

    assert "2024-02" in labels, "the 25-child group should keep its real label"
    assert "2024-03" not in labels, "the 5-child group must not be published under its real label"
    assert "other" in labels, "the 5-child group should be folded into 'other'"


def test_no_children_are_lost_or_duplicated(spark, apply_min_group_size):
    rows = [(f"child_{i}", "2024-02", "communication") for i in range(25)] + [
        (f"child_{i}", "2024-03", "communication") for i in range(5)
    ]
    df = _rows(spark, rows, ["child_id", "year_month", "category"])

    result = apply_min_group_size(df, group_cols=["year_month", "category"], relabel_cols=["year_month"])

    assert result.count() == 30, "bucketing must not drop or duplicate rows"
    assert result.select("child_id").distinct().count() == 30


def test_relabel_cols_only_affects_named_columns(spark, apply_min_group_size):
    # category must NOT be touched even for a small group — only year_month
    # (the actual segmenting dimension) should ever become "other".
    rows = [(f"child_{i}", "2024-03", "prosocial_behavior") for i in range(5)]
    df = _rows(spark, rows, ["child_id", "year_month", "category"])

    result = apply_min_group_size(df, group_cols=["year_month", "category"], relabel_cols=["year_month"])
    categories = {row["category"] for row in result.select("category").distinct().collect()}

    assert categories == {"prosocial_behavior"}, "category is a fixed 9-value catalog, never anonymized"


def test_exactly_20_is_not_bucketed(spark, apply_min_group_size):
    # boundary check: the rule is "< 20", so a group of exactly 20 must be published as-is.
    rows = [(f"child_{i}", "2024-04", "responsibility") for i in range(20)]
    df = _rows(spark, rows, ["child_id", "year_month", "category"])

    result = apply_min_group_size(df, group_cols=["year_month", "category"], relabel_cols=["year_month"])
    labels = {row["year_month"] for row in result.select("year_month").distinct().collect()}

    assert labels == {"2024-04"}, "a group of exactly 20 children is at the threshold, not below it"


def test_multiple_demographic_columns_bucketed_together(spark, apply_min_group_size):
    # mirrors agg_scores_by_segment's usage: relabel_cols has multiple columns,
    # all of them must flip to "other" together for a small group — never just one.
    rows = [
        ("child_1", "UK", "grandmother", "female", "13+", "peer_relationships"),
        ("child_2", "UK", "grandmother", "female", "13+", "peer_relationships"),
        ("child_3", "UK", "grandmother", "female", "13+", "peer_relationships"),
    ]
    df = _rows(
        spark, rows, ["child_id", "region", "relationship_to_child", "child_gender", "age_band", "category"]
    )

    result = apply_min_group_size(
        df,
        group_cols=["region", "relationship_to_child", "child_gender", "age_band", "category"],
        relabel_cols=["region", "relationship_to_child", "child_gender", "age_band"],
    )
    row = result.collect()[0]

    assert row["region"] == "other"
    assert row["relationship_to_child"] == "other"
    assert row["child_gender"] == "other"
    assert row["age_band"] == "other"
    assert row["category"] == "peer_relationships", "category is never part of relabel_cols"
