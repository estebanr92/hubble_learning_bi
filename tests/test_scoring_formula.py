"""
Spec test for the raw_value / normalized_value scoring formula (AGENTS.md section 3.1),
the core business logic in src/silver/fact_survey_answers.py.

STATUS: written 2026-08-11, NEVER RUN — see tests/conftest.py docstring for the
environment this needs.

IMPORTANT — this does NOT import the real fact_survey_answers() function. That
function reads from `spark.readStream.table(...)` (a chain of streaming reads back
to raw_survey_responses), which needs a real streaming source to exercise in a
test — meaningfully harder to fake than a batch read. Instead, this file keeps its
own copy of the same F.when(...) expression and checks it against the full truth
table we worked out by hand.

>>> If the formula in src/silver/fact_survey_answers.py ever changes, this copy
>>> must be updated to match, or this test will start asserting the OLD behavior
>>> instead of catching a real regression. A future improvement (once the pipeline
>>> can actually run) is converting this into a true streaming test so there's only
>>> one copy of the formula. Tracked as a known gap, not silently assumed correct.
"""

from pyspark.sql import functions as F


def _compute_raw_and_normalized(df):
    """Exact copy of the expression in src/silver/fact_survey_answers.py — see the
    module docstring above for why this isn't imported from the real file."""
    raw_value = (
        F.when(F.col("answer_value") == "no", F.lit(0))
        .when(F.col("answer_value") == "a_veces", F.lit(1))
        .when(F.col("answer_value") == "si", F.lit(2))
    )
    return df.withColumn("raw_value", raw_value).withColumn(
        "normalized_value",
        F.when(F.col("polarity") == "positive", F.col("raw_value")).when(
            F.col("polarity") == "negative", F.lit(2) - F.col("raw_value")
        ),
    )


def test_full_truth_table(spark):
    # (answer_value, polarity) -> (expected_raw_value, expected_normalized_value)
    cases = [
        ("no", "positive", 0, 0),
        ("a_veces", "positive", 1, 1),
        ("si", "positive", 2, 2),
        ("no", "negative", 0, 2),
        ("a_veces", "negative", 1, 1),
        ("si", "negative", 2, 0),
    ]
    df = spark.createDataFrame(
        [(av, pol) for av, pol, _, _ in cases], ["answer_value", "polarity"]
    )

    result = {
        (row["answer_value"], row["polarity"]): (row["raw_value"], row["normalized_value"])
        for row in _compute_raw_and_normalized(df).collect()
    }

    for answer_value, polarity, expected_raw, expected_normalized in cases:
        actual_raw, actual_normalized = result[(answer_value, polarity)]
        assert actual_raw == expected_raw, f"raw_value for {answer_value}/{polarity}"
        assert actual_normalized == expected_normalized, (
            f"normalized_value for {answer_value}/{polarity} — this is the score that "
            f"ends up averaged into every Gold table, a wrong value here silently "
            f"skews every downstream number"
        )


def test_a_veces_is_polarity_invariant(spark):
    # the midpoint case discussed at length in this project: inverting the midpoint
    # of a symmetric 0-2 scale must always give back the same midpoint.
    df = spark.createDataFrame([("a_veces", "positive"), ("a_veces", "negative")], ["answer_value", "polarity"])
    rows = _compute_raw_and_normalized(df).collect()

    assert all(row["normalized_value"] == 1 for row in rows)


def test_unmapped_answer_value_yields_null(spark):
    # any answer_value outside si/no/a_veces should fall through to NULL raw_value —
    # in the real pipeline this row then gets dropped by the valid_answer_value
    # expectation on fact_survey_answers, never silently scored as 0.
    df = spark.createDataFrame([("garbage", "positive")], ["answer_value", "polarity"])
    row = _compute_raw_and_normalized(df).collect()[0]

    assert row["raw_value"] is None
    assert row["normalized_value"] is None
