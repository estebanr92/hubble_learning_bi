"""
Spec test for the age_band derivation in src/silver/dim_child.py — the privacy-
critical step that replaces exact birth_date with a coarse band (AGENTS.md
section 4: exact birth_date/age must never leave Bronze).

STATUS: written 2026-08-11, NEVER RUN — see tests/conftest.py docstring for the
environment this needs.

Same caveat as test_scoring_formula.py: dim_child() itself reads from
`spark.readStream.table("raw_child_updates")`, so this keeps its own copy of the
age_band F.when(...) expression rather than importing the real function. If that
expression changes in src/silver/dim_child.py, update this copy too.
"""

from pyspark.sql import functions as F


def _compute_age_band(df):
    """Exact copy of the expression in src/silver/dim_child.py."""
    age_years = F.floor(F.months_between(F.col("created_at"), F.col("birth_date")) / 12)
    return df.withColumn("_age_years", age_years).withColumn(
        "age_band",
        F.when(F.col("_age_years").isNull(), "unknown")
        .when((F.col("_age_years") < 0) | (F.col("_age_years") > 20), "unknown")
        .when(F.col("_age_years") <= 3, "0-3")
        .when(F.col("_age_years") <= 6, "4-6")
        .when(F.col("_age_years") <= 9, "7-9")
        .when(F.col("_age_years") <= 12, "10-12")
        .otherwise("13+"),
    )


def test_band_boundaries(spark):
    # (age_in_years_at_registration, expected_band) — every boundary from the
    # scheme decided in this project: 0-3, 4-6, 7-9, 10-12, 13+.
    cases = [
        (0, "0-3"),
        (3, "0-3"),
        (4, "4-6"),
        (6, "4-6"),
        (7, "7-9"),
        (9, "7-9"),
        (10, "10-12"),
        (12, "10-12"),
        (13, "13+"),
        (20, "13+"),
        (25, "unknown"),  # older than the app's documented 0-20 range -> treated as bad data
        (-1, "unknown"),  # birth_date after created_at -> impossible, not silently banded
    ]
    rows = [
        (f"2024-06-15", f"{2024 - age}-06-15") for age, _ in cases
    ]  # created_at, birth_date approximating each age at registration
    df = spark.createDataFrame(
        [(created_at, birth_date) for created_at, birth_date in rows],
        ["created_at", "birth_date"],
    )
    df = df.withColumn("created_at", F.to_date("created_at")).withColumn(
        "birth_date", F.to_date("birth_date")
    )

    result = [row["age_band"] for row in _compute_age_band(df).collect()]
    expected = [band for _, band in cases]

    assert result == expected


def test_null_birth_date_is_unknown_not_dropped(spark):
    # a child with missing birth_date must still get a row (bucketed "unknown"),
    # never silently disappear from dim_child.
    df = spark.createDataFrame([("2024-06-15", None)], ["created_at", "birth_date"])
    df = df.withColumn("created_at", F.to_date("created_at"))

    row = _compute_age_band(df).collect()[0]
    assert row["age_band"] == "unknown"
