from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dim_child",
    comment=(
        "Silver-internal child dimension (grain: one row per child). Derives age_band "
        "from birth_date as of registration (created_at; KindScreen's target range is "
        "0-20 years) and drops the exact birth_date and exact age — per privacy rules, "
        "the exact birth date lives only in Bronze. Internal use only: this table has "
        "child-level grain and must never be published past Silver; child_id must not "
        "reach Gold."
    ),
)
@dp.expect_or_fail("valid_child_id", "child_id IS NOT NULL")
def dim_child():
    age_years = F.floor(F.months_between(F.col("created_at"), F.col("birth_date")) / 12)

    return (
        spark.readStream.table("raw_child_updates")
        .withColumn("_age_years", age_years)
        .withColumn(
            "age_band",
            F.when(F.col("_age_years").isNull(), "unknown")
            .when((F.col("_age_years") < 0) | (F.col("_age_years") > 20), "unknown")
            .when(F.col("_age_years") <= 3, "0-3")
            .when(F.col("_age_years") <= 6, "4-6")
            .when(F.col("_age_years") <= 9, "7-9")
            .when(F.col("_age_years") <= 12, "10-12")
            .otherwise("13+"),
        )
        .select(
            "child_id",
            "child_gender",
            "age_band",
            "relationship_to_child",
            "region",
            "locale",
            "created_at",
        )
    )
