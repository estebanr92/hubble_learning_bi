from pyspark import pipelines as dp
from pyspark.sql import functions as F

MIN_GROUP_SIZE = 20
OTHER_LABEL = "other"


def _apply_min_group_size(df, group_cols, relabel_cols, child_id_col="child_id"):
    """K-anonymity guard (AGENTS.md section 4): for any combination of group_cols
    backed by fewer than MIN_GROUP_SIZE distinct children, replace relabel_cols
    with OTHER_LABEL so the row can't be traced back to a small, identifiable
    group. Intentionally duplicated across Gold tables rather than imported from
    a shared module — see AGENTS.md section 8 for why."""
    group_sizes = df.groupBy(*group_cols).agg(F.countDistinct(child_id_col).alias("_group_child_count"))
    small_groups = group_sizes.filter(F.col("_group_child_count") < MIN_GROUP_SIZE).select(*group_cols).distinct()

    small_rows = df.join(F.broadcast(small_groups), on=group_cols, how="left_semi")
    large_rows = df.join(F.broadcast(small_groups), on=group_cols, how="left_anti")

    for c in relabel_cols:
        small_rows = small_rows.withColumn(c, F.lit(OTHER_LABEL))

    return large_rows.unionByName(small_rows)


@dp.materialized_view(
    name="agg_scores_by_cohort",
    comment=(
        "Gold: average category score by registration cohort (grain: year_month x "
        "category). No child_id. Cohort/category rows backed by fewer than 20 distinct "
        "children are folded into an 'other' cohort bucket (k-anonymity, AGENTS.md "
        "section 4) instead of being published as-is."
    ),
)
def agg_scores_by_cohort():
    child_category_scores = (
        spark.read.table("fact_survey_answers")
        .groupBy("child_id", "category")
        .agg(F.avg("normalized_value").alias("child_category_score"))
    )

    cohorts = child_category_scores.join(
        spark.read.table("dim_child").select("child_id", "created_at"),
        on="child_id",
        how="inner",
    ).withColumn("year_month", F.date_format("created_at", "yyyy-MM"))

    bucketed = _apply_min_group_size(
        cohorts,
        group_cols=["year_month", "category"],
        relabel_cols=["year_month"],
    )

    return bucketed.groupBy("year_month", "category").agg(
        F.avg("child_category_score").alias("avg_score"),
        F.countDistinct("child_id").alias("child_count"),
    )
