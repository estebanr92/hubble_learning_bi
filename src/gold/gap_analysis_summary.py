from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MIN_GROUP_SIZE = 20
OTHER_LABEL = "other"

# Decided together with the user 2026-08-11 — no source mapping existed (see AGENTS.md
# section 3.3). 5 of 15 skills have no reasonable category fit and are left unmapped
# on purpose rather than forced.
SKILL_TO_CATEGORY = {
    "communication": "communication",
    "selfControl": "self_regulation",
    "regulatingEmotions": "self_regulation",
    "patience": "self_regulation",
    "kindness": "prosocial_behavior",
    "listening": "communication",
    "stressManagment": "emotional_symptoms",
    "respectOthers": "peer_relationships",
    "workingWithOthers": "peer_relationships",
    "honesty": "responsibility",
}


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
    name="gap_analysis_summary",
    comment=(
        "Gold: % of eligible children whose weakest survey category matches the category "
        "implied by their #1 prioritized skill, via the skill->category mapping decided "
        "in AGENTS.md section 3.3. 'Eligible' excludes children whose #1 priority is one "
        "of the 5 intentionally-unmapped skills. Grain: year_month. No child_id. Cohorts "
        "backed by fewer than 20 distinct children are folded into an 'other' bucket "
        "(k-anonymity, AGENTS.md section 4)."
    ),
)
def gap_analysis_summary():
    category_scores = (
        spark.read.table("fact_survey_answers")
        .groupBy("child_id", "category")
        .agg(F.avg("normalized_value").alias("avg_score"))
    )

    weakest_category = (
        category_scores.withColumn(
            "rn", F.row_number().over(Window.partitionBy("child_id").orderBy(F.col("avg_score").asc()))
        )
        .filter(F.col("rn") == 1)
        .select("child_id", F.col("category").alias("weakest_category"))
    )

    skill_category_map = F.create_map([F.lit(x) for pair in SKILL_TO_CATEGORY.items() for x in pair])

    top_priority = (
        spark.read.table("raw_skill_priorities")
        .filter(F.col("rank") == 1)
        .withColumn("priority_category", skill_category_map[F.col("skill_id")])
        .filter(F.col("priority_category").isNotNull())
        .select("child_id", "priority_category")
    )

    gap = (
        weakest_category.join(top_priority, on="child_id", how="inner")
        .join(
            spark.read.table("dim_child").select("child_id", "created_at"),
            on="child_id",
            how="inner",
        )
        .withColumn("year_month", F.date_format("created_at", "yyyy-MM"))
        .withColumn("priority_matches_weakest", F.col("weakest_category") == F.col("priority_category"))
    )

    bucketed = _apply_min_group_size(
        gap,
        group_cols=["year_month"],
        relabel_cols=["year_month"],
    )

    return bucketed.groupBy("year_month").agg(
        F.countDistinct("child_id").alias("eligible_children"),
        F.sum(F.col("priority_matches_weakest").cast("int")).alias("matching_children"),
        (F.sum(F.col("priority_matches_weakest").cast("int")) / F.countDistinct("child_id")).alias(
            "pct_priority_matches_weakest"
        ),
    )
