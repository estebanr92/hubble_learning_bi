from pyspark import pipelines as dp
from pyspark.sql import functions as F

MIN_GROUP_SIZE = 20
OTHER_LABEL = "other"

# AGENTS.md section 3.2: only children who answered >=25 of 30 questions get a row
# in skill_priorities — this is the real completion gate the app itself uses.
SURVEY_COMPLETION_THRESHOLD = 25


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
    name="funnel_completion",
    comment=(
        "Gold: registration -> survey-completion -> skill-priority-selection funnel by "
        "registration cohort (grain: year_month). 'Completed' means >=25 of 30 questions "
        "answered — the same threshold that gates a child having a skill_priorities row "
        "in the source app (AGENTS.md section 3.2), not an invented cutoff. No child_id. "
        "Cohorts backed by fewer than 20 distinct children are folded into an 'other' "
        "bucket (k-anonymity, AGENTS.md section 4)."
    ),
)
def funnel_completion():
    answered_counts = (
        spark.read.table("fact_survey_answers")
        .groupBy("child_id")
        .agg(F.countDistinct("question_id").alias("questions_answered"))
    )

    chose_priorities = (
        spark.read.table("raw_skill_priorities")
        .select("child_id")
        .distinct()
        .withColumn("chose_priorities", F.lit(True))
    )

    funnel = (
        spark.read.table("dim_child")
        .select("child_id", "created_at")
        .withColumn("year_month", F.date_format("created_at", "yyyy-MM"))
        .join(answered_counts, on="child_id", how="left")
        .join(chose_priorities, on="child_id", how="left")
        .withColumn("questions_answered", F.coalesce(F.col("questions_answered"), F.lit(0)))
        .withColumn("completed_survey", F.col("questions_answered") >= SURVEY_COMPLETION_THRESHOLD)
        .withColumn("chose_priorities", F.coalesce(F.col("chose_priorities"), F.lit(False)))
    )

    bucketed = _apply_min_group_size(
        funnel,
        group_cols=["year_month"],
        relabel_cols=["year_month"],
    )

    return bucketed.groupBy("year_month").agg(
        F.countDistinct("child_id").alias("registered_children"),
        F.sum(F.col("completed_survey").cast("int")).alias("completed_survey_children"),
        F.sum(F.col("chose_priorities").cast("int")).alias("chose_priorities_children"),
        (F.sum(F.col("completed_survey").cast("int")) / F.countDistinct("child_id")).alias(
            "pct_completed_survey"
        ),
        (F.sum(F.col("chose_priorities").cast("int")) / F.countDistinct("child_id")).alias(
            "pct_chose_priorities"
        ),
    )
