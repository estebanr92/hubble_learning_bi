from pyspark import pipelines as dp


@dp.materialized_view(
    name="dim_skill",
    comment=(
        "Silver-clean catalog of the 15 skills a parent can prioritize for their child, "
        "published for downstream joins and BI use. skill_id values are preserved exactly "
        "as sourced from the app's surveySkillOptions, including the known 'stressManagment' "
        "typo — it is the real join key used in skill_priorities, not an error to fix."
    ),
)
@dp.expect_or_fail("valid_skill_id", "skill_id IS NOT NULL")
def dim_skill():
    return spark.read.table("dim_skills").select(
        "skill_id",
        "name_en",
        "name_es",
    )
