from pyspark import pipelines as dp


@dp.materialized_view(
    name="dim_question",
    comment=(
        "Silver-clean catalog of the 30 survey questions, published for downstream "
        "joins and BI use. Source of truth for category and polarity, used to compute "
        "normalized_value in fact_survey_answers."
    ),
)
@dp.expect_or_fail("valid_question_id", "question_id IS NOT NULL")
@dp.expect_or_fail("valid_polarity", "polarity IN ('positive', 'negative')")
@dp.expect_or_fail("valid_category", "category IS NOT NULL")
def dim_question():
    return spark.read.table("dim_questions").select(
        "question_id",
        "text_en",
        "text_es",
        "category",
        "polarity",
    )
