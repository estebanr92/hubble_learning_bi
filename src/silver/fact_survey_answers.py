from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(
    name="stg_survey_responses_clean",
    comment=(
        "Pipeline-private staging view: normalizes answer_value (trims whitespace, "
        "lowercases — covers the documented dirty variants like 'Si', 'NO ', 'A_Veces') "
        "and deduplicates by response_row_id, the true grain/primary key of this table. "
        "Not published to Unity Catalog — only fact_survey_answers reads this."
    ),
)
def stg_survey_responses_clean():
    return (
        spark.readStream.table("raw_survey_responses")
        .withColumn("answer_value", F.lower(F.trim(F.col("answer_value"))))
        .dropDuplicates(["response_row_id"])
        .select(
            "response_row_id",
            "response_id",
            "child_id",
            "question_id",
            "answer_value",
            "submitted_at",
            "locale",
        )
    )


@dp.table(
    name="fact_survey_answers",
    comment=(
        "Silver fact table (grain: one row per child's answer to one question). Joins "
        "the cleaned response stream against dim_question to attach category/polarity, "
        "then computes normalized_value per the scoring formula in AGENTS.md section 3.1 "
        "(raw_value inverted when polarity='negative'). Internal use only — never "
        "published past Silver; child_id must not reach Gold."
    ),
)
@dp.expect_or_drop("valid_answer_value", "answer_value IN ('si', 'no', 'a_veces')")
@dp.expect_or_drop("known_question", "category IS NOT NULL AND polarity IS NOT NULL")
def fact_survey_answers():
    responses = spark.readStream.table("stg_survey_responses_clean")
    questions = spark.read.table("dim_question")

    raw_value = (
        F.when(F.col("answer_value") == "no", F.lit(0))
        .when(F.col("answer_value") == "a_veces", F.lit(1))
        .when(F.col("answer_value") == "si", F.lit(2))
    )

    return (
        responses.join(F.broadcast(questions), on="question_id", how="left")
        .withColumn("raw_value", raw_value)
        .withColumn(
            "normalized_value",
            F.when(F.col("polarity") == "positive", F.col("raw_value")).when(
                F.col("polarity") == "negative", F.lit(2) - F.col("raw_value")
            ),
        )
        .select(
            "response_row_id",
            "response_id",
            "child_id",
            "question_id",
            "category",
            "polarity",
            "answer_value",
            "raw_value",
            "normalized_value",
            "submitted_at",
            "locale",
        )
    )
