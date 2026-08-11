from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dq_pipeline_observability",
    comment=(
        "Gold: pipeline observability by ingestion date (dt) — rows ingested, freshness "
        "(lag between the simulated export date and when we actually processed it), and "
        "% of rows affected by known data-quality issues (duplicates, dirty answer_value, "
        "unknown question_id). No k-anonymity threshold here — this aggregates pipeline "
        "volume, not children (AGENTS.md section 4 only applies to child-segmented "
        "tables)."
    ),
)
def dq_pipeline_observability():
    raw = spark.read.table("raw_survey_responses")
    known_questions = spark.read.table("dim_question").select("question_id").withColumn(
        "_is_known_question", F.lit(True)
    )

    dup_window = Window.partitionBy("response_row_id").orderBy(F.col("_ingested_at"))

    flagged = (
        raw.withColumn("_is_dirty_answer", F.col("answer_value") != F.lower(F.trim(F.col("answer_value"))))
        .withColumn("_is_duplicate", F.row_number().over(dup_window) > 1)
        .join(known_questions, on="question_id", how="left")
        .withColumn("_is_known_question", F.coalesce(F.col("_is_known_question"), F.lit(False)))
    )

    return (
        flagged.groupBy("dt")
        .agg(
            F.count("*").alias("rows_ingested"),
            F.min("_ingested_at").alias("first_ingested_at"),
            F.max("_ingested_at").alias("last_ingested_at"),
            F.datediff(F.to_date(F.min("_ingested_at")), F.to_date(F.col("dt"))).alias("ingestion_lag_days"),
            F.sum(F.col("_is_dirty_answer").cast("int")).alias("dirty_answer_value_rows"),
            F.sum(F.col("_is_duplicate").cast("int")).alias("duplicate_rows"),
            F.sum((~F.col("_is_known_question")).cast("int")).alias("unknown_question_rows"),
        )
        .withColumn("pct_dirty_answer_value", F.col("dirty_answer_value_rows") / F.col("rows_ingested"))
        .withColumn("pct_duplicate", F.col("duplicate_rows") / F.col("rows_ingested"))
        .withColumn("pct_unknown_question", F.col("unknown_question_rows") / F.col("rows_ingested"))
    )
