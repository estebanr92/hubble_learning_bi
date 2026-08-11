from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = "/Volumes/hubble_learning/dev/raw"


@dp.materialized_view(
    name="dim_questions",
    comment=(
        "Bronze snapshot of the 30-question survey catalog "
        "(question_id, text_en, text_es, category, polarity). "
        "Static reference file, loaded once as a batch read — not partitioned by dt=, "
        "unlike the daily export tables."
    ),
)
def dim_questions():
    return (
        spark.read.csv(f"{RAW_VOLUME_PATH}/dim_questions.csv", header=True, inferSchema=True)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{RAW_VOLUME_PATH}/dim_questions.csv"))
    )
