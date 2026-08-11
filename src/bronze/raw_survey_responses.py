from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = "/Volumes/hubble_learning/dev/raw"


@dp.table(
    name="raw_survey_responses",
    comment=(
        "Bronze streaming ingestion of the daily survey_responses.csv export "
        "(long format — one row per question answered). Auto Loader incrementally "
        "picks up new dt=YYYY-MM-DD/ partitions from the raw landing volume."
    ),
)
def raw_survey_responses():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "dt")
        .option("pathGlobFilter", "survey_responses.csv")
        .load(RAW_VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
