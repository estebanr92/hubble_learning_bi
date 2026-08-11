from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = "/Volumes/hubble_learning/dev/raw"


@dp.table(
    name="raw_skill_priorities",
    comment=(
        "Bronze streaming ingestion of the daily skill_priorities.csv export "
        "(one row per ranked skill choice, only present for children who completed "
        "at least 25 of the 30 questions). The file does not exist for every dt=YYYY-MM-DD/ "
        "partition — Auto Loader simply finds no match on days without one, no special "
        "handling required."
    ),
)
def raw_skill_priorities():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "dt")
        .option("pathGlobFilter", "skill_priorities.csv")
        .load(RAW_VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
