from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = "/Volumes/hubble_learning/dev/raw"


@dp.table(
    name="raw_child_updates",
    comment=(
        "Bronze streaming ingestion of the daily child_account_updates.csv export "
        "(one row per child account created/updated that day). Auto Loader incrementally "
        "picks up new dt=YYYY-MM-DD/ partitions from the raw landing volume."
    ),
)
def raw_child_updates():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "dt")
        .option("pathGlobFilter", "child_account_updates.csv")
        .load(RAW_VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
