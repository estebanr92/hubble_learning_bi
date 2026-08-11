from pyspark import pipelines as dp

CALENDAR_START = "2024-02-01"
CALENDAR_END = "2024-08-31"


@dp.materialized_view(
    name="dim_date",
    comment=(
        "Calendar dimension covering the full months of the 6-month pilot (Feb-Aug 2024). "
        "Generated, not sourced from a landing file. Join against business dates "
        "(created_at, submitted_at) for cohort/trend analysis — never against dt, which "
        "is the technical ingestion-date partition, not a business date."
    ),
)
def dim_date():
    return spark.sql(f"""
        SELECT
            date,
            year(date) AS year,
            month(date) AS month,
            date_format(date, 'MMMM') AS month_name,
            date_format(date, 'yyyy-MM') AS year_month,
            quarter(date) AS quarter,
            day(date) AS day_of_month,
            date_format(date, 'EEEE') AS day_of_week_name,
            dayofweek(date) IN (1, 7) AS is_weekend
        FROM (
            SELECT explode(sequence(to_date('{CALENDAR_START}'), to_date('{CALENDAR_END}'))) AS date
        )
    """)
