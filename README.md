# hubble_learning_bi

Data pipeline for KindScreen's socio-emotional skills survey. KindScreen ran a 6-month pilot
(February–August 2024) where parents filled out a 30-question survey about their child when
registering; the pilot was discontinued afterward, and this project turns that historical export
data into a retrospective BI dashboard.

## Architecture

Medallion architecture (Bronze → Silver → Gold) built with [Lakeflow Declarative
Pipelines](https://docs.databricks.com/aws/en/dlt/), deployed as a [Databricks Asset
Bundle](https://docs.databricks.com/dev-tools/bundles/index.html).

- Ingestion from a landing volume via Auto Loader, incremental, partitioned by export date
  (`dt=YYYY-MM-DD/`).
- Data quality enforced with declarative expectations (`expect_or_drop`, `expect_or_fail`), plus
  a `dq_pipeline_observability` table tracking ingestion volume, freshness, and rejected rows per
  day.
- Privacy: exact birth dates never leave Bronze — everything from Silver onward uses age bands
  instead. Every aggregated Gold table enforces a minimum group size of 20 children; smaller
  groups get folded into an `other` bucket rather than published as-is. `child_id` never reaches
  Gold.

## Where to read more

- [`medallion_architecture_overview.md`](medallion_architecture_overview.md) — what every table
  in Bronze, Silver, and Gold does and how they connect to each other. Start here.
- [`AGENTS.md`](AGENTS.md) — full technical and business context: the data model, the privacy
  rules, why each architecture decision was made, and the current project status. This is the
  source of truth for anyone picking up the project.

## Project structure

- `src/bronze/`, `src/silver/`, `src/gold/`: the pipeline's transformations, one Python file per
  table, following the medallion layers.
- `resources/`: the Lakeflow Pipeline resource definition (Databricks Asset Bundle).
- `tests/`: unit tests for shared Python code.
- `fixtures/`: local sample data fixtures.
- `databricks.yml`: the bundle definition — targets, catalog/schema variables, workspace config.

## Status

Bronze, Silver, and Gold are fully written (16 tables/views) and pass `databricks bundle
validate`. End-to-end validation against real data is currently blocked on a Databricks-side
compute provisioning issue on this workspace (open with Databricks support) — see AGENTS.md
section 8 for the live status. The dashboard has not been built yet.

## Working with this project

Choose how you want to work on it:

(a) Directly in your Databricks workspace — see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code — see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With the CLI — see https://docs.databricks.com/dev-tools/cli/databricks-cli.html.

If developing with an IDE, install dependencies with [uv](https://docs.astral.sh/uv/getting-started/installation/):

```
uv sync --dev
```

### Using the CLI

1. Authenticate, if you haven't already:
   ```
   databricks configure
   ```

2. Deploy to the `dev` target (the default — this project intentionally only uses `dev`, see
   AGENTS.md section 8):
   ```
   databricks bundle deploy
   ```

3. Run the pipeline:
   ```
   databricks bundle run hubble_learning_bi_pipeline
   ```

4. Run tests locally:
   ```
   uv run pytest
   ```
