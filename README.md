# Cloud-Native ETL Pipeline

## 1. Project Overview

- Cloud-native ETL pipeline for Transfermarkt squad and player season data.
- Separates ingestion into focused stages: roster scraping, player scraping, bronze aggregation, and local silver transformation.
- Supports both local execution and AWS Lambda deployment.
- Writes immutable snapshot-style outputs to partitioned storage for repeatable downstream processing.

## 2. Architecture

```text
Transfermarkt
    |
    v
+------------------------+
| scrape_roster Lambda   |
| - fetch squad list     |
| - write roster JSON    |
+------------------------+
    |
    v
+------------------------+
| scrape_players Lambda  |
| - fan out by player    |
| - write player JSON    |
+------------------------+
    |
    v
+-------------------------------+
| combine_player_json_to_csv    |
| - read bronze player JSON     |
| - aggregate rows to CSV       |
+-------------------------------+
    |
    v
S3 Bronze
    |
    v
Local cleaner / downstream analytics
    |
    v
S3 Silver or consumer datasets
```

- Lambda execution units are intentionally small and single-purpose.
- Step Functions is the natural orchestration layer for roster -> player -> aggregator sequencing.
- Bronze storage is append-only by snapshot date.
- Cleaner logic is kept separate from scraping and loading to preserve clear ETL boundaries.

## 3. Project Structure

```text
cloud-native_etl_pipeline/
├── lambda_deployment/
│   ├── scrape_roster_handler.py
│   ├── scrape_players_handler.py
│   └── combine_player_json_to_csv_handler.py
├── scripts/
│   ├── run_local_team_roster.py
│   ├── run_local_player_logs.py
│   ├── run_local_scrape_all.py
│   ├── run_local_clean_player_stats.py
│   ├── run_s3_load.py
│   ├── test_roster_lambda.py
│   ├── test_player_lambda.py
│   └── test_combine_player_csv_lambda.py
├── src/
│   ├── scraper/
│   │   ├── transfermarkt_client.py
│   │   ├── scrape_roster.py
│   │   └── scrape_player.py
│   ├── loader/
│   │   ├── local_bronze.py
│   │   └── s3_loader.py
│   └── cleaner/
│       └── transform_player_stats.py
├── tests/
├── utils/
│   ├── config.py
│   └── logger.py
├── build_lambda.sh
├── Makefile
└── requirements.txt
```

- `src/scraper`: HTTP client and scraping logic.
- `src/loader`: local file persistence and S3 persistence.
- `src/cleaner`: post-ingestion transformation for curated outputs.
- `lambda_deployment`: Lambda-specific entrypoints only.
- `scripts`: local execution and smoke-test utilities.
- `utils`: shared config and logging.

## 4. Data Flow

### Lambda Responsibilities

- `scrape_roster_handler.handler`
  - Inputs: `team`, `season`
  - Scrapes the squad roster for a club-season.
  - Writes one roster JSON snapshot to S3 bronze.

- `scrape_players_handler.handler`
  - Inputs: `team`, `season`, optional `player`, optional `competition`
  - Resolves the squad, scrapes player match logs, and writes one JSON file per player.
  - Supports targeted re-runs for a single player.

- `combine_player_json_to_csv_handler.handler`
  - Inputs: `team`, `season`, optional `scrape_date`
  - Reads the bronze player JSON snapshots for a season/date.
  - Produces one combined season CSV in bronze storage.

### S3 Bronze Layout

- Roster snapshot:

```text
s3://<bucket>/<bronze_prefix>/transfermarkt/<team>/team_roster/season=<YYYY>/snapshot_date=<YYYY-MM-DD>.json
```

- Player-level snapshot:

```text
s3://<bucket>/<bronze_prefix>/transfermarkt/<team>/player_detailed_stats_individual/<player_key>/season=<YYYY>/snapshot_date=<YYYY-MM-DD>.json
```

- Combined season snapshot:

```text
s3://<bucket>/<bronze_prefix>/transfermarkt/<team>/player_detailed_stats_combined/season=<YYYY>/snapshot_date=<YYYY-MM-DD>.csv
```

### Current Code Key Pattern

- The current implementation stores:

```text
<bronze_prefix>/transfermarkt/<team>/<artifact>/.../<season>/scrape_date=<YYYY-MM-DD>.<json|csv>
```

- This is functionally equivalent to season/date partitioning, but the naming is being standardized conceptually in this README as:
  - `season=<YYYY>`
  - `snapshot_date=<YYYY-MM-DD>`

## 5. Local Development

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

### Quality Checks

```bash
make format
make lint
make test
```

### Local Pipeline Runs

- Scrape roster:

```bash
python scripts/run_local_team_roster.py --team manchester_united --season 2025
```

- Scrape player logs and build combined bronze CSV:

```bash
python scripts/run_local_player_logs.py --team manchester_united --season 2025
```

- Run end-to-end local bronze generation:

```bash
python scripts/run_local_scrape_all.py --team manchester_united --season 2025
```

- Transform bronze to cleaned local silver output:

```bash
python scripts/run_local_clean_player_stats.py --team manchester_united --season 2025
```

- Upload local files to S3:

```bash
python scripts/run_s3_load.py --team manchester_united --season 2025 --bucket sport-analysis
```

### Local Output Layout

```text
data/
├── bronze/
│   └── transfermarkt/<team>/
│       ├── team_roster/<season>/scrape_date=<YYYY-MM-DD>.json
│       ├── player_detailed_stats_individual/<player_key>/<season>/scrape_date=<YYYY-MM-DD>.json
│       └── player_detailed_stats_combined/<season>/scrape_date=<YYYY-MM-DD>.csv
└── silver/
    └── transfermarkt/<team>/player_stats/<season>/scrape_date=<YYYY-MM-DD>.csv
```

## 6. Deployment

### Build Lambda Artifacts

```bash
./build_lambda.sh scrape-roster scrape_roster_handler.py
./build_lambda.sh scrape-players scrape_players_handler.py
./build_lambda.sh combine-player-json-to-csv combine_player_json_to_csv_handler.py
```

### Deploy via AWS CLI

- Update existing functions:

```bash
aws lambda update-function-code \
  --function-name scrape-roster \
  --zip-file fileb://scrape-roster.zip

aws lambda update-function-code \
  --function-name scrape-players \
  --zip-file fileb://scrape-players.zip

aws lambda update-function-code \
  --function-name combine-player-json-to-csv \
  --zip-file fileb://combine-player-json-to-csv.zip
```

- Example handler configuration:

```text
scrape_roster_handler.handler
scrape_players_handler.handler
combine_player_json_to_csv_handler.handler
```

- Example invoke commands:

```bash
aws lambda invoke \
  --function-name scrape-roster \
  --payload '{"team":"manchester_united","season":"2025"}' \
  roster-response.json

aws lambda invoke \
  --function-name scrape-players \
  --payload '{"team":"manchester_united","season":"2025"}' \
  players-response.json

aws lambda invoke \
  --function-name combine-player-json-to-csv \
  --payload '{"team":"manchester_united","season":"2025","scrape_date":"2026-03-26"}' \
  combine-response.json
```

### Recommended Orchestration

- Use AWS Step Functions to:
  - start roster ingestion
  - invoke player ingestion after roster completion
  - invoke aggregation after player snapshots complete
  - capture retry policy and execution history

## 7. Environment Variables

### Required for Lambda

```bash
S3_BUCKET=sport-analysis
```

### Optional

```bash
S3_BRONZE_PREFIX=bronze
LOG_PATH=/tmp/logs/scraper.log
```

### Runtime Notes

- Lambda file writes must target `/tmp`.
- AWS credentials and region should be provided by the Lambda execution role and runtime configuration.
- Team defaults are defined in `utils/config.py`.

## 8. Design Decisions

- Modular separation of concerns
  - Scraping, storage, transformation, and orchestration are isolated into distinct modules and handlers.

- Multiple Lambda functions instead of one monolith
  - Keeps each unit focused, easier to retry, easier to observe, and easier to scale independently.

- Step Functions for orchestration
  - Better fit than embedding workflow logic inside Lambda.
  - Provides explicit sequencing, retry control, and execution visibility.

- S3 bronze partitioning by season and snapshot date
  - Preserves point-in-time snapshots.
  - Supports deterministic reprocessing and auditability.
  - Enables targeted backfills by team, season, or snapshot date.

- Append-only raw storage
  - Raw bronze outputs are stored as immutable snapshots rather than overwritten state.

- Idempotent processing model
  - Re-running the same scrape writes a new dated snapshot instead of mutating prior outputs.
  - Aggregation can be repeated for a specific `scrape_date`.

- Local-first development workflow
  - Scrapers, loaders, and cleaners can be exercised locally before packaging for Lambda.

- Cloud-native storage boundary
  - S3 is the durable system of record for raw scrape outputs.
  - Lambda remains stateless apart from transient `/tmp` usage.
