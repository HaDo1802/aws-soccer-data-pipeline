# Cloud-Native ETL Pipeline

Transfermarkt scraping pipeline for:

1. Scraping a club roster and player season logs
2. Saving raw bronze outputs locally or to S3
3. Transforming combined bronze data into cleaned outputs

## Current Project Layout

```text
cloud-native_etl_pipeline/
├── docs/                              # Architecture and AWS notes
├── lambda_deployment/                 # Minimal Lambda deployment package
│   ├── lambda_function.py             # Lambda handler entrypoint
│   └── src/
├── scripts/                           # Local and S3 wrapper entrypoints
├── src/
│   ├── cleaner/
│   ├── loader/
│   └── scraper/
├── tests/
├── README.md
└── requirements.txt
```

## Local Entry Points

Run the S3 loader from the repository root:

```bash
python scripts/run_s3_load.py --dry-run
```

Module execution is also supported:

```bash
python -m scripts.run_s3_load --dry-run
```

Direct execution of `src/loader/s3_loader.py` is not supported because it bypasses the repository-root import context required by shared modules.

## Lambda Setup

The Lambda deployment entrypoint is [lambda_deployment/lambda_function.py](/Users/hado/Desktop/Career/Coding/Data Engineer/Project/cloud-native_etl_pipeline/lambda_deployment/lambda_function.py).

Use this handler value in AWS Lambda:

```text
lambda_function.handler
```

The Lambda package is intentionally small. It includes:

- `lambda_deployment/lambda_function.py`
- `lambda_deployment/src/scraper/...`
- `lambda_deployment/src/loader/s3_bronze.py`
- `lambda_deployment/src/runtime.py`

Required environment variables:

```bash
S3_BUCKET=sport-analysis
S3_BRONZE_PREFIX=bronze
```

`S3_BRONZE_PREFIX` is optional in code and defaults to `bronze` if omitted.

### Lambda Event Payload

The function accepts a simple event payload:

```json
{
  "team": "manchester_united",
  "season": "2025"