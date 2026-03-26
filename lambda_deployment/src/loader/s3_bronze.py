import csv
import io
import json
from datetime import datetime, timezone
from typing import Any, Optional

import boto3

from src.runtime import get_logger


LOGGER = get_logger(__name__)


def save_bronze_s3(
    data: dict,
    source: str,
    team: str,
    artifact_name: str,
    season: str,
    bucket: str,
    bronze_prefix: str = "bronze",
    entity: Optional[str] = None,
) -> str:
    key_parts = [bronze_prefix, source, team, artifact_name]
    if entity is not None:
        key_parts.append(entity)
    key_parts.extend(
        [
            season,
            f"scrape_date={datetime.now(timezone.utc).date().isoformat()}.json",
        ]
    )
    key = "/".join(key_parts)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    LOGGER.info("Wrote bronze data to s3://%s/%s", bucket, key)
    return key


def save_bronze_s3_csv(
    rows: list[dict[str, Any]],
    source: str,
    team: str,
    artifact_name: str,
    season: str,
    bucket: str,
    bronze_prefix: str = "bronze",
) -> Optional[str]:
    if not rows:
        return None

    key = "/".join(
        [
            bronze_prefix,
            source,
            team,
            artifact_name,
            season,
            f"scrape_date={datetime.now(timezone.utc).date().isoformat()}.csv",
        ]
    )

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    LOGGER.info("Wrote bronze CSV data to s3://%s/%s", bucket, key)
    return key
