import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


def save_local_individual_json(
    data: dict,
    source: str,
    team: str,
    artifact_name: str,
    season: str,
    entity: Optional[str] = None,
    config: Optional[Config] = None,
) -> Path:
    active_config = config or Config()
    base_path = Path(active_config.LOCAL_RAW_ROOT) / source / team / artifact_name
    if entity:
        base_path = base_path / entity
    base_path = base_path / season
    base_path.mkdir(parents=True, exist_ok=True)

    scrape_date = datetime.now(timezone.utc).date().isoformat()
    data_path = base_path / f"scrape_date={scrape_date}.json"
    data_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    LOGGER.info("Wrote bronze data to %s", data_path)
    return data_path


def save_local_combined_csv(
    rows: list[dict[str, Any]],
    source: str,
    team: str,
    artifact_name: str,
    season: str,
    config: Optional[Config] = None,
) -> Optional[Path]:
    if not rows:
        return None

    active_config = config or Config()
    base_path = (
        Path(active_config.LOCAL_RAW_ROOT)
        / source
        / team
        / artifact_name
        / season
    )
    base_path.mkdir(parents=True, exist_ok=True)

    scrape_date = datetime.now(timezone.utc).date().isoformat()
    scraped_at = datetime.now(timezone.utc).isoformat()
    data_path = base_path / f"scrape_date={scrape_date}.csv"
    enriched_rows = [
        {
            **row,
            "scrape_date": scrape_date,
            "scraped_at": scraped_at,
        }
        for row in rows
    ]
    fieldnames = list(enriched_rows[0].keys())
    with data_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_rows)

    LOGGER.info("Wrote bronze CSV data to %s", data_path)
    return data_path
