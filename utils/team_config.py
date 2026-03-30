from typing import Any

from utils.config import Config


def config_from_request(
    request: dict[str, Any],
    default_team: str = "manchester_united",
    require_transfermarkt_identity: bool = False,
) -> Config:
    base_config = Config()
    team = request.get("team", default_team)
    club_name = request.get("club_name")
    club_slug = request.get("club_slug")
    club_id = request.get("club_id")

    if team in base_config.TEAM_CONFIGS:
        return base_config.for_team(team)

    if all([club_name, club_slug, club_id]):
        return base_config.for_runtime_team(
            team_key=team,
            club_name=str(club_name),
            club_slug=str(club_slug),
            club_id=str(club_id),
        )

    if require_transfermarkt_identity:
        raise ValueError(
            f"Unsupported team '{team}'. Provide club_name, club_slug, and club_id in the request payload."
        )

    derived_club_name = str(club_name) if club_name else " ".join(part.capitalize() for part in str(team).split("_"))
    derived_club_slug = str(club_slug) if club_slug else str(team).replace("_", "-")
    derived_club_id = str(club_id) if club_id else base_config.TRANSFERMARKT_CLUB_ID
    return base_config.for_runtime_team(
        team_key=str(team),
        club_name=derived_club_name,
        club_slug=derived_club_slug,
        club_id=derived_club_id,
    )
