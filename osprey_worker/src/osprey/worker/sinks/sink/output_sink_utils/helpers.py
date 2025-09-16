from typing import Any, Dict, Optional

from osprey.worker.sinks.sink.output_sink_utils.constants import EntityType

USER_ID_FEATURE_NAME = 'UserId'  # assuming that if userid exists, it's in feature called 'UserId'


def get_user_id(entity_id: str, entity_type: str) -> Optional[int]:
    return int(entity_id) if entity_type == EntityType.USER else None


def get_guild_id(entity_id: str, entity_type: str) -> Optional[int]:
    return int(entity_id) if entity_type == EntityType.GUILD else None


def get_user_id_from_extracted_features(extracted_features: Dict[str, Any]) -> Optional[int]:
    user_id = extracted_features.get(USER_ID_FEATURE_NAME)
    return int(user_id) if (user_id and user_id.isnumeric()) else None
