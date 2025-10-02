import typing
from enum import StrEnum

if typing.TYPE_CHECKING:
    from osprey.engine.language_types.labels import LabelStatus

ENTITY_LABEL_MUTATION_DIMENSION_NAME = '__entity_label_mutations'
VERDICT_DIMENSION_NAME = '__verdicts'


def ENTITY_LABEL_MUTATION_DIMENSION_VALUE(entity_type: str, label_name: str, label_status: 'LabelStatus') -> str:
    return f'{entity_type}/{label_name}/{label_status}'


class OspreyEntityTypes(StrEnum):
    USER = 'User'
    GUILD = 'Guild'
    MESSAGE = 'Message'
    CHANNEL = 'Channel'
