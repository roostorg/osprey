import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from osprey.worker.ui_api.osprey.lib.abilities import Ability
from pydantic import BaseModel

logger = logging.getLogger(__name__)

DIR = Path(__file__).parent
ACL_DEFINITIONS_DIR = DIR / 'definitions'
ACL_ABILITY_GROUPS_DIR = DIR / 'groups'
DEV_ACL_ASSIGNMENTS_FILE = DIR / 'dev_acl_assignments.json'


class ACL(BaseModel):
    _acls: Dict[str, List[Ability[Any, Any]]] = {}
    _ability_groups: Dict[str, List[Ability[Any, Any]]] = {}

    @classmethod
    def _load(cls) -> None:
        # first load and validate ability groups
        for path in ACL_ABILITY_GROUPS_DIR.iterdir():
            if path.is_file() and path.suffix == '.json':
                group_name = path.stem.upper()
                logger.debug(f'Loading ability group `{group_name}`.')
                with open(path) as ability_group_file:
                    ability_group = json.load(ability_group_file)
                    group_abilities = ability_group.get('acl')
                    cls._ability_groups[group_name] = [Ability.validate(ability) for ability in group_abilities]

        # next load acl definition files
        for path in ACL_DEFINITIONS_DIR.iterdir():
            if path.is_file() and path.suffix == '.json':
                acl_name = path.stem.upper()
                logger.debug(f'Loading ACL definition `{acl_name}`.')
                with open(path) as acl_file:
                    acl = json.load(acl_file)
                    role_acls = acl.get('acl')
                    ability_groups = acl.get('ability_groups')

                    abilities = [Ability.validate(role) for role in role_acls]
                    if ability_groups:
                        for group in ability_groups:
                            abilities += cls._ability_groups.get(group, [])
                    cls._acls[acl_name] = abilities

    @classmethod
    def get_one(cls, name: str) -> List[Ability[Any, Any]]:
        abilities = cls._acls.get(name)
        if abilities is None:
            logger.warning(f'ACL `{name}` not found.')
        return abilities if abilities else []

    @classmethod
    def get_roles(cls) -> List[str]:
        return list(cls._acls.keys())


ACL._load()
