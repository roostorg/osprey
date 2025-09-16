from __future__ import annotations

import json
from typing import Any, Iterable, List, Mapping

from osprey.worker.lib.acls.acls import ACL, DEV_ACL_ASSIGNMENTS_FILE
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.sources_config import register_config_subkey
from osprey.worker.lib.storage.temporary_ability_token import TemporaryAbilityToken
from osprey.worker.ui_api.osprey.lib.abilities import Ability
from pydantic import BaseModel, validator

OKTA_GROUP_PREFIX = 'App-Osprey-'


class UsersConfig(BaseModel):
    roles: List[str] = []
    abilities: List[Ability[Any, Any]] = []


@register_config_subkey('acl')
class AclConfig(BaseModel):
    """
    hold the `acl` config

    Validates that the config is valid on being loaded
    """

    roles: Mapping[str, List[Ability[Any, Any]]] = {}
    users: Mapping[str, UsersConfig] = {}

    @validator('users', pre=False)
    def validate_user_roles(
        cls, users: Mapping[str, UsersConfig], values: Mapping[str, Any]
    ) -> Mapping[str, UsersConfig]:
        roles = values.get('roles')
        if roles is None:
            raise ValueError('`acl.roles` subkey failed to parse')
        registered_roles = set(roles.keys())
        for user in users.values():
            for role in user.roles:
                if role not in registered_roles:
                    raise ValueError(f'`{role}` is not defined in the `roles` acl config')
        return users

    def get_abilities_for_unregistered_user(self, user_email: str) -> List[Ability[BaseModel, Any]]:
        user_abilities = TemporaryAbilityToken.get_abilities(user_email)
        if user_abilities:
            metrics.increment('osprey.get_abilities_for_unregistered_user', tags=['success:true'])
            return user_abilities

        metrics.increment('osprey.get_abilities_for_unregistered_user', tags=['success:false'])
        return []

    def get_abilities_for_dev_user(self, user_email: str) -> List[Ability[BaseModel, Any]]:
        try:
            # assign local dev user acls from a file
            with open(DEV_ACL_ASSIGNMENTS_FILE) as acl_assignments_file:
                acl_assignments = json.load(acl_assignments_file)
                acl_user = acl_assignments.get(user_email, [])
                if acl_user:
                    return [ability for user_role in acl_user['roles'] for ability in ACL.get_one(user_role)]

                return []
        except FileNotFoundError:
            # otherwise, enforce super user by default if file not found
            return [ability for ability in ACL.get_one('SUPER_USER')]

    def get_abilities_for_user(self, user_email: str) -> List[Ability[BaseModel, Any]]:
        # For local dev, read ACLs from a file
        if CONFIG.instance().debug:
            return self.get_abilities_for_dev_user(user_email)

        # next try to fetch abilities for unregistered users
        acl_user = self.users.get(user_email)
        if acl_user is None:
            return self.get_abilities_for_unregistered_user(user_email)

        # if we're here, lets fetch user abilities from acl config
        return [ability for user_role in acl_user.roles for ability in self.roles[user_role]] + acl_user.abilities

    def _get_role_name_from_okta_group(self, okta_role: str) -> str:
        _trim = len(OKTA_GROUP_PREFIX)
        okta_role_parts: List[str] = []

        for role_part in okta_role[_trim:].split('-'):
            okta_role_parts.append(role_part.upper())

        # converts an okta group from okta/access naming convention to
        # osprey's uppercase snake case e.g. App-osprey-Community-Group
        # would return COMMUNITY_GROUP
        role_name = '_'.join(okta_role_parts)

        return role_name

    def get_abilities_for_okta_groups(self, okta_roles: Iterable[str]) -> List[Ability[BaseModel, Any]]:
        okta_roles = (
            self._get_role_name_from_okta_group(role)
            for role in okta_roles
            if (
                # ignore all okta groups that do not start with App-osprey- prefix
                # note that individual abilities are not granted from okta yet
                role.startswith(OKTA_GROUP_PREFIX)
                # ignore all okta groups that are not configured as an existing role
                and self._get_role_name_from_okta_group(role) in ACL.get_roles()
            )
        )
        return [ability for role in okta_roles for ability in ACL.get_one(role)]
