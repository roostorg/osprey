from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional, Type

from osprey.worker.lib.singletons import ENGINE
from osprey.worker.lib.sources_config.subkeys.acl_config import AclConfig
from osprey.worker.ui_api.osprey.lib.abilities import Ability, AbilityT
from pydantic import BaseModel


class User:
    """
    User that exists per flask request

    Contains methods for accessing the user's abilities
    """

    def __init__(self, email: str):
        self.email = email
        acl_config = ENGINE.instance().get_config_subkey(AclConfig)

        # users that haven't been registered in the acl_config can still access osprey if they got their `Ability`s by
        # consuming a `TemporaryAbilityToken`
        self.is_unregistered_user = acl_config.users.get(email) is None

        self._abilities: List[Ability[BaseModel, Any]] = acl_config.get_abilities_for_user(self.email)

    def get_ability(self, ability_class: Type[AbilityT]) -> Optional[AbilityT]:
        """
        Returns a single instance of `ability_class` that has all of the user's `ability_class` properties merged

        If the user does not have any `ability_class`, this returns `None`
        """
        abilities: List[AbilityT] = [ability for ability in self._abilities if isinstance(ability, ability_class)]
        return self._merge_abilities(ability_class, abilities) if abilities else None

    def get_all_abilities(self) -> Mapping[str, Ability[Any, Any]]:
        """
        Returns a Dict mapping `ability_name` to a single `Ability` that has all of the user's `Ability` allowances
        merged
        """
        ability_name_to_ability_list: Dict[str, List[Ability[Any, Any]]] = defaultdict(list)
        for ability in self._abilities:
            ability_name_to_ability_list[ability.name].append(ability)

        ability_name_to_merged_ability = {}
        for ability_name, ability_list in ability_name_to_ability_list.items():
            assert ability_list, 'A list of abilities (' + ability_name + ') was empty and could not be merged properly'
            merged_ability = self._merge_abilities(ability_list[0].__class__, ability_list)
            ability_name_to_merged_ability[ability_name] = merged_ability

        return ability_name_to_merged_ability

    def _merge_abilities(self, ability_class: Type[AbilityT], ability_list: List[AbilityT]) -> AbilityT:
        """
        Returns a single instance of Ability that has all of the user's Abilities (of the same type) merged.

        If the resulting merge leads to an empty Ability (allow_all=False, allow_specific=[], allow_all_except=[]),
        this method will throw an AssertionError.
        """
        ability_name: str = ability_class.class_name
        allow_all = any(ability.allow_all for ability in ability_list)
        if allow_all:
            return ability_class(name=ability_name, allow_all=True)
        allow_all_except = {
            disallowance
            for ability in ability_list
            if ability.allow_all_except
            for disallowance in ability.allow_all_except
        }
        allow_specific = {
            allowance for ability in ability_list if ability.allow_specific for allowance in ability.allow_specific
        }
        # If the merge creates a conflict between our two mutually-exclusive sets, we need to decide which ones
        # to prioritize. In the case of these ACLs, the priority is: allow_all > allow_all_except > allow_specific
        if len(allow_all_except) > 0 and len(allow_specific) > 0:
            allow_all_except = allow_all_except.difference(allow_specific)
            allow_specific = set()
            if not allow_all_except:
                return ability_class(name=ability_name, allow_all=True)
        if len(allow_all_except) > 0:
            return ability_class(name=ability_name, allow_all_except=allow_all_except)
        elif len(allow_specific) > 0:
            return ability_class(name=ability_name, allow_specific=allow_specific)
        assert False, 'An ability that was merged became completely empty. This should not occur.'

    def has_ability(self, ability_class: Type[Ability[Any, Any]]) -> bool:
        """
        Returns true if the user has `ability_class` regardless of the contents of `ability_class`
        """
        return any(isinstance(ability, ability_class) for ability in self._abilities)
