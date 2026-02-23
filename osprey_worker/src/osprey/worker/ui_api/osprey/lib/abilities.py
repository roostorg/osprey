from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import asdict
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Type, TypeVar

from flask import abort
from osprey.worker.ui_api.osprey.lib.druid import BaseDruidQuery
from osprey.worker.ui_api.osprey.validators.entities import (
    GetLabelsForEntityRequest,
    ManualEntityLabelMutationRequest,
)
from pydantic import BaseModel, root_validator, validator
from pydantic.dataclasses import dataclass
from pydantic.generics import GenericModel
from typing_extensions import Final

AbilityT = TypeVar('AbilityT', bound='Ability[Any, Any]')


class AbilityRegistry:
    """
    Used to register abilities

    sets `ability.class_name` on registration
    """

    registry: dict[str, Type[Ability[Any, Any]]] = {}

    def register(self, name: str) -> Callable[[Type[AbilityT]], Type[AbilityT]]:
        def inner_wrapper(wrapped_class: Type[AbilityT]) -> Type[AbilityT]:
            assert name not in self.registry, f'Two abilities are registered to the same name: {name}'
            wrapped_class.class_name = name
            self.registry[name] = wrapped_class
            return wrapped_class

        return inner_wrapper

    def get_ability_model(self, name: str) -> Type[Ability[Any, Any]]:
        return self.registry[name]


_ability_registry = AbilityRegistry()
register_ability = _ability_registry.register


class ExplicitHash(ABC):
    """
    Used to ensure mypy forces `allowances` to be hashable
    """

    @abstractmethod
    def __hash__(self) -> int:
        pass


@dataclass(frozen=True)
class HashableEntityKey(ExplicitHash):
    type: str
    id: str

    # This exists because mypy doesn't recognise this class as having `__hash__` from @dataclass(frozen=True)
    def __hash__(self) -> int:
        return hash((self.type, self.id))


ModelT = TypeVar('ModelT', bound=BaseModel)
ItemT = TypeVar('ItemT', bound=int | str | ExplicitHash)


# Note:
# If you modify how these abilities are parsed/validated, keep in mind that the
# osprey/osprey_lib/storage/temporary_ability_token.py will break, as the tokens stored
# in the temporary_ability_tokens table will be out-of-date with the new validation after deploy.
# To fix this, simply truncate the temporary_ability_tokens table after deploying a change involving
# ability validation.
class Ability(ABC, GenericModel, Generic[ModelT, ItemT]):
    """
    Represents a specific ability with optional `allowances` OR `disallowances` that a user can have.
    Abilities cannot have allowances AND disallowances.
    """

    #  This name is the name thats set in the `register` decorator
    class_name: ClassVar[str]
    #  This name is needed for the config to load
    name: str
    # The root validator method ensures the following of the 3 variables below:
    # - Only one of these 3 variables can be present in any given Ability, as they conflict with eachother
    # - At least one of these 3 variables has to be present in any given Ability. Empty abilities are not allowed
    # - allow_specific and allow_all_except are sets so they cannot be empty if they are present
    # - allow_all cannot be false if it is present
    allow_all: bool | None = None
    allow_specific: set[ItemT] | None = None
    allow_all_except: set[ItemT] | None = None

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True

    def item_is_allowed(self, item: ItemT) -> bool:
        if self.allow_all:
            return True
        elif self.allow_all_except:
            return item not in self.allow_all_except
        elif self.allow_specific:
            return item in self.allow_specific
        # All lists are empty and allow_all is false. This ability is virtually empty.
        return False

    def request_is_allowed(self, input_model: ModelT) -> bool:
        return self.allow_all or self._request_is_allowed(input_model)

    @abstractmethod
    def _request_is_allowed(self, input_model: ModelT) -> bool:
        raise NotImplementedError()

    @root_validator
    def ability_root_validator(cls, values: Any) -> Any:
        allow_all_value: bool | None = values.get('allow_all')
        allow_all_except_value: set[ItemT] | None = values.get('allow_all_except')
        allow_specific_value: set[ItemT] | None = values.get('allow_specific')
        if allow_all_value is None and allow_all_except_value is None and allow_specific_value is None:
            raise ValueError('`allow_all`, `allow_all_except`, and `allow_specific` cannot all be None')
        if allow_all_value is not None and allow_all_value is False:
            raise ValueError('`allow_all` can only be `true`')
        if allow_all_except_value is not None and len(allow_all_except_value) == 0:
            raise ValueError('`allow_all_except` cannot be empty')
        if allow_specific_value is not None and len(allow_specific_value) == 0:
            raise ValueError('`allow_specific` cannot be empty')
        if allow_specific_value is not None and allow_all_except_value is not None:
            raise ValueError(
                '`allow_all_except` and `allow_specific` are both present within the same scope.'
                ' The two values are mutually exclusive.'
            )
        if allow_specific_value is not None and allow_all_value is not None:
            raise ValueError(
                '`allow_all` is set to `true` but you have redundant additional `allow_specific`.'
                ' The two values are mutually exclusive.'
            )
        if allow_all_except_value is not None and allow_all_value is not None:
            raise ValueError(
                '`allow_all` is set to `true` but you have redundant additional `allow_all_except`.'
                ' The two values are mutually exclusive.'
            )
        return values

    # This is a hack around https://github.com/samuelcolvin/pydantic/issues/619 not existing yet
    @classmethod
    def validate(cls: Type[Ability[Any, Any]], value: Any) -> Ability[Any, Any]:
        if isinstance(value, Ability):
            return value
        if isinstance(value, dict):
            model_name = value.get('name')
            if not model_name:
                raise ValueError('Ability must include `name` field')
            try:
                model = _ability_registry.get_ability_model(model_name)
            except KeyError:
                raise ValueError(f"'{model_name}' is not a valid ability")
            return model.parse_obj(value)
        raise ValueError(f'Could not validate `Ability` `{cls.__name__}` with `{value}`')


class QueryFilterAbility(Ability[ModelT, ItemT], Generic[ModelT, ItemT]):
    """
    A type of ability which will be used to create a query filter from its allowances
    """

    def _request_is_allowed(self, input_model: ModelT) -> bool:
        return True

    # The default query filter (None)
    # This method works on the query that is going to be sent to Druid.
    @abstractmethod
    def _get_query_filter(self) -> dict[str, Any] | None:
        raise NotImplementedError()


"""
Start DataCensorAbility classes
"""


@dataclass(unsafe_hash=True)
class DictPath:
    action_name: str | None
    path: str
    is_star_path: bool  # True if this path applies to everything

    def __init__(self, item: str):
        if ':' in item:
            self.action_name, self.path = item.split(':', 1)
        else:
            self.action_name = None
            self.path = item
        self.is_star_path = self.path == '*'

    def has_next(self) -> bool:
        return len(self.path) > 0

    def get_next(self) -> str:
        next: str = self.path.split('.').pop(0)
        self.path = self.path[len(next) + 1 :]
        return next

    def append(self, path_to_append: str) -> DictPath:
        for path in path_to_append.split('.'):
            self.path = self.path + '.' + path
        return self

    def copy(self) -> DictPath:
        return DictPath(self.path)


class StringItem(BaseModel, ExplicitHash):
    __root__: str

    def __hash__(self) -> int:
        return hash(self.__root__)

    def __eq__(self, obj: Any) -> bool:
        return bool(self.__root__ == obj)

    def __str__(self) -> str:
        return self.__root__


class JsonPath(StringItem):
    @validator('__root__')
    def json_path_validator(cls, item: str) -> Any:
        action_name = None
        path = item
        if ':' in item:
            action_name, path = item.split(':', 1)
        if '*' in path:
            if path != '*' and not path.endswith('.*'):
                raise ValueError(f"'{path}' is not a valid path. Stars can only be used at the end of the path")
            elif path != '*' and path.endswith('.*'):
                path = path[: len(path) - 2]
        elif not path:
            raise ValueError(f"'{item}' does not have a path. Example with a path: guild_joined:user.id")
        if path.startswith('$.'):
            raise ValueError(f"'{path}' is not a valid path. Valid paths cannot start with $.")
        elif path.endswith('.'):
            raise ValueError(f"'{path}' is not a valid path. Valid paths cannot end with .")
        elif '..' in path:
            raise ValueError(f"'{path}' is not a valid path. No fields within a path can be empty.")
        elif action_name and '.' in action_name:
            raise ValueError(f"'{action_name}' is not a valid action name.")
        if action_name:
            return action_name + ':' + path
        return path


class FeatureName(StringItem):
    @validator('__root__')
    def feature_name_validator(cls, item: str) -> Any:
        action_name = None
        path = item
        if ':' in item:
            action_name, path = item.split(':', 1)
        if not path:
            raise ValueError(f"'{item}' does not have a feature name. Example with a feature name: guild_joined:UserId")
        if '*' in path and path != '*':
            raise ValueError(f"'{path}' is not a valid feature name.")
        elif path.startswith('$.'):
            raise ValueError(f"'{path}' is not a valid feature name. Feature names cannot start with $.")
        elif not path or '.' in path:
            raise ValueError(f"'{path}' is not a valid feature name.")
        elif action_name and '.' in action_name:
            raise ValueError(f"'{action_name}' is not a valid action name.")
        if action_name:
            return action_name + ':' + path
        return path


"""
Text used by DataCensorAbility to censor the leaf nodes with
"""
CENSOR_TEXT: Final[str] = '[CENSORED]'


class DataCensorAbility(Ability[ModelT, ItemT], Generic[ModelT, ItemT]):
    """
    A type of ability which will be used to censor data requested for osprey UI
    """

    if TYPE_CHECKING:
        # Duplicate these type definitions from the superclass to work around a bug in the Pydantic mypy plugin:
        # https://github.com/samuelcolvin/pydantic/issues/2613
        allow_specific: set[ItemT] | None
        allow_all_except: set[ItemT] | None

    def _request_is_allowed(self, input_model: BaseModel) -> bool:
        return True

    def _create_dict_path(self, item: ItemT) -> DictPath:
        raise NotImplementedError()

    def _item_applies_to_action_name(self, item: ItemT, action_name: str) -> bool:
        """
        Returns true if the provided censor path item applies to the provided action_name
        """
        dict_path = self._create_dict_path(item)
        return not dict_path.action_name or dict_path.action_name == action_name

    @classmethod
    def censor_all_leafs(
        cls, data: dict[str, Any], json_path_exceptions: set[DictPath] | None = None
    ) -> dict[str, Any]:
        """
        This method censors all of `data`, excluding any paths in `json_path_exceptions`
        """
        json_path_exceptions = json_path_exceptions or set()
        if any(path.is_star_path for path in json_path_exceptions):
            return data
        for key in data.keys():
            cls.censor_leafs(data, key, json_path_exceptions=json_path_exceptions)
        return data

    @staticmethod
    def censor_leafs(data: dict[str, Any], field: str, json_path_exceptions: set[DictPath] | None = None) -> None:
        """
        This method recursively censors all leaf values beneath data[field].
        Any leafs with paths matching the `json_path_exceptions` are skipped (uncensored).
        """
        json_path_exceptions = json_path_exceptions or set()

        if field == 'ActionName' and isinstance(data[field], str):
            """
            ActionName is not censorable. If you want to censor an aciton
            name, you should simply block the action from being queryable.
            """
            return

        def _recursively_censor_leafs(
            node: Any, json_path_exceptions: set[DictPath], current_path: DictPath
        ) -> Any | None:
            """
            This method recursively censors all leaf values beneath the given `node`.
            Any leafs matching the `json_path_exceptions` will be skipped (uncensored).
            """
            if any(current_path.path == json_path_exception.path for json_path_exception in json_path_exceptions):
                return node
            if not node:
                return CENSOR_TEXT
            if isinstance(node, dict):
                return {
                    key: _recursively_censor_leafs(val, json_path_exceptions, current_path.copy().append(key))
                    for key, val in node.items()
                }
            if isinstance(node, list):
                return [_recursively_censor_leafs(val, json_path_exceptions, current_path.copy()) for val in node]
            return CENSOR_TEXT

        start_path = DictPath(field)
        data[field] = _recursively_censor_leafs(data[field], json_path_exceptions, start_path)

    def censor_data(self, data: dict[str, Any], action_name: str) -> dict[str, Any]:
        """
        A method that takes a dict and outputs a censored version based on the
        ability's permissions/allowances.
        """

        if self.allow_all_except:

            def _censor_at_path(data: dict[str, Any], path: DictPath) -> None:
                """
                Recursively censors all leaf values at the provided `path`
                """
                if path.is_star_path:
                    DataCensorAbility.censor_all_leafs(data)
                    return
                key = path.get_next()
                val = data[key]
                if not path.has_next():
                    self.censor_leafs(data, key)
                    return
                if isinstance(val, dict):
                    _censor_at_path(val, path)
                    return
                if isinstance(val, list):
                    for item in val:
                        _censor_at_path(item, path.copy())
                    return

            for item in self.allow_all_except:
                if self._item_applies_to_action_name(item, action_name):
                    _censor_at_path(data, self._create_dict_path(item))

        if self.allow_specific:
            json_path_exceptions = {
                self._create_dict_path(item)
                for item in self.allow_specific
                if self._item_applies_to_action_name(item, action_name)
            }
            self.censor_all_leafs(data, json_path_exceptions)

        return data


@register_ability('CAN_VIEW_ACTION_DATA')
class CanViewActionData(DataCensorAbility[BaseModel, JsonPath]):
    """
    This is only necessary because Mypy has an internal error if we try and do it
    properly with Pydantic Generic models. Because it fails, we just use root validator
    to bypass
    """

    def _create_dict_path(self, item: JsonPath) -> DictPath:
        return DictPath(item.__root__)


@register_ability('CAN_VIEW_FEATURE_DATA')
class CanViewFeatureData(DataCensorAbility[BaseModel, FeatureName]):
    """
    This is only necessary because Mypy has an internal error if we try and do it
    properly with Pydantic Generic models. Because it fails, we just use root validator
    to bypass
    """

    def _create_dict_path(self, item: FeatureName) -> DictPath:
        return DictPath(item.__root__)


"""
End DataCensorAbility classes
"""


class _MarkerAbilityModel(BaseModel):
    """A 'fake' model to ensure we don't try to require an allowance with this ability"""


class _MarkerAbilityAllowance(ExplicitHash):
    """A 'fake' allowance to ensure we don't try to require an allowance with this ability"""

    def __hash__(self) -> int:
        return 0


def make_marker_ability() -> Type[Ability[_MarkerAbilityModel, _MarkerAbilityAllowance]]:
    """Creates a class for an Ability that is only intended to be used to check wholesale whether the user has that
    ability or not."""

    class MarkerAbility(Ability[Any, Any]):
        allow_all: bool = True

        def _request_is_allowed(self, input_model: _MarkerAbilityModel) -> bool:
            return True

        @root_validator(allow_reuse=True)
        def root_validator(cls, values: dict[str, object]) -> dict[str, object]:
            assert values['allow_all'], f'Marker ability {values["name"]} must have `allow_all` set to `true`'
            return values

    return MarkerAbility


@register_ability('CAN_VIEW_LABELS_FOR_ENTITY')
class CanViewLabelsForEntity(Ability[GetLabelsForEntityRequest, HashableEntityKey]):
    def _request_is_allowed(self, request_model: GetLabelsForEntityRequest) -> bool:
        return self.item_is_allowed(
            HashableEntityKey(
                **asdict(
                    request_model.entity,
                )
            )
        )


@register_ability('CAN_VIEW_LABELS')
class CanViewLabels(Ability[BaseModel, str]):
    # Note: Not actually a marker ability, also intended to be used to check if a given label is allowed.
    def _request_is_allowed(self, input_model: BaseModel) -> bool:
        return True


@register_ability('CAN_MUTATE_ENTITIES')
class CanMutateEntities(Ability[ManualEntityLabelMutationRequest, HashableEntityKey]):
    def _request_is_allowed(self, request_model: ManualEntityLabelMutationRequest) -> bool:
        return self.item_is_allowed(HashableEntityKey(**asdict(request_model.entity)))


@register_ability('CAN_MUTATE_LABELS')
class CanMutateLabels(Ability[ManualEntityLabelMutationRequest, str]):
    def _request_is_allowed(self, request_model: ManualEntityLabelMutationRequest) -> bool:
        return any(self.item_is_allowed(mutation.label_name) for mutation in request_model.mutations)
        # Original: any(mutation.label_name in label_names for mutation in request_model.mutations)


@register_ability('CAN_VIEW_EVENTS_BY_ENTITY')
class CanViewEventsByEntity(Ability[BaseDruidQuery, HashableEntityKey]):
    def _request_is_allowed(self, input_model: BaseDruidQuery) -> bool:
        if not input_model.entity:
            return False
        return self.item_is_allowed(HashableEntityKey(id=input_model.entity.id, type=input_model.entity.type))


@register_ability('CAN_VIEW_EVENTS_BY_ACTION')
class CanViewEventsByAction(QueryFilterAbility[BaseDruidQuery, str]):
    def _get_query_filter(self) -> dict[str, Any] | None:
        if self.allow_all:
            return None  # if allow all is set, then we don't need to inject an acl filter into the query

        if self.allow_specific:
            return {
                'type': 'or',
                'fields': [
                    {'type': 'selector', 'dimension': 'ActionName', 'value': allowance}
                    for allowance in self.allow_specific
                ],
            }

        if self.allow_all_except:
            return {
                'type': 'not',
                'field': {
                    'type': 'or',
                    'fields': [
                        {'type': 'selector', 'dimension': 'ActionName', 'value': allowance_exception}
                        for allowance_exception in self.allow_all_except
                    ],
                },
            }

        assert False, "Validation failed to assert that this ability had a present 'allow' variable"


CanViewDocs = register_ability('CAN_VIEW_DOCS')(make_marker_ability())
CanBulkLabel = register_ability('CAN_BULK_LABEL')(make_marker_ability())
CanBulkLabelWithNoLimit = register_ability('CAN_BULK_LABEL_WITH_NO_LIMIT')(make_marker_ability())
CanViewSavedQueries = register_ability('CAN_VIEW_SAVED_QUERIES')(make_marker_ability())
CanCreateAndEditSavedQueries = register_ability('CAN_CREATE_AND_EDIT_SAVED_QUERIES')(make_marker_ability())
CanBulkAction = register_ability('CAN_BULK_ACTION')(make_marker_ability())


def require_ability_with_request(request_model: ModelT, ability_class: Type[Ability[ModelT, ItemT]]) -> None:
    """
    Checks `request_model` against the current user's abilities and aborts request if the `request_model` isn't valid
    """
    from osprey.worker.ui_api.osprey.lib.auth import get_current_user  # Needed to avoid circular import

    current_user = get_current_user()
    ability = current_user.get_ability(ability_class)
    if not ability:
        raise abort(401, f"User `{current_user.email}` doesn't have ability `{ability_class.class_name}`")
    if not ability.request_is_allowed(request_model):
        raise abort(
            401,
            (
                f'User `{current_user.email}` with ability `{ability_class.__name__}` is not authorized to '
                f'access `{request_model}`'
            ),
        )


F = TypeVar('F', bound=Callable[..., object])


def require_ability(ability_class: Type[Ability[ModelT, ItemT]]) -> Callable[[F], F]:
    """
    Decorator for flask view functions that requires that the current user has `ability_class`

    Aborts request if ability is not found
    """

    def decorator(f: F) -> Any:
        @wraps(f)
        def decorated_function(*args: Any, **kwargs: Any) -> Any:
            from osprey.worker.ui_api.osprey.lib.auth import get_current_user

            current_user = get_current_user()
            if not get_current_user().has_ability(ability_class):
                raise abort(401, f"User `{current_user.email}` doesn't have ability `{ability_class.class_name}`")
            return f(*args, **kwargs)

        return decorated_function

    return decorator
