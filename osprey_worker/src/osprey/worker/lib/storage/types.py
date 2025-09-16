from typing import TYPE_CHECKING, Type, TypeVar

# Workaround taken from https://github.com/dropbox/sqlalchemy-stubs/issues/114
if TYPE_CHECKING:
    from sqlalchemy.sql.type_api import TypeEngine

    _T = TypeVar('_T')

    class Enum(TypeEngine[_T]):
        def __init__(self, enum: Type[_T], name: str = ..., create_type: bool = ...) -> None: ...


else:
    from sqlalchemy import Enum

__all__ = ['Enum']
