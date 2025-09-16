import importlib
import types
from pathlib import Path


def import_all_direct_children(module: types.ModuleType) -> None:
    """Dynamically import all child modules in a given package.

    The name *must* be absolute and *must* be a package. This can be useful in cases where the act of importing
    children can be meaningful, eg if those children register themselves in some central registry. Does not
    recursively import the children's children.
    """
    assert module.__file__ is not None  # Satisfy mypy, because this is Optional.

    module_name = module.__name__
    module_path = Path(module.__file__)
    assert module_path.name == '__init__.py', (
        f'Package `{module!r}` does not refer to a package. Expected `__init__.py` file, got `{module_path}`.'
    )

    for child in module_path.parent.iterdir():
        importlib.import_module(f'{module_name}.{child.stem}')
