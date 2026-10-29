import importlib
import inspect
import pkgutil

from osprey.engine.stdlib import udfs
from osprey.engine.udf.base import UDFBase
from osprey.worker._stdlibplugin.udf_register import register_udfs


def test_every_stdlib_udf_is_registered() -> None:
    defined_udfs = set()
    for module_info in pkgutil.iter_modules(udfs.__path__):
        if module_info.ispkg:
            continue
        module = importlib.import_module(f'{udfs.__name__}.{module_info.name}')
        for _, member in inspect.getmembers(module, inspect.isclass):
            # Skip classes imported from elsewhere and abstract bases that only exist to be subclassed.
            if issubclass(member, UDFBase) and member.__module__ == module.__name__ and not inspect.isabstract(member):
                defined_udfs.add(member)

    unregistered = sorted(udf.__name__ for udf in defined_udfs - set(register_udfs()))
    assert unregistered == []
