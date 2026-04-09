from typing import Optional

from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.ui_api.osprey.lib.event_query_backend import EventQueryBackend, get_event_query_backend


@hookimpl_osprey(trylast=True)
def register_event_query_backend(config: Config) -> Optional[EventQueryBackend]:
    backend_type = config.get_str('OSPREY_EVENT_QUERY_BACKEND', 'druid').lower()
    if backend_type == 'plugin':
        return None
    return get_event_query_backend(config=config)
