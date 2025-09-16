from typing import TYPE_CHECKING

from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.utils.trace import GCMetrics

if TYPE_CHECKING:
    from osprey.worker.lib.config import Config


def tracing_callback(config: 'Config') -> None:
    """
    Enable metrics collection if the `TRACING_COLLECT_METRICS` environment variable is set
    """
    if config.get_bool('TRACING_CONFIGURE_DATADOG', False):
        service_name = config.get_str('DD_SERVICE', 'unknown')
        metrics.constant_tags.append(f'service:{service_name}')

        # namespace and prefix are the same thing, using namespace as its the library supported option
        metrics.namespace = config.get_str('DATADOG_PREFIX', 'osprey')

    if config.get_bool('TRACING_COLLECT_DETAILED_GC', False):
        gc_metrics = GCMetrics()

        gc_metrics.configure(service_name=config.get_str('DD_SERVICE', ''))
        gc_metrics.register()
