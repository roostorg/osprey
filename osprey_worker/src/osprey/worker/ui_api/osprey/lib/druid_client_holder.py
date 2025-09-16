from typing import Optional

from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG
from pydruid.client import PyDruid


class DruidClientHolder:
    def __init__(self) -> None:
        self._client: Optional[PyDruid] = None
        self._datasource: Optional[str] = None
        CONFIG.instance().register_configuration_callback(self.init_from_config)

    def init_from_config(self, config: Config) -> None:
        url = config.expect_str('DRUID_URL')
        self._datasource = config.get_str('DRUID_DATASOURCE', 'osprey.execution_results')
        self._client = PyDruid(url, 'druid/v2')

    @property
    def client(self) -> PyDruid:
        if not self._client:
            raise Exception('Druid client not configured')
        return self._client

    @property
    def datasource(self) -> str:
        if not self._datasource:
            raise Exception('Druid datasource not configured')
        return self._datasource
