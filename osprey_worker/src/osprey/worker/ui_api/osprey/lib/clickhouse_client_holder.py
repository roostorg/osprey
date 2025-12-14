import threading
from typing import Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG


class ClickhouseClientHolder:
    def __init__(self):
        self._local = threading.local()
        self._host: Optional[str] = None
        self._port: Optional[int] = None
        self._user: Optional[str] = None
        self._password: Optional[str] = None
        self._database: Optional[str] = None
        self._table: Optional[str] = None
        CONFIG.instance().register_configuration_callback(self.init_from_config)

    def init_from_config(self, config: Config) -> None:
        self._host = config.get_str('OSPREY_CLICKHOUSE_HOST', 'localhost')
        self._port = config.get_int('OSPREY_CLICKHOUSE_PORT', 8123)
        self._user = config.get_str('OSPREY_CLICKHOUSE_USER', 'default')
        self._password = config.get_str('OSPREY_CLICKHOUSE_PASSWORD', 'clickhouse')
        self._database = config.get_str('OSPREY_CLICKHOUSE_DB', 'default')
        self._table = config.get_str('OSPREY_CLICKHOUSE_TABLE', 'osprey_execution_results')

    @property
    def client(self) -> Client:
        if not hasattr(self._local, 'client'):
            if not self._host:
                raise Exception('Clickhouse client not configured')
            self._local.client = clickhouse_connect.get_client(
                host=self._host,
                port=self._port,
                username=self._user,
                password=self._password,
            )
        return self._local.client

    @property
    def database(self) -> str:
        if not self._database:
            raise Exception('Clickhouse datasource not configured')
        return self._database

    @property
    def table(self) -> str:
        if not self._table:
            raise Exception('Clickhouse table not configured')
        return self._table
