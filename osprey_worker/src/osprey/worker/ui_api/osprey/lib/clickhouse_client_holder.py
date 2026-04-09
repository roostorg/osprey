from __future__ import annotations

from typing import Any, Protocol

from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG


class ClickHouseClientProtocol(Protocol):
    def query(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
    ) -> Any: ...


class ClickHouseClientHolder:
    def __init__(self) -> None:
        self._client: ClickHouseClientProtocol | None = None
        self._database: str | None = None
        self._table: str | None = None
        self._timeout_seconds: int = 30
        CONFIG.instance().register_configuration_callback(self.init_from_config)

    def init_from_config(self, config: Config) -> None:
        if config.get_str('OSPREY_EVENT_QUERY_BACKEND', 'druid').lower() != 'clickhouse':
            return

        try:
            import clickhouse_connect  # type: ignore[import-untyped]
        except ImportError as exc:
            raise RuntimeError('clickhouse-connect is required when OSPREY_EVENT_QUERY_BACKEND=clickhouse') from exc

        self._database = config.get_str('CLICKHOUSE_DATABASE', 'osprey')
        self._table = config.get_str('CLICKHOUSE_EVENT_TABLE', 'execution_results')
        self._timeout_seconds = config.get_int('CLICKHOUSE_QUERY_TIMEOUT_SECONDS', 30)

        self._client = clickhouse_connect.get_client(
            host=config.get_str('CLICKHOUSE_HOST', 'localhost'),
            port=config.get_int('CLICKHOUSE_PORT', 8123),
            username=config.get_str('CLICKHOUSE_USERNAME', 'default'),
            password=config.get_str('CLICKHOUSE_PASSWORD', ''),
            database=self._database,
        )

    @property
    def client(self) -> ClickHouseClientProtocol:
        if self._client is None:
            raise RuntimeError('ClickHouse client not configured')
        return self._client

    @property
    def database(self) -> str:
        if self._database is None:
            raise RuntimeError('ClickHouse database not configured')
        return self._database

    @property
    def table(self) -> str:
        if self._table is None:
            raise RuntimeError('ClickHouse event table not configured')
        return self._table

    @property
    def timeout_seconds(self) -> int:
        return self._timeout_seconds
