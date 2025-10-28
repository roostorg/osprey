# import logging
# import os
#
# import pytest
import logging
from abc import ABC, abstractmethod
from typing import Dict

from google.cloud.bigtable import Client
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.table import Table
from osprey.worker.lib.config import Config
from osprey.worker.lib.ddtrace_utils import pin_override
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.utils.bigtable import fix_bigtable_client_if_using_emulator

OSPREY_TABLES_TO_COLUMN_FAMILIES: Dict[str, Dict[str, None]] = {
    'stored_execution_result': {'execution_result': None},
    'audit_log': {'audit_log': None},
}
"""
A dict of base osprey BigTable table names (the key) & their respective column families (the value).
Add new tables to this dict to have them bootstrapped.
"""


class BigTableClient(ABC):
    _gcp_project: str
    _instance_id: str
    _admin_enabled: bool

    def _setup_client_instance(self) -> Instance:
        client = Client(project=self._gcp_project, admin=self._admin_enabled)
        fix_bigtable_client_if_using_emulator(client)

        return client.instance(self._instance_id)

    @property
    def _instance(self) -> Instance:
        if not getattr(self, '__instance', None):
            self.__instance = self._setup_client_instance()
        return self.__instance

    @abstractmethod
    def init_from_config(self, config: Config) -> None: ...

    @abstractmethod
    def table(self, table_name: str) -> Table: ...


class OspreyBigTable(BigTableClient):
    """
    A BigTable client wrapper for the Osprey BigTable instance
    """

    def __init__(self) -> None:
        CONFIG.instance().register_configuration_callback(self.init_from_config)

    def init_from_config(self, config: Config) -> None:
        """Initialize this bigtable client once configuration is available."""
        config = CONFIG.instance()
        self._gcp_project = config.get_str('OSPREY_GCP_PROJECT_ID', 'osprey-dev')
        self._instance_id = config.get_str('OSPREY_BIGTABLE_INSTANCE_ID', 'osprey-bigtable')
        self._admin_enabled = config.get_bool('OSPREY_BIGTABLE_ADMIN_ENABLED', True)

    #  staging/production tables are managed through Terraform
    def bootstrap(self) -> None:
        """
        Creates all base osprey BigTable tables
        """
        for table_name, column_families in OSPREY_TABLES_TO_COLUMN_FAMILIES.items():
            try:
                logging.info(f'creating {table_name} BigTable table')
                table = self.table(table_name)
                table.create(column_families)
            except Exception as e:
                logging.warn(str(e))

    def table(self, table_name: str) -> Table:
        """
        Get a Table instance for the requested table
        """
        t = self._instance.table(table_name)
        pin_override(
            t,
            service='osprey-bigtable-client',
            tags={'bigtable_instance': self._instance.instance_id, 'table_id': t.table_id},
        )
        return t


osprey_bigtable = OspreyBigTable()
