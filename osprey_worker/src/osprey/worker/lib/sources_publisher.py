import abc
import logging
from datetime import datetime
from typing import Dict, Optional, Set

from google.cloud import bigquery
from osprey.engine.ast.grammar import Assign, Call
from osprey.engine.ast.sources import Sources
from osprey.engine.ast_validator import validate_sources
from osprey.engine.ast_validator.validation_context import (
    ValidatedSources,
    ValidationFailed,
)
from osprey.engine.ast_validator.validator_registry import ValidatorRegistry
from osprey.engine.executor.dependency_chain import DependencyChain
from osprey.engine.executor.execution_graph import compile_execution_graph
from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators, bootstrap_udfs
from osprey.worker.lib.etcd import EtcdClient
from osprey.worker.lib.etcd.dict import EtcdDict
from osprey.worker.lib.sources_config import get_config_registry

logging.getLogger('ddtrace').setLevel(logging.CRITICAL)  # turn off ddtrace exceptions when pushing rules


class BaseSourcesPublisher(abc.ABC):
    """The opposite end of a sources provider. Publishes the sources to be consumed by the sources provider."""

    @abc.abstractmethod
    def publish_sources(self, sources: ValidatedSources) -> None:
        """Publishes a validated sources to the provider."""
        raise NotImplementedError


class EtcdSourcesPublisher(BaseSourcesPublisher):
    """Publishes the sources to be consumed by the EtcdSourcesProvider"""

    def __init__(self, etcd_key: str, etcd_client: Optional[EtcdClient] = None):
        self._etcd_dict: EtcdDict[str, str] = EtcdDict(etcd_key=etcd_key, etcd_client=etcd_client)

    def publish_sources(self, sources: ValidatedSources) -> None:
        self._etcd_dict.replace_with(sources.sources.to_dict())


def validate_and_push(
    sources: Sources,
    dry_run: bool = False,
    suppress_warnings: bool = False,
    quiet: bool = False,
    etcd_key: str = '/config/osprey/rules-sink-sources',
) -> bool:
    """Validate and push osprey rules to etcd. If dry_run is True, only validate.

    Returns False if rules failed validation, True otherwise.
    """
    lib_validator_registry = ValidatorRegistry.instance_with_additional_validators(
        get_config_registry().get_validator()
    )
    udf_registry, _ = bootstrap_udfs()
    bootstrap_ast_validators()

    try:
        validated_sources = validate_sources(
            sources=sources, udf_registry=udf_registry, validator_registry=lib_validator_registry
        )
    except ValidationFailed as e:
        print(e.rendered())
        return False
    else:
        if validated_sources.warnings and not suppress_warnings:
            print(ValidationFailed([], validated_sources.warnings).rendered())

    compile_execution_graph(validated_sources)
    if not dry_run:
        sources_publisher = EtcdSourcesPublisher(etcd_key=etcd_key)
        sources_publisher.publish_sources(validated_sources)

    if not quiet:
        if dry_run:
            print('OK! Rules validated.')
        else:
            print('OK! Rules pushed.')

    return True


def _dependency_chain_to_str_set(dependency_chain: DependencyChain) -> Set[str]:
    if dependency_chain.executor.node_type == Assign:
        output = set([dependency_chain.executor.node.target.identifier])  # type: ignore
    elif dependency_chain.executor.node_type == Call:
        output = set([dependency_chain.executor.node.func.identifier])  # type: ignore
    else:
        output = set()
    if len(dependency_chain.dependent_on) == 0:
        return output
    for chain in dependency_chain.dependent_on:
        output = output.union(_dependency_chain_to_str_set(chain))
    return output


def upload_dependencies_mapping(
    sources: Sources,
    suppress_warnings: bool = False,
) -> bool:
    """Create a mapping of Osprey Rules/UDFs/variables to their dependencies and upload to BigQuery."""
    lib_validator_registry = ValidatorRegistry.instance_with_additional_validators(
        get_config_registry().get_validator()
    )
    udf_registry, _ = bootstrap_udfs()
    bootstrap_ast_validators()

    try:
        validated_sources = validate_sources(
            sources=sources, udf_registry=udf_registry, validator_registry=lib_validator_registry
        )
    except ValidationFailed as e:
        print(e.rendered())
        return False
    else:
        if validated_sources.warnings and not suppress_warnings:
            print(ValidationFailed([], validated_sources.warnings).rendered())

    execution_graph = compile_execution_graph(validated_sources)

    dependencies: Dict[str, Set[str]] = {}
    for var_name, dependency_chain in execution_graph._assignment_executor_mapping.items():
        dependencies[var_name] = _dependency_chain_to_str_set(dependency_chain)

    # compute reversed dependencies (what relies on this variable)
    reversed_dependencies: Dict[str, Set[str]] = {}
    for key, val in dependencies.items():
        for v in val:
            if v not in reversed_dependencies:
                reversed_dependencies[v] = set()
            reversed_dependencies[v].add(key)

    # rules, dependencies, reversed_dependencies as bigquery upload read dict
    upload_records = []
    for rule in dependencies.keys():
        upload_dict = {
            'rule': rule,
            'depends_on': list(dependencies[rule]),
            'depended_on_by': list(reversed_dependencies.get(rule, [])),
        }
        upload_records.append(upload_dict)

    # upload the result to bigquery.
    # TODO: hardcoded output tables for now.
    BQ_PROJECT_ID = 'osprey-data-tns-prd'

    client = bigquery.Client(project=BQ_PROJECT_ID)

    # temp upload table
    TEMP_DATASET_ID = 'temp'
    TEMP_TABLE_ID = f'osprey_rules_dependencies_upload_{int(datetime.now().timestamp())}'
    temp_table_ref = client.dataset(TEMP_DATASET_ID).table(TEMP_TABLE_ID)

    # final destination table
    DEST_DATASET_ID = 'ml_safety'
    DEST_TABLE_ID = 'osprey_rules_dependencies'

    schema = [
        bigquery.SchemaField('rule', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('depends_on', 'STRING', mode='REPEATED'),
        bigquery.SchemaField('depended_on_by', 'STRING', mode='REPEATED'),
    ]
    table = bigquery.Table(temp_table_ref, schema=schema)
    table = client.create_table(table)

    _ = client.insert_rows_json(table, upload_records)

    MERGE_SQL_STATEMENTS = f"""
    -- Step 1: Mark existing records as historical
    MERGE INTO `{BQ_PROJECT_ID}.{DEST_DATASET_ID}.{DEST_TABLE_ID}` AS target
    USING `{BQ_PROJECT_ID}.{TEMP_DATASET_ID}.{TEMP_TABLE_ID}` AS source
    ON target.rule = source.rule
    WHEN MATCHED AND (TO_JSON_STRING(target.depends_on) != TO_JSON_STRING(source.depends_on)
                    OR TO_JSON_STRING(target.depended_on_by) != TO_JSON_STRING(source.depended_on_by))
    THEN UPDATE SET target.valid_end_ts = CURRENT_TIMESTAMP();

    -- Step 2: Insert new or updated records as new entries
    MERGE INTO `{BQ_PROJECT_ID}.{DEST_DATASET_ID}.{DEST_TABLE_ID}` AS target
    USING `{BQ_PROJECT_ID}.{TEMP_DATASET_ID}.{TEMP_TABLE_ID}` AS source
    ON target.rule = source.rule
    AND target.valid_end_ts = TIMESTAMP '9999-12-31 23:59:59 UTC'
    WHEN NOT MATCHED THEN
    INSERT (rule, depends_on, depended_on_by, valid_start_ts, valid_end_ts)
    VALUES (source.rule, source.depends_on, source.depended_on_by, CURRENT_TIMESTAMP(), TIMESTAMP '9999-12-31 23:59:59 UTC');
    """

    query_job = client.query(MERGE_SQL_STATEMENTS)
    query_job.result()
    return True
