from osprey.engine.ast import grammar
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.error_utils import SpanWithHint
from osprey.engine.ast_validator.base_validator import SourceValidator
from osprey.engine.stdlib.configs.labels_config import LABELS_CONFIG_SUBKEY, LabelsConfig
from osprey.engine.stdlib.udfs.labels import LabelArguments
from osprey.engine.utils.get_closest_string_within_threshold import get_closest_string_within_threshold

from ..validation_context import ValidationContext
from .feature_name_to_entity_type_mapping import FeatureNameToEntityTypeMapping
from .validate_call_kwargs import UDFNodeMapping, ValidateCallKwargs

### meow


class ValidateLabels(SourceValidator):
    # This needs to be a separate validator, as opposed to in the constructor of the `Label*` UDFs, since it relies on
    # the FeatureNameToEntityTypeMapping validator, which itself relies on ValidateCallKwargs, which is the validator
    # that constructs the UDFs. Therefore it would create a circular dependency.

    exclude_from_query_validation = True

    def __init__(self, context: 'ValidationContext'):
        super().__init__(context)
        self._udf_node_mapping: UDFNodeMapping = context.get_validator_result(ValidateCallKwargs)
        self._feature_name_to_entity_type = context.get_validator_result(FeatureNameToEntityTypeMapping)
        self._labels_config = context.get_config_subkey(LabelsConfig)

    def validate_source(self, source: 'grammar.Source') -> None:
        for call_node in filter_nodes(source.ast_root, grammar.Call):
            _, arguments = self._udf_node_mapping[id(call_node)]
            if isinstance(arguments, LabelArguments):
                self._validate_label(call_node, arguments)

    def _validate_label(self, call_node: grammar.Call, arguments: LabelArguments) -> None:
        entity_arg_node = arguments.get_argument_ast('entity')
        if not isinstance(entity_arg_node, grammar.Name):
            # If this isn't a name, other things will complain that we need to store entities into variables.
            return

        entity_type = self._feature_name_to_entity_type[entity_arg_node.identifier]

        label_name = arguments.label.value
        label_name_span = arguments.label.argument_span

        label_config = self._labels_config.labels.get(label_name)
        if label_config is None:
            extra_hint = ''
            closest_name = get_closest_string_within_threshold(
                string=label_name, candidate_strings=self._labels_config.labels
            )
            if closest_name is not None:
                extra_hint = f', did you mean `{closest_name}`?'
            self.context.add_error(
                message='unknown label',
                span=label_name_span,
                hint=f'there is no `{label_name}` label in the config{extra_hint}',
                additional_spans=[
                    SpanWithHint(
                        span=self.context.sources.config.closest_span_for_location(
                            [LABELS_CONFIG_SUBKEY], key_only=True
                        ),
                        hint='add the label to the config here',
                    )
                ],
            )
            return

        if entity_type not in label_config.valid_for:
            valid_types_str = ', '.join(f'`{valid_type}`' for valid_type in label_config.valid_for)
            self.context.add_error(
                message='label is not valid for this entity type',
                span=entity_arg_node.span,
                hint=f'entity has type `{entity_type}`, this label is valid for {valid_types_str}',
                additional_spans=[
                    SpanWithHint(
                        span=self.context.sources.config.closest_span_for_location(
                            [LABELS_CONFIG_SUBKEY, label_name, 'valid_for'], key_only=False
                        ),
                        hint='valid types for this label are set here',
                    )
                ],
            )
