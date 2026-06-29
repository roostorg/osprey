from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from flask import Blueprint
from osprey.engine.udf.base import MethodSpec
from osprey.worker.adaptor.plugin_manager import bootstrap_ast_validators, bootstrap_udfs
from osprey.worker.lib.osprey_engine import FeatureLocation
from osprey.worker.lib.singletons import ENGINE
from osprey.worker.ui_api.osprey.lib.abilities import CanViewDocs, require_ability
from pydantic import BaseModel

blueprint = Blueprint('docs', __name__)


class UdfCategory(BaseModel):
    name: str | None
    udfs: Sequence[MethodSpec]


class UdfDocsResponse(BaseModel):
    udf_categories: Sequence[UdfCategory]


@blueprint.route('/docs/udfs', methods=['GET'])
@require_ability(CanViewDocs)
def udf_docs() -> Any:
    specs_by_category = defaultdict(list)
    udf_registry, _ = bootstrap_udfs()
    bootstrap_ast_validators()

    for udf in udf_registry.iter_functions():
        specs_by_category[udf.category].append(udf.get_method_spec())

    categories = []
    # Need the extra `list(...)` here to make mypy happy (otherwise it thinks `sorted` outputs a `list[str]`).
    sorted_category_names: list[str | None] = list(sorted(name for name in specs_by_category if name is not None))
    if None in specs_by_category:
        sorted_category_names.append(None)

    for category_name in sorted_category_names:
        categories.append(
            UdfCategory(
                name=category_name,
                udfs=sorted(specs_by_category[category_name], key=lambda x: x.name),
            )
        )

    return UdfDocsResponse(udf_categories=categories)


class FeatureLocationsDocsResponse(BaseModel):
    locations: list[FeatureLocation]


@blueprint.route('/docs/feature-locations', methods=['GET'])
@require_ability(CanViewDocs)
def feature_locations_docs() -> Any:
    engine = ENGINE.instance()
    locations = engine.get_known_feature_locations()
    return FeatureLocationsDocsResponse(locations=locations)
