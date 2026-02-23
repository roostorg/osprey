from typing import Type, TypeVar

import pytest
from osprey.engine.ast import grammar
from osprey.engine.udf.arguments import ConstExpr

_T = TypeVar('_T')


@pytest.mark.parametrize(
    'ty, lit',
    [
        (str, grammar.String),
        (float, grammar.Number),
        (int, grammar.Number),
        (bool, grammar.Boolean),
        (type(None), grammar.None_),
        (None, grammar.None_),
    ],
)
def test_scalar_const_expr(ty: Type[_T], lit: Type[grammar.Literal]) -> None:
    ConstExprT: Type[ConstExpr[_T]] = ConstExpr[ty]  # type: ignore # Can't use runtime variable in a generic

    expected_ty = ty
    if ty is None:
        expected_ty = type(None)

    assert ConstExpr.get_type_class(ConstExprT) is expected_ty
    assert ConstExpr.get_literal_node_type(ConstExprT) is lit


@pytest.mark.parametrize(
    'ty, lit, inner_ty, inner_lit_ty',
    [
        (list[str], grammar.List, str, grammar.String),
        (list[int], grammar.List, int, grammar.Number),
        (list[float], grammar.List, float, grammar.Number),
        (list[bool], grammar.List, bool, grammar.Boolean),
        (list[None], grammar.List, type(None), grammar.None_),
    ],
)
def test_list_const_expr(
    ty: Type[list[_T]], lit: Type[grammar.Literal], inner_ty: Type[_T], inner_lit_ty: Type[grammar.Literal]
) -> None:
    ConstExprT: Type[ConstExpr[list[_T]]] = ConstExpr[ty]  # type: ignore # Can't use runtime variable in a generic

    assert ConstExpr.get_type_class(ConstExprT) is list
    assert ConstExpr.get_literal_node_type(ConstExprT) is lit
    assert ConstExpr.get_item_type_class(ConstExprT) is inner_ty
    assert ConstExpr.get_item_literal_node_type(ConstExprT) is inner_lit_ty
