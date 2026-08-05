from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr

StrConstExpr = ConstExpr[str]  # This being inside the below function is causing mypy to crash


def test_arguments_items() -> None:
    class Arguments(ArgumentsBase):
        foo: str
        bar: StrConstExpr

    items = Arguments.items()

    assert list(items) == ['foo', 'bar']
    assert items['foo'] is str
    assert items['bar'] is StrConstExpr


def test_arguments_items_cache_is_scoped_to_each_subclass() -> None:
    class FirstArguments(ArgumentsBase):
        first: str

    class SecondArguments(ArgumentsBase):
        second: int

    first_items = FirstArguments.items()
    SecondArguments.items()

    assert FirstArguments.items() is first_items


def test_arguments_can_be_none() -> None:
    class Arguments(ArgumentsBase):
        optional: str | None
        union: str | int | None
        none: None
        obj: object
        string: str
        integer: int

    assert Arguments.kwarg_can_be_none('optional')
    assert Arguments.kwarg_can_be_none('union')
    assert Arguments.kwarg_can_be_none('none')
    assert Arguments.kwarg_can_be_none('obj')
    assert not Arguments.kwarg_can_be_none('string')
    assert not Arguments.kwarg_can_be_none('integer')


def test_arguments_can_be_none_is_cached() -> None:
    class Arguments(ArgumentsBase):
        optional: str | None
        string: str

    hits_before = Arguments.kwarg_can_be_none.cache_info().hits

    assert Arguments.kwarg_can_be_none('optional') is True
    assert Arguments.kwarg_can_be_none('string') is False

    # Repeat the same calls; both should now be served from the cache.
    assert Arguments.kwarg_can_be_none('optional') is True
    assert Arguments.kwarg_can_be_none('string') is False

    assert Arguments.kwarg_can_be_none.cache_info().hits == hits_before + 2
