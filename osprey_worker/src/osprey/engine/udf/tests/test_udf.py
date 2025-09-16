import textwrap
from typing import Any

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase

from ..arguments import ArgumentSpec, ConstExpr
from ..base import MethodSpec


def test_udf_annotations_result() -> None:
    class Arguments(ArgumentsBase):
        foo: str

    class Test(UDFBase[Arguments, str]):
        def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:  # type: ignore[empty-body]
            pass

    assert Test.get_arguments_type() is Arguments
    assert Test.get_rvalue_type() is str
    assert Test.has_result()
    assert not Test.has_dynamic_result()


def test_udf_annotations_no_result() -> None:
    class Arguments(ArgumentsBase):
        foo: str

    class Test(UDFBase[Arguments, None]):
        def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> None:
            pass

    assert Test.get_arguments_type() is Arguments
    assert Test.get_rvalue_type() is None
    assert not Test.has_result()
    assert not Test.has_dynamic_result()


def test_udf_annotation_dynamic_result() -> None:
    class Arguments(ArgumentsBase):
        foo: str

    class Test(UDFBase[Arguments, Any]):
        def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> Any:
            pass

    assert Test.get_arguments_type() is Arguments
    assert Test.get_rvalue_type() is Any
    assert Test.has_result()
    assert Test.has_dynamic_result()


def test_get_method_spec() -> None:
    class DocArgumentsIntermediate(ArgumentsBase):
        foo: str
        """Single line doc for foo."""

    class DocArguments(DocArgumentsIntermediate):
        bar: ConstExpr[int] = ConstExpr.for_default('bar', 6)
        """Multi-line doc for bar.

        Here's some more details.
        """

    class Test(UDFBase[DocArguments, bool]):
        """UDF that does a thing.

        More docs on this UDF.
        """

        category = 'Tests and Examples'

        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: DocArguments
        ) -> bool:
            pass

    assert Test.get_method_spec() == MethodSpec(
        name='Test',
        doc='UDF that does a thing.\n\nMore docs on this UDF.',
        argument_specs=[
            ArgumentSpec(name='foo', type='str', default=None, doc='Single line doc for foo.'),
            ArgumentSpec(
                name='bar',
                type='ConstExpr[int]',
                default='6',
                doc="Multi-line doc for bar.\n\nHere's some more details.",
            ),
        ],
        return_type='bool',
        category='Tests and Examples',
    )

    assert Test.get_method_spec().stub() == textwrap.dedent(
        '''
        def Test(foo: str, bar: ConstExpr[int] = 6) -> bool:
            """UDF that does a thing.

            More docs on this UDF.

            :param foo: Single line doc for foo.
            :param bar: Multi-line doc for bar.
                Here's some more details.
            """
        '''.lstrip('\n').rstrip()
    )


def test_get_method_spec_no_docs() -> None:
    class NoDocArgumentsIntermediate(ArgumentsBase):
        foo: str

    class NoDocArguments(NoDocArgumentsIntermediate):
        bar: ConstExpr[int] = ConstExpr.for_default('bar', 6)

    class MixinWithDoc:
        """Some doc that shouldn't show up"""

    class Test(MixinWithDoc, UDFBase[NoDocArguments, bool]):
        def execute(  # type: ignore[empty-body]
            self, execution_context: ExecutionContext, arguments: NoDocArguments
        ) -> bool:
            pass

    assert Test.get_method_spec() == MethodSpec(
        name='Test',
        doc=None,
        argument_specs=[
            ArgumentSpec(name='foo', type='str', default=None, doc=None),
            ArgumentSpec(
                name='bar',
                type='ConstExpr[int]',
                default='6',
                doc=None,
            ),
        ],
        return_type='bool',
        category=None,
    )

    assert Test.get_method_spec().stub() == textwrap.dedent(
        """
        def Test(foo: str, bar: ConstExpr[int] = 6) -> bool:
            pass
        """.lstrip('\n').rstrip()
    )
