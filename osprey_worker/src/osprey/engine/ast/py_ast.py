"""This implements the Python AST -> Osprey AST transformer."""

import ast
from typing import Any, Type, TypeVar
from typing import List as ListT

from .ast_utils import iter_field_values
from .errors import OspreySyntaxError
from .grammar import (
    Add,
    And,
    Annotation,
    AnnotationWithVariants,
    Assign,
    ASTNode,
    Attribute,
    BinaryComparison,
    BinaryOperation,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    Boolean,
    BooleanOperand,
    BooleanOperation,
    Call,
    Comparator,
    Context,
    Divide,
    Equals,
    Expression,
    FloorDivide,
    FormatString,
    GreaterThan,
    GreaterThanEquals,
    In,
    Keyword,
    LeftShift,
    LessThan,
    LessThanEquals,
    List,
    Literal,
    Load,
    Modulo,
    Multiply,
    Name,
    None_,
    Not,
    NotEquals,
    NotIn,
    Number,
    Operator,
    Or,
    Pow,
    RightShift,
    Root,
    Source,
    Span,
    Statement,
    Store,
    String,
    Subtract,
    UnaryOperation,
    UnaryOperator,
    USub,
)

T = TypeVar('T')
V = TypeVar('V')


def transform(source: Source) -> Root:
    """Given a source file which is in the limited Python syntax, transform it
    into a osprey_ast node tree, given the specification in osprey_ast.grammar.

    Will throw an OspreySyntaxError if the source file contents are
    invalid.
    """
    try:
        parsed = ast.parse(source.contents, filename=source.path)
    except SyntaxError as e:
        lineno = e.lineno
        assert lineno is not None
        raise OspreySyntaxError(
            source=source,
            error=e.msg,
            span=Span(source=source, start_line=lineno, start_pos=max((e.offset or 1) - 1, 0)),
        )

    ast_root = OspreyAstNodeTransformer(source).transform(parsed)
    assert isinstance(ast_root, Root), 'AST transform did not return a Root Expression'
    fixup_parents(ast_root)
    return ast_root


class OspreyAstNodeTransformer:
    """Given a python ast node tree, transform it into a osprey_ast node
    tree."""

    def __init__(self, source: Source):
        self.source = source
        self.current_node: ast.AST | None = None

    def transform(self, node: ast.AST) -> ASTNode:
        # Store current node here, so that invariant will work in given node's context.
        prev_node = self.current_node
        self.current_node = node
        # Try and find a transformer for the given node type. If not found, it means that we
        # have encountered a node which does not have a translation to osprey_ast. In which case,
        # the source file is considered invalid.
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)
        self.invariant(
            transformer,  # type: ignore[arg-type]
            f'unexpected node type `{node.__class__.__name__}`',
            hint='the Osprey rule language is a simplified subset of python\nnot all Python is valid osprey rule syntax',  # noqa: E501
            node=node,
        )

        # Run the transformer, so that we'll recursively transform the node tree.
        ret = transformer(node)  # type: ignore[misc]
        assert isinstance(ret, ASTNode)
        # Before we return, restore the current node stack, such that any invariants that may be called
        # after the transform completes gets the correct node, rather than the child-most node.
        self.current_node = prev_node

        return ret

    def transform_Module(self, node: ast.Module) -> Root:
        statements = [self.expect(Statement, self.transform(n)) for n in node.body]
        # Modules don't have line/col, have it be the beginning of the file
        span = Span(self.source, start_line=1, start_pos=0)
        return Root(statements=statements, span=span)

    def transform_Assign(self, node: ast.Assign) -> Assign:
        self.invariant(len(node.targets) == 1, 'an assignment can only have a single target', node=node)

        target = self.expect(Name, self.transform(node.targets[0]), 'you can only assign to a Name, e.g. `Foo = Bar`')
        value = self.expect(Expression, self.transform(node.value))

        return Assign(span=self.span_for(node), target=target, value=value, annotation=None)

    def transform_AnnAssign(self, node: ast.AnnAssign) -> Assign:
        self.invariant(node.simple == 1, 'expected simple Annotated Assignment', node=node)
        target = self.expect(Name, self.transform(node.target))
        if node.value is None:
            raise self.make_syntax_error('assignment must have a value', span=self.span_for(node))
        value = self.expect(Expression, self.transform(node.value))
        annotation = pyast_ann_assign_annotation_to_annotation(self, node.annotation)
        if annotation.identifier == 'ExtractLiteral':
            self.expect_pyast_union(
                Literal,
                FormatString,
                value,
                hint=f'`{annotation.identifier}` assignment expects an rvalue of type `Union[Literal, FormatString]`',
            )
        return Assign(span=self.span_for(node), target=target, value=value, annotation=annotation)

    def transform_Name(self, node: ast.Name) -> Name:
        span = self.span_for(node)
        context = pyast_context_to_osprey_context(self, node.ctx, span=span)
        return Name(span=span, identifier=node.id, context=context)

    def transform_NameConstant(self, node: ast.NameConstant) -> Boolean | None_:
        self.invariant(node.value in (True, False, None), 'unexpected name constant', node=node)
        if node.value is None:
            return None_(span=self.span_for(node))

        return Boolean(span=self.span_for(node), value=node.value)

    # typeshed is missing ast.Constant as it is new in python 3.8
    # so we are typing as Any until it gains that definition.
    def transform_Constant(self, node: Any) -> String | Number | Boolean | None_:
        span = self.span_for(node)
        if isinstance(node.value, str):
            return String(span=span, value=node.value)
        elif isinstance(node.value, bool):
            # Note: This needs to be before the int/float check, because python
            # thinks booleans are also ints, for some weird backwards compatibility reason.
            return Boolean(span=span, value=node.value)
        elif isinstance(node.value, (int, float)):
            return Number(span=span, value=node.value)
        elif node.value is None:
            return None_(span=span)
        else:
            raise self.make_syntax_error(f'unexpected constant `{node.value!r}`', span=span)

    def transform_Str(self, node: ast.Str) -> String:
        return String(span=self.span_for(node), value=node.s)

    def transform_Num(self, node: ast.Num) -> Number:
        assert isinstance(node.n, (int, float)), 'Node is not an int or float.'
        return Number(span=self.span_for(node), value=node.n)

    def transform_Call(self, node: ast.Call) -> Call:
        self.invariant(len(node.args) == 0, 'functions with non-keyword arguments are not supported', node=node)
        func = self.expect_pyast_union(Attribute, Name, self.transform(node.func))
        kwargs = [
            Keyword(span=self.span_for(k.value), name=k.arg, value=self.expect(Expression, self.transform(k.value)))
            for k in node.keywords
            if k.arg
        ]
        return Call(span=self.span_for(node), func=func, arguments=kwargs)

    def transform_BinOp(self, node: ast.BinOp) -> BinaryOperation:
        left = self.expect(Expression, self.transform(node.left))
        right = self.expect(Expression, self.transform(node.right))
        operator = pyast_op_to_osprey_operator(self, node.op, span=self.span_for(node))

        return BinaryOperation(span=self.span_for(node), left=left, right=right, operator=operator)

    def transform_UnaryOp(self, node: ast.UnaryOp) -> UnaryOperation:
        operand = self.expect(Expression, self.transform(node.operand))
        operator = pyast_unaryop_to_osprey_unary_operator(self, node.op, span=self.span_for(node))

        return UnaryOperation(span=self.span_for(node), operand=operand, operator=operator)

    def transform_Compare(self, node: ast.Compare) -> BinaryComparison:
        self.invariant(
            len(node.ops) == 1 and len(node.comparators) == 1,
            'only binary comparisons are supported',
            hint='instead of `a < b < c`, consider: `a < b and b < c`',
            node=node,
        )

        left = self.expect(Expression, self.transform(node.left))
        right = self.expect(Expression, self.transform(node.comparators[0]))
        comparator = pyast_cmp_to_osprey_comparator(self, node.ops[0], span=self.span_for(node))

        return BinaryComparison(span=self.span_for(node), left=left, right=right, comparator=comparator)

    def transform_BoolOp(self, node: ast.BoolOp) -> BooleanOperation:
        operand: BooleanOperand
        span = self.span_for(node)
        if isinstance(node.op, ast.Or):
            operand = Or(span=span)
        elif isinstance(node.op, ast.And):
            operand = And(span=span)
        else:
            raise self.make_syntax_error(f'unexpected boolean operator `{node.op!r}`', span=span)

        values = [self.expect(Expression, self.transform(v)) for v in node.values]

        return BooleanOperation(span=span, operand=operand, values=values)

    def transform_Expr(self, node: ast.Expr) -> Expression:
        self.invariant(
            isinstance(node.value, ast.Call),
            'a bare expression is not allowed, unless it is a function call',
            hint='consider prefixing this with a variable assignment: `Foo =`',
            node=node,
        )
        transformed = self.transform(node.value)
        assert isinstance(transformed, Expression), 'Transform did not return expression.'
        return transformed

    def transform_Attribute(self, node: ast.Attribute) -> Attribute:
        name = self.expect(
            Name,
            self.transform(node.value),
            hint=(
                'you can only access attributes one level deep'
                '\nfor example:'
                '\n - `Foo.Bar` is OK'
                '\n - `Foo.Bar.Baz` is not OK'
            ),
        )
        span = self.span_for(node)
        context = pyast_context_to_osprey_context(self, node.ctx, span=span)

        return Attribute(span=span, name=name, attribute=node.attr, context=context)

    def transform_List(self, node: ast.List) -> List:
        self.invariant(
            isinstance(node.ctx, ast.Load),
            'a list can only be in the load context',
            hint=(
                'this generally means that a list must not be on the left-hand side of an `=`\n'
                'for example:\n'
                ' - `[Foo, Bar] = Baz` is not OK\n'
                ' - `Baz = [Foo, Bar]` is OK'
            ),
            node=node,
        )
        items = [self.expect(Expression, self.transform(v)) for v in node.elts]
        return List(span=self.span_for(node), items=items)

    def transform_JoinedStr(self, node: ast.JoinedStr) -> FormatString:
        format_string_parts = []
        names = []

        for value in node.values:
            if isinstance(value, ast.Str):
                format_string_parts.append(value.s)
                continue

            if isinstance(value, ast.FormattedValue):
                offset_by = sum(len(n) for n in format_string_parts) + 3
                self.invariant(
                    value.format_spec is None and value.conversion == -1,
                    'a format string cannot have a format spec',
                    hint=(
                        'format strings must be simple, for example:\n'
                        ' - `{Foo}` is OK, but:\n'
                        ' - `{Foo!r}` is not, as it has a `!r` or format spec attached'
                    ),
                    offset_by=offset_by,
                    node=node,
                )
                self.invariant(
                    isinstance(value.value, ast.Name),
                    'a format string can only interpolate a `Name`',
                    hint=(
                        'format strings are simple, and only interpolate names, for example:\n'
                        ' - `{Foo}` is OK, but:\n'
                        ' - `{1 + 1}`, `{"hello"}` or `{SomeFunc(bar="baz")}` is not!'
                    ),
                    offset_by=offset_by,
                    node=node,
                )

                name = self.expect(Name, self.transform(value.value))
                self.invariant(
                    isinstance(name.context, Load),
                    f'BUG: Somehow, a name is being used in `{name.context!r}` context within a format string',
                    node=node,
                )
                names.append(name)
                format_string_parts.append(f'{{{name.identifier}}}')

        format_string = ''.join(format_string_parts)
        return FormatString(span=self.span_for(node), format_string=format_string, names=names)

    def span_for(self, node: ast.AST, offset_by: int = 0) -> Span:
        start_line: int | None = getattr(node, 'lineno', None)
        start_pos: int | None = getattr(node, 'col_offset', None)
        assert start_line is not None, f'Every AST node should have a span, but {node!r} did not.'
        assert start_pos is not None, f'Every AST node should have a span, but {node!r} did not.'
        return Span(source=self.source, start_line=start_line, start_pos=start_pos + offset_by)

    def invariant(self, cond: bool, error: str, hint: str = '', offset_by: int = 0, *, node: ast.AST) -> bool:
        if not cond:
            raise self.make_syntax_error(error, hint, offset_by, span=self.span_for(node))

        return cond

    def make_syntax_error(self, error: str, hint: str = '', offset_by: int = 0, *, span: Span) -> OspreySyntaxError:
        if self.current_node:
            span = self.span_for(self.current_node, offset_by=offset_by)

        return OspreySyntaxError(source=self.source, span=span, error=error, hint=hint)

    def expect(self, ty: Type[T], node: ASTNode, hint: str = '') -> T:
        if not isinstance(node, ty):
            if isinstance(node, ASTNode):
                raise OspreySyntaxError(
                    source=self.source,
                    span=node.span,
                    error=f'expected `{ty.__name__}`, got `{node.__class__.__name__}`',
                    hint=hint,
                )
            else:
                raise Exception(f'BUG! `{node.__class__.__name__}` is not an AST node.')

        return node

    def expect_pyast_ty(self, ty: Type[T], expr: ast.AST, hint: str = '') -> T:
        if not isinstance(expr, ty):
            raise OspreySyntaxError(
                source=self.source,
                span=self.span_for(expr),
                error=f'expected `{ty.__name__}`, got `{expr.__class__.__name__}`',
                hint=hint,
            )

        return expr

    def expect_pyast_union(self, ty: Type[T], ty2: Type[V], node: ASTNode, hint: str = '') -> T | V:
        # This function is a bit hacky, but lets us convince mypy that we have a union type.
        if not (isinstance(node, ty) or isinstance(node, ty2)):
            if isinstance(node, ASTNode):
                raise OspreySyntaxError(
                    source=self.source,
                    span=node.span,
                    error=f'expected `{ty.__name__}` or `{ty2.__name__}`, got `{node.__class__.__name__}`',
                    hint=hint,
                )
            else:
                raise Exception(f'BUG! {node!r} is not an AST node.')

        return node


def pyast_op_to_osprey_operator(transformer: OspreyAstNodeTransformer, op: ast.operator, span: Span) -> Operator:
    """Given a python ast operator, convert it to the suitable osprey_ast
    Operator."""
    if isinstance(op, ast.Add):
        return Add(span)
    elif isinstance(op, ast.Sub):
        return Subtract(span)
    elif isinstance(op, ast.Mult):
        return Multiply(span)
    elif isinstance(op, ast.Div):
        return Divide(span)
    elif isinstance(op, ast.FloorDiv):
        return FloorDivide(span)
    elif isinstance(op, ast.Mod):
        return Modulo(span)
    elif isinstance(op, ast.Pow):
        return Pow(span)
    elif isinstance(op, ast.LShift):
        return LeftShift(span)
    elif isinstance(op, ast.RShift):
        return RightShift(span)
    elif isinstance(op, ast.BitOr):
        return BitwiseOr(span)
    elif isinstance(op, ast.BitXor):
        return BitwiseXor(span)
    elif isinstance(op, ast.BitAnd):
        return BitwiseAnd(span)
    else:
        raise transformer.make_syntax_error(f'unsupported operator `{op.__class__.__name__}` encountered', span=span)


def pyast_unaryop_to_osprey_unary_operator(
    transformer: OspreyAstNodeTransformer, op: ast.unaryop, span: Span
) -> UnaryOperator:
    if isinstance(op, ast.Not):
        return Not(span)
    elif isinstance(op, ast.USub):
        return USub(span)
    else:
        raise transformer.make_syntax_error(
            f'unsupported unary operator `{op.__class__.__name__}` encountered', span=span
        )


def pyast_cmp_to_osprey_comparator(transformer: OspreyAstNodeTransformer, op: ast.cmpop, span: Span) -> Comparator:
    """Given a python ast comparator, convert it to a osprey_ast comparator."""
    if isinstance(op, ast.Eq):
        return Equals(span)
    elif isinstance(op, ast.NotEq):
        return NotEquals(span)
    elif isinstance(op, ast.Lt):
        return LessThan(span)
    elif isinstance(op, ast.LtE):
        return LessThanEquals(span)
    elif isinstance(op, ast.Gt):
        return GreaterThan(span)
    elif isinstance(op, ast.GtE):
        return GreaterThanEquals(span)
    elif isinstance(op, ast.In):
        return In(span)
    elif isinstance(op, ast.NotIn):
        return NotIn(span)
    else:
        raise transformer.make_syntax_error(f'unsupported comparator `{op.__class__.__name__}` encountered', span=span)


def pyast_context_to_osprey_context(
    transformer: OspreyAstNodeTransformer, ctx: ast.expr_context, span: Span
) -> Context:
    """Given a python ast variable context, convert it to a osprey_ast
    Context."""
    if isinstance(ctx, ast.Store):
        return Store()
    elif isinstance(ctx, ast.Load):
        return Load()
    else:
        raise transformer.make_syntax_error(f'unexpected context `{ctx.__class__.__name__}`!', span=span)


def fixup_parents(root: Root) -> None:
    """Traverses the AST, updating the nodes to point to their parent nodes."""

    def recursively_insert_parents(parent_node: ASTNode | None, current_node: ASTNode) -> None:
        assert isinstance(current_node, ASTNode)
        current_node.parent = parent_node

        for _field_name, field_value in iter_field_values(current_node):
            if isinstance(field_value, ASTNode):
                recursively_insert_parents(current_node, field_value)
            elif isinstance(field_value, list):
                for item in field_value:
                    recursively_insert_parents(current_node, item)

    recursively_insert_parents(None, root)


# noinspection PyPep8Naming
def pyast_ann_assign_annotation_to_annotation(
    transformer: OspreyAstNodeTransformer, annotation: ast.expr
) -> Annotation | AnnotationWithVariants:
    annotation_hint: str = (
        'annotation must be in the form of `Target: Annotation = Value`, where `Annotation` is a `Name` '
        'or `Name[Name, ...]`'
    )
    if isinstance(annotation, ast.Name):
        return Annotation(identifier=annotation.id, span=transformer.span_for(annotation))

    if isinstance(annotation, ast.Constant) and annotation.value is None:
        return Annotation(identifier='None', span=transformer.span_for(annotation))

    if isinstance(annotation, ast.Subscript):
        variants: ListT[Annotation | AnnotationWithVariants] = []
        value = transformer.expect_pyast_ty(ast.Name, annotation.value, hint=annotation_hint)
        slice = annotation.slice

        if isinstance(slice, ast.Name):
            variants.append(Annotation(identifier=slice.id, span=transformer.span_for(slice)))
        elif isinstance(slice, ast.Tuple):
            for el in slice.elts:
                name = transformer.expect_pyast_ty(ast.Name, el, hint=annotation_hint)
                variants.append(Annotation(identifier=name.id, span=transformer.span_for(el)))
        else:
            variant = pyast_ann_assign_annotation_to_annotation(transformer, slice)
            variants.append(variant)

        return AnnotationWithVariants(identifier=value.id, variants=variants, span=transformer.span_for(annotation))

    raise transformer.make_syntax_error(
        error=f'unexpected node: `{annotation.__class__.__name__}`',
        span=transformer.span_for(annotation),
        hint=annotation_hint,
    )
