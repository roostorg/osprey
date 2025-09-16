from . import grammar


# noinspection PyUnusedLocal
class ASTPrinter:
    """Given a osprey_ast node tree, transform it back to the source representation."""

    def transform(self, node: grammar.ASTNode) -> str:
        method = 'transform_' + node.__class__.__name__
        transformer = getattr(self, method, None)
        assert transformer, f'Unknown AST expression {node.__class__.__name__}'
        ret = transformer(node)
        assert isinstance(ret, str)
        return ret

    def transform_Root(self, node: grammar.Root) -> str:
        return '\n'.join(self.transform(statement) for statement in node.statements)

    def transform_Name(self, node: grammar.Name) -> str:
        return node.identifier

    def transform_String(self, node: grammar.String) -> str:
        return repr(node.value)

    def transform_Number(self, node: grammar.Number) -> str:
        return repr(node.value)

    def transform_Boolean(self, node: grammar.Boolean) -> str:
        return repr(node.value)

    def transform_None_(self, node: grammar.None_) -> str:
        return 'None'

    def transform_Assign(self, node: grammar.Assign) -> str:
        target = self.transform(node.target)
        value = self.transform(node.value)

        if node.annotation:
            annotation = self.transform(node.annotation)
            return f'{target}: {annotation} = {value}'
        else:
            return f'{target} = {value}'

    def transform_Call(self, node: grammar.Call) -> str:
        func = self.transform(node.func)
        arguments = ', '.join(self.transform(argument) for argument in node.arguments)
        return f'{func}({arguments})'

    def transform_Keyword(self, node: grammar.Keyword) -> str:
        value = self.transform(node.value)
        return f'{node.name}={value}'

    def transform_Add(self, node: grammar.Add) -> str:
        return '+'

    def transform_Subtract(self, node: grammar.Subtract) -> str:
        return '-'

    def transform_Multiply(self, node: grammar.Multiply) -> str:
        return '*'

    def transform_Divide(self, node: grammar.Divide) -> str:
        return '/'

    def transform_FloorDivide(self, node: grammar.FloorDivide) -> str:
        return '//'

    def transform_Modulo(self, node: grammar.Modulo) -> str:
        return '%'

    def transform_Pow(self, node: grammar.Pow) -> str:
        return '**'

    def transform_LeftShift(self, node: grammar.LeftShift) -> str:
        return '<<'

    def transform_RightShift(self, node: grammar.RightShift) -> str:
        return '>>'

    def transform_BitwiseOr(self, node: grammar.BitwiseOr) -> str:
        return '|'

    def transform_BitwiseXor(self, node: grammar.BitwiseXor) -> str:
        return '^'

    def transform_BitwiseAnd(self, node: grammar.BitwiseAnd) -> str:
        return '&'

    def transform_BinaryOperation(self, node: grammar.BinaryOperation) -> str:
        left = self.transform(node.left)
        right = self.transform(node.right)
        operator = self.transform(node.operator)
        return f'({left} {operator} {right})'

    def transform_UnaryOperation(self, node: grammar.UnaryOperation) -> str:
        operand = self.transform(node.operand)
        operator = self.transform(node.operator)
        return f'({operator} {operand})'

    def transform_Not(self, node: grammar.Not) -> str:
        return 'not'

    def transform_USub(self, node: grammar.USub) -> str:
        return '-'

    def transform_Equals(self, node: grammar.Equals) -> str:
        return '=='

    def transform_NotEquals(self, node: grammar.NotEquals) -> str:
        return '!='

    def transform_LessThan(self, node: grammar.LessThan) -> str:
        return '<'

    def transform_LessThanEquals(self, node: grammar.LessThanEquals) -> str:
        return '<='

    def transform_GreaterThan(self, node: grammar.GreaterThan) -> str:
        return '>'

    def transform_GreaterThanEquals(self, node: grammar.GreaterThanEquals) -> str:
        return '>='

    def transform_In(self, node: grammar.In) -> str:
        return 'in'

    def transform_NotIn(self, node: grammar.NotIn) -> str:
        return 'not in'

    def transform_BinaryComparison(self, node: grammar.BinaryComparison) -> str:
        left = self.transform(node.left)
        right = self.transform(node.right)
        comparator = self.transform(node.comparator)
        return f'{left} {comparator} {right}'

    def transform_And(self, node: grammar.And) -> str:
        return 'and'

    def transform_Or(self, node: grammar.Or) -> str:
        return 'or'

    def transform_BooleanOperation(self, node: grammar.BooleanOperation) -> str:
        operand = self.transform(node.operand)
        joiner = f' {operand} '
        values = joiner.join(self.transform(value) for value in node.values)
        return f'({values})'

    def transform_Attribute(self, node: grammar.Attribute) -> str:
        name = self.transform(node.name)
        return f'{name}.{node.attribute}'

    def transform_List(self, node: grammar.List) -> str:
        items = ', '.join(self.transform(item) for item in node.items)
        return f'[{items}]'

    def transform_FormatString(self, node: grammar.FormatString) -> str:
        return f'f{node.format_string!r}'

    def transform_Annotation(self, node: grammar.Annotation) -> str:
        return node.identifier

    def transform_AnnotationWithVariants(self, node: grammar.AnnotationWithVariants) -> str:
        variants = ', '.join(self.transform(variant) for variant in node.variants)
        return f'{node.identifier}[{variants}]'


def print_ast(node: grammar.ASTNode) -> str:
    """Given an AST node, prints it back to source code format."""
    return ASTPrinter().transform(node)
