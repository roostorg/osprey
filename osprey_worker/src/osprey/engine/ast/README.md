# Osprey `osprey_ast`

Osprey defines a rules language that is used to represent classification and feature extraction of data.

This package holds the AST nodes and grammars of the language, and bootstraps the parser by leaning on Python's syntax and AST module.

## Grammar

Osprey's `osprey_ast` defines a very simple language that is python-like, however, much more slimmed down. For example, there are no classes, function definitions, if statements, etc...

At it's core, we're looking at something quite simple:

```python
Foo = 1 + 1
Baz = 1 < 2
Qux = Foo(bar=Baz, ban=Baz, dah="hello")
```

## `py_ast` transformations

In order to avoid writing a tokenizer and lexer, we're leaning upon python's AST parser to generate an AST node tree, we then transform and perform basic constraint validation of the node tree, such that it now becomes a strongly typed osprey_ast node tree. We do this, to simplify the surface area of what nodes the downstream validators need to traverse in order to perform semantic validation and typechecking.

So, given a source file, we:
  - parse as python
  - transform python's `ast.T` into osprey's `osprey_ast.ASTNode`.

Each `ASTNode` has an associated `Span`, which points to its position within the `Source` file.

## Usage

To parse a file, simple create a `Source` and then call `.parse()` on it!

```python
from osprey.engine.ast import Source, OspreySyntaxError

source = Source(path='README_EXAMPLE.py', contents='Foo = Bar')
try:
    root = source.ast_root
except OspreySyntaxError as e:
    print(e.rendered())
```
