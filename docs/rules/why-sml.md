# Why a Bespoke Language?

Why does Osprey use the bespoke SML ("Some Madeup Language") instead of supporting plain Python? The short answer is that SML's restrictions are what make Osprey's rule engine safe to run untrusted, third-party-authored logic in production—and fast enough to do it in real time.

- **The grammar is intentionally small.** The SML parser (`osprey_worker/src/osprey/engine/ast/py_ast.py`) only understands a restricted set of Python AST nodes: assignments, calls, comparisons, boolean/binary operators, literals, and f-strings. Anything outside that set raises a syntax error; i.e. there's no `for`/`while`, `def`/`class`, or arbitrary `import`. This helps rule out unbounded loops and recursion by construction, meaning a rule can't hang or DoS the worker.

- **Every name is assign-once.** The `UniqueStoredNames` validator rejects re-declaring the same name anywhere in the ruleset, and `NoUnusedLocals` rejects declaring a name that's never read. Combined, every rule and feature compiles down to a single, static, acyclic dependency graph (`execution_graph.py`) rather than an imperative script with mutable state.

- **The static graph makes the engine fast and introspectable.** Because nothing can have side effects or run in a loop, the executor can safely evaluate independent branches of the graph in parallel using gevent greenlets (`executor.py`); the UI can also render the exact dependency graph for any rule or feature, which powers the [Rules Visualizer](../user/manage.md#rules-visualizer) and the cross-referencing in the [Rules](../user/manage.md#rules-registry) and [Features Registry](../user/manage.md#features-registry) pages.

- **Types are checked before rules run.** `validate_static_types.py` and `validate_call_rvalue.py` catch type errors and misused return values at validation/push time, not as a runtime `AttributeError` in production the first time a rare code path executes.

Since SML is a legal subset of Python syntax, pointing your editor at Python-mode gets you reasonable syntax highlighting. See [IDE Setup](../development/ide.md) for current editor setup.
