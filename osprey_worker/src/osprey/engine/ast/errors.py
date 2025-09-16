from .grammar import Source, Span


class OspreySyntaxError(Exception):
    """Some error happened while trying to transform the python ast into Osprey
    ast."""

    def __init__(self, source: Source, span: Span, error: str, hint: str = ''):
        super().__init__(span, error)
        self.span = span
        self.error = error
        self.hint = hint
        self.source = source

    def rendered(self) -> str:
        if not self.span:
            return f'error: {self.error}'

        from .error_utils import render_span_context_with_message

        return render_span_context_with_message(self.error, self.span, self.hint)
