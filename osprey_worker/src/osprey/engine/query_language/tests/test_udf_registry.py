from osprey.engine.query_language.udfs.registry import UDF_REGISTRY


def test_no_udf_registrations_were_rejected() -> None:
    """Build-time guard for https://github.com/roostorg/osprey/issues/439.

    `register()` (osprey.engine.query_language.udfs.registry) rejects a UDF that isn't a
    QueryUdfBase by logging and skipping it, rather than crashing the worker or letting it silently
    misbehave — but that alone wouldn't fail CI, since a skipped UDF just quietly isn't queryable.
    This checks the registry's own rejection list instead, which every call goes through regardless
    of where the UDF is defined, so a misconfigured UDF fails the test suite no matter which file it
    lives in. By the time this test runs, everything imported during test collection (including via
    `query_language`'s own `import_all_direct_children`) has already had a chance to register and,
    if rejected, appear here.
    """
    assert not UDF_REGISTRY.rejected_registrations, (
        'These UDFs were registered as query functions but rejected for not subclassing QueryUdfBase: '
        f'{", ".join(cls.__name__ for cls in UDF_REGISTRY.rejected_registrations)}'
    )
