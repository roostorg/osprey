import sentry_sdk

from ..singletons import CONFIG


def init_sentry_from_config() -> None:
    config = CONFIG.instance()
    sentry_dsn = config.get_str('SENTRY_RULES_SINK_DSN', '')
    if sentry_dsn:
        sentry_sdk.init(sentry_dsn)
    return
