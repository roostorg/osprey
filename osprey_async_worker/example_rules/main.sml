# Example rules for the EXPERIMENTAL asyncio worker.
#
# These use only stdlib UDFs, so they compile with `osprey-async-cli run`
# (the Phase-0 stdlib engine, no plugins required). The top-level ./example_rules
# is for the gevent worker and depends on plugin UDFs (TextContains, BanUser),
# which the stdlib async engine does not provide.
Require(rule='rules/long_message.sml')
