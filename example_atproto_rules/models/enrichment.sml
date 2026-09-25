# Opt-in profile enrichment.
#
# JetStream carries only the actor DID, so these features resolve it to a handle
# and display name via a per-event call to the Bluesky public API (cached per DID,
# absent when the account can't be resolved or the API rate-limits).
#
# Because that is an external dependency you don't want in a load test, this file
# is NOT imported by main.sml by default. To enable it for demos, add
# 'models/enrichment.sml' to main.sml's imports and add Handle / DisplayName to
# config/ui_config.yaml. See the plugin README for how to extend it with more
# profile fields (account age, follower counts, existing labels).
#
# Note the difference between a UDF being registered and being invoked. The plugin
# registers AtprotoHandle / AtprotoDisplayName whenever it is installed, the same
# as every other example UDF, so the compiler can resolve them. That registration
# is inert: it makes no API calls. The getProfile lookups happen only when a rule
# references the UDFs, which happens only through this file. So leaving it out of
# main.sml's imports is what keeps enrichment off; there is no separate switch.

Did: str = JsonData(
  path='$.did',
  required=False,
)

Handle: str = AtprotoHandle(did=Did)

DisplayName: str = AtprotoDisplayName(did=Did)
