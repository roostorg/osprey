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

Did: str = JsonData(
  path='$.did',
  required=False,
)

Handle: str = AtprotoHandle(did=Did)

DisplayName: str = AtprotoDisplayName(did=Did)
