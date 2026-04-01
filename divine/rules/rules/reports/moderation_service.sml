# Divine Moderation Service Auto-Ban (kind 1984 reports)
#
# Handles kind 1984 events published by moderation-service for automated
# classifications AND human moderator overrides. Both use NOSTR_PRIVATE_KEY
# and the MOD namespace with labels NS/VI/AI.
#
# This is one of two paths for moderation-service output into Osprey:
#   - Kind 1984 (this file): automated AI flags + human override reports
#   - Kind 1985 (content/label_routing.sml): human-verified label events
#
# The kind 1984 reports use the MOD namespace. Content JSON includes
# scores, type, and source ('ai' or 'human-moderator').
#
# NOTE: ReportReason values below still don't match the actual MOD
# labels (NS, VI, AI). These need alignment with the kind 1984 tag
# structure. The rule currently won't match because it checks for
# 'ai_generated' etc. but the reports use 'NS', 'VI', 'AI'.

Import(
  rules=[
    'models/base.sml',
    'models/nostr/kind1984_report.sml',
  ]
)

ModerationServiceBan = Rule(
  when_all=[
    Kind == 1984,
    HasLabel(entity=Pubkey, label='moderation_service'),
    ReportReason in ['ai_generated', 'deepfake', 'self_harm', 'offensive'],
  ],
  description='Divine moderation service flagged content for permanent ban',
)

WhenRules(
  rules_any=[ModerationServiceBan],
  then=[
    BanNostrEvent(event_id=ReportedEventId, pubkey=ReportedPubkey, reason='Content flagged by moderation service'),
    DeclareVerdict(verdict='auto_ban'),
  ],
)
