# Divine Moderation Service Auto-Ban
# Automatically bans content flagged by the AI moderation service (kind 1984 reports).

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
