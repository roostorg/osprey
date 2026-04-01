# New Account Spam Detection
# Flags new accounts (< 1 hour old) posting without verification.
# Covers text notes (kind 1) and video events (kinds 34235, 34236).

Import(
  rules=[
    'models/base.sml',
    'models/nostr/kind1_note.sml',
  ]
)

NewAccountSpam = Rule(
  when_all=[
    Kind in [1, 34235, 34236],
    NostrAccountAge(created_at=CreatedAt) < 3600,
    not HasLabel(entity=Pubkey, label='verified'),
  ],
  description='New account (< 1 hour old) posting without verification',
)

WhenRules(
  rules_any=[NewAccountSpam],
  then=[
    DeclareVerdict(verdict='flag_for_review'),
    LabelAdd(entity=Pubkey, label='new_account_activity', expires_after=TimeDelta(days=7)),
  ],
)
