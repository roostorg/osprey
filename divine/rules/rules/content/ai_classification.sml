# AI Classification Routing
#
# Routes Hive AI classification results from moderation-service into
# Osprey verdicts. The moderation-service classifies uploads into four tiers:
#   SAFE / REVIEW / AGE_RESTRICTED / PERMANENT_BAN
#
# NOTE: This rule assumes the Nostr-Kafka bridge or moderation-service
# publishes classification results in a format Osprey can consume.
# The CheckModerationResult UDF is currently a stub returning 'unknown'.
# Until it's wired to actual data, these rules won't fire.
#
# The moderation-service currently publishes NIP-32 labels (kind 1985),
# not kind 1984 reports. The bridge or an adapter needs to normalize
# these into the event format the models expect.

Import(
  rules=[
    'models/base.sml',
    'models/nostr/video_event.sml',
  ]
)

# Video classified as requiring age restriction (nudity, suggestive content
# that doesn't meet the ban threshold).
AgeRestricted = Rule(
  when_all=[
    Kind in [34235, 34236],
    CheckModerationResult(video_hash=VideoHash) == 'age_restricted',
    not HasLabel(entity=EventId, label='human_reviewed'),
  ],
  description='AI classified video as age-restricted',
)

WhenRules(
  rules_any=[AgeRestricted],
  then=[
    LabelAdd(entity=EventId, label='age_restricted'),
    LabelAdd(entity=EventId, label='ai_classified'),
    DeclareVerdict(verdict='restrict'),
  ],
)

# Video classified as requiring human review (borderline content,
# classification confidence below threshold).
NeedsReview = Rule(
  when_all=[
    Kind in [34235, 34236],
    CheckModerationResult(video_hash=VideoHash) == 'review',
    not HasLabel(entity=EventId, label='human_reviewed'),
  ],
  description='AI classified video as needing human review',
)

WhenRules(
  rules_any=[NeedsReview],
  then=[
    LabelAdd(entity=EventId, label='ai_classified'),
    DeclareVerdict(verdict='flag_for_review'),
  ],
)

# Video classified as permanent ban (CSAM, extreme violence, etc.).
# Auto-enforce without waiting for human review.
PermanentBan = Rule(
  when_all=[
    Kind in [34235, 34236],
    CheckModerationResult(video_hash=VideoHash) == 'permanent_ban',
  ],
  description='AI classified video for permanent ban',
)

WhenRules(
  rules_any=[PermanentBan],
  then=[
    BanNostrEvent(event_id=EventId, pubkey=Pubkey, reason='AI classification: permanent ban'),
    LabelAdd(entity=EventId, label='ai_classified'),
    LabelAdd(entity=Pubkey, label='warned'),
    DeclareVerdict(verdict='ban'),
  ],
)
