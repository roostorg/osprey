# Repeat Offender -- Graduated Enforcement
# Escalates from warned -> suspended -> banned.
#
# NOTE (matt, 2026-03-06): This maps to the strike system planned in
# support-trust-safety #64. The escalation ladder (warn -> suspend -> ban)
# is sound, but needs a trigger -- something has to apply the initial
# 'warned' label. Currently no rule does that except ai_classification's
# PermanentBan, which goes straight to 'warned' on first offense.
# Consider: should any 'flag_for_review' verdict that a human confirms
# as a violation result in LabelAdd(warned)?
#
# Also: BanNostrEvent bans a single event, but the intent here is to ban
# the user. Consider adding a BanPubkey UDF/effect to match relay-manager's
# 'banpubkey' RPC method.

Import(
  rules=[
    'models/base.sml',
  ]
)

PreviouslyWarned = Rule(
  when_all=[
    HasLabel(entity=Pubkey, label='warned'),
  ],
  description='Account has a prior warning',
)

PreviouslySuspended = Rule(
  when_all=[
    HasLabel(entity=Pubkey, label='suspended'),
  ],
  description='Account is currently suspended',
)

WhenRules(
  rules_any=[PreviouslyWarned],
  then=[
    LabelAdd(entity=Pubkey, label='suspended', expires_after=TimeDelta(days=30)),
    LabelRemove(entity=Pubkey, label='warned'),
    DeclareVerdict(verdict='suspend'),
  ],
)

WhenRules(
  rules_any=[PreviouslySuspended],
  then=[
    LabelAdd(entity=Pubkey, label='banned'),
    BanNostrEvent(event_id=EventId, pubkey=Pubkey, reason='Repeat offender escalation to ban'),
    DeclareVerdict(verdict='ban'),
  ],
)
