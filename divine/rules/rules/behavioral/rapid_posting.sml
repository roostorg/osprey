# Rapid Posting — Rate Limiting
# Rate-limits new accounts that are already flagged and not verified.

Import(
  rules=[
    'models/base.sml',
  ]
)

RapidPosting = Rule(
  when_all=[
    Kind in [1, 34235, 34236],
    HasLabel(entity=Pubkey, label='new_account_activity'),
    not HasLabel(entity=Pubkey, label='verified'),
  ],
  description='Flagged account posting without verification',
)

WhenRules(
  rules_any=[RapidPosting],
  then=[
    DeclareVerdict(verdict='rate_limit'),
    LabelAdd(entity=Pubkey, label='rate_limited', expires_after=TimeDelta(hours=1)),
  ],
)
