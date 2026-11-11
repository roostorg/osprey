# Real-time scam detection with an OPEN MODEL against a WRITTEN POLICY.
#
# CoPE-B-S1 (via Zentropi, free inference) scores each post's text against the
# policy string below and returns a violation probability. When it clears the
# threshold, the author is labeled `scam`. Change the policy text to change what
# gets flagged — no retraining. (The CopePolicyScore UDF lives in example_plugins.)

Import(
  rules=[
    'models/base.sml',
    'models/post.sml',
  ]
)

# Policy is written to DESCRIBE the violating content (CoPE-B-S1 expects this
# framing; "do not…" prohibitions invert its scores).
ScamScore = CopePolicyScore(
  text=PostText,
  policy='Content that tries to deceive people out of money, property, or personal information. This includes promises of guaranteed or unrealistic financial returns; cryptocurrency doubling or giveaway schemes; requests to send money or crypto in order to receive more back; fake prizes, lotteries, grants, or inheritances that require an upfront fee; and impersonation of a person, brand, bank, or government agency to solicit money or information.',
)

ScamPost = Rule(
  when_all=[
    EventType == 'create_post',
    ScamScore > 0.8,
  ],
  description='CoPE-B-S1 scored this post as violating the scams policy (> 0.8)',
)

WhenRules(
  rules_any=[ScamPost],
  then=[
    LabelAdd(entity=UserId, label='scam'),
  ],
)
