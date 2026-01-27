Import(
  rules=[
    'models/account.sml',
    'models/base.sml',
    'models/message.sml',
    'models/post.sml',
  ]
)

# Detect spam links in direct messages
MessageContainsSpamLink = Rule(
  when_all=[
    EventType == 'send_message',
    RegexMatch(target=MessageText, pattern='bit\.ly|tinyurl|free-money|click-here|limited-offer', case_insensitive=True)
  ],
  description='Direct message contains suspicious spam link',
)

# Detect spam links in posts
PostContainsSpamLink = Rule(
  when_all=[
    EventType == 'create_post',
    RegexMatch(target=PostText, pattern='bit\.ly|tinyurl|free-money|click-here|limited-offer', case_insensitive=True)
  ],
  description='Post contains suspicious spam link',
)

# Detect crypto scam patterns
CryptoScamPattern = Rule(
  when_all=[
    EventType == 'send_message',
    RegexMatch(target=MessageText, pattern='crypto|bitcoin|ethereum|investment|guaranteed returns', case_insensitive=True)
  ],
  description='Message contains crypto scam patterns',
)

# New account sending links is suspicious
NewAccountSpamming = Rule(
  when_all=[
    EventType == 'send_message',
    PostCount < 5,
    RegexMatch(target=MessageText, pattern='https?://', case_insensitive=True)
  ],
  description='New account sending links in messages',
)

WhenRules(
  rules_any=[MessageContainsSpamLink, CryptoScamPattern],
  then=[
    LabelAdd(entity=UserId, label='spam_sender'),
    BanUser(entity=UserId, comment='Spam link detected in message'),
  ],
)

WhenRules(
  rules_any=[PostContainsSpamLink],
  then=[
    LabelAdd(entity=UserId, label='spam_poster'),
  ],
)

WhenRules(
  rules_any=[NewAccountSpamming],
  then=[
    LabelAdd(entity=UserId, label='suspicious_new_account'),
  ],
)
