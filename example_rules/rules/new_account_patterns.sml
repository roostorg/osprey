Import(
  rules=[
    'models/account.sml',
    'models/base.sml',
    'models/message.sml',
    'models/metadata.sml',
    'models/post.sml',
  ]
)

# Detect bot-like posting behavior from new accounts
NewAccountHighVolume = Rule(
  when_all=[
    EventType == 'create_post',
    PostCount > 50,
    FollowerCount < 5
  ],
  description='New account with high post count but few followers (bot pattern)',
)

# Detect new accounts mass messaging
NewAccountMassMessaging = Rule(
  when_all=[
    EventType == 'send_message',
    FollowerCount < 3,
    PostCount < 5
  ],
  description='Very new account sending direct messages',
)

# Detect automated user agent patterns
AutomatedUserAgent = Rule(
  when_all=[
    RegexMatch(target=UserAgent, pattern='curl|python-requests|bot|scraper|automated', case_insensitive=True)
  ],
  description='Request from automated user agent',
)

# Detect new account posting links
NewAccountPostingLinks = Rule(
  when_all=[
    EventType == 'create_post',
    PostCount < 10,
    RegexMatch(target=PostText, pattern='https?://', case_insensitive=True)
  ],
  description='New account posting links',
)

# Detect low engagement ratio (many posts, no followers)
LowEngagementRatio = Rule(
  when_all=[
    EventType == 'create_post',
    PostCount > 100,
    FollowerCount < 10
  ],
  description='Account with very low engagement ratio',
)

WhenRules(
  rules_any=[NewAccountHighVolume, LowEngagementRatio],
  then=[
    LabelAdd(entity=UserId, label='suspected_bot'),
  ],
)

WhenRules(
  rules_any=[NewAccountMassMessaging],
  then=[
    LabelAdd(entity=UserId, label='mass_messaging'),
  ],
)

WhenRules(
  rules_any=[AutomatedUserAgent],
  then=[
    LabelAdd(entity=UserId, label='automated_client'),
  ],
)

WhenRules(
  rules_any=[NewAccountPostingLinks],
  then=[
    LabelAdd(entity=UserId, label='new_account_links'),
  ],
)
