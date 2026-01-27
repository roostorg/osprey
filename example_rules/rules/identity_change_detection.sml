Import(
  rules=[
    'models/account.sml',
    'models/base.sml',
    'models/identity.sml',
  ]
)

# Detect handle change to impersonate others
HandleImpersonation = Rule(
  when_all=[
    EventType == 'identity_change',
    ChangeType == 'handle',
    RegexMatch(target=NewHandle, pattern='admin|official|support|moderator|staff', case_insensitive=True)
  ],
  description='User changed handle to impersonate authority figure',
)

# Detect rapid handle changes (evasion behavior)
RapidHandleChange = Rule(
  when_all=[
    EventType == 'identity_change',
    ChangeType == 'handle',
    PostCount < 10
  ],
  description='New account rapidly changing handle',
)

# Detect display name with suspicious patterns
SuspiciousDisplayName = Rule(
  when_all=[
    EventType == 'identity_change',
    ChangeType == 'display_name',
    RegexMatch(target=DisplayName, pattern='FREE|GIVEAWAY|DM ME|CLICK', case_insensitive=True)
  ],
  description='Display name contains promotional/spam content',
)

# Detect handle changes that remove previous identity
IdentityWipe = Rule(
  when_all=[
    EventType == 'identity_change',
    ChangeType == 'handle',
    TextContains(text=NewHandle, phrase='user_')
  ],
  description='User changed to generic handle (possible identity wipe)',
)

WhenRules(
  rules_any=[HandleImpersonation],
  then=[
    LabelAdd(entity=UserId, label='impersonation_attempt'),
    BanUser(entity=UserId, comment='Attempted to impersonate authority figure'),
  ],
)

WhenRules(
  rules_any=[RapidHandleChange, IdentityWipe],
  then=[
    LabelAdd(entity=UserId, label='identity_evasion'),
  ],
)

WhenRules(
  rules_any=[SuspiciousDisplayName],
  then=[
    LabelAdd(entity=UserId, label='spam_display_name'),
  ],
)
