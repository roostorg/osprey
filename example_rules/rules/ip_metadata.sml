Import(
  rules=[
    'models/account.sml',
    'models/base.sml',
    'models/metadata.sml',
  ]
)

# Detect known VPN/proxy IP ranges (example patterns)
VpnProxyDetection = Rule(
  when_all=[
    RegexMatch(target=IpAddress, pattern='^10\.|^172\.16\.|^192\.168\.', case_insensitive=False)
  ],
  description='Request from internal/private IP range (possible proxy)',
)

# Detect datacenter user agents
DatacenterUserAgent = Rule(
  when_all=[
    RegexMatch(target=UserAgent, pattern='Headless|PhantomJS|Selenium|puppeteer', case_insensitive=True)
  ],
  description='Request from headless browser (automation tool)',
)

# Detect mobile app spoofing (desktop UA with mobile claims)
UserAgentSpoofing = Rule(
  when_all=[
    RegexMatch(target=UserAgent, pattern='Mobile', case_insensitive=True),
    RegexMatch(target=UserAgent, pattern='curl|python|node', case_insensitive=True)
  ],
  description='User agent appears to be spoofed',
)

# Detect requests without proper user agent
MissingUserAgent = Rule(
  when_all=[
    RegexMatch(target=UserAgent, pattern='^$|^-$|^null$|^undefined$', case_insensitive=True)
  ],
  description='Request with missing or invalid user agent',
)

# Detect new account from suspicious IP
NewAccountSuspiciousIp = Rule(
  when_all=[
    PostCount < 5,
    RegexMatch(target=IpAddress, pattern='^10\.|^172\.', case_insensitive=False)
  ],
  description='New account from suspicious IP range',
)

WhenRules(
  rules_any=[VpnProxyDetection],
  then=[
    LabelAdd(entity=UserId, label='proxy_user'),
  ],
)

WhenRules(
  rules_any=[DatacenterUserAgent],
  then=[
    LabelAdd(entity=UserId, label='automation_detected'),
    BanUser(entity=UserId, comment='Headless browser detected'),
  ],
)

WhenRules(
  rules_any=[UserAgentSpoofing, MissingUserAgent],
  then=[
    LabelAdd(entity=UserId, label='suspicious_client'),
  ],
)

WhenRules(
  rules_any=[NewAccountSuspiciousIp],
  then=[
    LabelAdd(entity=UserId, label='suspicious_new_account'),
  ],
)
