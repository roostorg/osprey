Import(
  rules=[
    'models/base.sml',
    'models/post.sml',
  ]
)

ContainsHello = Rule(
  when_all=[
    EventType == 'create_post',
    TextContains(text=PostText, phrase='hello')
  ],
  description='Post contains the word "hello"',
)

WhenRules(
  rules_any=[ContainsHello],
  then=[BanUser(entity=UserId, comment='User said "hello"')],
)
