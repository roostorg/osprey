Import(
  rules=[
    'models/base.sml',
  ]
)

PostContainsTest = Rule(
  when_all=[
    EventType == 'create_post',
    TextContains(text=PostText, phrase='test')
  ],
  description='ATProto post contains the word "test"',
)

WhenRules(
  rules_any=[PostContainsTest],
  then=[
    LabelAdd(entity=Did, label='test_poster'),
  ],
)
