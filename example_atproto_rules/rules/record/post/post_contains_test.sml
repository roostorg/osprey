Import(
  rules=[
    'models/base.sml',
    'models/record/base.sml',
    'models/record/post.sml',
  ],
)

PostContainsTestRule = Rule(
  when_all=[
    TextContains(text=PostText, phrase='test'),
  ],
  description='ATProto post contains the word "test"',
)

WhenRules(
  rules_any=[PostContainsTestRule],
  then=[
    LabelAdd(entity=UserId, label='test-poster'),
  ],
)
