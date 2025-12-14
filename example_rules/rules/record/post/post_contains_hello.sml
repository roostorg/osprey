Import(
  rules=[
    'models/base.sml',
    'models/record/base.sml',
    'models/record/post.sml',
  ],
)

PostContainsHelloRule = Rule(
  when_all=[
    'hello' in StringToLower(s=PostText),
  ],
  description='Post text contains hello',
)
