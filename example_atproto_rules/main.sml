Import(
  rules=[
    'models/base.sml',
    'models/identity.sml',
    'models/record/base.sml',
    'models/record/follow.sml',
    'models/record/like.sml',
    'models/record/post.sml',
  ],
)

Require(rule='rules/index.sml')
