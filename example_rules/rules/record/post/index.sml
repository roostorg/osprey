Import(
  rules=[
    'models/base.sml',
    'models/record/base.sml',
    'models/record/post.sml',
  ],
)

Require(rule='rules/record/post/post_contains_hello.sml')
