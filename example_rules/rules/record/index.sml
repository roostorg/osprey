Import(
  rules=[
    'models/base.sml',
    'models/record/base.sml',
  ],
)

Require(
  rule='rules/record/post/index.sml',
  require_if=(IsCreate or IsUpdate) and Collection == 'app.bsky.feed.post',
)

Require(
  rule='rules/record/like/index.sml',
  require_if=(IsCreate or IsUpdate) and Collection == 'app.bsky.feed.like',
)
