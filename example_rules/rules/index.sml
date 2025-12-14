Import(
  rules=[
    'models/base.sml',
  ],
)

Require(
  rule='rules/record/index.sml', 
  require_if=IsOperation,
)
