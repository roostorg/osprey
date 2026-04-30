Import(rules=['models/base.sml'])

PostText: str = JsonData(
  path='$.commit.record.text',
  required=False,
  coerce_type=True,
)
