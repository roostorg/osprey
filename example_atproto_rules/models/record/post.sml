Import(rules=['models/base.sml'])

PostText: str = JsonData(
  path='$.operation.record.text',
  required=False,
  coerce_type=True,
)
