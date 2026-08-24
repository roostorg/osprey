Import(rules=['models/base.sml'])

IdentityHandle: str = JsonData(
  path='$.identity.handle',
  required=False,
)
