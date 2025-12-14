ActionName=GetActionName()

UserId: Entity[str] = EntityJson(
  type='UserId',
  path='$.did',
  required=False,
)

Handle: Entity[str] = EntityJson(
  type='Handle',
  path='$.eventMetadata.handle',
  required=False,
)

PdsHost: Entity[str] = EntityJson(
  type='PdsHost',
  path='$.eventMetadata.pdsHost',
  required=False,
)

AccountCreatedAt: Optional[str] = JsonData(
  path='$.eventMetadata.didCreatedAt',
  required=False,
)

AccountAgeSeconds: Optional[str] = JsonData(
  path='$.eventMetadata.accountAge',
  required=False,
)

OperationKind: Optional[str] = JsonData(
  path='$.operation.action',
  required=False,
)

IsOperation = OperationKind != None
