ActionName = GetActionName()

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

OperationKind: Optional[str] = JsonData(
  path='$.operation.action',
  required=False,
)

IsOperation = OperationKind != None

Second: int = 1
Minute: int = Second * 60
Hour: int = Minute * 60
Day: int = Hour * 24
Week: int = Day * 7
