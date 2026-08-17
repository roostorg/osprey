ActionName = GetActionName()

UserId: Entity[str] = EntityJson(
  type='UserId',
  path='$.did',
  required=False,
)

OperationKind: Optional[str] = JsonData(
  path='$.commit.operation',
  required=False,
)

IsOperation = OperationKind != None

Second: int = 1
Minute: int = Second * 60
Hour: int = Minute * 60
Day: int = Hour * 24
Week: int = Day * 7
