Import(rules=['models/base.sml'])

IsCreate = OperationKind == 'create'
IsUpdate = OperationKind == 'update'
IsDelete = OperationKind == 'delete'

Collection: str = JsonData(
  path='$.commit.collection',
  required=False,
)

Rkey: str = JsonData(
  path='$.commit.rkey',
  required=False,
)

Cid: str = JsonData(
  path='$.commit.cid',
  required=False,
)
